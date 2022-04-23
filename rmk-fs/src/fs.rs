use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::BufReader,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    json,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    physical_plan::{
        common::compute_record_batch_statistics, memory::MemoryStream, project_schema,
        ExecutionPlan, SendableRecordBatchStream,
    },
};
use datafusion::{error::DataFusionError, prelude::*};
use fuser::{Filesystem, MountOption};
use glob::glob;
use log::{debug, info};
use rmk_notebook::{read_metadata, Metadata};

use crate::errors::{RmkFsError, RmkFsResult};

#[derive(Clone)]
pub struct RmkFs {
    schema: SchemaRef,
    inner: Arc<RwLock<RmkFsInner>>,
}

struct RmkFsInner {
    data: HashMap<String, Metadata>,
    root: PathBuf,
}

impl RmkFsInner {
    fn new(root: PathBuf) -> RmkFsInner {
        RmkFsInner {
            data: HashMap::new(),
            root,
        }
    }

    fn scan(&mut self) -> RmkFsResult<()> {
        info!("Scanning filesystem at {}", self.root.display());
        let pattern = self.root.join("*.metadata");

        let pattern = pattern.to_str().ok_or_else(|| RmkFsError::ScanError {
            root: self.root.clone(),
        })?;

        let files = glob(pattern)
            .map_err(|_source| RmkFsError::ScanError {
                root: self.root.clone(),
            })?
            .flatten();

        for f in files {
            debug!("{:?}", f);

            let (id, metadata) = read_metadata(&f)?;
            self.data.insert(id.to_string(), metadata);
        }

        Ok(())
    }
}

impl RmkFs {
    pub fn new(root: &PathBuf) -> Self {
        RmkFs {
            schema: SchemaRef::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("parent", DataType::Utf8, true),
            ])),
            inner: Arc::new(RwLock::new(RmkFsInner::new(root.clone()))),
        }
    }

    pub fn mount(self, mountpoint: &str) -> RmkFsResult<fuser::BackgroundSession> {
        fuser::spawn_mount2(self, mountpoint.clone(), &[MountOption::AutoUnmount]).map_err(
            |source| RmkFsError::MountError {
                mountpoint: mountpoint.to_string(),
                source,
            },
        )
    }

    pub fn scan(&self) -> RmkFsResult<()> {
        self.inner.write().unwrap().scan()
    }
}

impl Debug for RmkFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RmkFs {{}}")
    }
}

#[async_trait]
impl TableProvider for RmkFs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(FsExecPlan::try_new(
            self.clone(),
            projection.clone(),
        )?))
    }
}

impl Filesystem for RmkFs {}

#[derive(Clone, Debug)]
struct FsExecPlan {
    table: RmkFs,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl FsExecPlan {
    fn try_new(table: RmkFs, projection: Option<Vec<usize>>) -> Result<Self, DataFusionError> {
        let projected_schema = project_schema(&table.schema(), projection.as_ref())?;

        Ok(Self {
            table,
            projected_schema,
            projection,
        })
    }
}

#[async_trait]
impl ExecutionPlan for FsExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(
        &self,
    ) -> Option<&[datafusion::physical_plan::expressions::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let nodes = {
            let table = self.table.inner.read().unwrap();
            table.data.clone()
        };

        let mut id_array = StringBuilder::new(nodes.len());
        let mut type_array = StringBuilder::new(nodes.len());
        let mut name_array = StringBuilder::new(nodes.len());
        let mut parent_array = StringBuilder::new(nodes.len());

        for (id, metadata) in nodes {
            id_array.append_value(&id)?;
            type_array.append_value(&metadata.typ)?;
            name_array.append_value(&metadata.visible_name)?;
            parent_array.append_value(&metadata.parent)?;
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(id_array.finish()),
                    Arc::new(type_array.finish()),
                    Arc::new(name_array.finish()),
                    Arc::new(parent_array.finish()),
                ],
            )?],
            self.schema(),
            self.projection.clone(),
        )?))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }
}
