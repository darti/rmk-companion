use arrow::{
    array::{ArrayBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::context::TaskContext,
    logical_expr::TableType,
    physical_plan::{
        memory::MemoryStream, project_schema, ExecutionPlan, SendableRecordBatchStream,
    },
};
use glob::glob;
use std::{
    any::Any,
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::{Debug, Display},
    hash::Hash,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use log::{debug, info};
use rmk_notebook::{read_metadata, Metadata};
use std::hash::Hasher;

use crate::errors::{RmkFsError, RmkFsResult};

pub fn schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("parent", DataType::Utf8, true),
        Field::new("ino", DataType::UInt64, false),
        Field::new("parent_ino", DataType::UInt64, false),
    ]))
}

pub struct RmkNode<'a> {
    pub id: &'a str,
    pub typ: &'a str,
    pub name: &'a str,
    pub parent: Option<&'a str>,
    pub ino: u64,
    pub parent_ino: u64,
}

impl<'a> RmkNode<'a> {
    pub fn new(
        id: &'a str,
        typ: &'a str,
        name: &'a str,
        parent: Option<&'a str>,
        ino: u64,
        parent_ino: u64,
    ) -> Self {
        Self {
            id,
            typ,
            name,
            parent,
            ino,
            parent_ino,
        }
    }
}

struct RmkTableInner {
    data: HashMap<String, Metadata>,
    root: PathBuf,
}

impl RmkTableInner {
    fn new(root: PathBuf) -> RmkTableInner {
        RmkTableInner {
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

impl Debug for RmkTableInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RmkTableInner {{ root: {:?} }}", self.root)
    }
}

#[derive(Clone, Debug)]
pub struct RmkTable {
    schema: SchemaRef,
    inner: Arc<RwLock<RmkTableInner>>,
}

impl Display for RmkTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner.read().unwrap())
    }
}

impl RmkTable {
    pub fn new(root: &PathBuf) -> Self {
        Self {
            schema: schema(),
            inner: Arc::new(RwLock::new(RmkTableInner::new(root.clone()))),
        }
    }

    pub fn scan(&self) -> RmkFsResult<()> {
        self.inner.write().unwrap().scan()
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl TableProvider for RmkTable {
    fn table_type(&self) -> TableType {
        TableType::Base
    }

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

#[derive(Clone, Debug)]
struct FsExecPlan {
    table: RmkTable,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl FsExecPlan {
    fn try_new(table: RmkTable, projection: Option<Vec<usize>>) -> Result<Self, DataFusionError> {
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
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let nodes = {
            let table = self.table.inner.read().unwrap();
            table.data.clone()
        };

        let projection = self
            .projection
            .as_ref()
            .map(|p| p.clone())
            .unwrap_or_else(|| (0..self.table.schema().fields().len()).collect());

        let mut arrays: Vec<Box<dyn ArrayBuilder>> = projection
            .iter()
            .map(|i| match i {
                0 => Box::new(StringBuilder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                1 => Box::new(StringBuilder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                2 => Box::new(StringBuilder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                3 => Box::new(StringBuilder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                4 => Box::new(UInt64Builder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                5 => Box::new(UInt64Builder::new(nodes.len())) as Box<dyn ArrayBuilder>,
                _ => unreachable!(),
            })
            .collect();

        for (id, metadata) in nodes {
            for (i, p) in projection.iter().enumerate() {
                match p {
                    0 => arrays[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .map(|a| a.append_value(&id)),

                    1 => arrays[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .map(|a| a.append_value(&metadata.typ)),

                    2 => arrays[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .map(|a| a.append_value(&metadata.visible_name)),

                    3 => {
                        if let Some(parent) = &metadata.parent {
                            arrays[i]
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .map(|a| a.append_value(&parent))
                        } else {
                            arrays[i]
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .map(|a| a.append_null())
                        }
                    }

                    4 => {
                        let mut s = DefaultHasher::new();
                        id.hash(&mut s);

                        arrays[i]
                            .as_any_mut()
                            .downcast_mut::<UInt64Builder>()
                            .map(|a| a.append_value(s.finish()))
                    }

                    5 => {
                        let p = match &metadata.parent {
                            Some(p) => {
                                let mut s = DefaultHasher::new();
                                p.hash(&mut s);
                                s.finish()
                            }
                            None => 1,
                        };

                        arrays[i]
                            .as_any_mut()
                            .downcast_mut::<UInt64Builder>()
                            .map(|a| a.append_value(p))
                    }

                    _ => None,
                };
            }
        }

        let arrays = arrays.iter_mut().map(|a| a.finish()).collect();

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(self.projected_schema.clone(), arrays)?],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }
}
