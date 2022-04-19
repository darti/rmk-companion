use std::{any::Any, fs::File, io::BufReader, path::PathBuf, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    json,
};
use async_trait::async_trait;
use datafusion::{datasource::TableProvider, physical_plan::ExecutionPlan};
use datafusion::{error::DataFusionError, prelude::*};
use fuser::{Filesystem, MountOption};
use glob::glob;
use log::{debug, info};

use crate::errors::{RmkFsError, RmkFsResult};

pub struct RmkFs {
    root: PathBuf,
}

impl RmkFs {
    pub fn new(root: &PathBuf) -> Self {
        RmkFs { root: root.clone() }
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

        let schema = Arc::new(Schema::new(vec![
            Field::new("type", DataType::Utf8, false),
            Field::new("visibleName", DataType::Utf8, false),
        ]));

        for f in files {
            debug!("{:?}", f);

            let file = File::open(f).unwrap();

            let mut json = json::Reader::new(BufReader::new(file), schema.clone(), 1024, None);
            let batch = json.next().unwrap().unwrap();
        }

        Ok(())
    }
}

#[async_trait]
impl TableProvider for RmkFs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("type", DataType::Utf8, false),
            Field::new("visibleName", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}

impl Filesystem for RmkFs {}
