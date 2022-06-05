use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::from_slice::FromSlice;
use datafusion::prelude::SessionContext;
use log::debug;
use log::info;

use crate::errors::RmkFsResult;
use crate::table::RmkNode;
use crate::RmkTable;

pub struct TableActor {
    table: Arc<RmkTable>,
    context: SessionContext,
}

impl TableActor {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = SessionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let static_files = [
            RmkNode::new(".", "CollectionType", ".", None),
            RmkNode::new(".", "CollectionType", "..", None),
            // RmkNode::new(
            //     ".VolumeIcon.icns",
            //     "CollectionType",
            //     ".VolumeIcon.icns",
            //     None,
            // ),
        ];

        let mut acc = (vec![], vec![], vec![], vec![], vec![], vec![]);

        for n in static_files.iter() {
            acc.0.push(n.id);
            acc.1.push(n.typ);
            acc.2.push(n.name);
            acc.3.push(n.parent);
            acc.4.push(n.ino);
            acc.5.push(n.parent_ino);
        }

        let batch = RecordBatch::try_new(
            table.schema(),
            vec![
                Arc::new(StringArray::from(acc.0)),
                Arc::new(StringArray::from(acc.1)),
                Arc::new(StringArray::from(acc.2)),
                Arc::new(StringArray::from(acc.3)),
                Arc::new(UInt64Array::from(acc.4)),
                Arc::new(UInt64Array::from(acc.5)),
            ],
        )?;

        let provider = Arc::new(MemTable::try_new(table.schema(), vec![vec![batch]])?);

        let fs = Self {
            table: table.clone(),
            context,
        };

        fs.context.register_table("metadata_dynamic", table)?;
        fs.context.register_table("metadata_static", provider)?;

        let union = fs
            .context
            .table("metadata_dynamic")?
            .union(fs.context.table("metadata_static")?)?;

        fs.context.register_table("metadata", union)?;

        Ok(fs)
    }
}

impl Actor for TableActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Scan;

impl Handler<Scan> for TableActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, _msg: Scan, _ctx: &mut Self::Context) -> Self::Result {
        info!("Scanning filesystem...");

        self.table.scan()
    }
}

#[derive(Message)]
#[rtype(result = "Result<Arc<DataFrame>, DataFusionError>")]
pub struct Query(String);

impl Query {
    pub fn new(query: &str) -> Self {
        Self(query.to_string())
    }
}

impl Handler<Query> for TableActor {
    type Result = AtomicResponse<Self, Result<Arc<DataFrame>, DataFusionError>>;

    fn handle(&mut self, msg: Query, _ctx: &mut Self::Context) -> Self::Result {
        let context = self.context.clone();

        let query = msg.0.clone();

        debug!("Executing query: {}", query);

        AtomicResponse::new(Box::pin(
            async move { context.sql(&query).await }
                .into_actor(self)
                .map(|out, _this, _| out),
        ))
    }
}
