use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;

use datafusion::dataframe::DataFrame;

use datafusion::error::DataFusionError;

use datafusion::prelude::SessionContext;
use log::debug;
use log::info;

use crate::create_static;
use crate::errors::RmkFsResult;
use crate::NotebookActor;
use crate::RmkTable;

pub struct TableActor {
    table: Arc<RmkTable>,
    context: SessionContext,
    renderer: Addr<NotebookActor>,
}

impl TableActor {
    pub fn try_new(root: &PathBuf, renderer: Addr<NotebookActor>) -> Result<Self, DataFusionError> {
        let context = SessionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let fs = Self {
            table: table.clone(),
            context,
            renderer,
        };

        let (metadata_static, content_static) = create_static()?;

        fs.context.register_table("metadata_dynamic", table)?;
        fs.context
            .register_table("metadata_static", metadata_static)?;

        fs.context
            .register_table("content_static", content_static)?;

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
