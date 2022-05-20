use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use log::debug;
use log::info;

use crate::errors::RmkFsResult;
use crate::RmkTable;

pub struct TableActor {
    table: Arc<RmkTable>,
    context: SessionContext,
}

impl TableActor {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = SessionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let mut fs = Self {
            table: table.clone(),
            context,
        };

        fs.context.register_table("metadata", table)?;

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
        let mut context = self.context.clone();

        let query = msg.0.clone();

        debug!("Executing query: {}", query);

        AtomicResponse::new(Box::pin(
            async move { context.sql(&query).await }
                .into_actor(self)
                .map(|out, _this, _| out),
        ))
    }
}
