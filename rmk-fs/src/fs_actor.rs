use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::prelude::ExecutionContext;
use log::info;

use crate::errors::RmkFsError;
use crate::errors::RmkFsResult;
use crate::RmkTable;

pub struct RmkFsActor {
    table: Arc<RmkTable>,
    context: ExecutionContext,
}

impl RmkFsActor {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = ExecutionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let mut fs = Self {
            table: table.clone(),
            context,
        };

        fs.context.register_table("metadata", table)?;

        Ok(fs)
    }
}

impl Actor for RmkFsActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Scan;

impl Handler<Scan> for RmkFsActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, _msg: Scan, _ctx: &mut Self::Context) -> Self::Result {
        info!("Scanning filesystem...");

        self.table.scan()
    }
}

#[derive(Message)]
#[rtype(result = "Result<Arc<(dyn DataFrame + 'static)>, DataFusionError>")]
pub struct Query(String);

impl Query {
    pub fn new(query: &str) -> Self {
        Self(query.to_string())
    }
}

impl Handler<Query> for RmkFsActor {
    type Result = AtomicResponse<Self, Result<Arc<(dyn DataFrame + 'static)>, DataFusionError>>;

    fn handle(&mut self, msg: Query, ctx: &mut Self::Context) -> Self::Result {
        let mut context = self.context.clone();

        let query = msg.0.clone();

        AtomicResponse::new(Box::pin(
            async move { context.sql(&query).await }
                .into_actor(self)
                .map(|out, this, _| out),
        ))
    }
}
