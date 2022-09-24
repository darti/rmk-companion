use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;

use log::debug;
use log::info;
use polars::prelude::DataFrame;
use polars::prelude::PolarsError;

use crate::errors::RmkFsResult;
use crate::NotebookActor;
use crate::RmkTable;

pub struct TableActor {
    table: Arc<RmkTable>,
    renderer: Addr<NotebookActor>,
}

impl TableActor {
    pub fn try_new(root: &PathBuf, renderer: Addr<NotebookActor>) -> RmkFsResult<Self> {
        let table = Arc::new(RmkTable::new(root)?);

        let fs = Self {
            table: table.clone(),
            renderer,
        };

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
#[rtype(result = "RmkFsResult<Arc<DataFrame>>")]
pub struct Query(String);

impl Query {
    pub fn new(query: &str) -> Self {
        Self(query.to_string())
    }
}

impl Handler<Query> for TableActor {
    type Result = AtomicResponse<Self, RmkFsResult<Arc<DataFrame>>>;

    fn handle(&mut self, msg: Query, _ctx: &mut Self::Context) -> Self::Result {
        let query = msg.0.clone();

        debug!("Executing query: {}", query);

        // AtomicResponse::new(Box::pin(
        //     async move { context.sql(&query).await }
        //         .into_actor(self)
        //         .map(|out, _this, _| out),
        // ))

        todo!()
    }
}
