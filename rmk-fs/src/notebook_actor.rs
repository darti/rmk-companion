use std::sync::Arc;

use actix::{Actor, Handler, Message};
use rmk_notebook::Notebook;

use crate::errors::RmkFsResult;

pub struct NotebookActor {}

impl NotebookActor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor for NotebookActor {
    type Context = actix::SyncContext<Self>;
}

pub struct Render {
    pub notebook: Arc<Notebook>,
}

impl Message for Render {
    type Result = RmkFsResult<Vec<u8>>;
}

impl Handler<Render> for NotebookActor {
    type Result = RmkFsResult<Vec<u8>>;

    fn handle(&mut self, msg: Render, _: &mut Self::Context) -> Self::Result {
        let mut buffer = Vec::new();
        msg.notebook.render(&mut buffer)?;

        Ok(buffer)
    }
}
