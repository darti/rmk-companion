use actix::{Actor, Addr, Context};
use actix_web_actors::HttpContext;
use rmk_fs::TableActor;

#[derive(Debug, Clone)]
pub struct DebugActor {
    table: Addr<TableActor>,
}

impl DebugActor {
    pub fn new(table: Addr<TableActor>) -> Self {
        Self { table }
    }
}

impl Actor for DebugActor {
    type Context = HttpContext<Self>;
}
