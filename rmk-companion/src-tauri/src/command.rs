use actix::{Actor, Addr, Context, Handler, Message};
use log::info;
use once_cell::sync::OnceCell;
use rmk_fs::TableActor;

pub struct CommandActor {
    fs: Addr<TableActor>,
}

impl CommandActor {
    pub fn new(fs: Addr<TableActor>) -> Self {
        Self { fs }
    }
}

impl Actor for CommandActor {
    type Context = Context<Self>;
}

pub struct FsQuery(String);

impl Message for FsQuery {
    type Result = String;
}

impl Handler<FsQuery> for CommandActor {
    type Result = String;

    fn handle(&mut self, msg: FsQuery, ctx: &mut Self::Context) -> Self::Result {
        info!("Query : {}", msg.0);

        "".to_string()
    }
}

#[derive(Clone)]
pub struct AppState {
    command_actor: OnceCell<Addr<CommandActor>>,
}

impl AppState {
    pub fn initialize(
        &mut self,
        command_actor: Addr<CommandActor>,
    ) -> Result<(), Addr<CommandActor>> {
        self.command_actor.set(command_actor.clone())
    }

    pub async fn query(&self, query: String) -> String {
        self.command_actor
            .get()
            .unwrap()
            .send(FsQuery(query))
            .await
            .unwrap()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            command_actor: Default::default(),
        }
    }
}

#[tauri::command]
pub async fn run_query(query: String, state: tauri::State<'_, AppState>) -> Result<String, String> {
    println!("I was invoked from JS!");

    Ok(state.query(query).await)
}
