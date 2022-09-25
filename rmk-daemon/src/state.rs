use actix::{Actor, Addr, Context, Handler, Message, SyncArbiter, SystemRunner};
use log::info;
use rmk_fs::{errors::RmkFsError, NotebookActor, Scan, TableActor};

use crate::settings::SETTINGS;

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

pub struct RmkDaemon {
    system: SystemRunner,
}

impl RmkDaemon {
    pub fn try_new() -> Result<Self, RmkFsError> {
        let system = actix::System::with_tokio_rt(|| {
            tokio::runtime::Builder::new_multi_thread()
                .build()
                .expect("Actix runtime failed to initialize")
        });

        system
            .block_on(async {
                let notebook_renderer = SyncArbiter::start(4, || NotebookActor::new());

                let file_watcher =
                    TableActor::try_new(&SETTINGS.cache_root(), notebook_renderer)?.start();
                file_watcher.send(Scan).await??;

                Ok::<(), anyhow::Error>(())
            })
            .map_err(|_e| RmkFsError::DaemonError)?;

        Ok(Self { system })
    }

    pub fn stop(&self) {
        info!("Shutting down daemon");

        info!("Daemon shutdown complete");
    }
}
