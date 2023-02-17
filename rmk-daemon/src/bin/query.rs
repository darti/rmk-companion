use std::sync::Arc;

use log::info;
use rmk_daemon::{
    shutdown::shutdown_manager,
    state::RmkDaemon,
    ui::tui::{run_app, App},
};
use tokio::{runtime::Handle, sync::mpsc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Builder::from_env(Env::new().default_filter_or("info")).init();
    tui_logger::init_logger(log::LevelFilter::Info)?;

    let daemon = RmkDaemon::try_new().await?;

    let handle = Handle::current();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();
    let (send, mut recv) = mpsc::channel(1);

    let app = App::default();

    let f = run_app(shutdown_send, send.clone(), app);

    handle.spawn(f);

    drop(send);

    handle
        .spawn(shutdown_manager(shutdown_recv, async move {
            let daemon = daemon.clone();

            info!("Waiting for shutdown to complete...");
            let _ = recv.recv().await;

            daemon.stop()
        }))
        .await??;

    info!("Exiting...");

    Ok(())
}
