use futures::stream::StreamExt;
use log::info;
use signal_hook::consts::*;
use signal_hook_tokio::Signals;
use tauri::AppHandle;
use tokio::sync::mpsc;

pub async fn shutdown_manager(mut recv: mpsc::UnboundedReceiver<()>, app: AppHandle) {
    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT]).unwrap();
    let handle = signals.handle();
    let mut signals = signals.fuse();

    info!("Shutdown hook started");

    tokio::select! {
        _ = recv.recv() => info!("Received shutdown signal"),
        _ = signals.next() => info!("Received signal"),
    };

    info!("Signal: Shutting down...");

    handle.close();

    app.exit(0);
}
