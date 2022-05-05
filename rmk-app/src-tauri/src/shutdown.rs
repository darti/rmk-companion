use futures::stream::StreamExt;
use log::info;
use signal_hook::consts::*;
use signal_hook_tokio::Signals;

use tokio::sync::mpsc;

pub async fn shutdown_manager<R, F>(mut recv: mpsc::UnboundedReceiver<()>, on_terminate: F) -> R
where
    F: FnOnce() -> R,
{
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

    on_terminate()
}
