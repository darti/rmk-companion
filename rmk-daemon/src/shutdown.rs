use futures::stream::StreamExt;
use futures::Future;
use log::info;
use signal_hook::consts::*;
use signal_hook_tokio::Signals;
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn shutdown_manager<R, F>(mut shutdown_recv: UnboundedReceiver<()>, on_terminate: F) -> R
where
    F: Future<Output = R>,
{
    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT]).unwrap();
    let handle = signals.handle();
    let mut signals = signals.fuse();

    info!("Shutdown hook started");

    tokio::select! {
        _ = signals.next() => info!("Received signal"),
        _ = shutdown_recv.recv() => info!("Received shutdown request"),
    };

    info!("Signal: Shutting down...");

    let r = on_terminate.await;

    handle.close();

    r
}
