use futures::stream::StreamExt;
use futures::Future;
use log::info;
use signal_hook::consts::*;
use signal_hook_tokio::Signals;

pub async fn shutdown_manager<R, F>(on_terminate: F) -> R
where
    F: Future<Output = R>,
{
    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT]).unwrap();
    let handle = signals.handle();
    let mut signals = signals.fuse();

    info!("Shutdown hook started");

    tokio::select! {
        _ = signals.next() => info!("Received signal"),
    };

    info!("Signal: Shutting down...");

    let r = on_terminate.await;

    handle.close();

    r
}
