use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use futures::stream::StreamExt;
use log::info;
use rmk_fs::RmkFs;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use tokio::sync::Mutex;

async fn handle_signals(mut signals: Signals, exit_flag: Arc<Mutex<bool>>) {
    while let Some(signal) = signals.next().await {
        info!("Signal: {}", signal);

        let mut lock = exit_flag.lock().await;
        *lock = true;

        // match signal {
        //     SIGHUP => {
        //         // Reload configuration
        //         // Reopen the log file
        //     }
        //     SIGTERM | SIGINT | SIGQUIT => {
        //         // Shutdown the system;
        //     }
        //     _ => unreachable!(),
        // }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    info!("Starting rmk-daemon");

    let exit_flag = Arc::new(Mutex::new(false));

    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();

    let signals_task = tokio::spawn(handle_signals(signals, exit_flag.clone()));

    let fs = RmkFs::try_new(&PathBuf::from("../dump/xochitl"))?;
    let fs_task = fs.mount("../mnt");

    loop {
        if *exit_flag.lock().await {
            break;
        }
    }

    info!("Shutting down rmk-daemon");

    handle.close();
    signals_task.await?;

    Ok(())
}
