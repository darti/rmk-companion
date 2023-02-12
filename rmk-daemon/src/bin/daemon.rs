use log::info;

use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};

use anyhow::Result;

use std::process::Command;

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    Command::new("umount").arg("-f").arg("remarkable").status();

    info!("Daemon started");

    let mut daemon = RmkDaemon::try_new().await?;

    daemon.mount().await?;

    shutdown_manager(async {
        daemon.umount().await?;
        daemon.stop()
    })
    .await?;

    Ok(())
}
