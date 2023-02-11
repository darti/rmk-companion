use log::info;

use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};

use anyhow::Result;

use std::process::Command;

fn main() -> Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    Command::new("umount").arg("-f").arg("remarkable").status();

    let rt = actix::System::with_tokio_rt(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Actix runtime failed to initialize")
    });

    info!("Daemon started");

    rt.block_on(async {
        let daemon = RmkDaemon::try_new().await?;

        daemon.mount().await?;

        shutdown_manager(async {
            daemon.umount().await?;
            daemon.stop()
        })
        .await
    })?;

    Ok(())
}
