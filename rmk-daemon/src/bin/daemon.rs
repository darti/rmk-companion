use log::info;

use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};

use anyhow::Result;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let daemon = RmkDaemon::try_new()?;

    info!("Daemon started");

    let status = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            shutdown_manager(async {
                daemon.stop();
            })
            .await
        });

    Ok(status)
}
