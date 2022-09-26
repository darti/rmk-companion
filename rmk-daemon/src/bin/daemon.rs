use log::info;

use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};

use anyhow::Result;

fn main() -> Result<()> {
    pretty_env_logger::init();

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
