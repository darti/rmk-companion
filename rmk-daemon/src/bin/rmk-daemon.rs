use std::process::Command;

use log::info;

use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{settings::SETTINGS, shutdown::shutdown_manager};

use anyhow::Result;
use rmk_fs::RmkFs;

use tokio;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    Command::new("umount").arg("-f").arg("remarkable").status();

    let mut fs = RmkFs::new(&SETTINGS.cache_root(), SETTINGS.ttl(), Handle::current()).await?;

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    {
        let fs = fs.clone();
        // Handle::current().spawn(async move { fs.clone().scan() });

        fs.scan()?;
    }

    fs.mount(&SETTINGS.mount_point())?;

    info!("RmkFs started");

    shutdown_manager(shutdown_recv, async { fs.umount() }).await?;

    Ok(())
}
