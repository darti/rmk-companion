use std::process::Command;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use log::info;

use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{settings::SETTINGS, shutdown::shutdown_manager};

use anyhow::Result;
use rmk_fs::{init_tables, RmkFs};

use tokio;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    Command::new("umount").arg("-f").arg("remarkable").status();

    let ctx = SessionContext::default();

    let backend = init_tables(ctx.clone(), SETTINGS.cache_root()).await?;

    ctx.sql("SELECT * FROM metadata").await?.show().await?;

    let mut fs = Arc::new(RmkFs::default());

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    fs.mount(
        Handle::current(),
        ctx,
        SETTINGS.ttl(),
        &SETTINGS.mount_point(),
    )?;

    info!("RmkFs started");

    shutdown_manager(shutdown_recv, async { fs.umount() }).await?;

    Ok(())
}
