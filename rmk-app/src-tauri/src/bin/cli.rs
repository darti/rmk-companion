use std::path::PathBuf;

use anyhow::Result;

use log::info;
use rmk_app::shutdown;
use rmk_fs::RmkFs;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let mut root = "../dump/xochitl".to_string();
    let mut mount_point = "../mnt".to_string();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    let fs = RmkFs::try_new(&PathBuf::from(root))?;
    let mnt_guard = fs.mount(&mount_point)?;

    let shd = tokio::spawn(shutdown::shutdown_manager(shutdown_recv, move || {
        info!("Unmounting...");
        mnt_guard.join();
        info!("Shutdown");
    }));

    tokio::try_join!(shd)?;

    Ok(())
}
