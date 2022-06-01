use std::path::PathBuf;

use actix::prelude::*;
use anyhow::Result;

use log::info;
use rmk_app::shutdown::shutdown_manager;
use rmk_fs::{FsActor, Mount, Scan, TableActor, Umount};

#[actix::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let root = PathBuf::from("../dump/xochitl");
    let mountpoint = PathBuf::from("../mnt");

    let file_watcher = TableActor::try_new(&root)?.start();
    file_watcher.send(Scan).await??;

    let fs_mounter = FsActor::new(&mountpoint, file_watcher.clone()).start();

    fs_mounter.send(Mount).await??;

    shutdown_manager(async {
        fs_mounter.send(Umount).await.unwrap().unwrap();
    })
    .await;

    info!("Exiting...");

    Ok(())
}
