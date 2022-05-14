use std::path::PathBuf;

use actix::prelude::*;
use anyhow::Result;
use log::info;
use rmk_fs::{Query, RmkFsActor, Scan};

#[actix_rt::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let root = "../dump/xochitl".to_string();

    let fs_watcher = RmkFsActor::try_new(&PathBuf::from(root))?.start();

    fs_watcher.send(Scan).await??;
    let df = fs_watcher
        .send(Query::new("select * from metadata"))
        .await??;

    df.show().await?;

    Ok(())
}
