use std::{path::PathBuf, sync::Arc};

use datafusion::prelude::*;
use rmk_fs::{errors::RmkFsResult, RmkFs};

#[tokio::main]
async fn main() -> RmkFsResult<()> {
    pretty_env_logger::init();
    let fs = RmkFs::new(&PathBuf::from("./rmk-notebook/samples"));

    fs.scan()?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("fs", Arc::new(fs))?;

    let df = ctx.sql("SELECT * FROM fs").await?;

    df.show().await?;

    Ok(())
}
