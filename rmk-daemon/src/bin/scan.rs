use std::{path::PathBuf, sync::Arc};

use datafusion::prelude::*;
use rmk_fs::{errors::RmkFsResult, RmkTable};

#[tokio::main]
async fn main() -> RmkFsResult<()> {
    pretty_env_logger::init();
    let fs = RmkTable::new(&PathBuf::from("../dump/xochitl"));

    fs.scan()?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("fs", Arc::new(fs))?;

    let df = ctx
        .sql("SELECT * FROM fs WHERE parent = '37d53cbe-e82d-4969-86fe-e5bc365a9f1f'")
        .await?;

    df.show().await?;

    Ok(())
}
