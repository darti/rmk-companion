use std::sync::Arc;

use datafusion::datasource::{file_format::json::JsonFormat, listing::ListingOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let mut ctx = ExecutionContext::new();

    let file_format = JsonFormat::default();

    let listing_options = ListingOptions {
        file_extension: ".metadata".to_string(),
        format: Arc::new(file_format),
        table_partition_cols: vec![],
        collect_stat: true,
        target_partitions: 1,
    };

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "my_table",
        "./rmk-notebook/samples/test",
        listing_options,
        None,
    )
    .await?;

    let df = ctx.sql("SELECT * FROM my_table").await?;

    df.show().await?;

    Ok(())
}
