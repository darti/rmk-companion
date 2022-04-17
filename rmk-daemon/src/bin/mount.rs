use anyhow::Result;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    info!("Starting rmk-daemon");

    Ok(())
}
