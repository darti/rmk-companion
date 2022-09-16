use dialoguer::{theme::ColorfulTheme, Confirm};
use log::info;

use rmk_daemon::state::RmkDaemon;

use anyhow::Result;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let daemon = RmkDaemon::try_new();

    info!("Daemon started");

    while !Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Do you want to quit?")
        .default(true)
        .show_default(false)
        .interact()
        .unwrap()
    {}

    Ok(())
}
