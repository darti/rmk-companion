use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::state::RmkDaemon;

use anyhow::Result;

use clap::{Parser, Subcommand};

/// Utility to interact with your Remarkable tablet
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// query the rmk filesystem
    Query {
        /// lists test values
        #[arg(short, long)]
        sql: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();
    let cli = Cli::parse();

    let daemon = RmkDaemon::try_new().await?;

    match cli.command {
        Commands::Query { sql } => {
            let df = daemon.query(sql).await?;
            df.show().await?;
        }
    }

    Ok(())
}
