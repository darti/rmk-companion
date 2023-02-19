use log::{debug, info};

use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use datafusion::arrow::util::pretty::pretty_format_batches;

use warp::Filter;

use std::{convert::Infallible, process::Command};

#[derive(Deserialize, Serialize, Debug)]
struct Query {
    query: String,
}

fn with_daemon(
    daemon: RmkDaemon,
) -> impl Filter<Extract = (RmkDaemon,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || daemon.clone())
}

async fn run_query(query: Query, daemon: RmkDaemon) -> Result<impl warp::Reply, Infallible> {
    match daemon.query(query.query).await {
        Ok(df) => {
            let batches = df.collect().await.unwrap();
            let s = pretty_format_batches(&batches).unwrap();
            Ok(s.to_string())
        }
        Err(e) => {
            debug!("Error: {:?}", e);
            Ok(e.to_string())
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    Command::new("umount").arg("-f").arg("remarkable").status();

    info!("Daemon started");

    let mut daemon = RmkDaemon::try_new().await?;

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    daemon.mount().await?;

    let query = warp::post()
        .and(warp::path("query"))
        .and(warp::body::content_length_limit(1024 * 32))
        .and(warp::body::json())
        .and(with_daemon(daemon.clone()))
        .and_then(run_query);

    tokio::spawn(warp::serve(query).run(([0, 0, 0, 0], 8080)));

    shutdown_manager(shutdown_recv, async {
        daemon.umount().await?;
        daemon.stop()
    })
    .await?;

    Ok(())
}
