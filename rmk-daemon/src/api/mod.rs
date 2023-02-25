use log::debug;

use warp::Filter;

use serde::{Deserialize, Serialize};
use std::convert::Infallible;

use datafusion::arrow::util::pretty::pretty_format_batches;

#[derive(Deserialize, Serialize, Debug)]
struct Query {
    query: String,
}

// fn with_fs(fs: RmkFs) -> impl Filter<Extract = (RmkFs,), Error = std::convert::Infallible> + Clone {
//     warp::any().map(move || fs.clone())
// }

// async fn run_query(query: Query, fs: RmkFs) -> Result<impl warp::Reply, Infallible> {
//     match fs.query(&query.query).await {
//         Ok(df) => {
//             let batches = df.collect().await.unwrap();
//             let s = pretty_format_batches(&batches).unwrap();
//             Ok(s.to_string())
//         }
//         Err(e) => {
//             debug!("Error: {:?}", e);
//             Ok(e.to_string())
//         }
//     }
// }

// pub fn server(fs: RmkFs) {
//     let query = warp::post()
//         .and(warp::path("query"))
//         .and(warp::body::content_length_limit(1024 * 32))
//         .and(warp::body::json())
//         .and(with_fs(fs.clone()))
//         .and_then(run_query);

//     tokio::spawn(warp::serve(query).run(([0, 0, 0, 0], 8080)));
// }
