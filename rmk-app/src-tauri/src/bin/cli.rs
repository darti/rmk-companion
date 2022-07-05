use std::path::PathBuf;

use actix::prelude::*;
use actix_web::{get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use anyhow::Result;

use log::info;
use rmk_app::{debug_actor::DebugActor, shutdown::shutdown_manager};
use rmk_fs::{FsActor, Mount, NotebookActor, Scan, TableActor, Umount};

#[get("/query")]
async fn query(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[actix::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let root = PathBuf::from("../dump/xochitl");
    let mountpoint = PathBuf::from("../mnt");

    let notebook_renderer = SyncArbiter::start(4, || NotebookActor::new());

    let file_watcher = TableActor::try_new(&root, notebook_renderer)?.start();
    file_watcher.send(Scan).await??;

    let fs_mounter = FsActor::new(&mountpoint, file_watcher.clone()).start();

    fs_mounter.send(Mount).await??;

    actix::spawn(async move {
        let debug = DebugActor::new(file_watcher.clone());
        HttpServer::new(move || App::new().app_data(debug.clone()).service(query))
            .bind(("127.0.0.1", 8080))?
            .run()
            .await
    });

    shutdown_manager(async {
        fs_mounter.send(Umount).await.unwrap().unwrap();
    })
    .await;

    info!("Exiting...");

    Ok(())
}
