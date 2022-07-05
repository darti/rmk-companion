#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::path::PathBuf;

use actix::prelude::*;
use actix::SyncArbiter;
use log::{debug, info};

use rmk_app::shutdown::shutdown_manager;
use rmk_fs::{FsActor, Mount, NotebookActor, Scan, TableActor};
use tauri::api::cli::get_matches;
use tauri::{
    App, CustomMenuItem, Manager, SystemTray, SystemTrayEvent, SystemTrayMenu, SystemTrayMenuItem,
};
use tokio::sync::mpsc;

use anyhow::Result;

use anyhow::Context;

fn build_ui(shutdown_send: mpsc::UnboundedSender<()>) -> Result<App> {
    let options = CustomMenuItem::new("options".to_string(), "Options");
    let quit = CustomMenuItem::new("quit".to_string(), "Quit");
    let tray_menu = SystemTrayMenu::new()
        .add_item(options)
        .add_native_item(SystemTrayMenuItem::Separator)
        .add_item(quit);

    let system_tray = SystemTray::new().with_menu(tray_menu);

    tauri::Builder::default()
        .system_tray(system_tray)
        .on_system_tray_event(move |_app, event| match event {
            SystemTrayEvent::MenuItemClick { id, .. } => match id.as_str() {
                "quit" => shutdown_send.send(()).unwrap(),

                _ => {}
            },
            _ => {}
        })
        .setup(|app| {
            let matches =
                get_matches(app.config().tauri.cli.as_ref().unwrap(), app.package_info())?;
            debug!("{:?}", matches);
            Ok(())
        })
        .build(tauri::generate_context!())
        .context("error while running tauri application")
}

#[actix::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let (shutdown_send, _shutdown_recv) = mpsc::unbounded_channel();

    let app = build_ui(shutdown_send)?;

    let root = PathBuf::from("../dump/xochitl");
    let mountpoint = PathBuf::from("../mnt");

    let notebook_renderer = SyncArbiter::start(4, || NotebookActor::new());

    let file_watcher = TableActor::try_new(&root, notebook_renderer)?.start();
    file_watcher.send(Scan).await??;

    let fs_mounter = FsActor::new(&mountpoint, file_watcher.clone()).start();

    fs_mounter.send(Mount).await??;

    let handle = app.app_handle();

    app.run(|_app_handle, e| match e {
        tauri::RunEvent::Exit => info!("Exiting..."),
        tauri::RunEvent::ExitRequested { api: _, .. } => info!("Exit requested..."),

        _ => {}
    });

    info!("Narf");

    shutdown_manager(async {
        // fs_mounter.send(Umount).await.unwrap().unwrap();
        handle.exit(0);
    })
    .await;

    info!("Exiting...");

    Ok(())
}
