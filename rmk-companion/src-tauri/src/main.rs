#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod shutdown;

use std::fs;
use std::path::PathBuf;
use std::thread;

use actix::prelude::*;
use actix::SyncArbiter;
use anyhow::Context;
use anyhow::Result;
use log::info;
use rmk_fs::FsActor;
use rmk_fs::Mount;
use rmk_fs::NotebookActor;
use rmk_fs::Scan;
use rmk_fs::TableActor;
use rmk_fs::Umount;
use tauri::{App, CustomMenuItem, SystemTray, SystemTrayEvent, SystemTrayMenu, SystemTrayMenuItem};

use crate::shutdown::shutdown_manager;

fn build_ui() -> Result<App> {
    let context = tauri::generate_context!();

    let options = CustomMenuItem::new("options".to_string(), "Options");
    let quit = CustomMenuItem::new("quit".to_string(), "Quit");
    let tray_menu = SystemTrayMenu::new()
        .add_item(options)
        .add_native_item(SystemTrayMenuItem::Separator)
        .add_item(quit);

    let system_tray = SystemTray::new().with_menu(tray_menu);

    tauri::Builder::default()
        .menu(if cfg!(target_os = "macos") {
            tauri::Menu::os_default(&context.package_info().name)
        } else {
            tauri::Menu::default()
        })
        .system_tray(system_tray)
        .on_system_tray_event(move |app, event| match event {
            SystemTrayEvent::MenuItemClick { id, .. } => match id.as_str() {
                "quit" => app.exit(0),

                _ => {}
            },
            _ => {}
        })
        .build(context)
        .context("error while running tauri application")
}

fn main() -> Result<()> {
    pretty_env_logger::init();

    thread::Builder::new()
        .name("Device Handler".into())
        .spawn(|| {
            actix::System::new()
                .block_on(async move {
                    let root = PathBuf::from("../dump/xochitl");
                    let mountpoint = PathBuf::from("../mnt");

                    let notebook_renderer = SyncArbiter::start(4, || NotebookActor::new());

                    let file_watcher = TableActor::try_new(&root, notebook_renderer)?.start();
                    file_watcher.send(Scan).await??;

                    let fs_mounter = FsActor::new(&mountpoint, file_watcher.clone()).start();

                    // fs_mounter.send(Mount).await??;

                    shutdown_manager(async {
                        fs_mounter.send(Umount).await.unwrap().unwrap();
                    })
                    .await;

                    info!("Exiting...");

                    Ok::<(), anyhow::Error>(())
                })
                .unwrap();
        })?;

    let app = build_ui()?;

    app.run(|_app_handle, e| match e {
        tauri::RunEvent::Exit => info!("Exiting gui..."),
        tauri::RunEvent::ExitRequested { api: _, .. } => info!("Exit requested..."),

        _ => {}
    });

    Ok(())
}
