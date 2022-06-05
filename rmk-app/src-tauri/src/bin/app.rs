#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use std::path::PathBuf;

use log::{debug, info};

use tauri::api::cli::get_matches;
use tauri::{App, CustomMenuItem, SystemTray, SystemTrayEvent, SystemTrayMenu, SystemTrayMenuItem};
use tokio::sync::mpsc;

use anyhow::{Context, Result};

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
        .on_system_tray_event(move |app, event| match event {
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

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    let app = build_ui(shutdown_send)?;

    app.run(|app_handle, e| match e {
        tauri::RunEvent::Exit => info!("Exiting..."),
        tauri::RunEvent::ExitRequested { api, .. } => info!("Exit requested..."),

        _ => {}
    });

    Ok(())
}
