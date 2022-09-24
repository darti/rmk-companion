#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use anyhow::Context;
use anyhow::Result;

use log::info;

use tauri::{App, CustomMenuItem, SystemTray, SystemTrayEvent, SystemTrayMenu, SystemTrayMenuItem};

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
        // .manage(state)
        // .invoke_handler(tauri::generate_handler![run_query])
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

    let app = build_ui()?;

    app.run(|_app_handle, e| match e {
        tauri::RunEvent::Exit => info!("Exiting gui..."),
        tauri::RunEvent::ExitRequested { api: _, .. } => info!("Exit requested..."),

        _ => {}
    });

    Ok(())
}
