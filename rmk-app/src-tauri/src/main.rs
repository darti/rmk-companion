#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod shutdown;

use log::info;
use tauri::{
    CustomMenuItem, Manager, SystemTray, SystemTrayEvent, SystemTrayMenu, SystemTrayMenuItem,
};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

    let options = CustomMenuItem::new("options".to_string(), "Options");
    let quit = CustomMenuItem::new("quit".to_string(), "Quit");
    let tray_menu = SystemTrayMenu::new()
        .add_item(options)
        .add_native_item(SystemTrayMenuItem::Separator)
        .add_item(quit);

    let system_tray = SystemTray::new().with_menu(tray_menu);

    let app = tauri::Builder::default()
        .system_tray(system_tray)
        .on_system_tray_event(move |app, event| match event {
            SystemTrayEvent::MenuItemClick { id, .. } => match id.as_str() {
                "quit" => shutdown_send.send(()).unwrap(),

                _ => {}
            },
            _ => {}
        })
        .build(tauri::generate_context!())
        .expect("error while running tauri application");

    tokio::spawn(shutdown::shutdown_manager(shutdown_recv, app.handle()));

    app.run(|app_handle, e| match e {
        tauri::RunEvent::Exit => info!("Exiting..."),
        tauri::RunEvent::ExitRequested { api, .. } => info!("Exit requested..."),

        _ => {}
    });
}
