#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use log::info;
use tauri::{
    AppHandle, CustomMenuItem, Runtime, SystemTray, SystemTrayEvent, SystemTrayMenu,
    SystemTrayMenuItem,
};

fn main() {
    pretty_env_logger::init();
    let options = CustomMenuItem::new("options".to_string(), "Options");
    let quit = CustomMenuItem::new("quit".to_string(), "Quit");
    let tray_menu = SystemTrayMenu::new()
        .add_item(options)
        .add_native_item(SystemTrayMenuItem::Separator)
        .add_item(quit);

    let system_tray = SystemTray::new().with_menu(tray_menu);

    tauri::Builder::default()
        .system_tray(system_tray)
        .on_system_tray_event(|app, event| match event {
            SystemTrayEvent::MenuItemClick { id, .. } => match id.as_str() {
                "quit" => {
                    std::process::exit(0);
                }

                _ => {}
            },
            SystemTrayEvent::LeftClick { position, size, .. } => {
                info!("Left click: {:?} {:?}", position, size)
            }
            SystemTrayEvent::RightClick { position, size, .. } => {
                info!("Right click: {:?} {:?}", position, size)
            }
            SystemTrayEvent::DoubleClick { position, size, .. } => {
                info!("Double click: {:?} {:?}", position, size)
            }
            _ => todo!(),
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
