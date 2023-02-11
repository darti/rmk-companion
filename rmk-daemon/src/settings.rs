use std::{fs, path::PathBuf, time::Duration};

use config::{Config, Environment, File};
use directories::ProjectDirs;
use lazy_static::lazy_static;
use log::{debug, info};
use serde::{Deserialize, Serialize};

pub struct Settings {
    config: Configuration,
    config_path: PathBuf,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Configuration {
    cache_root: PathBuf,
    mount_point: PathBuf,
    ttl: u64,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            cache_root: DIRS.data_dir().join("xochitl"),
            mount_point: DIRS.data_dir().join("mnt"),
            ttl: 1,
        }
    }
}

impl Settings {
    pub fn new() -> Self {
        let config_path = DIRS.config_dir().join("config.toml");

        info!("Looking for config at: {}", config_path.to_string_lossy());

        if config_path.exists() {
            debug!("Found config at: {}", config_path.to_string_lossy());

            let config: Configuration = Config::builder()
                .add_source(File::from(config_path.clone()))
                .add_source(Environment::with_prefix("rmk"))
                .build()
                .unwrap()
                .try_deserialize()
                .expect("Failed to read config");

            Settings {
                config,
                config_path,
            }
        } else {
            debug!(
                "No config at: {}, creating new",
                config_path.to_string_lossy()
            );
            let config = Configuration::default();

            fs::create_dir_all(DIRS.config_dir()).unwrap();

            fs::write(
                &config_path.clone(),
                toml::to_string_pretty(&config).unwrap(),
            )
            .expect("Failed to write config");

            Settings {
                config,
                config_path: config_path,
            }
        }
    }

    pub fn cache_root(&self) -> PathBuf {
        self.config.cache_root.clone()
    }

    pub fn mount_point(&self) -> PathBuf {
        self.config.mount_point.clone()
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.config.ttl)
    }
}

lazy_static! {
    pub static ref SETTINGS: Settings = Settings::new();
}

lazy_static! {
    static ref DIRS: ProjectDirs = ProjectDirs::from("", "", "rmk-companion").unwrap();
}
