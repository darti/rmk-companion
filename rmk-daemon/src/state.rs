use std::sync::Arc;

use log::info;
use rmk_fs::{
    errors::{RmkFsError, RmkFsResult},
    RmkFs,
};
use tokio::{runtime::Handle, sync::RwLock};

use crate::settings::SETTINGS;

#[derive(Clone)]
pub struct RmkDaemon {
    fs: Arc<RwLock<RmkFs>>,
}

impl RmkDaemon {
    pub async fn try_new() -> Result<Self, RmkFsError> {
        let fs = RmkFs::new(&SETTINGS.cache_root(), SETTINGS.ttl(), Handle::current()).await?;

        Ok(Self {
            fs: Arc::new(RwLock::new(fs)),
        })
    }

    pub fn stop(&self) -> RmkFsResult<()> {
        info!("Shutting down daemon");

        info!("Daemon shutdown complete");

        Ok(())
    }

    pub async fn mount(&mut self) -> RmkFsResult<()> {
        info!("Mounting");

        self.fs
            .clone()
            .write()
            .await
            .mount(&SETTINGS.mount_point())?;

        info!("Mounted");

        Ok(())
    }

    pub async fn umount(&mut self) -> RmkFsResult<()> {
        info!("Unounting");

        self.fs.clone().write().await.umount()?;

        info!("Unmounted");

        Ok(())
    }
}
