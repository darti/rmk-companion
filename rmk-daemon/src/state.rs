use log::info;
use rmk_fs::{
    errors::{RmkFsError, RmkFsResult},
    RmkFs,
};
use tokio::runtime::Handle;

use crate::settings::SETTINGS;

pub struct RmkDaemon {
    fs: RmkFs,
}

impl RmkDaemon {
    pub async fn try_new() -> Result<Self, RmkFsError> {
        let fs = RmkFs::new(&SETTINGS.cache_root(), SETTINGS.ttl(), Handle::current()).await?;

        Ok(Self { fs })
    }

    pub fn stop(&self) -> RmkFsResult<()> {
        info!("Shutting down daemon");

        info!("Daemon shutdown complete");

        Ok(())
    }

    pub fn mount(&mut self) -> RmkFsResult<()> {
        info!("Mounting");

        self.fs.mount(&SETTINGS.mount_point())?;

        info!("Mounted");

        Ok(())
    }

    pub async fn umount(&mut self) -> RmkFsResult<()> {
        info!("Unounting");

        self.fs.umount()?;

        info!("Unmounted");

        Ok(())
    }
}
