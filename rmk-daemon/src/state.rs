use actix::{Actor, Addr, SyncArbiter, System, SystemRunner};
use log::info;
use rmk_fs::{
    errors::{RmkFsError, RmkFsResult},
    FsActor, Mount, NotebookActor, Scan, TableActor, Umount,
};
use tokio::runtime::Handle;

use crate::settings::SETTINGS;

pub struct RmkDaemon {
    fs: Option<Addr<FsActor>>,
}

impl RmkDaemon {
    pub async fn try_new() -> Result<Self, RmkFsError> {
        let mut fs = None;

        let notebook_renderer = SyncArbiter::start(4, || NotebookActor::new());

        let file_watcher = TableActor::try_new(&SETTINGS.cache_root(), notebook_renderer)?.start();
        file_watcher.send(Scan).await??;

        fs = Some(FsActor::new(&SETTINGS.mount_point(), file_watcher.clone()).start());

        Ok::<(), anyhow::Error>(())
            .map_err(|_e| RmkFsError::DaemonError("failed to start".into()))?;

        Ok(Self { fs })
    }

    pub fn stop(&self) -> RmkFsResult<()> {
        info!("Shutting down daemon");

        info!("Daemon shutdown complete");

        Ok(())
    }

    pub async fn mount(&self) -> RmkFsResult<()> {
        info!("Mounting");

        if let Some(fs) = &self.fs {
            let fs = fs.clone();

            fs.send(Mount).await??;
        } else {
            return Err(RmkFsError::DaemonError("command actor not started".into()));
        };

        info!("Mounted");

        Ok(())
    }

    pub fn umount(&self) -> RmkFsResult<()> {
        info!("Unounting");

        if let Some(fs) = &self.fs {
            // self.system.block_on(async { fs.send(Umount).await })??;
        } else {
            return Err(RmkFsError::DaemonError("command actor not started".into()));
        };

        info!("Unmounted");

        Ok(())
    }
}
