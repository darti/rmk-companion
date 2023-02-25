use std::{path::Path, time::Duration};

use fuser::{BackgroundSession, MountOption};

use log::info;
use parking_lot::Mutex;
use tokio::runtime::Handle;

use crate::{
    errors::{RmkFsError, RmkFsResult},
    fs::fs_fuse::FsInner,
};

use datafusion::prelude::*;

#[derive(Default)]
pub struct RmkFs {
    session: Mutex<Option<BackgroundSession>>,
}

impl RmkFs {
    pub fn mount(
        &self,
        rt: Handle,
        context: SessionContext,
        ttl: Duration,
        mountpoint: &Path,
    ) -> RmkFsResult<()> {
        info!("Mounting filesystem...");

        let options = &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::FSName("remarkable".to_string()),
            MountOption::RO,
            MountOption::CUSTOM("volname=Remarkable".to_string()),
        ];

        let fs = FsInner::new(rt, context, ttl);

        *self.session.lock() = Some(
            fuser::spawn_mount2(fs, mountpoint.clone(), options).map_err(|source| {
                RmkFsError::MountError {
                    mountpoint: mountpoint.clone().to_string_lossy().to_string(),
                    source,
                }
            })?,
        );

        Ok(())
    }

    pub fn umount(&self) -> RmkFsResult<()> {
        if !self.is_mounted() {
            info!("Filesystem is not mounted, doing nothing...");

            return Ok(());
        }

        info!("Unmounting filesystem...");

        let session = self.session.lock().take();

        match session {
            Some(session) => {
                session.join();
                Ok(())
            }

            None => Err(RmkFsError::UmountError),
        }
    }

    pub fn is_mounted(&self) -> bool {
        self.session.lock().is_some()
    }
}

impl Drop for RmkFs {
    fn drop(&mut self) {
        if self.is_mounted() {
            self.umount().unwrap();
        }
    }
}
