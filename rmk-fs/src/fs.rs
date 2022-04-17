use std::path::PathBuf;

use fuser::{Filesystem, MountOption};

use crate::errors::{RmkFsError, RmkFsResult};

pub struct RmkFs {
    root: PathBuf,
}

impl RmkFs {
    pub fn new(root: &PathBuf) -> Self {
        RmkFs { root: root.clone() }
    }

    pub fn mount(self, mountpoint: &str) -> RmkFsResult<fuser::BackgroundSession> {
        fuser::spawn_mount2(self, mountpoint.clone(), &[MountOption::AutoUnmount]).map_err(
            |source| RmkFsError::MountError {
                mountpoint: mountpoint.to_string(),
                source,
            },
        )
    }
}

impl Filesystem for RmkFs {}
