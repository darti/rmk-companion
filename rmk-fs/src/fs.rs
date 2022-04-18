use std::path::PathBuf;

use fuser::{Filesystem, MountOption};
use glob::glob;

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

    pub fn scan(&self) -> RmkFsResult<()> {
        let pattern = self.root.join("*.metadata");

        let pattern = pattern.to_str().ok_or_else(|| RmkFsError::ScanError {
            root: self.root.clone(),
        })?;

        for f in glob(pattern) {}

        Ok(())
    }
}

impl Filesystem for RmkFs {}
