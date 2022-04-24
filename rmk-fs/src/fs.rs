use std::{fmt::Debug, path::PathBuf, sync::Arc};

use datafusion::{error::DataFusionError, prelude::ExecutionContext};
use fuser::{Filesystem, MountOption};

use crate::{
    errors::{RmkFsError, RmkFsResult},
    table::RmkTable,
};

#[derive(Clone)]
pub struct RmkFs {
    table: Arc<RmkTable>,
    context: ExecutionContext,
}

impl RmkFs {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = ExecutionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let mut fs = RmkFs {
            table: table.clone(),
            context,
        };

        fs.context.register_table("metadata", table)?;

        Ok(fs)
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

impl Debug for RmkFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RmkFs {{}}")
    }
}

impl Filesystem for RmkFs {}
