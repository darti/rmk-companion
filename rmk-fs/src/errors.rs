use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RmkFsError {
    #[error("failed to mount RmkFS at {mountpoint}")]
    MountError {
        mountpoint: String,
        source: std::io::Error,
    },

    #[error("no mounted RmkFS")]
    UmountError,

    #[error("failed to scan RmkFS at {root}")]
    ScanError { root: PathBuf },

    #[error(transparent)]
    NotebookError(#[from] rmk_notebook::Error),
    #[error(transparent)]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("fuser error")]
    FuserError,

    #[error("ino {0} not found")]
    NotFound(u64),

    #[error("unknown file type: {0}")]
    UnknownFileType(String),

    #[error("daemon failure: {0}")]
    DaemonError(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("enable to acquire lock")]
    ConcurrencyError,
}

pub type RmkFsResult<T> = Result<T, RmkFsError>;
