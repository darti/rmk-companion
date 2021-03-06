use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RmkFsError {
    #[error("failed to mount RmkFS at {mountpoint}")]
    MountError {
        mountpoint: String,
        source: std::io::Error,
    },

    #[error("failed to scan RmkFS at {root}")]
    ScanError { root: PathBuf },

    #[error(transparent)]
    NotebookError(#[from] rmk_notebook::Error),
    #[error(transparent)]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("fuser error")]
    FuserError,
}

pub type RmkFsResult<T> = Result<T, RmkFsError>;
