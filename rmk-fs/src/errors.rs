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
    PolarError(#[from] polars::error::PolarsError),
    #[error(transparent)]
    ActorError(#[from] actix::MailboxError),

    #[error("fuser error")]
    FuserError,

    #[error("ino {0} not found")]
    NotFound(u64),

    #[error("unknown file type: {0}")]
    UnknownFileType(String),

    #[error("failed to start daemon")]
    DaemonError,
}

pub type RmkFsResult<T> = Result<T, RmkFsError>;
