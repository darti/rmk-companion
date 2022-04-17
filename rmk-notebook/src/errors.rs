use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),

    #[error("invalid metadata: {0}")]
    InvalidFile(#[from] io::Error),

    #[error("failed to write file: {0}")]
    WriteError(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error(transparent)]
    InvalidMetadata(#[from] serde_json::Error),

    #[error("unsupported version string: {0}")]
    UnsupportedVersion(String),

    #[error("unknown brush type: {0}")]
    UnknownBrush(i32),

    #[error("unknown color: {0}")]
    UnknownColor(i32),

    #[error("invalid Segment index: {0}")]
    InvalidSegmentIndex(usize),
}

pub type Result<T> = std::result::Result<T, Error>;
