use glob::glob;
use polars::prelude::{DataFrame, PolarsError};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use log::{debug, info, warn};
use rmk_notebook::{read_metadata, Metadata};

use crate::{
    create_static,
    errors::{RmkFsError, RmkFsResult},
};

struct RmkTableInner {
    data: HashMap<String, Metadata>,
    root: PathBuf,

    metadata: DataFrame,
}

impl RmkTableInner {
    fn new(root: PathBuf) -> RmkFsResult<Self> {
        let (metadata_static, content_static) = create_static()?;

        Ok(RmkTableInner {
            data: HashMap::new(),
            root,
            metadata: metadata_static,
        })
    }

    fn scan(&mut self) -> RmkFsResult<()> {
        match std::fs::canonicalize(&self.root) {
            Ok(_) => info!("Scanning filesystem at {}", self.root.display()),
            Err(_) => {
                warn!("Invalid scan path: {}", self.root.display());
                return Err(RmkFsError::ScanError {
                    root: self.root.clone(),
                });
            }
        };

        let pattern = self.root.join("*.metadata");

        let pattern = pattern.to_str().ok_or_else(|| RmkFsError::ScanError {
            root: self.root.clone(),
        })?;

        let files = glob(pattern)
            .map_err(|_source| RmkFsError::ScanError {
                root: self.root.clone(),
            })?
            .flatten();

        for f in files {
            debug!("{:?}", f);

            let (id, metadata) = read_metadata(&f)?;
            self.data.insert(id.to_string(), metadata);
        }

        Ok(())
    }
}

impl Debug for RmkTableInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RmkTableInner {{ root: {:?} }}", self.root)
    }
}

#[derive(Clone, Debug)]
pub struct RmkTable {
    inner: Arc<RwLock<RmkTableInner>>,
}

impl Display for RmkTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner.read().unwrap())
    }
}

impl RmkTable {
    pub fn new(root: &PathBuf) -> RmkFsResult<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(RmkTableInner::new(root.clone())?)),
        })
    }

    pub fn scan(&self) -> RmkFsResult<()> {
        self.inner.write().unwrap().scan()
    }
}
