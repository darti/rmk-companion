use std::{fmt::Debug, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{error::DataFusionError, prelude::ExecutionContext};
use fuser::{FileAttr, Filesystem, MountOption};
use libc::ENOENT;
use log::info;
use tokio::runtime::Handle;

use crate::{
    attr::{volume_icon_attr, HELLO_TXT_ATTR, TTL},
    errors::{RmkFsError, RmkFsResult},
    table::RmkTable,
};

#[derive(Clone)]
pub struct RmkFs {
    table: Arc<RmkTable>,
    context: ExecutionContext,
    runtime: Handle,
}

impl RmkFs {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = ExecutionContext::new();
        let table = Arc::new(RmkTable::new(root));

        let mut fs = RmkFs {
            table: table.clone(),
            context,
            runtime: Handle::current(),
        };

        fs.context.register_table("metadata", table)?;

        Ok(fs)
    }

    pub fn mount(self, mountpoint: &str) -> RmkFsResult<fuser::BackgroundSession> {
        self.scan()?;

        info!(
            "Mount point: {:?}",
            PathBuf::from(mountpoint).canonicalize().unwrap()
        );

        let options = &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::FSName("remarkable".to_string()),
            MountOption::RO,
            MountOption::CUSTOM("modules=volname:volicon".to_string()),
            MountOption::CUSTOM("volname=Remarkable".to_string()),
            MountOption::CUSTOM("iconpath=.VolumeIcon.icns".to_string()),
        ];

        fuser::spawn_mount2(self, mountpoint.clone(), options).map_err(|source| {
            RmkFsError::MountError {
                mountpoint: mountpoint.to_string(),
                source,
            }
        })
    }

    pub fn scan(&self) -> RmkFsResult<()> {
        self.table.scan()
    }
}

impl Debug for RmkFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RmkFs {{}}")
    }
}

impl Filesystem for RmkFs {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_str().unwrap().to_owned();

        // let fut =
        let task = self.runtime.spawn(self.lookup_async(parent, name));

        match self.runtime.block_on(task) {
            Ok(Ok((ttl, attr, genetation))) => {
                reply.entry(&ttl, &attr, genetation);
            }
            Err(_) => reply.error(ENOENT),
        }
    }
}

impl RmkFs {
    async fn lookup_async(
        &self,
        parent: u64,
        name: String,
    ) -> RmkFsResult<(Duration, FileAttr, u64)> {
        if parent == 1 && name == "hello.txt" {
            Ok((TTL, HELLO_TXT_ATTR, 0))
        } else if parent == 1 && name == ".VolumeIcon.icns" {
            Ok((TTL, volume_icon_attr(), 0))
        } else {
            Err(RmkFsError::FuserError)
        }
    }

    // fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
    //     match ino {
    //         1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
    //         2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
    //         3 => reply.attr(&TTL, &volume_icon_attr()),
    //         _ => reply.error(ENOENT),
    //     }
    // }

    // fn read(
    //     &mut self,
    //     _req: &Request,
    //     ino: u64,
    //     _fh: u64,
    //     offset: i64,
    //     size: u32,
    //     _flags: i32,
    //     _lock: Option<u64>,
    //     reply: ReplyData,
    // ) {
    //     let from = offset as usize;
    //     let to = from + size as usize;

    //     if ino == 2 {
    //         reply.data(&HELLO_TXT_CONTENT.as_bytes()[from..to]);
    //     } else if ino == 3 {
    //         let from = offset as usize;
    //         let to = from + size as usize;
    //         reply.data(&ICON_BYTES[from..to])
    //     } else {
    //         reply.error(ENOENT);
    //     }
    // }

    // fn readdir(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     _fh: u64,
    //     offset: i64,
    //     mut reply: fuser::ReplyDirectory,
    // ) {
    //     if ino != 1 {
    //         reply.error(ENOENT);
    //         return;
    //     }

    //     let entries = vec![
    //         (1, FileType::Directory, "."),
    //         (1, FileType::Directory, ".."),
    //         (2, FileType::RegularFile, "hello.txt"),
    //         (3, FileType::RegularFile, ".VolumeIcon.icns"),
    //     ];

    //     for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
    //         // i + 1 means the index of the next entry
    //         if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
    //             break;
    //         }
    //     }
    //     reply.ok();
    // }
}
