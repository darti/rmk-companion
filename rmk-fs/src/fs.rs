use std::{
    ffi::OsStr,
    fmt::Debug,
    path::PathBuf,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use datafusion::{error::DataFusionError, parquet::data_type::AsBytes, prelude::ExecutionContext};
use fuser::{FileAttr, FileType, Filesystem, MountOption, ReplyData, Request};
use libc::ENOENT;
use log::info;

use crate::{
    attr::{volume_icon_attr, HELLO_DIR_ATTR, HELLO_TXT_ATTR, HELLO_TXT_CONTENT, ICON_BYTES, TTL},
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
        if parent == 1 && name.to_str() == Some("hello.txt") {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else if parent == 1 && name.to_str() == Some(".VolumeIcon.icns") {
            reply.entry(&TTL, &volume_icon_attr(), 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            3 => reply.attr(&TTL, &volume_icon_attr()),
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let from = offset as usize;
        let to = from + size as usize;

        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[from..to]);
        } else if ino == 3 {
            let from = offset as usize;
            let to = from + size as usize;
            reply.data(&ICON_BYTES[from..to])
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
            (3, FileType::RegularFile, ".VolumeIcon.icns"),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
}
