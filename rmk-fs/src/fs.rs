use std::{
    fmt::Debug,
    path::PathBuf,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use datafusion::{error::DataFusionError, prelude::ExecutionContext};
use fuser::{FileAttr, FileType, Filesystem, MountOption};
use libc::ENOENT;

use crate::{
    errors::{RmkFsError, RmkFsResult},
    table::RmkTable,
};

const TTL: Duration = Duration::from_secs(1); // 1 second

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
        let options = &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::FSName("remarkable".to_string()),
            MountOption::RO,
            MountOption::CUSTOM("volname=Remarkable".to_string()),
            // MountOption::CUSTOM(
            //     "modules=volicon,iconpath=../resources/remarkable.icns".to_string(),
            // ),
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
    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match ino {
            1 => reply.attr(
                &TTL,
                &FileAttr {
                    ino: 1,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                },
            ),
            _ => reply.error(ENOENT),
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
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        reply.error(ENOENT);
    }
}
