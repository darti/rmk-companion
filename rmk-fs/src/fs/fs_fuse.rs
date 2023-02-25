use std::{
    ffi::OsString,
    time::{Duration, UNIX_EPOCH},
};

use datafusion::{
    arrow::array::{Array, BinaryArray, StringArray, UInt64Array},
    parquet::data_type::AsBytes,
    prelude::SessionContext,
};
use fuser::{FileAttr, FileType, Filesystem, ReplyData, Request};
use indoc::formatdoc;
use itertools::izip;
use log::{debug, error};
use tokio::runtime::Handle;

use crate::errors::RmkFsResult;

pub(crate) struct FsInner {
    context: SessionContext,
    rt: Handle,
    ttl: Duration,
}

impl FsInner {
    pub fn new(rt: Handle, context: SessionContext, ttl: Duration) -> Self {
        Self { context, rt, ttl }
    }
}

impl FsInner {
    pub fn query_attr(
        &self,
        condition: &str,
        with_size: bool,
    ) -> RmkFsResult<Vec<(OsString, FileAttr)>> {
        let query = formatdoc!(
            "
            SELECT distinct
                ino, 
                type,
                name, 
                {} 
            FROM metadata 
            {}
            WHERE {}",
            if with_size {
                "size"
            } else {
                "CAST(0 AS BIGINT UNSIGNED) AS size"
            },
            if with_size {
                "LEFT OUTER JOIN content ON metadata.id = content.id"
            } else {
                ""
            },
            condition
        );

        debug!("Query: {}", query);

        self.rt.block_on(async {
            let df = self.context.sql(&query).await?;
            let batches = df.collect().await?;

            let mut attrs = Vec::new();

            for batch in batches {
                let inos = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                let types = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                let names = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                let sizes = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                for (ino, typ, name, size) in izip!(inos, types, names, sizes) {
                    if let (Some(ino), Some(typ), Some(name), Some(size)) = (ino, typ, name, size) {
                        let kind = match typ {
                            DOCUMENT_TYPE => FileType::RegularFile,
                            COLLECTION_TYPE => FileType::Directory,
                            _ => continue,
                        };

                        let blksize = 512;
                        let blocks = (size + blksize - 1) / blksize;

                        attrs.push((
                            name.into(),
                            FileAttr {
                                ino,
                                size,
                                blocks,
                                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                                mtime: UNIX_EPOCH,
                                ctime: UNIX_EPOCH,
                                crtime: UNIX_EPOCH,
                                kind,
                                perm: 0o755,
                                nlink: 2,
                                uid: 501,
                                gid: 20,
                                rdev: 0,
                                flags: 0,
                                blksize: blksize as u32,
                            },
                        ));
                    }
                }
            }

            Ok(attrs)
        })
    }

    fn read_content(&self, ino: u64) -> RmkFsResult<Option<Vec<u8>>> {
        let query = formatdoc!(
            "
            SELECT 
                content.content 
            FROM metadata 
            JOIN content ON metadata.id = content.id 
            WHERE metadata.ino = {}
            LIMIT 1",
            ino
        );

        self.rt.block_on(async {
            let df = self.context.sql(&query).await?;
            let batches = df.collect().await?;

            for batch in batches {
                let content = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();

                if content.len() > 0 {
                    return Ok(Some(content.value(0).to_vec()));
                }
            }

            Ok(None)
        })
    }
}

impl Filesystem for FsInner {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let nodes = self.query_attr(
            format!(
                "parent_ino = {} AND name = '{}' LIMIT 1",
                parent,
                name.to_string_lossy()
            )
            .as_str(),
            true,
        );

        match nodes {
            Ok(attrs) => {
                if let Some((_, attr)) = attrs.first() {
                    reply.entry(&self.ttl, attr, 0)
                } else {
                    error!("node not found: {} ->  {}", parent, name.to_string_lossy());
                    reply.error(libc::ENOENT)
                }
            }

            Err(err) => {
                error!("lookup: {:?}", err);
                reply.error(libc::ENOENT)
            }
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let nodes = self.query_attr(format!("ino = {}  LIMIT 1", ino).as_str(), true);

        match nodes {
            Ok(attrs) => {
                if let Some((_, attr)) = attrs.first() {
                    reply.attr(&self.ttl, attr)
                } else {
                    error!("node not found: {}", ino);
                    reply.error(libc::ENOENT)
                }
            }

            Err(err) => {
                error!("getattr: {:?}", err);
                reply.error(libc::ENOENT)
            }
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
        let nodes = self.query_attr(
            format!("parent_ino = {}  SORT BY ino OFFSET {} ", ino, offset).as_str(),
            false,
        );

        match nodes {
            Ok(attrs) => {
                for (i, (name, attr)) in attrs.iter().enumerate() {
                    if reply.add(attr.ino, (i + 1) as i64, attr.kind, name) {
                        break;
                    }
                }
            }

            Err(err) => {
                error!("readdir: {:?}", err);
                reply.error(libc::ENOENT);
                return;
            }
        }

        reply.ok();
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
        match self.read_content(ino) {
            Ok(Some(content)) => {
                let from = offset as usize;
                let to = from + size as usize;

                reply.data(&content[from..to].as_bytes())
            }
            _ => reply.error(libc::ENOENT),
        };
    }
}
