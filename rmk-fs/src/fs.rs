use std::{
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use fuser::{FileAttr, FileType, Filesystem, ReplyData, Request};
use indoc::formatdoc;
use itertools::izip;
use log::error;
use tokio::runtime::Runtime;

use crate::{create_static, errors::RmkFsResult, RmkTable};

use datafusion::{
    arrow::array::{StringArray, UInt64Array},
    prelude::*,
};

pub(crate) struct Fs {
    table_dyn: Arc<RmkTable>,
    rt: Arc<Runtime>,
    context: SessionContext,
    ttl: Duration,
}

impl Fs {
    pub async fn new<P>(root: P, ttl: Duration, rt: Arc<Runtime>) -> RmkFsResult<Self>
    where
        P: AsRef<Path>,
    {
        let context = SessionContext::new();

        let table_dyn = Arc::new(RmkTable::new(root)?);

        let (metadata_static, content_static) = create_static()?;

        context.register_table("metadata_dynamic", table_dyn.clone())?;
        context.register_table("metadata_static", metadata_static)?;

        let metadata = context
            .table("metadata_dynamic")
            .await?
            .union(context.table("metadata_static").await?)?;

        context.register_table("metadata", metadata.into_view())?;

        context.register_table("content_static", content_static)?;

        Ok(Self {
            table_dyn,
            context,
            rt,
            ttl,
        })
    }

    pub fn query_attr(&self, condition: &str) -> RmkFsResult<Vec<FileAttr>> {
        let query = formatdoc!(
            "SELECT 
             distinct ino, 
             type, 
             size 
             FROM metadata 
             LEFT OUTER JOIN content ON metadata.id = content.id 
             WHERE {}",
            condition
        );

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

                let sizes = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                for (ino, typ, size) in izip!(inos, types, sizes) {
                    if let (Some(ino), Some(typ), Some(size)) = (ino, typ, size) {
                        match typ {
                            DOCUMENT_TYPE => {
                                let blksize = 512;
                                let blocks = (size + blksize - 1) / blksize;

                                attrs.push(FileAttr {
                                    ino,
                                    size,
                                    blocks,
                                    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                                    mtime: UNIX_EPOCH,
                                    ctime: UNIX_EPOCH,
                                    crtime: UNIX_EPOCH,
                                    kind: FileType::RegularFile,
                                    perm: 0o755,
                                    nlink: 2,
                                    uid: 501,
                                    gid: 20,
                                    rdev: 0,
                                    flags: 0,
                                    blksize: blksize as u32,
                                });
                            }

                            COLLECTION_TYPE => {
                                attrs.push(FileAttr {
                                    ino,
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
                                });
                            }

                            t => {
                                error!("Unknown type: {}", t);
                            }
                        };
                    }
                }
            }

            Ok(attrs)
        })
    }
}

impl Filesystem for Fs {
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
        );

        match nodes {
            Ok(attrs) => {
                if let Some(attr) = attrs.first() {
                    reply.entry(&self.ttl, attr, 0)
                } else {
                    error!("node not found: {} ->  {}", parent, name.to_string_lossy());
                    reply.error(libc::ENOENT)
                }
            }

            Err(err) => {
                error!("{:?}", err);
                reply.error(libc::ENOENT)
            }
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        // let table = self.table.clone();

        // let sys = System::new();

        // let r = sys.block_on(async move { table.send(GetAttr { ino }).await });

        // match r {
        //     Ok(Ok(Some(attr))) => reply.attr(&TTL, &attr),
        //     Ok(Ok(None)) => reply.error(libc::ENOENT),
        //     Ok(Err(err)) => {
        //         error!("{:?}", err);
        //         reply.error(libc::ENOENT)
        //     }
        //     Err(err) => {
        //         error!("{:?}", err);
        //         reply.error(libc::ENOENT)
        //     }
        // }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        // let table = self.table.clone();

        // let sys = System::new();

        // let r = sys.block_on(async {
        //     table
        //         .send(ReadDir {
        //             offset: offset as usize,
        //             ino,
        //         })
        //         .await
        //         .unwrap()
        //         .unwrap()
        // });

        // for (ino, i, typ, name) in r {
        //     if reply.add(ino, i, typ, name) {
        //         break;
        //     }
        // }

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
        // let table = self.table.clone();

        // let sys = System::new();

        // let r = sys.block_on(async {
        //     table
        //         .send(Read { ino, offset, size })
        //         .await
        //         .unwrap()
        //         .unwrap()
        // });

        // match r {
        //     Some(content) => {
        //         let from = offset as usize;
        //         let to = from + size as usize;

        //         reply.data(&content[from..to].as_bytes())
        //     }
        //     None => reply.error(libc::ENOENT),
        // };
    }
}
