use std::path::PathBuf;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use actix::prelude::*;
use actix::Actor;
use arrow::array::Array;
use arrow::array::BinaryArray;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use fuser::BackgroundSession;
use fuser::FileAttr;
use fuser::FileType;

use fuser::MountOption;

use log::error;
use log::info;

use crate::errors::RmkFsError;
use crate::errors::RmkFsResult;
use crate::fs::Fs;
use crate::Query;
use crate::TableActor;
use itertools::izip;

pub const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct FsActor {
    mountpoint: PathBuf,
    session: Option<BackgroundSession>,
    table: Addr<TableActor>,
}

impl FsActor {
    pub fn new(mountpoint: &PathBuf, addr: Addr<TableActor>) -> Self {
        Self {
            mountpoint: mountpoint.clone(),
            session: None,
            table: addr,
        }
    }

    async fn search(table: Addr<TableActor>, condition: &str) -> RmkFsResult<Vec<FileAttr>> {
        let query = format!(
            "select distinct ino, type, size from metadata left join content_static on metadata.ino = content_static.ino where {}",
            condition
        );

        let batch = table.send(Query::new(&query)).await;

        let batches = match batch {
            Ok(Ok(batch)) => batch,
            Ok(Err(err)) => return Err(RmkFsError::DataFusionError(err)),
            Err(err) => return Err(RmkFsError::ActorError(err)),
        };

        let batches = batches.collect().await.unwrap();

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
                        "DocumentType" => {
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

                        "CollectionType" => {
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
    }
}

impl Actor for FsActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Mount;

impl Handler<Mount> for FsActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, _msg: Mount, ctx: &mut Self::Context) -> Self::Result {
        println!("Mounting filesystem...");

        let options = &[
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::FSName("remarkable".to_string()),
            MountOption::RO,
            MountOption::CUSTOM("volname=Remarkable".to_string()),
        ];

        let fs = Fs::new(ctx.address());

        self.session = Some(
            fuser::spawn_mount2(fs, self.mountpoint.clone(), options).map_err(|source| {
                RmkFsError::MountError {
                    mountpoint: self.mountpoint.clone().to_string_lossy().to_string(),
                    source,
                }
            })?,
        );

        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Umount;

impl Handler<Umount> for FsActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, _msg: Umount, _ctx: &mut Self::Context) -> Self::Result {
        info!("Unmounting filesystem...");

        let session = self.session.take();
        if let Some(session) = session {
            session.join();
        }

        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<Vec<(u64, i64, FileType, String)>>")]
pub struct ReadDir {
    pub offset: usize,
    pub ino: u64,
}

impl Handler<ReadDir> for FsActor {
    type Result = ResponseFuture<RmkFsResult<Vec<(u64, i64, FileType, String)>>>;

    fn handle(&mut self, msg: ReadDir, _ctx: &mut Self::Context) -> Self::Result {
        let table = self.table.clone();

        Box::pin(async move {
            let batches = table
                .send(Query::new(&format!(
                    "select ino, type, name, parent_ino from metadata where parent_ino = {}",
                    msg.ino
                )))
                .await
                .unwrap()
                .unwrap();

            let batches = batches.collect().await.unwrap();

            let mut dirs = Vec::new();

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

                for (i, (ino, typ, name)) in izip!(inos, types, names).enumerate().skip(msg.offset)
                {
                    if let (Some(ino), Some(typ), Some(name)) = (ino, typ, name) {
                        let typ = if typ == "DocumentType" {
                            FileType::RegularFile
                        } else {
                            FileType::Directory
                        };

                        dirs.push((ino, (i + 1) as i64, typ, name.to_owned()));
                    }
                }
            }

            Ok(dirs)
        })
    }
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<Option<FileAttr>>")]
pub struct GetAttr {
    pub ino: u64,
}

impl Handler<GetAttr> for FsActor {
    type Result = ResponseFuture<RmkFsResult<Option<FileAttr>>>;

    fn handle(&mut self, msg: GetAttr, _ctx: &mut Self::Context) -> Self::Result {
        let table = self.table.clone();

        Box::pin(async move {
            let condition = format!("ino = {}", msg.ino);
            Ok(FsActor::search(table, &condition).await.unwrap().pop())
        })
    }
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<Option<FileAttr>>")]
pub struct Lookup {
    pub name: String,
    pub parent: u64,
}

impl Handler<Lookup> for FsActor {
    type Result = ResponseFuture<RmkFsResult<Option<FileAttr>>>;

    fn handle(&mut self, msg: Lookup, _ctx: &mut Self::Context) -> Self::Result {
        let table = self.table.clone();

        Box::pin(async move {
            let condition = format!("parent_ino = {} and name = '{}'", msg.parent, msg.name);
            Ok(FsActor::search(table, &condition).await.unwrap().pop())
        })
    }
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<Option<Vec<u8>>>")]
pub struct Read {
    pub ino: u64,
    pub offset: i64,
    pub size: u32,
}

impl Handler<Read> for FsActor {
    type Result = ResponseFuture<RmkFsResult<Option<Vec<u8>>>>;

    fn handle(&mut self, msg: Read, _ctx: &mut Self::Context) -> Self::Result {
        let table = self.table.clone();

        Box::pin(async move {
            let query = format!("select content from content_static where ino = {}", msg.ino);

            let batches = table.send(Query::new(&query)).await.unwrap().unwrap();

            let batches = batches.collect().await.unwrap();

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
