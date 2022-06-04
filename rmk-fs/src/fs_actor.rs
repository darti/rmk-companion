use std::path::PathBuf;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use actix::prelude::*;
use actix::Actor;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use fuser::BackgroundSession;
use fuser::FileAttr;
use fuser::FileType;
use fuser::Filesystem;
use fuser::MountOption;

use log::error;
use log::info;

use crate::errors::RmkFsError;
use crate::errors::RmkFsResult;
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
            "select distinct ino, type from metadata where {}",
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

            for (ino, typ) in izip!(inos, types) {
                if let (Some(ino), Some(typ)) = (ino, typ) {
                    let attr = match typ {
                        "DocumentType" => {
                            attrs.push(FileAttr {
                                ino,
                                size: 0,
                                blocks: 0,
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
                                blksize: 512,
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
            MountOption::CUSTOM("modules=volname:volicon".to_string()),
            MountOption::CUSTOM("volname=Remarkable".to_string()),
            MountOption::CUSTOM("iconpath=.VolumeIcon.icns".to_string()),
        ];

        let fs = Fs {
            table: ctx.address(),
        };

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
    offset: usize,
    ino: u64,
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

            batches.show().await.unwrap();

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
    ino: u64,
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
    name: String,
    parent: u64,
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

struct Fs {
    table: Addr<FsActor>,
}

impl Filesystem for Fs {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let table = self.table.clone();

        let sys = System::new();

        let r = sys.block_on(async move {
            table
                .send(Lookup {
                    name: name.to_string_lossy().into_owned(),
                    parent,
                })
                .await
        });

        match r {
            Ok(Ok(Some(attr))) => reply.entry(&TTL, &attr, 0),
            Ok(Ok(None)) => reply.error(libc::ENOENT),
            Ok(Err(err)) => {
                error!("{:?}", err);
                reply.error(libc::ENOENT)
            }
            Err(err) => {
                error!("{:?}", err);
                reply.error(libc::ENOENT)
            }
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let table = self.table.clone();

        let sys = System::new();

        let r = sys.block_on(async move { table.send(GetAttr { ino }).await });

        match r {
            Ok(Ok(Some(attr))) => reply.attr(&TTL, &attr),
            Ok(Ok(None)) => reply.error(libc::ENOENT),
            Ok(Err(err)) => {
                error!("{:?}", err);
                reply.error(libc::ENOENT)
            }
            Err(err) => {
                error!("{:?}", err);
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
        let table = self.table.clone();

        let sys = System::new();

        let r = sys.block_on(async {
            table
                .send(ReadDir {
                    offset: offset as usize,
                    ino,
                })
                .await
                .unwrap()
                .unwrap()
        });

        for (ino, i, typ, name) in r {
            if reply.add(ino, i, typ, name) {
                break;
            }
        }

        reply.ok();
    }
}
