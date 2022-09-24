use actix::{Addr, System};

use fuser::{Filesystem, ReplyData, Request};
use log::error;

use crate::{FsActor, GetAttr, Lookup, Read, ReadDir, TTL};

pub(crate) struct Fs {
    table: Addr<FsActor>,
}

impl Fs {
    pub fn new(table: Addr<FsActor>) -> Self {
        Self { table }
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
        let table = self.table.clone();

        let sys = System::new();

        let r = sys.block_on(async {
            table
                .send(Read { ino, offset, size })
                .await
                .unwrap()
                .unwrap()
        });

        match r {
            Some(content) => {
                let from = offset as usize;
                let to = from + size as usize;

                reply.data(content[from..to].into())
            }
            None => reply.error(libc::ENOENT),
        };
    }
}
