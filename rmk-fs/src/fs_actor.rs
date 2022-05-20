use std::path::PathBuf;

use actix::prelude::*;
use actix::Actor;
use fuser::BackgroundSession;
use fuser::Filesystem;
use fuser::MountOption;
use log::info;
use tokio::runtime::Runtime;

use crate::errors::RmkFsError;
use crate::errors::RmkFsResult;
use crate::Query;
use crate::TableActor;

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
}

impl Actor for FsActor {
    type Context = SyncContext<Self>;
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Mount;

impl Handler<Mount> for FsActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, msg: Mount, ctx: &mut Self::Context) -> Self::Result {
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
            table: self.table.clone(),
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

struct Fs {
    table: Addr<TableActor>,
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let r = rt
            .block_on(tokio::spawn(async move {
                table.send(Query::new("select * from metadata")).await
            }))
            .unwrap()
            .unwrap()
            .unwrap();

        r.show();

        // let r = r.join().unwrap().unwrap();
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let table = self.table.clone();

        table.send(Query::new("select * from metadata"));

        // let arb = Arbiter::new();
        let sys = System::new();

        let r = sys.block_on(async {
            let df = table
                .send(Query::new("select * from metadata"))
                .await
                .unwrap()
                .unwrap()
                .collect()
                .await
                .unwrap();
        });

        // let r = rt
        //     .block_on(tokio::spawn(async move {
        //         table.send(Query::new("select * from metadata")).await
        //     }))
        //     .unwrap()
        //     .unwrap()
        //     .unwrap();

        // r.show();
    }
}
