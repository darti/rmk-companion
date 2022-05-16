use std::path::PathBuf;

use actix::prelude::*;
use actix::Actor;
use fuser::BackgroundSession;
use fuser::Filesystem;
use fuser::MountOption;
use log::info;
use tokio::fs::File;

use crate::errors::RmkFsError;
use crate::errors::RmkFsResult;

pub struct FsActor {
    mountpoint: PathBuf,
    session: Option<BackgroundSession>,
}

impl FsActor {
    pub fn new(mountpoint: &PathBuf) -> Self {
        Self {
            mountpoint: mountpoint.clone(),
            session: None,
        }
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

    fn handle(&mut self, msg: Mount, _ctx: &mut Self::Context) -> Self::Result {
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

        let fs = Fs {};

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

struct Fs {}

impl Filesystem for Fs {}
