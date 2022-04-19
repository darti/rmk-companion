use std::path::PathBuf;

use rmk_fs::{errors::RmkFsResult, RmkFs};

fn main() -> RmkFsResult<()> {
    pretty_env_logger::init();
    let fs = RmkFs::new(&PathBuf::from("./rmk-notebook/samples"));

    fs.scan()
}
