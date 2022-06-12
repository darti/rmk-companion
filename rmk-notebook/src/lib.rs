use std::path::PathBuf;

pub mod errors;
mod notebook;
mod parse;
mod render;
mod rm;

pub use errors::*;
use notebook::read_metadata_with_id;
pub use notebook::*;
use rm::Page;

pub struct Notebook {
    metadata: Metadata,
    content: Content,
    pagedata: Vec<String>,
    pages: Vec<Page>,
}

pub fn read_notebook(root: &PathBuf, id: &str) -> Result<Notebook> {
    let metadata = read_metadata_with_id(root, id)?;
    let content = read_content_with_id(root, id)?;
    let pagedata = read_pagedata_with_id(root, id)?;
    let pages = read_rm(root, id, &content.pages)?;

    Ok(Notebook {
        metadata,
        content,
        pagedata,
        pages,
    })
}
