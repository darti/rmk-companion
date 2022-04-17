use std::path::PathBuf;

mod errors;
mod notebook;
mod parse;
mod render;
mod rm;

pub use errors::*;
use notebook::{read_content, read_metadata, read_pagedata, read_rm, Content, Metadata};
use rm::Page;

pub struct Notebook {
    metadata: Metadata,
    content: Content,
    pagedata: Vec<String>,
    pages: Vec<Page>,
}

pub fn read_notebook(root: &PathBuf, id: &str) -> Result<Notebook> {
    let metadata = read_metadata(root, id)?;
    let content = read_content(root, id)?;
    let pagedata = read_pagedata(root, id)?;
    let pages = read_rm(root, id, &content.pages)?;

    Ok(Notebook {
        metadata,
        content,
        pagedata,
        pages,
    })
}
