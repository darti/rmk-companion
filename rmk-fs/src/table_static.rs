use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use polars::prelude::*;

pub struct RmkNode<'a> {
    pub id: &'a str,
    pub typ: &'a str,
    pub name: &'a str,
    pub parent: Option<&'a str>,
    pub ino: u64,
    pub parent_ino: u64,
    pub content: Option<&'a [u8]>,
}

impl<'a> RmkNode<'a> {
    pub fn new(
        id: &'a str,
        typ: &'a str,
        name: &'a str,
        parent: Option<&'a str>,
        content: Option<&'a [u8]>,
    ) -> Self {
        let ino = if id == "." {
            1
        } else {
            let mut s = DefaultHasher::new();
            id.hash(&mut s);
            s.finish()
        };

        let parent_ino = {
            let mut s = DefaultHasher::new();
            match parent {
                Some(p) => {
                    p.hash(&mut s);
                    s.finish()
                }
                None => 1,
            }
        };

        Self {
            id,
            typ,
            name,
            parent,
            ino,
            parent_ino,
            content,
        }
    }
}

pub fn create_static() -> Result<(DataFrame, DataFrame)> {
    let static_files = [
        RmkNode::new(".", "CollectionType", ".", None, None),
        RmkNode::new(".", "CollectionType", "..", None, None),
        RmkNode::new(
            ".VolumeIcon.icns",
            "DocumentType",
            ".VolumeIcon.icns",
            None,
            Some(include_bytes!("../resources/.VolumeIcon.icns")),
        ),
        RmkNode::new(
            "._.VolumeIcon.icns",
            "DocumentType",
            "._.VolumeIcon.icns",
            None,
            Some(include_bytes!("../resources/._.VolumeIcon.icns")),
        ),
        RmkNode::new(
            "._.",
            "DocumentType",
            "._.",
            None,
            Some(include_bytes!("../resources/._.")),
        ),
        RmkNode::new(
            "._.com.apple.timemachine.donotpresent",
            "DocumentType",
            "._.com.apple.timemachine.donotpresent",
            None,
            Some(include_bytes!(
                "../resources/._.com.apple.timemachine.donotpresent"
            )),
        ),
    ];

    let n = static_files.len();

    let mut ids = Vec::with_capacity(n);
    let mut types = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut parents = Vec::with_capacity(n);
    let mut inos = Vec::with_capacity(n);
    let mut parent_inos = Vec::with_capacity(n);

    let mut sizes = Vec::with_capacity(n);
    let mut contents = Vec::with_capacity(n);

    for node in static_files.iter() {
        ids.push(node.id);
        types.push(node.typ);
        names.push(node.name);
        parents.push(node.parent);
        inos.push(node.ino);
        parent_inos.push(node.parent_ino);

        sizes.push(node.content.map_or(0, |c| c.len() as u64));
        contents.push(node.content);
    }

    let metadata_df = df![
        "id" => ids.clone(),
        "type" => types,
        "name" => names,
        "parent" => parents,
        "ino" => inos,
        "parent_ino" => parent_inos
    ]?;

    let content_df = df![
        "id" => ids,
        "size" => sizes,
        // "content" => content
    ]?;

    Ok((metadata_df, content_df))
}
