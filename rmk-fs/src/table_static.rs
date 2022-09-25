use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{BinaryArray, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::datasource::{MemTable, TableProvider};

use crate::SCHEMAS;

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

pub fn create_static() -> Result<(Arc<dyn TableProvider>, Arc<dyn TableProvider>), DataFusionError>
{
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

    let mut metadata = (
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
    );
    let mut content = (
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
    );

    for node in static_files.iter() {
        metadata.0.push(node.id);
        metadata.1.push(node.typ);
        metadata.2.push(node.name);
        metadata.3.push(node.parent);
        metadata.4.push(node.ino);
        metadata.5.push(node.parent_ino);

        content.0.push(node.id);
        content.1.push(node.content.map_or(0, |c| c.len() as u64));
        content.2.push(node.content);
    }

    let metadata_provider = Arc::new(MemTable::try_new(
        SCHEMAS.metadata(),
        vec![vec![RecordBatch::try_new(
            SCHEMAS.metadata(),
            vec![
                Arc::new(StringArray::from(metadata.0)),
                Arc::new(StringArray::from(metadata.1)),
                Arc::new(StringArray::from(metadata.2)),
                Arc::new(StringArray::from(metadata.3)),
                Arc::new(UInt64Array::from(metadata.4)),
                Arc::new(UInt64Array::from(metadata.5)),
            ],
        )?]],
    )?);

    let content_provider = Arc::new(MemTable::try_new(
        SCHEMAS.content(),
        vec![vec![RecordBatch::try_new(
            SCHEMAS.content(),
            vec![
                Arc::new(StringArray::from(content.0)),
                Arc::new(UInt64Array::from(content.1)),
                Arc::new(BinaryArray::from(content.2)),
            ],
        )?]],
    )?);

    Ok((metadata_provider, content_provider))
}
