use std::path::PathBuf;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use arrow::array::BinaryArray;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;

use datafusion::prelude::SessionContext;
use log::debug;
use log::info;

use crate::errors::RmkFsResult;
use crate::table::RmkNode;
use crate::RmkTable;

pub struct TableActor {
    table: Arc<RmkTable>,
    context: SessionContext,
}

impl TableActor {
    pub fn try_new(root: &PathBuf) -> Result<Self, DataFusionError> {
        let context = SessionContext::new();
        let table = Arc::new(RmkTable::new(root));

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

            content.0.push(node.ino);
            content.1.push(node.content.map_or(0, |c| c.len() as u64));
            content.2.push(node.content);
        }

        let fs = Self {
            table: table.clone(),
            context,
        };

        let metadata_provider = Arc::new(MemTable::try_new(
            table.schema(),
            vec![vec![RecordBatch::try_new(
                table.schema(),
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

        let content_schema = SchemaRef::new(Schema::new(vec![
            Field::new("ino", DataType::UInt64, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("content", DataType::Binary, true),
        ]));

        let content_provider = Arc::new(MemTable::try_new(
            content_schema.clone(),
            vec![vec![RecordBatch::try_new(
                content_schema,
                vec![
                    Arc::new(UInt64Array::from(content.0)),
                    Arc::new(UInt64Array::from(content.1)),
                    Arc::new(BinaryArray::from(content.2)),
                ],
            )?]],
        )?);

        fs.context.register_table("metadata_dynamic", table)?;
        fs.context
            .register_table("metadata_static", metadata_provider)?;

        fs.context
            .register_table("content_static", content_provider)?;

        let union = fs
            .context
            .table("metadata_dynamic")?
            .union(fs.context.table("metadata_static")?)?;

        fs.context.register_table("metadata", union)?;

        Ok(fs)
    }
}

impl Actor for TableActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "RmkFsResult<()>")]
pub struct Scan;

impl Handler<Scan> for TableActor {
    type Result = RmkFsResult<()>;

    fn handle(&mut self, _msg: Scan, _ctx: &mut Self::Context) -> Self::Result {
        info!("Scanning filesystem...");

        self.table.scan()
    }
}

#[derive(Message)]
#[rtype(result = "Result<Arc<DataFrame>, DataFusionError>")]
pub struct Query(String);

impl Query {
    pub fn new(query: &str) -> Self {
        Self(query.to_string())
    }
}

impl Handler<Query> for TableActor {
    type Result = AtomicResponse<Self, Result<Arc<DataFrame>, DataFusionError>>;

    fn handle(&mut self, msg: Query, _ctx: &mut Self::Context) -> Self::Result {
        let context = self.context.clone();

        let query = msg.0.clone();

        debug!("Executing query: {}", query);

        AtomicResponse::new(Box::pin(
            async move { context.sql(&query).await }
                .into_actor(self)
                .map(|out, _this, _| out),
        ))
    }
}
