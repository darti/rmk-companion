use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use once_cell::sync::Lazy;

pub static SCHEMAS: Lazy<Schemas> = Lazy::new(|| Schemas::new());

pub struct Schemas {
    metadata_schema: SchemaRef,
    content_schema: SchemaRef,
}

impl Schemas {
    fn new() -> Self {
        Self {
            metadata_schema: SchemaRef::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("type", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("parent", DataType::Utf8, true),
                Field::new("ino", DataType::UInt64, false),
                Field::new("parent_ino", DataType::UInt64, false),
            ])),
            content_schema: SchemaRef::new(Schema::new(vec![
                Field::new("ino", DataType::UInt64, false),
                Field::new("size", DataType::UInt64, false),
                Field::new("content", DataType::Binary, true),
            ])),
        }
    }

    pub fn metadata(&self) -> SchemaRef {
        self.metadata_schema.clone()
    }

    pub fn content(&self) -> SchemaRef {
        self.content_schema.clone()
    }
}
