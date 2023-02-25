mod os;
mod table;
mod table_static;

use std::{path::Path, sync::Arc};

use datafusion::prelude::SessionContext;

use crate::errors::RmkFsResult;

use self::{os::STATIC_FILES, table::RmkTable, table_static::create_static};

pub async fn init_tables<P>(context: SessionContext, root: P) -> RmkFsResult<()>
where
    P: AsRef<Path>,
{
    let table_dyn = Arc::new(RmkTable::new(root)?);

    let (metadata_static, content_static) = create_static(STATIC_FILES.iter())?;

    context.register_table("metadata_dynamic", table_dyn.clone())?;
    context.register_table("metadata_static", metadata_static)?;

    let metadata = context
        .table("metadata_dynamic")
        .await?
        .union(context.table("metadata_static").await?)?;

    context.register_table("metadata", metadata.into_view())?;

    // TODO: add content_dynamic
    context.register_table("content", content_static)?;

    Ok(())
}
