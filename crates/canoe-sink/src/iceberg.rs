use crate::Sink;
use async_trait::async_trait;
use serde_json::Value;

/// Placeholder for Apache Iceberg table writes. Establishes the trait
/// boundary now so a real implementation (via `iceberg-rust`, committing
/// through a REST/Glue catalog) can be dropped in without touching the
/// coordinator or config schema.
pub struct IcebergSink {
    catalog: String,
    table: String,
    #[allow(dead_code)]
    partition_by: Vec<String>,
}

impl IcebergSink {
    pub fn new(catalog: String, table: String, partition_by: Vec<String>) -> Self {
        Self {
            catalog,
            table,
            partition_by,
        }
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn write_batch(&self, _rows: &[Value]) -> anyhow::Result<()> {
        anyhow::bail!(
            "iceberg sink not yet implemented (catalog='{}', table='{}')",
            self.catalog,
            self.table
        )
    }

    fn name(&self) -> &str {
        "iceberg"
    }
}
