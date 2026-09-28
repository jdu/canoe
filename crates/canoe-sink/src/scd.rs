use crate::Sink;
use async_trait::async_trait;
use serde_json::Value;

/// Placeholder for slowly-changing-dimension (Type 2 by default) merges
/// into a datalake table. Real implementation will diff incoming rows
/// against current table state on `keys`, close out changed rows
/// (set `valid_to`/`is_current = false`) and insert new versions,
/// on top of the same Iceberg-backed storage as `IcebergSink`.
pub struct ScdSink {
    catalog: String,
    table: String,
    #[allow(dead_code)]
    keys: Vec<String>,
    scd_type: u8,
}

impl ScdSink {
    pub fn new(catalog: String, table: String, keys: Vec<String>, scd_type: u8) -> Self {
        Self {
            catalog,
            table,
            keys,
            scd_type,
        }
    }
}

#[async_trait]
impl Sink for ScdSink {
    async fn write_batch(&self, _rows: &[Value]) -> anyhow::Result<()> {
        anyhow::bail!(
            "scd sink not yet implemented (catalog='{}', table='{}', type={})",
            self.catalog,
            self.table,
            self.scd_type
        )
    }

    fn name(&self) -> &str {
        "scd"
    }
}
