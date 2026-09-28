//! Writes a chain's raw topic traffic to S3 as Parquet, for durability
//! and debugging — independent of, and secondary to, the pipeline's own
//! sinks. Deliberately simpler than `ParquetSink`: no dataset_id/run_id
//! nesting, no `schema.avsc`/`latest.json`, no Hive partitioning — just
//! one flat timestamped file per flush, because the point here is "what
//! did this task actually see," not a queryable dataset.

use crate::parquet_encode::{encode_parquet, infer_schema};
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use serde_json::Value;
use std::sync::Arc;
use url::Url;

/// One archiver per managed topic. Every flush writes
/// `<base_path>/<pipeline>/<task>/<ulid>.parquet` — a
/// [ULID](https://github.com/ulid/spec) so, same as a sink's `run_id`,
/// files sort into flush order by name alone with no separate index to
/// consult.
pub struct TopicArchiver {
    store: Arc<dyn ObjectStore>,
    path_prefix: StorePath,
}

impl TopicArchiver {
    pub fn new(base_path_uri: &str, pipeline: &str, task: &str) -> anyhow::Result<Self> {
        let url = Url::parse(base_path_uri)
            .map_err(|e| anyhow::anyhow!("invalid archive base_path '{base_path_uri}': {e}"))?;
        let (store, base_path) = parse_url(&url)?;
        let path_prefix = base_path.child(pipeline).child(task);
        Ok(Self {
            store: Arc::from(store),
            path_prefix,
        })
    }

    /// No-op on an empty batch (never writes a zero-row file). Schema is
    /// inferred fresh from whatever this flush happened to see, same
    /// per-batch simplification as `ParquetSink` — there's no
    /// `schema.avsc` here to keep in sync, so unlike the sink, that
    /// tradeoff has no visible artifact at all, just files whose column
    /// shape can differ flush to flush if the topic's messages do.
    pub async fn write_batch(&self, rows: &[Value]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let schema = Arc::new(infer_schema(rows)?);
        let bytes = encode_parquet(schema, rows)?;
        let file_name = format!("{}.parquet", ulid::Ulid::generate());
        let path = self.path_prefix.child(file_name.as_str());
        self.store.put(&path, Bytes::from(bytes).into()).await?;
        Ok(())
    }
}
