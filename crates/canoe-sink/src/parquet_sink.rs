use crate::run_writer::write_run;
use crate::Sink;
use async_trait::async_trait;
use bytes::Bytes;
use canoe_config::EncryptConfig;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

/// Writes batches of JSON rows to Parquet files in an object store. Every
/// flush is its own immutable "run": rows land under
/// `<base_path>/dataset_id=<dataset_id>/run_id=<run_id>/`, Hive-style
/// partitioned by `partition_by` columns within that run directory (e.g.
/// `.../run_id=<run_id>/dt=2026-09-09/part-*.parquet`). `run_id` is a
/// [ULID](https://github.com/ulid/spec) — lexically sortable by
/// generation time, so listing a dataset's `run_id=*` directories in
/// plain alphabetical order already shows them oldest-to-newest.
///
/// Each run directory also gets its own `schema.avsc` (written by
/// `crate::run_writer::write_run`, shared with every other node type that
/// writes runs) — a full per-run schema history, not just a snapshot of
/// the most recent write, so schema drift across a dataset's lifetime (or
/// between two different steps/sinks) is a plain file diff away.
///
/// Every flush also (re)writes `latest.json` directly under
/// `dataset_id=<dataset_id>/`: `{"run_id", "path", "written_at",
/// "row_count"}` for the run that flush just wrote, so a reader who just
/// wants "the current data" doesn't need to list `run_id=*` directories
/// and sort them itself.
///
/// A sink with a `checkpoint` block additionally gets a compacted,
/// deduplicated `checkpoint=<ulid>/` view folded from this run history —
/// see `crate::checkpoint::Checkpointer`, run by the coordinator after a
/// successful flush, not by `ParquetSink` itself.
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    /// Human-readable form of `base_path` (scheme + bucket + prefix, as
    /// given in config) — kept alongside the object_store `Path` (which
    /// only knows the key within a bucket) so `latest.json` can report a
    /// full `s3://...` URI a person can paste elsewhere.
    base_uri: String,
    base_path: StorePath,
    dataset_id: String,
    partition_by: Vec<String>,
    /// This sink's own `encrypt { fields = [...]; key = "..." }` blocks —
    /// applied to every flush by `write_run`, before schema inference and
    /// before writing. See `canoe_config::EncryptConfig`'s docs.
    encrypt: Vec<EncryptConfig>,
    /// This sink's own compiled `metadata = "..."` column metadata —
    /// embedded into every flush's `schema.avsc` by `write_run`. See
    /// `canoe_config::SinkStep::column_metadata`'s docs.
    column_metadata: BTreeMap<String, BTreeMap<String, Value>>,
}

impl ParquetSink {
    pub fn new(
        base_path_uri: &str,
        dataset_id: String,
        partition_by: Vec<String>,
        encrypt: Vec<EncryptConfig>,
        column_metadata: BTreeMap<String, BTreeMap<String, Value>>,
    ) -> anyhow::Result<Self> {
        let url = Url::parse(base_path_uri)
            .map_err(|e| anyhow::anyhow!("invalid sink base_path '{base_path_uri}': {e}"))?;
        let (store, base_path) = parse_url(&url)?;
        Ok(Self {
            store: Arc::from(store),
            base_uri: base_path_uri.trim_end_matches('/').to_string(),
            base_path,
            dataset_id,
            partition_by,
            encrypt,
            column_metadata,
        })
    }

    fn dataset_path(&self) -> StorePath {
        self.base_path.child(format!("dataset_id={}", self.dataset_id).as_str())
    }
}

#[async_trait]
impl Sink for ParquetSink {
    async fn write_batch(&self, rows: &[Value]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let dataset_path = self.dataset_path();
        let result = write_run(
            self.store.as_ref(),
            &dataset_path,
            &self.dataset_id,
            rows,
            &self.partition_by,
            &self.encrypt,
            &self.column_metadata,
        )
        .await?;

        let latest = json!({
            "run_id": result.run_id,
            "path": format!("{}/dataset_id={}/run_id={}/", self.base_uri, self.dataset_id, result.run_id),
            "written_at": chrono::Utc::now().to_rfc3339(),
            "row_count": result.row_count,
        });
        self.store
            .put(
                &dataset_path.child("latest.json"),
                Bytes::from(serde_json::to_vec_pretty(&latest)?).into(),
            )
            .await?;

        Ok(())
    }

    fn name(&self) -> &str {
        "parquet"
    }
}
