//! Compacts a `ParquetSink` dataset's `run_id=*` write history into one
//! deduplicated `checkpoint=<ulid>/` view, keyed by a `checkpoint { keys
//! = [...] }` block's fields.
//!
//! Run by the coordinator (`crates/canoe-coordinator/src/sink_runner.rs`)
//! after a sink's successful flush — see that module for why it's
//! spawned rather than awaited inline (a checkpoint pass shouldn't add
//! latency to the sink's own write path) and how concurrent passes for
//! the same sink are prevented.
//!
//! # Algorithm
//!
//! 1. Read `latest_checkpoint.json` (if any) for `through_run_id` — the
//!    newest run already folded into the last checkpoint.
//! 2. List `run_id=*` directories newer than `through_run_id` (or *every*
//!    run, if there's no previous checkpoint), sorted oldest to newest —
//!    this is what makes a checkpoint incremental: a second pass only
//!    ever re-reads the runs that landed since the first, not the whole
//!    history again.
//! 3. If there are no new runs, do nothing (`run` returns `Ok(None)`).
//! 4. Otherwise, start from the previous checkpoint's own rows (if any)
//!    as the base, then fold in each new run's rows in order, inserting
//!    into a map keyed by `keys`' values — a row overwrites whatever
//!    previously occupied the same key, so the newest run to touch a
//!    given key always wins, exactly like the last-write-wins semantics
//!    a real changelog compaction (Kafka's own topic compaction, a
//!    Delta/Iceberg `MERGE`) provides, just computed from scratch each
//!    pass rather than maintained incrementally row by row.
//! 5. Write the merged rows to a new `checkpoint=<ulid>/`, Hive-partitioned
//!    the same way the sink's own `partition_by` is, and update
//!    `latest_checkpoint.json` to point at it.
//!
//! Every checkpoint is a full, standalone materialization (never deletes
//! or depends on reading an older `checkpoint=*` after this pass) — the
//! *reading* of the previous checkpoint's rows is incremental input, the
//! *output* is not itself an incremental diff.

use crate::parquet_encode::write_partitioned;
use crate::parquet_read::read_rows_under_prefix;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub struct Checkpointer {
    store: Arc<dyn ObjectStore>,
    base_uri: String,
    dataset_path: StorePath,
    keys: Vec<String>,
    partition_by: Vec<String>,
}

pub struct CheckpointResult {
    pub checkpoint_id: String,
    pub through_run_id: String,
    pub row_count: usize,
}

#[derive(Deserialize)]
struct LatestCheckpoint {
    checkpoint_id: String,
    through_run_id: String,
}

impl Checkpointer {
    pub fn new(base_path_uri: &str, dataset_id: &str, keys: Vec<String>, partition_by: Vec<String>) -> anyhow::Result<Self> {
        let url = Url::parse(base_path_uri)
            .map_err(|e| anyhow::anyhow!("invalid sink base_path '{base_path_uri}': {e}"))?;
        let (store, base_path) = parse_url(&url)?;
        let dataset_path = base_path.child(format!("dataset_id={dataset_id}").as_str());
        Ok(Self {
            store: Arc::from(store),
            base_uri: base_path_uri.trim_end_matches('/').to_string(),
            dataset_path,
            keys,
            partition_by,
        })
    }

    /// Runs one incremental checkpoint pass. `Ok(None)` means there was
    /// nothing new to fold in since the last checkpoint (or, if this is
    /// the first ever pass and no runs exist yet, nothing to checkpoint
    /// at all) — not an error, just a no-op worth distinguishing from an
    /// actual checkpoint for logging.
    pub async fn run(&self) -> anyhow::Result<Option<CheckpointResult>> {
        let previous = self.read_latest_checkpoint().await?;
        let all_run_ids = self.list_run_ids().await?;

        let new_run_ids: Vec<&String> = match &previous {
            Some(p) => all_run_ids.iter().filter(|r| r.as_str() > p.through_run_id.as_str()).collect(),
            None => all_run_ids.iter().collect(),
        };
        if new_run_ids.is_empty() {
            return Ok(None);
        }

        // Keyed by this checkpoint's dedup key, insertion order is fold
        // order: the previous checkpoint's own rows first (the base),
        // then each new run oldest to newest — a later insert for the
        // same key always overwrites the earlier one.
        let mut merged: HashMap<String, Value> = HashMap::new();

        if let Some(p) = &previous {
            let prev_path = self.dataset_path.child(format!("checkpoint={}", p.checkpoint_id).as_str());
            for row in read_rows_under_prefix(self.store.as_ref(), &prev_path).await? {
                merged.insert(self.dedup_key(&row), row);
            }
        }
        for run_id in &new_run_ids {
            let run_path = self.dataset_path.child(format!("run_id={run_id}").as_str());
            for row in read_rows_under_prefix(self.store.as_ref(), &run_path).await? {
                merged.insert(self.dedup_key(&row), row);
            }
        }

        let rows: Vec<Value> = merged.into_values().collect();
        let checkpoint_id = ulid::Ulid::generate().to_string();
        let checkpoint_path = self.dataset_path.child(format!("checkpoint={checkpoint_id}").as_str());
        write_partitioned(self.store.as_ref(), &checkpoint_path, &rows, &self.partition_by).await?;

        // Safe to unwrap: new_run_ids came from all_run_ids, which
        // list_run_ids already returns sorted (ULIDs sort lexically by
        // generation time), and we just checked it's non-empty above.
        let through_run_id = (*new_run_ids.last().unwrap()).clone();
        let row_count = rows.len();
        let latest = json!({
            "checkpoint_id": checkpoint_id,
            "path": format!("{}/{}/", self.base_uri, checkpoint_path.as_ref()),
            "through_run_id": through_run_id,
            "written_at": chrono::Utc::now().to_rfc3339(),
            "row_count": row_count,
        });
        self.store
            .put(
                &self.dataset_path.child("latest_checkpoint.json"),
                Bytes::from(serde_json::to_vec_pretty(&latest)?).into(),
            )
            .await?;

        Ok(Some(CheckpointResult { checkpoint_id, through_run_id, row_count }))
    }

    async fn read_latest_checkpoint(&self) -> anyhow::Result<Option<LatestCheckpoint>> {
        let path = self.dataset_path.child("latest_checkpoint.json");
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(serde_json::from_slice(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// One level down from `dataset_path` (not recursive — `run_id=*`
    /// directories, not the parquet files inside them), sorted so ULIDs
    /// come out oldest-first.
    async fn list_run_ids(&self) -> anyhow::Result<Vec<String>> {
        let listing = self.store.list_with_delimiter(Some(&self.dataset_path)).await?;
        let mut run_ids: Vec<String> = listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().and_then(|f| f.strip_prefix("run_id=")).map(str::to_string))
            .collect();
        run_ids.sort();
        Ok(run_ids)
    }

    fn dedup_key(&self, row: &Value) -> String {
        self.keys
            .iter()
            .map(|k| {
                row.get(k)
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\u{1}")
    }
}
