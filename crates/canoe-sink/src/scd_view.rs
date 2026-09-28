//! Rolls a `ParquetSink` dataset's `run_id=*` write history up into a
//! slowly-changing-dimension (Type 2) view, keyed by a `scd { keys =
//! [...] }` block's fields — an `scd=<ulid>/` directory analogous to
//! `checkpoint.rs`'s `checkpoint=<ulid>/`, but preserving every version of
//! a row instead of collapsing to the latest one.
//!
//! **Not to be confused with** `SinkConfig::Scd`/`ScdSink` (`scd.rs`) —
//! that's a different, still-unimplemented sink *format* (`format =
//! "scd"`) that would write straight into an Iceberg table with its own
//! merge logic. This module instead post-processes a `format = "parquet"`
//! sink's own `run_id=*` history after the fact, the exact same relationship
//! `Checkpointer` has to `ParquetSink` — see that module's docs, which this
//! mirrors closely.
//!
//! Run by the coordinator (`crates/canoe-coordinator/src/sink_runner.rs`)
//! after a sink's successful flush, same non-blocking/concurrency-guarded
//! spawn as a checkpoint pass, just with its own independent "pass in
//! progress" guard — a checkpoint and an scd view for the same sink can
//! run concurrently without interfering (each only ever reads `run_id=*`
//! and writes its own `checkpoint=*`/`scd=*` prefix).
//!
//! # Algorithm
//!
//! 1. Read `latest_scd.json` (if any) for `through_run_id`.
//! 2. List `run_id=*` directories newer than `through_run_id` (or every
//!    run, on a first pass), oldest to newest — same incremental-input
//!    convention as `Checkpointer`.
//! 3. If there are no new runs, do nothing (`run` returns `Ok(None)`).
//! 4. Otherwise, start from the previous `scd=<ulid>/` output (if any),
//!    splitting it into already-closed rows (`_scd_is_current = false`,
//!    carried forward untouched — history is never rewritten) and each
//!    key's current row (`_scd_is_current = true`).
//! 5. Fold in each new run's rows, oldest to newest. For each row:
//!    - an unseen key becomes a brand new current version (`_scd_version
//!      = 1`);
//!    - a key whose tracked fields (see `track_fields`) differ from its
//!      existing current row closes that row out (`_scd_is_current =
//!      false`, `_scd_valid_to` set) and opens a new one
//!      (`_scd_version` incremented, `_scd_valid_from` set);
//!    - a key whose tracked fields are unchanged is a no-op — an
//!      identical row landing again doesn't create a new version.
//!
//!    "Now" (or `effective_field`'s value on the incoming row, if
//!    configured) becomes both the closed row's `_scd_valid_to` and the
//!    new row's `_scd_valid_from`, so a lookup "as of time T" never has a
//!    key with more than one matching version.
//! 6. Write closed rows + every key's (possibly still-open) current row to
//!    a new `scd=<ulid>/`, Hive-partitioned the same way as the sink's own
//!    `partition_by`, and update `latest_scd.json`.
//!
//! Every pass is a full, standalone materialization of the *entire*
//! accumulated history (not just what changed this pass) — same
//! "incremental input, non-incremental output" shape as `Checkpointer`.

use crate::parquet_encode::write_partitioned;
use crate::parquet_read::read_rows_under_prefix;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use url::Url;

/// The four columns every row gets, named with a leading underscore to
/// keep them visually distinct from real data columns — canoe doesn't
/// namespace or escape a genuine column that happens to collide with one
/// of these (see README.md's "Status / what's stubbed").
const VALID_FROM: &str = "_scd_valid_from";
const VALID_TO: &str = "_scd_valid_to";
const IS_CURRENT: &str = "_scd_is_current";
const VERSION: &str = "_scd_version";

pub struct ScdView {
    store: Arc<dyn ObjectStore>,
    base_uri: String,
    dataset_path: StorePath,
    keys: Vec<String>,
    track_fields: Vec<String>,
    effective_field: Option<String>,
    partition_by: Vec<String>,
}

pub struct ScdViewResult {
    pub scd_id: String,
    pub through_run_id: String,
    pub row_count: usize,
    pub current_row_count: usize,
}

#[derive(Deserialize)]
struct LatestScd {
    scd_id: String,
    through_run_id: String,
}

impl ScdView {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_path_uri: &str,
        dataset_id: &str,
        keys: Vec<String>,
        track_fields: Vec<String>,
        effective_field: Option<String>,
        partition_by: Vec<String>,
    ) -> anyhow::Result<Self> {
        let url = Url::parse(base_path_uri)
            .map_err(|e| anyhow::anyhow!("invalid sink base_path '{base_path_uri}': {e}"))?;
        let (store, base_path) = parse_url(&url)?;
        let dataset_path = base_path.child(format!("dataset_id={dataset_id}").as_str());
        Ok(Self {
            store: Arc::from(store),
            base_uri: base_path_uri.trim_end_matches('/').to_string(),
            dataset_path,
            keys,
            track_fields,
            effective_field,
            partition_by,
        })
    }

    /// Runs one incremental SCD pass. `Ok(None)` means nothing new landed
    /// since the last pass (or there's no run history yet at all) — a
    /// no-op, not an error.
    pub async fn run(&self) -> anyhow::Result<Option<ScdViewResult>> {
        let previous = self.read_latest_scd().await?;
        let all_run_ids = self.list_run_ids().await?;

        let new_run_ids: Vec<&String> = match &previous {
            Some(p) => all_run_ids.iter().filter(|r| r.as_str() > p.through_run_id.as_str()).collect(),
            None => all_run_ids.iter().collect(),
        };
        if new_run_ids.is_empty() {
            return Ok(None);
        }

        // Rows already closed out by a previous pass — carried forward
        // untouched, since history is never rewritten.
        let mut closed: Vec<Value> = Vec::new();
        // Each key's current (still-open) version, if any.
        let mut current_by_key: HashMap<String, Value> = HashMap::new();

        if let Some(p) = &previous {
            let prev_path = self.dataset_path.child(format!("scd={}", p.scd_id).as_str());
            for row in read_rows_under_prefix(self.store.as_ref(), &prev_path).await? {
                if row.get(IS_CURRENT) == Some(&Value::Bool(true)) {
                    current_by_key.insert(self.dedup_key(&row), row);
                } else {
                    closed.push(row);
                }
            }
        }

        let now = chrono::Utc::now().to_rfc3339();
        for run_id in &new_run_ids {
            let run_path = self.dataset_path.child(format!("run_id={run_id}").as_str());
            for row in read_rows_under_prefix(self.store.as_ref(), &run_path).await? {
                self.apply(row, &now, &mut closed, &mut current_by_key);
            }
        }

        let current_row_count = current_by_key.len();
        let mut rows: Vec<Value> = closed;
        rows.extend(current_by_key.into_values());
        let row_count = rows.len();

        let scd_id = ulid::Ulid::generate().to_string();
        let scd_path = self.dataset_path.child(format!("scd={scd_id}").as_str());
        write_partitioned(self.store.as_ref(), &scd_path, &rows, &self.partition_by).await?;

        // Safe to unwrap: new_run_ids is non-empty (checked above) and
        // list_run_ids already returns them sorted oldest to newest.
        let through_run_id = (*new_run_ids.last().unwrap()).clone();
        let latest = json!({
            "scd_id": scd_id,
            "path": format!("{}/{}/", self.base_uri, scd_path.as_ref()),
            "through_run_id": through_run_id,
            "written_at": now,
            "row_count": row_count,
            "current_row_count": current_row_count,
        });
        self.store
            .put(&self.dataset_path.child("latest_scd.json"), Bytes::from(serde_json::to_vec_pretty(&latest)?).into())
            .await?;

        Ok(Some(ScdViewResult { scd_id, through_run_id, row_count, current_row_count }))
    }

    /// Folds one incoming row into `closed`/`current_by_key` — the whole
    /// per-row decision described in the module docs' step 5.
    fn apply(&self, row: Value, now: &str, closed: &mut Vec<Value>, current_by_key: &mut HashMap<String, Value>) {
        let key = self.dedup_key(&row);
        let effective_ts = self
            .effective_field
            .as_ref()
            .and_then(|f| row.get(f))
            .and_then(|v| v.as_str())
            .unwrap_or(now)
            .to_string();

        match current_by_key.get(&key) {
            None => {
                let mut new_row = row;
                set_scd_fields(&mut new_row, &effective_ts, None, true, 1);
                current_by_key.insert(key, new_row);
            }
            Some(existing) => {
                if self.tracked_fields_changed(existing, &row) {
                    let existing_valid_from =
                        existing.get(VALID_FROM).and_then(|v| v.as_str()).unwrap_or(&effective_ts).to_string();
                    let existing_version = existing.get(VERSION).and_then(|v| v.as_u64()).unwrap_or(1);

                    let mut closed_row = existing.clone();
                    set_scd_fields(&mut closed_row, &existing_valid_from, Some(&effective_ts), false, existing_version);
                    closed.push(closed_row);

                    let mut new_row = row;
                    set_scd_fields(&mut new_row, &effective_ts, None, true, existing_version + 1);
                    current_by_key.insert(key, new_row);
                }
                // else: no tracked-field change, existing current row stands untouched.
            }
        }
    }

    /// `track_fields` set explicitly: compare just those. Left empty
    /// (the default): compare every field present on either side except
    /// the dedup `keys` and the `_scd_*` bookkeeping columns themselves.
    fn tracked_fields_changed(&self, existing: &Value, incoming: &Value) -> bool {
        if !self.track_fields.is_empty() {
            return self.track_fields.iter().any(|f| existing.get(f) != incoming.get(f));
        }
        let (Some(e), Some(i)) = (existing.as_object(), incoming.as_object()) else {
            return true; // malformed (non-object) row — treat as changed rather than silently no-op
        };
        let mut fields: HashSet<&String> = e.keys().chain(i.keys()).collect();
        for k in &self.keys {
            fields.remove(k);
        }
        for meta in [VALID_FROM, VALID_TO, IS_CURRENT, VERSION] {
            fields.remove(&meta.to_string());
        }
        fields.iter().any(|f| e.get(f.as_str()) != i.get(f.as_str()))
    }

    async fn read_latest_scd(&self) -> anyhow::Result<Option<LatestScd>> {
        let path = self.dataset_path.child("latest_scd.json");
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(serde_json::from_slice(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

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

fn set_scd_fields(row: &mut Value, valid_from: &str, valid_to: Option<&str>, is_current: bool, version: u64) {
    let Value::Object(map) = row else {
        return; // non-object row: leave it alone, same "assume rows are JSON objects" convention as Checkpointer
    };
    let map: &mut Map<String, Value> = map;
    map.insert(VALID_FROM.to_string(), Value::String(valid_from.to_string()));
    map.insert(VALID_TO.to_string(), valid_to.map(|v| Value::String(v.to_string())).unwrap_or(Value::Null));
    map.insert(IS_CURRENT.to_string(), Value::Bool(is_current));
    map.insert(VERSION.to_string(), Value::Number(version.into()));
}
