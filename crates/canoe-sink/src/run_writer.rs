//! Writes one immutable "run" of rows to an object store: Hive-partitioned
//! Parquet part files, plus a `schema.avsc` **inside that same run
//! directory** — so a dataset's schema history is fully preserved (one
//! snapshot per run, diffable against any other run of the same node, or
//! against a *different* node's run for step-to-step comparison), not
//! just the shape of the most recent write. `ParquetSink` (`parquet_sink.rs`)
//! is the first caller; the S3-signal-driven step/split/dynamic_step
//! runtime (see README.md's "S3 + signal-driven pipeline transport")
//! calls this same function so every kind of node gets identical,
//! comparable output layout.

use crate::avro::arrow_schema_to_avsc;
use crate::crypto::encrypt_rows;
use crate::parquet_encode::{infer_schema, write_partitioned};
use bytes::Bytes;
use canoe_config::EncryptConfig;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct RunWriteResult {
    pub run_id: String,
    pub run_path: StorePath,
    pub row_count: usize,
}

/// Writes `rows` as one new run under `dataset_path`, Hive-partitioned by
/// `partition_by` within the run directory — `dataset_path.child("run_id=<ulid>")`.
/// `record_name` is used only for the Avro schema's own `name` field (see
/// `arrow_schema_to_avsc`); callers typically pass their dataset/step name.
///
/// Does **not** write a `latest.json` pointer — callers that want one
/// (today: `ParquetSink`) write it themselves, since what "latest" means
/// and where it lives is caller-specific (a sink's dataset root today; a
/// step's own output root once the S3-signal runtime lands).
///
/// `encrypt` (a node's own `EncryptConfig` list, usually empty) is
/// applied to `rows` *before* schema inference and before writing —
/// see `crate::crypto`'s module docs for the wire format. An encrypted
/// field always ends up typed as a plain string in the inferred schema,
/// same as any other string field, regardless of its original type.
///
/// `column_metadata` (a sink's own `SinkStep.column_metadata`, usually
/// empty) is embedded into `schema.avsc` as a `canoe.metadata` custom
/// property per matching field — independent of `encrypt`: a field can be
/// both encrypted and have descriptive metadata (e.g. `sensitivity` on an
/// encrypted `email` field is exactly the intended use), one doesn't
/// gate the other.
pub async fn write_run(
    store: &dyn ObjectStore,
    dataset_path: &StorePath,
    record_name: &str,
    rows: &[Value],
    partition_by: &[String],
    encrypt: &[EncryptConfig],
    column_metadata: &BTreeMap<String, BTreeMap<String, Value>>,
) -> anyhow::Result<RunWriteResult> {
    let rows = encrypt_rows(rows, encrypt)?;
    let rows = rows.as_ref();

    // One schema for the whole run: used to write schema.avsc, and
    // (independently, inside write_partitioned) to encode every
    // partition's Parquet file — inferred twice from the same rows rather
    // than threaded through, to keep write_partitioned's signature
    // reusable for already-merged rows (Checkpointer, ScdView) too.
    let schema = Arc::new(infer_schema(rows)?);

    let run_id = ulid::Ulid::generate().to_string();
    let run_path = dataset_path.child(format!("run_id={run_id}").as_str());

    write_partitioned(store, &run_path, rows, partition_by).await?;

    let avsc = arrow_schema_to_avsc(&schema, record_name, column_metadata);
    store
        .put(
            &run_path.child("schema.avsc"),
            Bytes::from(serde_json::to_vec_pretty(&avsc)?).into(),
        )
        .await?;

    Ok(RunWriteResult {
        run_id,
        run_path,
        row_count: rows.len(),
    })
}

/// Lists `run_id=*` directories directly under `dataset_path` (one level
/// down, not recursive), sorted so ULIDs come out oldest-first — the same
/// listing `crate::checkpoint::Checkpointer::list_run_ids` does, pulled
/// out here so `canoe-k8s`'s `S3BacklogInspector` (the coordinator-side
/// "how many unprocessed runs does this step have" check that replaces
/// Kafka consumer lag for S3-transport steps) can reuse it without
/// depending on `Checkpointer`'s own dedup/fold machinery.
pub async fn list_run_ids(store: &dyn ObjectStore, dataset_path: &StorePath) -> anyhow::Result<Vec<String>> {
    let listing = store.list_with_delimiter(Some(dataset_path)).await?;
    let mut run_ids: Vec<String> = listing
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().and_then(|f| f.strip_prefix("run_id=")).map(str::to_string))
        .collect();
    run_ids.sort();
    Ok(run_ids)
}
