//! Shared JSON-rows-to-Parquet-bytes encoding, used by `ParquetSink` (the
//! pipeline sink), `TopicArchiver` (the debug/durability dump of a
//! chain's internal topics), and `Checkpointer` (the compacted,
//! deduplicated view of a sink's run history) so none of the three drift
//! on how a batch gets turned into Parquet files on disk.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Cursor;

/// Encodes `rows` as an in-memory Parquet file against a fixed `schema`
/// (rather than inferring one from just these rows), so multiple calls
/// from the same flush — e.g. one per Hive partition — can't disagree on
/// column shape just because a given partition's rows happen not to
/// exercise every field.
pub fn encode_parquet(schema: SchemaRef, rows: &[Value]) -> anyhow::Result<Vec<u8>> {
    let ndjson = rows.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");
    let mut decoder = ReaderBuilder::new(schema.clone()).build(Cursor::new(ndjson))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = decoder.next() {
        batches.push(batch?);
    }

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    Ok(buf)
}

/// Infers an Arrow schema from a batch of JSON rows — the same
/// schema-per-batch simplification `ParquetSink` and `TopicArchiver`
/// both make (see their module docs for the tradeoff).
pub fn infer_schema(rows: &[Value]) -> anyhow::Result<arrow::datatypes::Schema> {
    Ok(arrow::json::reader::infer_json_schema_from_iterator(
        rows.iter().map(|v| Ok::<_, arrow::error::ArrowError>(v.clone())),
    )?)
}

/// The Hive-style `col=value/col2=value2` path segment for one row, given
/// `partition_by` column names — empty string (write directly under
/// `prefix`, no subdirectory) if `partition_by` is empty.
pub fn partition_key(row: &Value, partition_by: &[String]) -> String {
    if partition_by.is_empty() {
        return String::new();
    }
    partition_by
        .iter()
        .map(|col| {
            let v = row
                .get(col)
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "__null__".to_string());
            format!("{col}={v}")
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Writes `rows` under `prefix`, Hive-partitioned by `partition_by` (one
/// `part-<uuid>.parquet` file per distinct partition value combination —
/// same layout `ParquetSink` writes a run's data in, and what
/// `Checkpointer` writes a compacted `checkpoint=<ulid>/` in). No-op on
/// an empty batch (never writes a zero-row file). Infers one Arrow
/// schema from the whole batch up front, so every partition's file
/// agrees on column shape even when a given partition's rows happen not
/// to exercise every field.
pub async fn write_partitioned(
    store: &dyn ObjectStore,
    prefix: &StorePath,
    rows: &[Value],
    partition_by: &[String],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let schema = std::sync::Arc::new(infer_schema(rows)?);

    let mut partitions: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for row in rows {
        partitions.entry(partition_key(row, partition_by)).or_default().push(row.clone());
    }

    for (partition, part_rows) in partitions {
        let bytes = encode_parquet(schema.clone(), &part_rows)?;
        let file_name = format!("part-{}.parquet", uuid::Uuid::new_v4());
        let path = if partition.is_empty() {
            prefix.child(file_name.as_str())
        } else {
            let mut p = prefix.clone();
            for segment in partition.split('/') {
                p = p.child(segment);
            }
            p.child(file_name.as_str())
        };
        store.put(&path, Bytes::from(bytes).into()).await?;
    }
    Ok(())
}
