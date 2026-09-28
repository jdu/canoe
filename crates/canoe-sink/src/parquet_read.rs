//! Reading Parquet back out of an object store — the inverse of
//! `parquet_encode`. Nothing in canoe needed this before `Checkpointer`:
//! every other sink/archiver only ever writes. Used to reload a run's (or
//! a previous checkpoint's) rows so they can be folded into a new
//! compacted checkpoint.

use arrow::array::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

/// Reads every `.parquet` file found anywhere under `prefix` (recursing
/// through any Hive partition subdirectories, since `object_store`'s
/// `list` is already recursive) and returns their rows as JSON, in
/// whatever order the store happens to list the files — callers that
/// care about order (a `Checkpointer` folding runs oldest to newest) sort
/// at the *prefix* level (which run/checkpoint to read), not the row
/// level, so this doesn't need to guarantee anything about the order of
/// rows within one prefix.
pub async fn read_rows_under_prefix(store: &dyn ObjectStore, prefix: &StorePath) -> anyhow::Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut stream = store.list(Some(prefix));
    while let Some(meta) = stream.try_next().await? {
        if !meta.location.as_ref().ends_with(".parquet") {
            continue;
        }
        let bytes = store.get(&meta.location).await?.bytes().await?;
        rows.extend(decode_parquet_rows(bytes)?);
    }
    Ok(rows)
}

fn decode_parquet_rows(bytes: Bytes) -> anyhow::Result<Vec<Value>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch: RecordBatch = batch?;
        let mut buf = Vec::new();
        {
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            writer.finish()?;
        }
        for line in String::from_utf8(buf)?.lines() {
            rows.push(serde_json::from_str(line)?);
        }
    }
    Ok(rows)
}
