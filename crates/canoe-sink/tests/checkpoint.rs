use arrow::array::RecordBatch;
use canoe_sink::{Checkpointer, ParquetSink, Sink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("canoe-checkpoint-test-{name}-{}", std::process::id()))
}

/// Reads every `.parquet` file under `dir` (recursing into any Hive
/// partition subdirectories) back into JSON rows — same
/// decode-via-Arrow-JSON-writer approach `canoe_sink::parquet_read` uses
/// internally, duplicated here since that module isn't public; this is
/// the test's own way of proving what `Checkpointer` actually wrote to
/// disk, independent of its own read path.
fn read_all_parquet_rows(dir: &std::path::Path) -> Vec<Value> {
    let mut rows = Vec::new();
    collect(dir, &mut rows);
    rows
}

fn collect(dir: &std::path::Path, out: &mut Vec<Value>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "parquet") {
            let bytes = std::fs::read(&path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch: RecordBatch = batch.unwrap();
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                }
                for line in String::from_utf8(buf).unwrap().lines() {
                    out.push(serde_json::from_str(line).unwrap());
                }
            }
        }
    }
}

fn by_key<'a>(rows: &'a [Value], key_field: &str, key_value: &str) -> &'a Value {
    rows.iter()
        .find(|r| r.get(key_field).and_then(|v| v.as_str()) == Some(key_value))
        .unwrap_or_else(|| panic!("no row with {key_field}={key_value}"))
}

#[tokio::test]
async fn first_pass_checkpoints_the_whole_history_and_dedupes_by_key() {
    let dir = temp_dir("first-pass");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    // Run 1: order 1 created.
    sink.write_batch(&[json!({"order_id": "1", "status": "created"})]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    // Run 2: order 1 updated, order 2 created.
    sink.write_batch(&[
        json!({"order_id": "1", "status": "shipped"}),
        json!({"order_id": "2", "status": "created"}),
    ])
    .await
    .unwrap();

    let checkpointer = Checkpointer::new(&base_uri, "orders", vec!["order_id".to_string()], vec![]).unwrap();
    let result = checkpointer.run().await.unwrap().expect("first pass should produce a checkpoint");
    assert_eq!(result.row_count, 2, "order 1's two versions should dedupe to one row");

    let checkpoint_dir = dir.join("dataset_id=orders").join(format!("checkpoint={}", result.checkpoint_id));
    let rows = read_all_parquet_rows(&checkpoint_dir);
    assert_eq!(rows.len(), 2);
    assert_eq!(by_key(&rows, "order_id", "1")["status"], "shipped", "later run must win for the same key");
    assert_eq!(by_key(&rows, "order_id", "2")["status"], "created");

    let latest: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("dataset_id=orders").join("latest_checkpoint.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(latest["checkpoint_id"], result.checkpoint_id);
    assert_eq!(latest["through_run_id"], result.through_run_id);
    assert_eq!(latest["row_count"], 2);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn second_pass_only_processes_runs_since_the_last_checkpoint() {
    let dir = temp_dir("incremental");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let checkpointer = Checkpointer::new(&base_uri, "orders", vec!["order_id".to_string()], vec![]).unwrap();

    sink.write_batch(&[json!({"order_id": "1", "status": "created"})]).await.unwrap();
    let first = checkpointer.run().await.unwrap().unwrap();
    assert_eq!(first.row_count, 1);

    // A run landing before the second checkpoint pass should be folded
    // in on top of the first checkpoint's own data, not replayed from
    // scratch (order 1's original row is only ever readable through the
    // first checkpoint's output, which the second pass reads instead of
    // the raw run again).
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"order_id": "2", "status": "created"})]).await.unwrap();
    let second = checkpointer.run().await.unwrap().unwrap();
    assert_eq!(second.row_count, 2, "second checkpoint must include both order 1 (from the base) and order 2 (new)");
    assert_ne!(second.checkpoint_id, first.checkpoint_id);

    let checkpoint_dir = dir.join("dataset_id=orders").join(format!("checkpoint={}", second.checkpoint_id));
    let rows = read_all_parquet_rows(&checkpoint_dir);
    assert_eq!(by_key(&rows, "order_id", "1")["status"], "created");
    assert_eq!(by_key(&rows, "order_id", "2")["status"], "created");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn no_new_runs_since_last_checkpoint_is_a_no_op() {
    let dir = temp_dir("noop");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let checkpointer = Checkpointer::new(&base_uri, "orders", vec!["order_id".to_string()], vec![]).unwrap();

    sink.write_batch(&[json!({"order_id": "1", "status": "created"})]).await.unwrap();
    let first = checkpointer.run().await.unwrap().unwrap();

    let second = checkpointer.run().await.unwrap();
    assert!(second.is_none(), "no new runs landed, so a second pass should do nothing");

    let latest: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("dataset_id=orders").join("latest_checkpoint.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(latest["checkpoint_id"], first.checkpoint_id);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn no_runs_at_all_is_a_no_op() {
    let dir = temp_dir("empty");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let checkpointer = Checkpointer::new(&base_uri, "orders", vec!["order_id".to_string()], vec![]).unwrap();

    let result = checkpointer.run().await.unwrap();
    assert!(result.is_none());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn dedup_key_can_be_composite() {
    let dir = temp_dir("composite-key");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let checkpointer = Checkpointer::new(
        &base_uri,
        "orders",
        vec!["order_id".to_string(), "line_no".to_string()],
        vec![],
    )
    .unwrap();

    sink.write_batch(&[
        json!({"order_id": "1", "line_no": 1, "qty": 2}),
        json!({"order_id": "1", "line_no": 2, "qty": 5}),
    ])
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"order_id": "1", "line_no": 1, "qty": 3})]).await.unwrap();

    let result = checkpointer.run().await.unwrap().unwrap();
    assert_eq!(result.row_count, 2, "(order 1, line 1) and (order 1, line 2) are distinct keys");

    let checkpoint_dir = dir.join("dataset_id=orders").join(format!("checkpoint={}", result.checkpoint_id));
    let rows = read_all_parquet_rows(&checkpoint_dir);
    let line1 = rows.iter().find(|r| r["line_no"] == 1).unwrap();
    let line2 = rows.iter().find(|r| r["line_no"] == 2).unwrap();
    assert_eq!(line1["qty"], 3, "line 1 should reflect the later run's update");
    assert_eq!(line2["qty"], 5, "line 2 was untouched by the later run");

    std::fs::remove_dir_all(&dir).ok();
}
