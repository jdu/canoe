use arrow::array::RecordBatch;
use canoe_sink::{ParquetSink, ScdView, Sink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("canoe-scd-view-test-{name}-{}", std::process::id()))
}

/// Same decode-via-Arrow-JSON-writer approach as
/// `tests/checkpoint.rs::read_all_parquet_rows` — duplicated for the same
/// reason (`parquet_read` isn't a public module).
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

fn versions_for<'a>(rows: &'a [Value], key_field: &str, key_value: &str) -> Vec<&'a Value> {
    let mut v: Vec<&Value> = rows.iter().filter(|r| r.get(key_field).and_then(|v| v.as_str()) == Some(key_value)).collect();
    v.sort_by_key(|r| r["_scd_version"].as_u64().unwrap());
    v
}

fn current<'a>(rows: &'a [Value], key_field: &str, key_value: &str) -> &'a Value {
    rows.iter()
        .find(|r| r.get(key_field).and_then(|v| v.as_str()) == Some(key_value) && r["_scd_is_current"] == true)
        .unwrap_or_else(|| panic!("no current row for {key_field}={key_value}"))
}

#[tokio::test]
async fn new_key_becomes_version_one_and_current() {
    let dir = temp_dir("new-key");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free"})]).await.unwrap();

    let scd = ScdView::new(&base_uri, "customers", vec!["customer_id".to_string()], vec![], None, vec![]).unwrap();
    let result = scd.run().await.unwrap().expect("first pass should produce an scd view");
    assert_eq!(result.row_count, 1);
    assert_eq!(result.current_row_count, 1);

    let scd_dir = dir.join("dataset_id=customers").join(format!("scd={}", result.scd_id));
    let rows = read_all_parquet_rows(&scd_dir);
    let row = current(&rows, "customer_id", "c1");
    assert_eq!(row["_scd_version"], 1);
    assert_eq!(row["_scd_is_current"], true);
    assert_eq!(row["_scd_valid_to"], Value::Null);
    assert!(row["_scd_valid_from"].is_string());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn unchanged_row_does_not_create_a_new_version() {
    let dir = temp_dir("unchanged");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let scd = ScdView::new(&base_uri, "customers", vec!["customer_id".to_string()], vec![], None, vec![]).unwrap();

    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free"})]).await.unwrap();
    let first = scd.run().await.unwrap().unwrap();
    assert_eq!(first.row_count, 1);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    // Same plan value lands again in a later run.
    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free"})]).await.unwrap();
    let second = scd.run().await.unwrap().unwrap();
    assert_eq!(second.row_count, 1, "identical row should not create a second version");
    assert_eq!(second.current_row_count, 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn changed_tracked_field_closes_old_version_and_opens_a_new_one() {
    let dir = temp_dir("changed");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let scd = ScdView::new(&base_uri, "customers", vec!["customer_id".to_string()], vec![], None, vec![]).unwrap();

    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free"})]).await.unwrap();
    scd.run().await.unwrap().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"customer_id": "c1", "plan": "pro"})]).await.unwrap();
    let result = scd.run().await.unwrap().unwrap();
    assert_eq!(result.row_count, 2, "old and new version both present");
    assert_eq!(result.current_row_count, 1);

    let scd_dir = dir.join("dataset_id=customers").join(format!("scd={}", result.scd_id));
    let rows = read_all_parquet_rows(&scd_dir);
    let versions = versions_for(&rows, "customer_id", "c1");
    assert_eq!(versions.len(), 2);

    let v1 = versions[0];
    assert_eq!(v1["plan"], "free");
    assert_eq!(v1["_scd_is_current"], false);
    assert!(v1["_scd_valid_to"].is_string(), "closed version must have a valid_to");

    let v2 = versions[1];
    assert_eq!(v2["plan"], "pro");
    assert_eq!(v2["_scd_is_current"], true);
    assert_eq!(v2["_scd_valid_to"], Value::Null);
    assert_eq!(v2["_scd_valid_from"], v1["_scd_valid_to"], "new version's valid_from must match the closed version's valid_to");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn second_pass_is_incremental_and_preserves_closed_history() {
    let dir = temp_dir("incremental");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let scd = ScdView::new(&base_uri, "customers", vec!["customer_id".to_string()], vec![], None, vec![]).unwrap();

    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free"})]).await.unwrap();
    let first = scd.run().await.unwrap().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"customer_id": "c1", "plan": "pro"})]).await.unwrap();
    let second = scd.run().await.unwrap().unwrap();
    assert_ne!(second.scd_id, first.scd_id);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"customer_id": "c2", "plan": "free"})]).await.unwrap();
    let third = scd.run().await.unwrap().unwrap();
    assert_eq!(third.row_count, 3, "c1's two historical versions plus c2's new one");
    assert_eq!(third.current_row_count, 2);

    let scd_dir = dir.join("dataset_id=customers").join(format!("scd={}", third.scd_id));
    let rows = read_all_parquet_rows(&scd_dir);
    assert_eq!(versions_for(&rows, "customer_id", "c1").len(), 2, "c1's history must survive across passes");
    assert_eq!(current(&rows, "customer_id", "c2")["plan"], "free");

    let no_op = scd.run().await.unwrap();
    assert!(no_op.is_none(), "no new runs since the third pass");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn track_fields_limits_which_changes_trigger_a_new_version() {
    let dir = temp_dir("track-fields");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    // Only `plan` is tracked — a change to `last_login` alone shouldn't
    // version the row.
    let scd = ScdView::new(
        &base_uri,
        "customers",
        vec!["customer_id".to_string()],
        vec!["plan".to_string()],
        None,
        vec![],
    )
    .unwrap();

    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free", "last_login": "2026-01-01"})]).await.unwrap();
    scd.run().await.unwrap().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free", "last_login": "2026-02-01"})]).await.unwrap();
    let result = scd.run().await.unwrap().unwrap();
    assert_eq!(result.current_row_count, 1, "last_login isn't tracked, so no new version");
    assert_eq!(result.row_count, 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn effective_field_drives_valid_from_instead_of_processing_time() {
    let dir = temp_dir("effective-field");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();
    let scd = ScdView::new(
        &base_uri,
        "customers",
        vec!["customer_id".to_string()],
        vec![],
        Some("changed_at".to_string()),
        vec![],
    )
    .unwrap();

    sink.write_batch(&[json!({"customer_id": "c1", "plan": "free", "changed_at": "2020-01-01T00:00:00Z"})])
        .await
        .unwrap();
    let first = scd.run().await.unwrap().unwrap();

    let scd_dir = dir.join("dataset_id=customers").join(format!("scd={}", first.scd_id));
    let rows = read_all_parquet_rows(&scd_dir);
    let row = current(&rows, "customer_id", "c1");
    assert_eq!(row["_scd_valid_from"], "2020-01-01T00:00:00Z", "should use the row's own effective_field, not wall-clock time");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn no_runs_at_all_is_a_no_op() {
    let dir = temp_dir("empty");
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let scd = ScdView::new(&base_uri, "customers", vec!["customer_id".to_string()], vec![], None, vec![]).unwrap();

    let result = scd.run().await.unwrap();
    assert!(result.is_none());

    std::fs::remove_dir_all(&dir).ok();
}
