use canoe_sink::ParquetSink;
use canoe_sink::Sink;
use serde_json::json;

#[tokio::test]
async fn writes_run_scoped_layout_with_schema_and_latest_pointer() {
    let dir = std::env::temp_dir().join(format!("canoe-sink-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec!["region".to_string()], vec![], std::collections::BTreeMap::new()).unwrap();

    let rows = vec![
        json!({"id": 1, "amount": 9.5, "region": "US"}),
        json!({"id": 2, "amount": 3.0, "region": "EU"}),
    ];
    sink.write_batch(&rows).await.unwrap();

    let dataset_dir = dir.join("dataset_id=orders");
    assert!(dataset_dir.is_dir(), "dataset_id=orders directory should exist");

    // latest.json lives directly under dataset_id=; schema.avsc now lives
    // *inside* the run_id= directory it describes, not at the dataset root.
    assert!(!dataset_dir.join("schema.avsc").is_file(), "schema.avsc must not live at the dataset root any more");

    let latest_path = dataset_dir.join("latest.json");
    assert!(latest_path.is_file());
    let latest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&latest_path).unwrap()).unwrap();
    let run_id = latest["run_id"].as_str().unwrap().to_string();
    assert_eq!(latest["row_count"], 2);
    assert!(latest["path"].as_str().unwrap().contains(&format!("run_id={run_id}")));

    // Exactly one run_id= directory should exist, matching latest.json,
    // partitioned by region underneath it, with its own schema.avsc.
    let run_dir = dataset_dir.join(format!("run_id={run_id}"));
    assert!(run_dir.is_dir(), "run_id directory named in latest.json should exist");
    assert!(run_dir.join("region=US").is_dir());
    assert!(run_dir.join("region=EU").is_dir());

    let avsc_path = run_dir.join("schema.avsc");
    assert!(avsc_path.is_file(), "schema.avsc should exist inside the run directory at {avsc_path:?}");
    let avsc: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&avsc_path).unwrap()).unwrap();
    assert_eq!(avsc["type"], "record");
    let field_names: Vec<&str> = avsc["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"amount"));

    let run_id_dirs: Vec<_> = std::fs::read_dir(&dataset_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("run_id="))
        .collect();
    assert_eq!(run_id_dirs.len(), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn column_metadata_is_embedded_in_the_written_schema_avsc() {
    let dir = std::env::temp_dir().join(format!("canoe-sink-test-metadata-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let mut email_meta = std::collections::BTreeMap::new();
    email_meta.insert("sensitivity".to_string(), json!("restricted"));
    email_meta.insert("label".to_string(), json!("Email Address"));
    let mut column_metadata = std::collections::BTreeMap::new();
    column_metadata.insert("email".to_string(), email_meta);

    let sink = ParquetSink::new(&base_uri, "customers".to_string(), vec![], vec![], column_metadata).unwrap();
    sink.write_batch(&[json!({"id": 1, "email": "a@example.com"})]).await.unwrap();

    let dataset_dir = dir.join("dataset_id=customers");
    let latest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dataset_dir.join("latest.json")).unwrap()).unwrap();
    let run_id = latest["run_id"].as_str().unwrap();
    let avsc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dataset_dir.join(format!("run_id={run_id}")).join("schema.avsc")).unwrap(),
    )
    .unwrap();

    let email_field = avsc["fields"].as_array().unwrap().iter().find(|f| f["name"] == "email").unwrap();
    assert_eq!(email_field["canoe.metadata"]["sensitivity"], "restricted");
    assert_eq!(email_field["canoe.metadata"]["label"], "Email Address");

    let id_field = avsc["fields"].as_array().unwrap().iter().find(|f| f["name"] == "id").unwrap();
    assert!(id_field.get("canoe.metadata").is_none(), "id has no declared metadata, should be untouched");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn second_flush_creates_a_new_lexically_later_run_id() {
    let dir = std::env::temp_dir().join(format!("canoe-sink-test2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let sink = ParquetSink::new(&base_uri, "orders".to_string(), vec![], vec![], std::collections::BTreeMap::new()).unwrap();

    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    sink.write_batch(&[json!({"id": 2, "region": "EU"})]).await.unwrap();

    let dataset_dir = dir.join("dataset_id=orders");
    let mut run_ids: Vec<String> = std::fs::read_dir(&dataset_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_string_lossy().strip_prefix("run_id=").map(str::to_string))
        .collect();
    run_ids.sort();
    assert_eq!(run_ids.len(), 2, "each flush should get its own run_id directory");

    let latest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dataset_dir.join("latest.json")).unwrap()).unwrap();
    assert_eq!(latest["run_id"], run_ids[1], "latest.json should point at the most recent (lexically greatest) run_id");

    // Each run keeps its own schema.avsc, independently reflecting that
    // run's own rows — the second run's schema (which has an extra
    // "region" field the first run's rows didn't have) doesn't overwrite
    // or get merged with the first run's, proving history is preserved
    // per run rather than collapsed to "most recent" the way it used to be.
    let schema_1: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dataset_dir.join(format!("run_id={}", run_ids[0])).join("schema.avsc")).unwrap(),
    )
    .unwrap();
    let schema_2: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dataset_dir.join(format!("run_id={}", run_ids[1])).join("schema.avsc")).unwrap(),
    )
    .unwrap();
    let fields_of = |s: &serde_json::Value| -> Vec<String> {
        s["fields"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap().to_string()).collect()
    };
    assert!(!fields_of(&schema_1).contains(&"region".to_string()), "first run had no region field");
    assert!(fields_of(&schema_2).contains(&"region".to_string()), "second run's own schema reflects its own rows");

    std::fs::remove_dir_all(&dir).ok();
}
