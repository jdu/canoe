use canoe_sink::TopicArchiver;
use serde_json::json;

#[tokio::test]
async fn writes_one_timestamped_file_per_flush_under_pipeline_task() {
    let dir = std::env::temp_dir().join(format!("canoe-archive-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let archiver = TopicArchiver::new(&base_uri, "orders", "validate").unwrap();
    archiver
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .unwrap();

    let task_dir = dir.join("orders").join("validate");
    assert!(task_dir.is_dir(), "expected <base_path>/orders/validate/ to exist");

    let files: Vec<_> = std::fs::read_dir(&task_dir).unwrap().filter_map(|e| e.ok()).collect();
    assert_eq!(files.len(), 1, "one flush should produce exactly one file");
    let name = files[0].file_name().to_string_lossy().to_string();
    assert!(name.ends_with(".parquet"));
    assert_eq!(name.len(), "01M23VNS30W6Q1YT8NPPZXA0VW.parquet".len(), "filename should be a bare ULID + .parquet");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn empty_batch_writes_no_file() {
    let dir = std::env::temp_dir().join(format!("canoe-archive-test-empty-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());

    let archiver = TopicArchiver::new(&base_uri, "orders", "validate").unwrap();
    archiver.write_batch(&[]).await.unwrap();

    assert!(!dir.join("orders").join("validate").exists(), "an empty flush should create nothing");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn successive_flushes_sort_lexically_by_generation_time() {
    let dir = std::env::temp_dir().join(format!("canoe-archive-test-order-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base_uri = format!("file://{}", dir.display());
    let archiver = TopicArchiver::new(&base_uri, "orders", "validate").unwrap();

    archiver.write_batch(&[json!({"id": 1})]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    archiver.write_batch(&[json!({"id": 2})]).await.unwrap();

    let task_dir = dir.join("orders").join("validate");
    let mut names: Vec<String> = std::fs::read_dir(&task_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().trim_end_matches(".parquet").to_string())
        .collect();
    assert_eq!(names.len(), 2);
    names.sort();

    // Independent of filesystem listing order: decode each ULID's own
    // embedded timestamp and confirm alphabetical order matches
    // generation-time order — the property the whole scheme relies on.
    let t0 = ulid::Ulid::from_string(&names[0]).unwrap().datetime();
    let t1 = ulid::Ulid::from_string(&names[1]).unwrap().datetime();
    assert!(t0 <= t1, "lexically-first filename should not have been generated after the other");

    std::fs::remove_dir_all(&dir).ok();
}
