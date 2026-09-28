//! Phase 1 verification: `S3BacklogInspector::backlog_for` correctly
//! counts unprocessed `run_id=*` directories against a checkpoint,
//! against a `file://`-backed fixture standing in for S3 — same pattern
//! `crates/canoe-k8s/src/autoscaler.rs`'s existing pure-function tests use
//! for `desired_replicas`, just exercised through the real object-store
//! listing path this time.

use canoe_k8s::S3BacklogInspector;
use object_store::path::Path as StorePath;
use object_store::{parse_url, ObjectStore};
use serde_json::json;
use url::Url;

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("canoe-k8s-backlog-test-{name}-{}", std::process::id()))
}

async fn seed_run(dataset_uri: &str, rows: &[serde_json::Value]) -> String {
    let url = Url::parse(dataset_uri).unwrap();
    let (store, dataset_path): (Box<dyn ObjectStore>, StorePath) = parse_url(&url).unwrap();
    let result = canoe_sink::write_run(store.as_ref(), &dataset_path, "test", rows, &[], &[], &Default::default())
        .await
        .unwrap();
    result.run_id
}

async fn write_checkpoint(checkpoint_uri: &str, through_run_id: &str) {
    let url = Url::parse(checkpoint_uri).unwrap();
    let (store, path): (Box<dyn ObjectStore>, StorePath) = parse_url(&url).unwrap();
    let payload = json!({ "through_run_id": through_run_id });
    store.put(&path, bytes::Bytes::from(serde_json::to_vec(&payload).unwrap()).into()).await.unwrap();
}

#[tokio::test]
async fn counts_every_run_as_unprocessed_when_there_is_no_checkpoint_yet() {
    let dir = temp_dir("no-checkpoint");
    std::fs::create_dir_all(&dir).unwrap();
    let dataset_uri = format!("file://{}", dir.display());

    seed_run(&dataset_uri, &[json!({"id": 1})]).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    seed_run(&dataset_uri, &[json!({"id": 2})]).await;

    let inspector = S3BacklogInspector::new(&dataset_uri).unwrap();
    let checkpoint_uri = format!("{dataset_uri}/.checkpoint.json");
    let backlog = inspector.backlog_for(&dataset_uri, &checkpoint_uri).await.unwrap();
    assert_eq!(backlog.unprocessed_run_count, 2);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn only_counts_runs_newer_than_the_checkpoint() {
    let dir = temp_dir("with-checkpoint");
    std::fs::create_dir_all(&dir).unwrap();
    let dataset_uri = format!("file://{}", dir.display());

    let first_run_id = seed_run(&dataset_uri, &[json!({"id": 1})]).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    seed_run(&dataset_uri, &[json!({"id": 2})]).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    seed_run(&dataset_uri, &[json!({"id": 3})]).await;

    let checkpoint_uri = format!("{dataset_uri}/.checkpoint.json");
    write_checkpoint(&checkpoint_uri, &first_run_id).await;

    let inspector = S3BacklogInspector::new(&dataset_uri).unwrap();
    let backlog = inspector.backlog_for(&dataset_uri, &checkpoint_uri).await.unwrap();
    assert_eq!(backlog.unprocessed_run_count, 2, "only the two runs after the checkpointed one should count");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn reports_zero_backlog_when_the_checkpoint_is_fully_caught_up() {
    let dir = temp_dir("caught-up");
    std::fs::create_dir_all(&dir).unwrap();
    let dataset_uri = format!("file://{}", dir.display());

    let run_id = seed_run(&dataset_uri, &[json!({"id": 1})]).await;
    let checkpoint_uri = format!("{dataset_uri}/.checkpoint.json");
    write_checkpoint(&checkpoint_uri, &run_id).await;

    let inspector = S3BacklogInspector::new(&dataset_uri).unwrap();
    let backlog = inspector.backlog_for(&dataset_uri, &checkpoint_uri).await.unwrap();
    assert_eq!(backlog.unprocessed_run_count, 0);

    std::fs::remove_dir_all(&dir).ok();
}
