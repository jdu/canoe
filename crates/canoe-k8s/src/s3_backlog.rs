//! S3 + signal transport's equivalent of `canoe_kafka::LagInspector`: an
//! S3-transport step (`PythonStep`/`UvStep`) has no Kafka consumer group
//! lag to read any more, so `Autoscaler` needs a different "how much
//! unprocessed work does this step have" signal to drive replica counts.
//! The answer is the same one `crate::checkpoint::Checkpointer` already
//! computes for compaction: list `run_id=*` directories under the step's
//! `input_path` newer than whatever `checkpoint_path` last recorded.

use canoe_sink::list_run_ids;
use object_store::parse_url;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use serde::Deserialize;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone, Copy, Default)]
pub struct Backlog {
    /// How many `run_id=*` directories under `input_path` are newer than
    /// `checkpoint_path`'s last recorded position — the S3-transport
    /// equivalent of `canoe_kafka::ConsumerLag::total_lag`, and fed into
    /// `desired_replicas` the same way.
    pub unprocessed_run_count: i64,
}

/// Read by any step's `CANOE_CHECKPOINT_PATH` file — see
/// `PythonStep::input_signal`'s docs and `pod_manager.rs`'s
/// `CANOE_CHECKPOINT_PATH` env var. Deliberately a small, worker-owned
/// file (not `crate::checkpoint::Checkpointer`'s compaction-view
/// `latest_checkpoint.json`, a different concept entirely) recording only
/// "how far this one consumer has gotten," written by the Python/uv
/// worker itself after each successful flush.
#[derive(Debug, Deserialize)]
struct WorkerCheckpoint {
    through_run_id: Option<String>,
}

pub struct S3BacklogInspector {
    store: Arc<dyn ObjectStore>,
}

impl S3BacklogInspector {
    /// `input_path`/`checkpoint_path` are full `s3://...`-style URIs; only
    /// used here to resolve which concrete `ObjectStore` implementation to
    /// talk to (matching `object_store::parse_url`'s own scheme-based
    /// dispatch, same as `ParquetSink::new`) — every call after
    /// construction takes the already-parsed store-relative path.
    pub fn new(any_uri: &str) -> anyhow::Result<Self> {
        let url = Url::parse(any_uri).map_err(|e| anyhow::anyhow!("invalid S3 URI '{any_uri}': {e}"))?;
        let (store, _) = parse_url(&url)?;
        Ok(Self { store: Arc::from(store) })
    }

    /// `input_path`/`checkpoint_path` are full `s3://...`-style URIs (the
    /// step's resolved `input_path`, and `input_path`'s own
    /// `.checkpoint.json` — see `pod_manager.rs`'s `CANOE_CHECKPOINT_PATH`,
    /// which is derived from a step's *output* path since a step's
    /// checkpoint tracks its own consumption progress, stored alongside
    /// its own output rather than the producer's).
    pub async fn backlog_for(&self, input_path: &str, checkpoint_path: &str) -> anyhow::Result<Backlog> {
        let input_url =
            Url::parse(input_path).map_err(|e| anyhow::anyhow!("invalid input_path '{input_path}': {e}"))?;
        let (_, dataset_path) = parse_url(&input_url)?;

        let all_run_ids = list_run_ids(self.store.as_ref(), &dataset_path).await?;

        let through_run_id = self.read_checkpoint(checkpoint_path).await?;
        let unprocessed = match through_run_id {
            Some(through) => all_run_ids.iter().filter(|id| id.as_str() > through.as_str()).count(),
            None => all_run_ids.len(),
        };

        Ok(Backlog {
            unprocessed_run_count: unprocessed as i64,
        })
    }

    async fn read_checkpoint(&self, checkpoint_path: &str) -> anyhow::Result<Option<String>> {
        let checkpoint_url =
            Url::parse(checkpoint_path).map_err(|e| anyhow::anyhow!("invalid checkpoint_path '{checkpoint_path}': {e}"))?;
        let (_, path): (Box<dyn ObjectStore>, StorePath) = parse_url(&checkpoint_url)?;
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let parsed: WorkerCheckpoint = serde_json::from_slice(&bytes)?;
                Ok(parsed.through_run_id)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
