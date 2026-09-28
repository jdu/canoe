//! Datalake sinks: the terminal step of a chain. Rows arrive as JSON
//! records (the wire format used on all canoe Kafka topics) and are
//! written to durable storage.
//!
//! `ParquetSink` is the only fully implemented sink today. `IcebergSink`
//! and `ScdSink` are stubs that establish the trait boundary so they can
//! be dropped in later without touching the coordinator.

mod archive;
mod avro;
mod checkpoint;
mod crypto;
mod iceberg;
mod parquet_encode;
mod parquet_read;
mod parquet_sink;
mod run_writer;
mod scd;
mod scd_view;

pub use archive::TopicArchiver;
pub use avro::arrow_schema_to_avsc;
pub use checkpoint::{CheckpointResult, Checkpointer};
pub use crypto::{encrypt_field, encrypt_row, env_var_name_for_key, resolve_key_bytes, ENVELOPE_PREFIX};
pub use iceberg::IcebergSink;
pub use parquet_encode::{infer_schema, partition_key, write_partitioned};
pub use parquet_read::read_rows_under_prefix;
pub use parquet_sink::ParquetSink;
pub use run_writer::{list_run_ids, write_run, RunWriteResult};
pub use scd::ScdSink;
pub use scd_view::{ScdView, ScdViewResult};

use async_trait::async_trait;
use canoe_config::{EncryptConfig, SinkConfig};
use serde_json::Value;
use std::sync::Arc;

#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of JSON rows to the sink's target. Called by the
    /// coordinator's sink-runner once a step's buffer reaches
    /// `batch_size` rows or `flush_interval` elapses, whichever first.
    async fn write_batch(&self, rows: &[Value]) -> anyhow::Result<()>;

    fn name(&self) -> &str;
}

/// Build the concrete Sink implementation for a step's `SinkConfig`.
/// `encrypt`/`column_metadata` are the owning `SinkStep`'s own fields
/// (usually empty) — only meaningful for `SinkConfig::Parquet` today;
/// ignored for the still-stubbed `Iceberg`/`Scd` sinks.
pub fn build_sink(
    config: &SinkConfig,
    encrypt: &[EncryptConfig],
    column_metadata: &std::collections::BTreeMap<String, std::collections::BTreeMap<String, Value>>,
) -> anyhow::Result<Arc<dyn Sink>> {
    match config {
        SinkConfig::Parquet {
            base_path,
            dataset_id,
            partition_by,
            ..
        } => Ok(Arc::new(ParquetSink::new(
            base_path,
            dataset_id.clone(),
            partition_by.clone(),
            encrypt.to_vec(),
            column_metadata.clone(),
        )?)),
        SinkConfig::Iceberg {
            catalog,
            table,
            partition_by,
        } => Ok(Arc::new(IcebergSink::new(
            catalog.clone(),
            table.clone(),
            partition_by.clone(),
        ))),
        SinkConfig::Scd {
            catalog,
            table,
            keys,
            scd_type,
        } => Ok(Arc::new(ScdSink::new(
            catalog.clone(),
            table.clone(),
            keys.clone(),
            *scd_type,
        ))),
    }
}
