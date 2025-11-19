use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket::TokenBucket;
use crate::node::NodeWrapper;

/// Bucket data export format for migration
#[derive(Debug, Serialize, Deserialize)]
pub struct BucketExport {
    pub bucket_id: u32,
    pub client_data: Vec<ClientBucketData>,
    pub export_timestamp: u64,
}

/// Individual client rate limiting data (matches TokenBucket structure)
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientBucketData {
    pub client_id: String,
    pub tokens: f64,
    pub last_call: i64,
}

/// Health check response for cluster operations
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub node_id: String,
    pub status: String,
    pub replication_active: bool,
    pub buckets_owned: Vec<u32>,
    pub buckets_replicated: Vec<u32>,
}

/// Export all rate limiting data for a specific bucket
#[instrument(skip(state), level = "info")]
pub async fn export_bucket(
    Path(bucket_id): Path<u32>,
    State(state): State<NodeWrapper>,
) -> Result<Json<BucketExport>> {
    let client_data = match &state {
        NodeWrapper::Hashring(node) => {
            // Check if this node owns the bucket (hashring nodes own exactly one bucket)
            let owned_bucket = node.get_owned_bucket();
            if owned_bucket != bucket_id {
                return Err(ColibriError::Api(format!(
                    "Node owns bucket {} but was asked to export bucket {}",
                    owned_bucket, bucket_id
                )));
            }

            // Export all token buckets from the rate limiter
            let rate_limiter = node.rate_limiter.lock().map_err(|_| {
                ColibriError::Api("Failed to acquire rate limiter lock".to_string())
            })?;
            let all_buckets = rate_limiter.export_all_buckets();
            drop(rate_limiter); // Release lock early

            // Convert to our export format
            all_buckets
                .into_iter()
                .map(|(client_id, token_bucket)| ClientBucketData {
                    client_id,
                    tokens: token_bucket.tokens,
                    last_call: token_bucket.last_call,
                })
                .collect()
        }
        NodeWrapper::Single(node) => {
            // Single nodes own all data, bucket_id is ignored
            let rate_limiter = node.rate_limiter.lock().map_err(|_| {
                ColibriError::Api("Failed to acquire rate limiter lock".to_string())
            })?;
            let all_buckets = rate_limiter.export_all_buckets();
            drop(rate_limiter);

            all_buckets
                .into_iter()
                .map(|(client_id, token_bucket)| ClientBucketData {
                    client_id,
                    tokens: token_bucket.tokens,
                    last_call: token_bucket.last_call,
                })
                .collect()
        }
        NodeWrapper::Gossip(_node) => {
            // TODO: Implement gossip node export logic
            return Err(ColibriError::Api(
                "Gossip node export not yet implemented".to_string()
            ));
        }
    };    let export = BucketExport {
        bucket_id,
        client_data,
        export_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    tracing::info!(
        "Exported {} client records from bucket {}",
        export.client_data.len(),
        bucket_id
    );

    Ok(Json(export))
}

/// Import rate limiting data into a specific bucket
#[instrument(skip(state), level = "info")]
pub async fn import_bucket(
    Path(bucket_id): Path<u32>,
    State(state): State<NodeWrapper>,
    Json(import_data): Json<BucketExport>,
) -> Result<StatusCode> {
    // Validate bucket ID matches
    if import_data.bucket_id != bucket_id {
        return Err(ColibriError::Api(format!(
            "Bucket ID mismatch: expected {}, got {}",
            bucket_id, import_data.bucket_id
        )));
    }

    match &state {
        NodeWrapper::Hashring(node) => {
            // Check if this node owns the bucket
            let owned_bucket = node.get_owned_bucket();
            if owned_bucket != bucket_id {
                return Err(ColibriError::Api(format!(
                    "Node owns bucket {} but was asked to import into bucket {}",
                    owned_bucket, bucket_id
                )));
            }

            // Store count before moving the data
            let client_count = import_data.client_data.len();

            // Convert import data to TokenBucket format
            let token_buckets: std::collections::HashMap<String, TokenBucket> =
                import_data.client_data
                    .into_iter()
                    .map(|client_data| {
                        (client_data.client_id, TokenBucket {
                            tokens: client_data.tokens,
                            last_call: client_data.last_call,
                        })
                    })
                    .collect();

            // Import into rate limiter
            let rate_limiter = node.rate_limiter.lock().map_err(|_| {
                ColibriError::Api("Failed to acquire rate limiter lock".to_string())
            })?;
            rate_limiter.import_buckets(token_buckets);
            drop(rate_limiter);

            tracing::info!(
                "Successfully imported {} client records into bucket {}",
                client_count,
                bucket_id
            );
        }
        NodeWrapper::Single(node) => {
            // Single nodes can import all data, bucket_id is ignored
            let client_count = import_data.client_data.len();

            let token_buckets: std::collections::HashMap<String, TokenBucket> =
                import_data.client_data
                    .into_iter()
                    .map(|client_data| {
                        (client_data.client_id, TokenBucket {
                            tokens: client_data.tokens,
                            last_call: client_data.last_call,
                        })
                    })
                    .collect();

            let rate_limiter = node.rate_limiter.lock().map_err(|_| {
                ColibriError::Api("Failed to acquire rate limiter lock".to_string())
            })?;
            rate_limiter.import_buckets(token_buckets);
            drop(rate_limiter);

            tracing::info!(
                "Successfully imported {} client records into single node",
                client_count
            );
        }
        NodeWrapper::Gossip(_node) => {
            return Err(ColibriError::Api(
                "Gossip node import not yet implemented".to_string()
            ));
        }
    }

    Ok(StatusCode::OK)
}

/// Health check with cluster-specific information
#[instrument(skip(state), level = "info")]
pub async fn cluster_health(
    State(state): State<NodeWrapper>,
) -> Result<Json<HealthResponse>> {
    let response = match &state {
        NodeWrapper::Hashring(node) => {
            let owned_bucket = node.get_owned_bucket();
            let replica_buckets = node.get_replica_buckets();

            HealthResponse {
                node_id: node.node_id.to_string(),
                status: "healthy".to_string(),
                replication_active: !replica_buckets.is_empty(),
                buckets_owned: vec![owned_bucket],
                buckets_replicated: replica_buckets,
            }
        }
        NodeWrapper::Gossip(node) => {
            HealthResponse {
                node_id: node.node_id.to_string(),
                status: "healthy".to_string(),
                replication_active: true, // Gossip uses eventual consistency
                buckets_owned: vec![], // Gossip nodes own all data conceptually
                buckets_replicated: vec![],
            }
        }
        NodeWrapper::Single(node) => {
            HealthResponse {
                node_id: node.node_id.to_string(),
                status: "healthy".to_string(),
                replication_active: false,
                buckets_owned: vec![], // Single nodes own all data
                buckets_replicated: vec![],
            }
        }
    };

    Ok(Json(response))
}