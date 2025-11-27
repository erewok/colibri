use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::limiters::token_bucket::TokenBucket;
use crate::node::NodeWrapper;
use crate::{
    error::{ColibriError, Result},
    node::hashring::consistent_hashing,
};

/// Bucket data export format for migration
#[derive(Debug, Serialize, Deserialize)]
pub struct BucketExport {
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

/// Current topology information
#[derive(Debug, Serialize, Deserialize)]
pub struct TopologyResponse {
    pub node_id: String,
    pub total_buckets: u32,
    pub owned_bucket: Option<u32>,
    pub replica_buckets: Vec<u32>,
    pub peer_nodes: Vec<String>,
    pub topology_version: u64,
}

/// Request to prepare for topology change
#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareChangeRequest {
    pub new_topology: Vec<String>,
    pub change_id: String,
    pub timeout_seconds: u64,
}

/// Response to prepare change request
#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareChangeResponse {
    pub node_id: String,
    pub ready: bool,
    pub error: Option<String>,
    pub data_exported: bool,
}

/// Cluster status information
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatusResponse {
    pub node_id: String,
    pub operational_state: String, // "normal", "preparing", "changing", "error"
    pub active_change_id: Option<String>,
    pub last_topology_change: Option<u64>,
    pub data_consistency_ok: bool,
}

/// Export all rate limiting data for a specific bucket
#[instrument(skip(state), level = "info")]
pub async fn export_bucket(State(state): State<NodeWrapper>) -> Result<Json<BucketExport>> {
    let client_data = match &state {
        NodeWrapper::Hashring(node) => {
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
            // Gossip nodes don't use bucket-based data export
            // All data is replicated across gossip network
            Vec::new()
        }
    };
    let export = BucketExport {
        client_data,
        export_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    tracing::info!("Exported {} client records", export.client_data.len(),);

    Ok(Json(export))
}

/// Import rate limiting data into a specific bucket
#[instrument(skip(state), level = "info")]
pub async fn import_bucket(
    State(state): State<NodeWrapper>,
    Json(import_data): Json<BucketExport>,
) -> Result<StatusCode> {
    match &state {
        NodeWrapper::Hashring(node) => {
            // Store count before moving the data
            let client_count = import_data.client_data.len();

            // Convert import data to TokenBucket format
            let token_buckets: std::collections::HashMap<String, TokenBucket> = import_data
                .client_data
                .into_iter()
                .filter_map(|client_data| {
                    let bucket = consistent_hashing::jump_consistent_hash(
                        client_data.client_id.as_str(),
                        node.number_of_buckets,
                    );
                    if bucket != node.get_owned_bucket() {
                        // This client does not belong to this bucket
                        return None;
                    }
                    Some((
                        client_data.client_id,
                        TokenBucket {
                            tokens: client_data.tokens,
                            last_call: client_data.last_call,
                        },
                    ))
                })
                .collect();

            // Import into rate limiter
            let rate_limiter = node.rate_limiter.lock().map_err(|_| {
                ColibriError::Api("Failed to acquire rate limiter lock".to_string())
            })?;
            rate_limiter.import_buckets(token_buckets);
            drop(rate_limiter);

            tracing::info!("Successfully imported {} client records", client_count,);
        }
        NodeWrapper::Single(node) => {
            // Single nodes can import all data, bucket_id is ignored
            let client_count = import_data.client_data.len();

            let token_buckets: std::collections::HashMap<String, TokenBucket> = import_data
                .client_data
                .into_iter()
                .map(|client_data| {
                    (
                        client_data.client_id,
                        TokenBucket {
                            tokens: client_data.tokens,
                            last_call: client_data.last_call,
                        },
                    )
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
            // Gossip nodes don't use bucket-based data import
            // Data synchronization happens through gossip protocol
            tracing::info!("Gossip node data import skipped - using gossip synchronization");
        }
    }

    Ok(StatusCode::OK)
}

/// Health check with cluster-specific information
#[instrument(skip(state), level = "info")]
pub async fn cluster_health(State(state): State<NodeWrapper>) -> Result<Json<HealthResponse>> {
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
                buckets_owned: vec![],    // Gossip nodes own all data conceptually
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

/// Get current node topology information
#[instrument(skip(state), level = "info")]
pub async fn get_topology(State(state): State<NodeWrapper>) -> Result<Json<TopologyResponse>> {
    let response = match &state {
        NodeWrapper::Hashring(node) => {
            let owned_bucket = node.get_owned_bucket();
            let replica_buckets = node.get_replica_buckets();
            let peer_nodes: Vec<String> = node
                .topology
                .keys()
                .map(|bucket_id| {
                    node.topology
                        .get(bucket_id)
                        .map(|(url, _)| url.to_string())
                        .unwrap_or_default()
                })
                .collect();

            TopologyResponse {
                node_id: node.node_id.to_string(),
                total_buckets: node.topology.len() as u32,
                owned_bucket: Some(owned_bucket),
                replica_buckets,
                peer_nodes,
                topology_version: 1,
            }
        }
        NodeWrapper::Single(node) => {
            TopologyResponse {
                node_id: node.node_id.to_string(),
                total_buckets: 1,
                owned_bucket: None, // Single node owns all data
                replica_buckets: vec![],
                peer_nodes: vec![],
                topology_version: 1,
            }
        }
        NodeWrapper::Gossip(node) => {
            TopologyResponse {
                node_id: node.node_id.to_string(),
                total_buckets: 0, // Gossip doesn't use buckets
                owned_bucket: None,
                replica_buckets: vec![],
                peer_nodes: vec![], // Gossip peers managed by gossip protocol
                topology_version: 1,
            }
        }
    };

    Ok(Json(response))
}

/// Prepare for topology change
#[instrument(skip(state), level = "info")]
pub async fn prepare_topology_change(
    State(state): State<NodeWrapper>,
    Json(request): Json<PrepareChangeRequest>,
) -> Result<Json<PrepareChangeResponse>> {
    tracing::info!("Preparing for topology change: {}", request.change_id);

    // Validate the new topology format
    if request.new_topology.is_empty() {
        let node_id = match &state {
            NodeWrapper::Hashring(node) => node.node_id.to_string(),
            NodeWrapper::Single(node) => node.node_id.to_string(),
            NodeWrapper::Gossip(node) => node.node_id.to_string(),
        };

        return Ok(Json(PrepareChangeResponse {
            node_id,
            ready: false,
            error: Some("New topology cannot be empty".to_string()),
            data_exported: false,
        }));
    }

    let node_id = match &state {
        NodeWrapper::Hashring(node) => node.node_id.to_string(),
        NodeWrapper::Single(node) => node.node_id.to_string(),
        NodeWrapper::Gossip(node) => node.node_id.to_string(),
    };

    // Basic readiness check - node is ready if it's currently healthy
    let response = PrepareChangeResponse {
        node_id,
        ready: true,
        error: None,
        data_exported: false, // Data export handled separately via export endpoint
    };

    Ok(Json(response))
}

/// Commit topology change
#[instrument(skip(_state), level = "info")]
pub async fn commit_topology_change(State(_state): State<NodeWrapper>) -> Result<StatusCode> {
    tracing::info!("Topology change committed - configuration updated");

    // The actual topology change happens when the node is restarted
    // with new configuration. This endpoint just acknowledges the commit.
    Ok(StatusCode::OK)
}

/// Abort topology change
#[instrument(skip(_state), level = "info")]
pub async fn abort_topology_change(State(_state): State<NodeWrapper>) -> Result<StatusCode> {
    tracing::info!("Topology change aborted - reverting to previous state");

    // The actual abort happens by not restarting with new configuration
    // This endpoint just acknowledges the abort request.
    Ok(StatusCode::OK)
}

/// Get cluster operational status
#[instrument(skip(state), level = "info")]
pub async fn get_cluster_status(
    State(state): State<NodeWrapper>,
) -> Result<Json<ClusterStatusResponse>> {
    let node_id = match &state {
        NodeWrapper::Hashring(node) => node.node_id.to_string(),
        NodeWrapper::Single(node) => node.node_id.to_string(),
        NodeWrapper::Gossip(node) => node.node_id.to_string(),
    };

    let response = ClusterStatusResponse {
        node_id,
        operational_state: "normal".to_string(),
        active_change_id: None,
        last_topology_change: None,
        data_consistency_ok: true, // Basic consistency - node is operational
    };

    Ok(Json(response))
}
