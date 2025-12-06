/// Cluster administration messages - sent over internal transport only
/// These are NOT part of the public API - they are for admin tools and internal cluster management
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};

use std::net::SocketAddr;

/// Administrative command types sent to cluster nodes
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum AdminCommand {
    /// Add a new node to cluster membership
    AddNode { address: SocketAddr },
    /// Remove a node from cluster membership
    RemoveNode { address: SocketAddr },
    /// Mark a node as unresponsive (stop sending to it)
    MarkUnresponsive { address: SocketAddr },
    /// Mark a node as responsive (resume sending to it)
    MarkResponsive { address: SocketAddr },
    /// Export all rate limiting data (for cluster migration)
    ExportBuckets,
    /// Import rate limiting data (for cluster migration)
    ImportBuckets { data: BucketExport },
    /// Get cluster health status
    GetClusterHealth,
    /// Get current topology information
    GetTopology,
    /// Prepare for topology change (hashring nodes need this)
    PrepareTopologyChange { new_topology: Vec<SocketAddr> },
}

/// Response from administrative commands
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum AdminResponse {
    /// Simple acknowledgment
    Ack,
    /// Error response
    Error { message: String },
    /// Bucket export data
    BucketExport(BucketExport),
    /// Health status response
    ClusterHealth(StatusResponse),
    /// Topology information
    Topology(TopologyResponse),
}

/// Rate limiting bucket export/import format
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct BucketExport {
    pub client_data: Vec<ClientBucketData>,
    pub metadata: ExportMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ClientBucketData {
    pub client_id: String,
    pub remaining_tokens: i64,
    pub last_refill: u64, // timestamp
    pub bucket_id: Option<u32>, // for hashring nodes
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ExportMetadata {
    pub node_id: String,
    pub export_timestamp: u64,
    pub node_type: String, // "gossip", "hashring", "single"
    pub bucket_count: usize,
}

/// Cluster health and status information
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct StatusResponse {
    pub node_id: String,
    pub node_type: String,
    pub status: ClusterStatus,
    pub active_clients: usize,
    pub last_topology_change: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ClusterStatus {
    Healthy,
    Degraded,
    Offline,
}

/// Topology information and node assignments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TopologyResponse {
    pub node_id: String,
    pub node_type: String,
    pub owned_bucket: Option<u32>, // for hashring nodes
    pub replica_buckets: Vec<u32>, // for hashring nodes
    pub cluster_nodes: Vec<SocketAddr>, // all known cluster members
    pub peer_nodes: Vec<String>, // peer addresses as strings
    pub errors: Option<Vec<String>>,
}

/// Legacy type aliases for compatibility with existing API endpoints
/// These will be moved to admin-only transport later
pub type TopologyChangeRequest = PrepareTopologyChangeRequest;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct PrepareTopologyChangeRequest {
    pub new_topology: Vec<SocketAddr>,
}