use bincode::{Decode, Encode};
/// Cluster administration messages - sent over internal transport only
/// These are NOT part of the public API - they are for admin tools and internal cluster management
use serde::{Deserialize, Serialize};

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
    pub last_refill: u64,       // timestamp
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
    pub owned_bucket: Option<u32>,      // for hashring nodes
    pub replica_buckets: Vec<u32>,      // for hashring nodes
    pub cluster_nodes: Vec<SocketAddr>, // all known cluster members
    pub peer_nodes: Vec<String>,        // peer addresses as strings
    pub errors: Option<Vec<String>>,
}

/// Legacy type aliases for compatibility with existing API endpoints
/// These will be moved to admin-only transport later
pub type TopologyChangeRequest = PrepareTopologyChangeRequest;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct PrepareTopologyChangeRequest {
    pub new_topology: Vec<SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_command_serialization() {
        let config = bincode::config::standard();

        // Test AddNode command
        let cmd = AdminCommand::AddNode {
            address: "127.0.0.1:8080".parse().unwrap(),
        };
        let serialized = bincode::encode_to_vec(&cmd, config).unwrap();
        let (deserialized, _): (AdminCommand, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        match deserialized {
            AdminCommand::AddNode { address } => {
                assert_eq!(address.to_string(), "127.0.0.1:8080");
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_admin_response_serialization() {
        let config = bincode::config::standard();

        // Test simple Ack
        let response = AdminResponse::Ack;
        let serialized = bincode::encode_to_vec(&response, config).unwrap();
        let (deserialized, _): (AdminResponse, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        match deserialized {
            AdminResponse::Ack => {}
            _ => panic!("Wrong response type"),
        }

        // Test Error response
        let error_response = AdminResponse::Error {
            message: "Node not found".to_string(),
        };
        let serialized = bincode::encode_to_vec(&error_response, config).unwrap();
        let (deserialized, _): (AdminResponse, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        match deserialized {
            AdminResponse::Error { message } => {
                assert_eq!(message, "Node not found");
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_status_response() {
        let status = StatusResponse {
            node_id: "test-node".to_string(),
            node_type: "gossip".to_string(),
            status: ClusterStatus::Healthy,
            active_clients: 42,
            last_topology_change: Some(1234567890),
        };

        // Test serialization
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&status, config).unwrap();
        let (deserialized, _): (StatusResponse, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        assert_eq!(deserialized.node_id, "test-node");
        assert_eq!(deserialized.active_clients, 42);
        match deserialized.status {
            ClusterStatus::Healthy => {}
            _ => panic!("Wrong status"),
        }
    }

    #[test]
    fn test_bucket_export_structure() {
        let export = BucketExport {
            client_data: vec![ClientBucketData {
                client_id: "test-client".to_string(),
                remaining_tokens: 100,
                last_refill: 1234567890,
                bucket_id: Some(0),
            }],
            metadata: ExportMetadata {
                node_id: "test-node".to_string(),
                export_timestamp: 1234567890,
                node_type: "single".to_string(),
                bucket_count: 1,
            },
        };

        // Test that it serializes/deserializes
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&export, config).unwrap();
        let (deserialized, _): (BucketExport, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        assert_eq!(deserialized.client_data.len(), 1);
        assert_eq!(deserialized.client_data[0].client_id, "test-client");
        assert_eq!(deserialized.metadata.node_id, "test-node");
    }
}
