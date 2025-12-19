/// Cluster commands are within a single node.
use std::net::SocketAddr;

use bincode::{Decode, Encode};
use tokio::sync::oneshot;

use crate::error::Result;
use crate::node::{
    messages::{CheckCallsRequest, CheckCallsResponse, Status, StatusResponse, TopologyResponse},
    NodeName,
};
use crate::settings::{NamedRateLimitRule, RateLimitSettings, RunMode};

/// Rate limiting bucket export/import format
#[derive(Debug, Clone, Encode, Decode)]
pub struct BucketExport {
    pub client_data: Vec<ClientBucketData>,
    pub metadata: ExportMetadata,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ClientBucketData {
    pub client_id: String,
    pub remaining_tokens: i64,
    pub last_refill: u64,       // timestamp
    pub bucket_id: Option<u32>, // for hashring nodes
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ExportMetadata {
    #[bincode(with_serde)]
    pub node_name: NodeName,
    #[bincode(with_serde)]
    pub node_type: RunMode,
    pub export_timestamp: u64,
    pub bucket_count: usize,
}

/// Administrative command types sent to cluster nodes
#[derive(Debug, Clone, Encode, Decode)]
pub enum AdminCommand {
    /// Add a new node to cluster membership
    AddNode { name: String, address: SocketAddr },
    /// Remove a node from cluster membership
    RemoveNode { name: String, address: SocketAddr },
    /// Export all rate limiting data (for cluster migration)
    ExportBuckets,
    /// Import rate limiting data (for cluster migration)
    ImportBuckets { data: BucketExport },
    /// Get cluster health status
    HealthCheck,
    /// Get current topology information
    GetTopology,
    /// Get Status
    GetStatus,
}

/// Response from administrative commands
#[derive(Debug, Clone, Encode, Decode)]
pub enum AdminResponse {
    /// Simple acknowledgment
    Ack,
    /// Error response
    Error { message: String },
    /// Bucket export data
    BucketExport(BucketExport),
    /// Health status response
    ClusterHealth(Status),
    /// Status & Topology information
    Status(StatusResponse),
    /// Topology information
    Topology(TopologyResponse),
}

/// Commands that can be sent between components within a cluster node
#[derive(Debug)]
pub enum ClusterCommand {
    /// Heartbeat
    Heartbeat,
    /// Expire old keys
    ExpireKeys,

    /// Check remaining calls for a client
    CheckLimit {
        request: CheckCallsRequest,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Apply rate limiting for a client
    RateLimit {
        request: CheckCallsRequest,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Create a named rate limiting rule
    CreateNamedRule {
        rule_name: String,
        settings: RateLimitSettings,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Delete a named rate limiting rule
    DeleteNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Get a specific named rule
    GetNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<Option<NamedRateLimitRule>>>,
    },
    /// List all named rate limiting rules
    ListNamedRules {
        resp_chan: oneshot::Sender<Result<Vec<NamedRateLimitRule>>>,
    },
    /// Handle admin commands from cluster
    AdminCommand {
        command: AdminCommand,
        source: SocketAddr,
        resp_chan: oneshot::Sender<Result<AdminResponse>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_command_serialization() {
        let config = bincode::config::standard();

        // Test AddNode command
        let cmd = AdminCommand::AddNode {
            name: "node-1".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
        };
        let serialized = bincode::encode_to_vec(&cmd, config).unwrap();
        let (deserialized, _): (AdminCommand, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        match deserialized {
            AdminCommand::AddNode { name, address } => {
                assert_eq!(name, "node-1");
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
            node_name: "test-node".into(),
            node_type: RunMode::Gossip,
            status: Status::Healthy,
            bucket_count: Some(42),
            last_topology_change: Some(1234567890),
            errors: None,
        };

        // Test serialization
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&status, config).unwrap();
        let (deserialized, _): (StatusResponse, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        assert_eq!(deserialized.node_name.as_str(), "test-node");
        assert_eq!(deserialized.bucket_count, Some(42));
        match deserialized.status {
            Status::Healthy => {}
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
                node_name: "test-node".into(),
                export_timestamp: 1234567890,
                node_type: RunMode::Single,
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
        assert_eq!(deserialized.metadata.node_name.as_str(), "test-node");
    }
}
