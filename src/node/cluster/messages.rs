use std::collections::HashMap;
use std::net::SocketAddr;

use bincode::{Decode, Encode};
use crdts::VClock;
use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// All possible gossip message types for cluster communication
#[derive(Debug, Clone, Decode, Encode)]
pub enum ClusterMessage {
    /// Membership management messages
    MembershipUpdate {
        membership: ClusterMembership,
        #[bincode(with_serde)]
        sender_node_id: NodeId,
    },

    /// Node requesting to join the cluster
    NodeJoinRequest {
        new_node: NodeInfo,
        #[bincode(with_serde)]
        requesting_node_id: NodeId,
    },

    /// Response to join request with cluster state
    NodeJoinResponse {
        accepted: bool,
        current_membership: ClusterMembership,
        #[bincode(with_serde)]
        vlock: VClock<NodeId>,
        #[bincode(with_serde)]
        responder_node_id: NodeId,
    },

    /// Notification that a node is leaving gracefully
    NodeLeaveNotification {
        #[bincode(with_serde)]
        leaving_node_id: NodeId,
        new_membership: ClusterMembership,
    },

    /// Alert that a node has been detected as failed
    NodeFailureDetected {
        #[bincode(with_serde)]
        failed_node_id: NodeId,
        #[bincode(with_serde)]
        detector_node_id: NodeId,
        proposed_membership: ClusterMembership,
    },

    /// Heartbeat and discovery messages
    Heartbeat {
        #[bincode(with_serde)]
        node_id: NodeId,
        timestamp: u64,
        membership_version: u64,
        #[bincode(with_serde)]
        vlock: VClock<NodeId>,
    },

    /// Periodic membership synchronization
    MembershipSync {
        membership: ClusterMembership,
        #[bincode(with_serde)]
        sender_node_id: NodeId,
        #[bincode(with_serde)]
        vlock: VClock<NodeId>,
    },
}

/// Health check response for cluster operations
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterStatus {
    Active,
    Isolated,
    Error,
}

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
pub struct StatusResponse {
    pub node_id: String,
    pub status: ClusterStatus,
    pub client_key_count: u32,
    pub last_topology_change: Option<u64>,
}

/// Request to prepare for topology change
#[derive(Debug, Serialize, Deserialize)]
pub struct TopologyChangeRequest {
    pub new_topology: Vec<String>,
    pub topology_change_clock: u64,
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
    // address of other nodes mapped to an error string
    pub errors: Option<HashMap<String, String>>,
}


/// Cluster membership state shared between nodes
#[derive(Clone, Debug, Decode, Encode)]
pub struct ClusterMembership {
    pub nodes: HashMap<u32, NodeInfo>,
    pub membership_version: u64,
}

/// Node information in the cluster
#[derive(Clone, Debug, Decode, Encode)]
pub struct NodeInfo {
    pub node_id: u32,
    pub address: SocketAddr,
    pub status: NodeStatus,
    pub joined_at: u64,
    pub last_seen: u64,
}

/// Node status in cluster lifecycle
#[derive(Clone, Debug, Decode, Encode)]
pub enum NodeStatus {
    Joining,
    Active,
    Leaving,
    Failed,
    Left,
}