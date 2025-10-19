use std::collections::HashMap;

use bincode::{Decode, Encode};

use super::membership::{ClusterMembership, NodeInfo};
use crate::versioned_bucket::VersionedTokenBucket;

/// All possible gossip message types for cluster communication
#[derive(Debug, Clone, Decode, Encode)]
pub enum ClusterMessage {
    /// Membership management messages
    MembershipUpdate {
        membership: ClusterMembership,
        sender_node_id: u32,
    },

    /// Node requesting to join the cluster
    NodeJoinRequest {
        new_node: NodeInfo,
        requesting_node_id: u32,
    },

    /// Response to join request with cluster state
    NodeJoinResponse {
        accepted: bool,
        current_membership: ClusterMembership,
        state_to_migrate: HashMap<String, VersionedTokenBucket>,
        responder_node_id: u32,
    },

    /// Notification that a node is leaving gracefully
    NodeLeaveNotification {
        leaving_node_id: u32,
        state_handoff: HashMap<String, VersionedTokenBucket>,
        new_membership: ClusterMembership,
    },

    /// Alert that a node has been detected as failed
    NodeFailureDetected {
        failed_node_id: u32,
        detector_node_id: u32,
        proposed_membership: ClusterMembership,
    },

    /// Heartbeat and discovery messages
    Heartbeat {
        node_id: u32,
        timestamp: u64,
        membership_version: u64,
    },

    /// Periodic membership synchronization
    MembershipSync {
        membership: ClusterMembership,
        sender_node_id: u32,
    },
}
