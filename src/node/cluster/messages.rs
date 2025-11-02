use bincode::{Decode, Encode};
use crdts::VClock;

use super::membership::{ClusterMembership, NodeInfo};
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
