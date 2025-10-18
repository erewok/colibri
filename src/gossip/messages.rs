//! Gossip Message Protocol
//!
//! Defines all message types for cluster communication. All messages use serde traits
//! for serialization but are serialized differently based on context:
//!
//! - INTERNAL cluster communication (UDP gossip): Uses bincode for compact binary format
//! - EXTERNAL API communication (HTTP): Uses serde_json for human-readable format
//!

use crate::gossip::versioned_bucket::VersionedTokenBucket;
use bincode::{Decode, Encode};
use std::collections::HashMap;

/// All possible gossip message types for cluster communication
#[derive(Debug, Clone, Decode, Encode)]
pub enum GossipMessage {
    /// State synchronization messages for sharing rate limit buckets
    StateSync {
        entries: HashMap<String, VersionedTokenBucket>, // client_id -> versioned_bucket
        sender_node_id: u32,
        membership_version: u64, // To detect membership changes
    },

    /// Request specific state from other nodes
    StateRequest {
        requesting_node_id: u32,
        client_ids: Option<Vec<String>>, // None = full sync
        membership_version: u64,
    },

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

/// Cluster membership information shared between nodes
#[derive(Clone, Debug, Decode, Encode)]
pub struct ClusterMembership {
    pub nodes: HashMap<u32, NodeInfo>,
    pub membership_version: u64, // Incremented on each change
}

/// Information about a cluster node
#[derive(Clone, Debug, Decode, Encode)]
pub struct NodeInfo {
    pub node_id: u32,
    pub address: std::net::SocketAddr,
    pub status: NodeStatus,
    pub joined_at: u64, // Unix timestamp
    pub last_seen: u64, // Unix timestamp
    pub capabilities: NodeCapabilities,
}

/// Status of a node in the cluster
#[derive(Clone, Debug, Decode, Encode)]
pub enum NodeStatus {
    Joining, // Node is joining the cluster
    Active,  // Node is fully active
    Leaving, // Node announced it's leaving
    Failed,  // Node failed to respond
    Left,    // Node has left cleanly
}

/// Capabilities and configuration of a node
#[derive(Clone, Debug, Decode, Encode)]
pub struct NodeCapabilities {
    pub max_rate_limit_keys: u32,
    pub gossip_protocol_version: u16,
}

/// GossipPacket wraps messages for network transmission
#[derive(Debug, Clone, Decode, Encode)]
pub struct GossipPacket {
    pub message: GossipMessage,
    pub compression_used: bool,
}

impl GossipPacket {
    /// Create a new gossip packet
    pub fn new(message: GossipMessage) -> Self {
        Self {
            message,
            compression_used: false,
        }
    }

    /// Serialize for INTERNAL cluster communication (UDP gossip)
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        let config = bincode::config::standard().with_big_endian();
        bincode::encode_to_vec(self, config)
    }

    /// Deserialize from INTERNAL cluster communication (UDP gossip)
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let config = bincode::config::standard().with_big_endian();
        let (result, _) = bincode::decode_from_slice(data, config)?;
        Ok(result)
    }
}

impl ClusterMembership {
    /// Create initial cluster membership with local node
    pub fn new(local_node_id: u32, local_address: std::net::SocketAddr) -> Self {
        let mut nodes = HashMap::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        nodes.insert(
            local_node_id,
            NodeInfo {
                node_id: local_node_id,
                address: local_address,
                status: NodeStatus::Active,
                joined_at: now,
                last_seen: now,
                capabilities: NodeCapabilities {
                    max_rate_limit_keys: 100_000,
                    gossip_protocol_version: 1,
                },
            },
        );

        Self {
            nodes,
            membership_version: 1,
        }
    }

    /// Add a new node to the cluster
    pub fn add_node(&mut self, node_info: NodeInfo) -> bool {
        if self.nodes.contains_key(&node_info.node_id) {
            return false; // Node already exists
        }

        self.nodes.insert(node_info.node_id, node_info);
        self.membership_version += 1;
        true
    }

    /// Remove a node from the cluster
    pub fn remove_node(&mut self, node_id: u32) -> Option<NodeInfo> {
        if let Some(node) = self.nodes.remove(&node_id) {
            self.membership_version += 1;
            Some(node)
        } else {
            None
        }
    }

    /// Update the status of a node
    pub fn update_node_status(&mut self, node_id: u32, status: NodeStatus) -> bool {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.status = status;
            node.last_seen = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.membership_version += 1;
            true
        } else {
            false
        }
    }

    /// Get all active nodes in the cluster
    pub fn get_active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|node| matches!(node.status, NodeStatus::Active))
            .collect()
    }

    /// Detect nodes that have failed based on last seen timeout
    pub fn detect_failed_nodes(&mut self, failure_timeout: std::time::Duration) -> Vec<u32> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let timeout_secs = failure_timeout.as_secs();

        let mut failed_nodes = Vec::new();

        for (node_id, node) in &mut self.nodes {
            if matches!(node.status, NodeStatus::Active)
                && now.saturating_sub(node.last_seen) > timeout_secs
            {
                node.status = NodeStatus::Failed;
                failed_nodes.push(*node_id);
            }
        }

        if !failed_nodes.is_empty() {
            self.membership_version += 1;
        }

        failed_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_gossip_packet_serialization() {
        let message = GossipMessage::Heartbeat {
            node_id: 1,
            timestamp: 1234567890,
            membership_version: 5,
        };
        let packet = GossipPacket::new(message);

        // Test bincode serialization (for internal cluster communication)
        let serialized = packet.serialize().expect("Failed to serialize packet");
        let deserialized =
            GossipPacket::deserialize(&serialized).expect("Failed to deserialize packet");

        match deserialized.message {
            GossipMessage::Heartbeat {
                node_id,
                timestamp,
                membership_version,
            } => {
                assert_eq!(node_id, 1);
                assert_eq!(timestamp, 1234567890);
                assert_eq!(membership_version, 5);
            }
            _ => panic!("Wrong message type after deserialization"),
        }
    }

    #[test]
    fn test_state_sync_message() {
        let mut entries = HashMap::new();
        entries.insert(
            "client1".to_string(),
            VersionedTokenBucket::new(
                crate::token_bucket::TokenBucket::new(100, std::time::Duration::from_secs(60)),
                1,
            ),
        );

        let message = GossipMessage::StateSync {
            entries,
            sender_node_id: 2,
            membership_version: 10,
        };

        let packet = GossipPacket::new(message);
        let serialized = packet.serialize().expect("Failed to serialize");
        let deserialized = GossipPacket::deserialize(&serialized).expect("Failed to deserialize");

        match deserialized.message {
            GossipMessage::StateSync {
                entries,
                sender_node_id,
                membership_version,
            } => {
                assert_eq!(sender_node_id, 2);
                assert_eq!(membership_version, 10);
                assert!(entries.contains_key("client1"));
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_cluster_membership_creation() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
        let membership = ClusterMembership::new(1, addr);

        assert_eq!(membership.nodes.len(), 1);
        assert!(membership.nodes.contains_key(&1));
        assert_eq!(membership.membership_version, 1);

        let node = membership.nodes.get(&1).unwrap();
        assert_eq!(node.node_id, 1);
        assert_eq!(node.address, addr);
        assert!(matches!(node.status, NodeStatus::Active));
    }

    #[test]
    fn test_cluster_membership_add_remove_node() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7947);
        let mut membership = ClusterMembership::new(1, addr1);

        // Add second node
        let node2 = NodeInfo {
            node_id: 2,
            address: addr2,
            status: NodeStatus::Active,
            joined_at: 1234567890,
            last_seen: 1234567890,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 50_000,
                gossip_protocol_version: 1,
            },
        };

        assert!(membership.add_node(node2));
        assert_eq!(membership.nodes.len(), 2);
        assert_eq!(membership.membership_version, 2);

        // Try to add same node again (should fail)
        let node2_duplicate = NodeInfo {
            node_id: 2,
            address: addr2,
            status: NodeStatus::Active,
            joined_at: 1234567891,
            last_seen: 1234567891,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 60_000,
                gossip_protocol_version: 1,
            },
        };
        assert!(!membership.add_node(node2_duplicate));
        assert_eq!(membership.membership_version, 2); // Should not increment

        // Remove node
        let removed = membership.remove_node(2);
        assert!(removed.is_some());
        assert_eq!(membership.nodes.len(), 1);
        assert_eq!(membership.membership_version, 3);

        // Try to remove non-existent node
        let removed = membership.remove_node(999);
        assert!(removed.is_none());
        assert_eq!(membership.membership_version, 3); // Should not increment
    }

    #[test]
    fn test_node_status_update() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
        let mut membership = ClusterMembership::new(1, addr);

        // Update status of existing node
        assert!(membership.update_node_status(1, NodeStatus::Leaving));
        assert_eq!(membership.membership_version, 2);

        let node = membership.nodes.get(&1).unwrap();
        assert!(matches!(node.status, NodeStatus::Leaving));

        // Try to update non-existent node
        assert!(!membership.update_node_status(999, NodeStatus::Failed));
        assert_eq!(membership.membership_version, 2); // Should not increment
    }

    #[test]
    fn test_active_nodes_filtering() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7947);
        let mut membership = ClusterMembership::new(1, addr1);

        // Add second node
        let node2 = NodeInfo {
            node_id: 2,
            address: addr2,
            status: NodeStatus::Active,
            joined_at: 1234567890,
            last_seen: 1234567890,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 50_000,
                gossip_protocol_version: 1,
            },
        };
        membership.add_node(node2);

        // Both nodes are active
        let active_nodes = membership.get_active_nodes();
        assert_eq!(active_nodes.len(), 2);

        // Mark one node as failed
        membership.update_node_status(2, NodeStatus::Failed);
        let active_nodes = membership.get_active_nodes();
        assert_eq!(active_nodes.len(), 1);
        assert_eq!(active_nodes[0].node_id, 1);
    }
}
