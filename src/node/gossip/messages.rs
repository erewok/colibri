//! Gossip Message Protocol
//!
//! Defines all message types for cluster communication. All messages use serde traits
//! for serialization but are serialized differently based on context:
//!
//! - INTERNAL cluster communication (UDP gossip): Uses bincode for compact binary format
//! - EXTERNAL API communication (HTTP): Uses serde_json for human-readable format
//!
use std::net::SocketAddr;

use bincode::{Decode, Encode};
use crdts::VClock;
use tokio::sync::oneshot;

use crate::limiters::distributed_bucket::DistributedBucketExternal;
use crate::node::{CheckCallsResponse, NodeId};

/// Gossip message types for production delta-state protocol
#[derive(Debug, Clone, Decode, Encode)]
pub enum GossipMessage {
    /// Delta-state synchronization - only recently updated keys
    DeltaStateSync {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        updates: Vec<DistributedBucketExternal>, // Only changed keys
        #[bincode(with_serde)]
        sender_node_id: NodeId,
        propagation_factor: u8, // To limit spread
    },

    /// Request for specific state (anti-entropy)
    StateRequest {
        missing_keys: Option<Vec<String>>, // None = full sync
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        requesting_node_id: NodeId,
    },

    /// Response to state request with missing data
    StateResponse {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        responding_node_id: NodeId,
        #[bincode(with_serde)]
        requested_data: DistributedBucketExternal,
    },

    /// Heartbeat with version vectors for anti-entropy
    Heartbeat {
        response_addr: SocketAddr,
        timestamp: u64,
        #[bincode(with_serde)]
        node_id: NodeId,
        #[bincode(with_serde)]
        vclock: VClock<NodeId>,
    },

    /// Rate limit configuration synchronization messages
    RateLimitConfigCreate {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        sender_node_id: NodeId,
        rule_name: String,
        #[bincode(with_serde)]
        settings: crate::settings::RateLimitSettings,
        timestamp: u64,
    },

    RateLimitConfigDelete {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        sender_node_id: NodeId,
        rule_name: String,
        timestamp: u64,
    },

    RateLimitConfigRequest {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        requesting_node_id: NodeId,
        rule_name: Option<String>, // None = request all rules
    },

    RateLimitConfigResponse {
        response_addr: SocketAddr,
        #[bincode(with_serde)]
        responding_node_id: NodeId,
        #[bincode(with_serde)]
        rules: Vec<crate::settings::NamedRateLimitRule>,
    },
}

/// GossipPacket wraps messages for network transmission
#[derive(Debug, Clone, Decode, Encode)]
pub struct GossipPacket {
    pub message: GossipMessage,
    pub packet_id: u64, // For deduplication
}

impl GossipPacket {
    /// Create a new gossip packet
    pub fn new(message: GossipMessage) -> Self {
        Self {
            message,
            packet_id: rand::random(), // Generate random packet ID
        }
    }

    /// Create a new gossip packet with specific ID
    pub fn new_with_id(message: GossipMessage, packet_id: u64) -> Self {
        Self { message, packet_id }
    }

    /// Serialize for INTERNAL cluster communication (UDP gossip)
    pub fn serialize(&self) -> Result<bytes::Bytes, bincode::error::EncodeError> {
        let config = bincode::config::standard().with_big_endian();
        bincode::encode_to_vec(self, config).map(bytes::Bytes::from)
    }

    /// Deserialize from INTERNAL cluster communication (UDP gossip)
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let config = bincode::config::standard().with_big_endian();
        let (result, _) = bincode::decode_from_slice(data, config)?;
        Ok(result)
    }
}

pub enum GossipCommand {
    ExpireKeys,
    CheckLimit {
        client_id: String,
        resp_chan: oneshot::Sender<crate::error::Result<CheckCallsResponse>>,
    },
    RateLimit {
        client_id: String,
        resp_chan: oneshot::Sender<crate::error::Result<Option<CheckCallsResponse>>>,
    },
    GossipMessageReceived {
        data: bytes::Bytes,
        peer_addr: SocketAddr,
    },

    // Rate limit configuration commands
    CreateNamedRule {
        rule_name: String,
        settings: crate::settings::RateLimitSettings,
        resp_chan: oneshot::Sender<crate::error::Result<()>>,
    },
    DeleteNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<crate::error::Result<()>>,
    },
    GetNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<crate::error::Result<crate::settings::NamedRateLimitRule>>,
    },
    ListNamedRules {
        resp_chan: oneshot::Sender<crate::error::Result<Vec<crate::settings::NamedRateLimitRule>>>,
    },
    RateLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<crate::error::Result<Option<CheckCallsResponse>>>,
    },
    CheckLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<crate::error::Result<CheckCallsResponse>>,
    },
}

impl GossipCommand {
    pub fn from_incoming_message(data: bytes::Bytes, peer_addr: SocketAddr) -> Self {
        GossipCommand::GossipMessageReceived { data, peer_addr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gossip_packet_serialization() {
        let message = GossipMessage::StateRequest {
            requesting_node_id: NodeId::new(1),
            missing_keys: None,
            response_addr: "127.0.0.1:8410".parse().unwrap(),
        };
        let packet = GossipPacket::new(message);

        // Test bincode serialization (for internal cluster communication)
        let serialized = packet.serialize().expect("Failed to serialize packet");
        let deserialized =
            GossipPacket::deserialize(&serialized).expect("Failed to deserialize packet");

        match deserialized.message {
            GossipMessage::StateRequest {
                requesting_node_id,
                missing_keys,
                response_addr,
            } => {
                assert_eq!(requesting_node_id, NodeId::new(1));
                assert!(missing_keys.is_none());
                assert_eq!(response_addr, "127.0.0.1:8410".parse().unwrap());
            }
            _ => panic!("Wrong message type after deserialization"),
        }
    }
}
