//! Gossip Message Protocol
//!
//! Defines all message types for cluster communication.
use std::net::SocketAddr;

use bincode::{Decode, Encode};
use crdts::VClock;

use crate::limiters::distributed_bucket::DistributedBucketExternal;
use crate::node::NodeId;

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


// impl GossipCommand {
//     pub fn from_incoming_message(data: bytes::Bytes, peer_addr: SocketAddr) -> Self {
//         GossipCommand::GossipMessageReceived { data, peer_addr }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeName;

    #[test]
    fn test_gossip_packet_serialization() {
        let message = GossipMessage::StateRequest {
            requesting_node_id: NodeName::from("node-1").node_id(),
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
                assert_eq!(requesting_node_id, NodeName::from("node-1").node_id());
                assert!(missing_keys.is_none());
                assert_eq!(response_addr, "127.0.0.1:8410".parse().unwrap());
            }
            _ => panic!("Wrong message type after deserialization"),
        }
    }
}
