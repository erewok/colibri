//! Gossip Message Protocol
//!
//! Defines all message types for cluster communication. All messages use serde traits
//! for serialization but are serialized differently based on context:
//!
//! - INTERNAL cluster communication (UDP gossip): Uses bincode for compact binary format
//! - EXTERNAL API communication (HTTP): Uses serde_json for human-readable format
//!
use std::collections::HashMap;

use bincode::{Decode, Encode};

use crate::limiters::versioned_bucket::VersionedTokenBucket;

/// Gossip message types for production delta-state protocol
#[derive(Debug, Clone, Decode, Encode)]
pub enum GossipMessage {
    /// Delta-state synchronization - only recently updated keys
    DeltaStateSync {
        updates: HashMap<String, VersionedTokenBucket>, // Only changed keys
        sender_node_id: u32,
        gossip_round: u64,
        last_seen_versions: HashMap<u32, u64>, // For anti-entropy
    },

    /// Request for specific state (anti-entropy)
    StateRequest {
        requesting_node_id: u32,
        missing_keys: Option<Vec<String>>, // None = full sync
        since_version: HashMap<u32, u64>,  // What we already have
    },

    /// Response to state request with missing data
    StateResponse {
        responding_node_id: u32,
        requested_data: HashMap<String, VersionedTokenBucket>,
        current_versions: HashMap<u32, u64>, // Our current knowledge
    },

    /// Heartbeat with version vectors for anti-entropy
    Heartbeat {
        node_id: u32,
        timestamp: u64,
        version_vector: HashMap<u32, u64>, // Our knowledge of all nodes
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::limiters::token_bucket::Bucket;
    use crate::limiters::versioned_bucket::VersionedTokenBucket;
    use crate::settings;

    #[test]
    fn test_gossip_packet_serialization() {
        let message = GossipMessage::StateRequest {
            requesting_node_id: 1,
            missing_keys: None,
            since_version: HashMap::new(),
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
                since_version,
            } => {
                assert_eq!(requesting_node_id, 1);
                assert!(missing_keys.is_none());
                assert!(since_version.is_empty());
            }
            _ => panic!("Wrong message type after deserialization"),
        }
    }

    fn get_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            node_id: 1,
        }
    }
}
