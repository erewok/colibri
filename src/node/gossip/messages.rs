//! Gossip Message Protocol
//!
//! Defines all message types for cluster communication.
use std::net::SocketAddr;

use crdts::VClock;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::limiters::distributed_bucket::DistributedBucketExternal;
use crate::limiters::{RuleList, RuleName, SerializableRule};
use crate::node::NodeId;

/// Wire protocol version. Bump this whenever the `GossipMessage` enum or
/// `GossipPacket` layout changes in a backward-incompatible way.
///
/// Since postcard is position-dependent (enum variant discriminants are
/// positional), removing or reordering variants silently corrupts traffic on a
/// mixed-version cluster. Bumping this constant and rejecting mismatches on the
/// receive side turns that silent corruption into a hard, logged error.
pub const GOSSIP_PROTOCOL_VERSION: u16 = 1;

/// Error returned by [`GossipPacket::from_wire`].
#[derive(Debug)]
pub enum GossipDecodeError {
    /// Postcard deserialization failed (truncated, corrupt, or wrong format).
    Decode(postcard::Error),
    /// The packet's protocol version does not match [`GOSSIP_PROTOCOL_VERSION`].
    VersionMismatch { expected: u16, got: u16 },
}

impl std::fmt::Display for GossipDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(e) => write!(f, "gossip decode error: {}", e),
            Self::VersionMismatch { expected, got } => write!(
                f,
                "gossip protocol version mismatch: expected {}, got {}",
                expected, got
            ),
        }
    }
}

impl From<postcard::Error> for GossipDecodeError {
    fn from(e: postcard::Error) -> Self {
        Self::Decode(e)
    }
}

/// Gossip message types for production delta-state protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Delta-state synchronization - only recently updated keys
    DeltaStateSync {
        response_addr: SocketAddr,
        updates: Vec<DistributedBucketExternal>, // Only changed keys
        sender_node_id: NodeId,
        propagation_factor: u8, // To limit spread
    },

    /// Request for specific state (anti-entropy)
    StateRequest {
        missing_keys: Option<Vec<String>>, // None = full sync
        response_addr: SocketAddr,
        requesting_node_id: NodeId,
    },

    /// Response to state request with missing data
    StateResponse {
        response_addr: SocketAddr,
        responding_node_id: NodeId,
        requested_data: DistributedBucketExternal,
    },

    /// Heartbeat with version vectors for anti-entropy
    Heartbeat {
        response_addr: SocketAddr,
        timestamp: u64,
        node_id: NodeId,
        vclock: VClock<NodeId>,
    },

    /// Rate limit configuration synchronization messages
    RateLimitConfigCreate {
        response_addr: SocketAddr,
        sender_node_id: NodeId,
        rule: SerializableRule,
        timestamp: u64,
    },

    RateLimitConfigDelete {
        response_addr: SocketAddr,
        sender_node_id: NodeId,
        rule_name: RuleName,
        timestamp: u64,
    },

    RateLimitConfigRequest {
        response_addr: SocketAddr,
        requesting_node_id: NodeId,
        rule_name: Option<RuleName>, // None = request all rules
    },

    RateLimitConfigResponse {
        response_addr: SocketAddr,
        responding_node_id: NodeId,
        rules: RuleList,
    },
}

/// GossipPacket wraps messages for network transmission.
///
/// Field order matters for postcard: `protocol_version` is first so that a
/// receiver on a different version sees an immediately-wrong version number
/// rather than silently misinterpreting the payload as a different message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipPacket {
    /// Wire protocol version. Always set to [`GOSSIP_PROTOCOL_VERSION`].
    pub protocol_version: u16,
    /// Random ID for receive-side deduplication.
    pub packet_id: u64,
    pub message: GossipMessage,
}

impl GossipPacket {
    /// Create a new gossip packet with a random packet ID.
    pub fn new(message: GossipMessage) -> Self {
        Self {
            protocol_version: GOSSIP_PROTOCOL_VERSION,
            packet_id: rand::random(),
            message,
        }
    }

    /// Create a new gossip packet with a specific ID (useful in tests).
    pub fn new_with_id(message: GossipMessage, packet_id: u64) -> Self {
        Self {
            protocol_version: GOSSIP_PROTOCOL_VERSION,
            packet_id,
            message,
        }
    }

    /// Serialize for INTERNAL cluster communication (UDP gossip).
    pub fn serialize(&self) -> Result<bytes::Bytes, postcard::Error> {
        to_allocvec(self).map(bytes::Bytes::from)
    }

    /// Deserialize and version-check a raw gossip packet from the wire.
    ///
    /// Returns `Err(GossipDecodeError::VersionMismatch)` when the packet's
    /// `protocol_version` differs from [`GOSSIP_PROTOCOL_VERSION`]. This turns
    /// what would otherwise be silent postcard corruption on a mixed-version
    /// cluster into a hard, logged error.
    pub fn from_wire(data: &[u8]) -> Result<Self, GossipDecodeError> {
        let packet: Self = from_bytes(data)?;
        if packet.protocol_version != GOSSIP_PROTOCOL_VERSION {
            return Err(GossipDecodeError::VersionMismatch {
                expected: GOSSIP_PROTOCOL_VERSION,
                got: packet.protocol_version,
            });
        }
        Ok(packet)
    }
}

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

        // Test postcard serialization (for internal cluster communication)
        let serialized = packet.serialize().expect("Failed to serialize packet");
        let deserialized = GossipPacket::from_wire(&serialized).expect("Failed to decode packet");
        assert_eq!(deserialized.protocol_version, GOSSIP_PROTOCOL_VERSION);

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

    #[test]
    fn test_version_mismatch_is_rejected() {
        let message = GossipMessage::StateRequest {
            requesting_node_id: NodeName::from("node-1").node_id(),
            missing_keys: None,
            response_addr: "127.0.0.1:8410".parse().unwrap(),
        };
        // Construct a packet with a stale version number (simulates a node
        // running an old binary sending to a node running this binary).
        let old_packet = GossipPacket {
            protocol_version: 0, // wrong version
            packet_id: 42,
            message,
        };
        let serialized = old_packet.serialize().expect("Failed to serialize");

        let result = GossipPacket::from_wire(&serialized);
        assert!(
            matches!(result, Err(GossipDecodeError::VersionMismatch { expected: GOSSIP_PROTOCOL_VERSION, got: 0 })),
            "expected VersionMismatch error, got {:?}", result.map(|_| ())
        );
    }
}
