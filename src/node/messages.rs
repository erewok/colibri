/// Cluster messages are sent over internal transport only.
use std::collections::HashMap;
use std::net::SocketAddr;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::error::{ColibriError, Result};
use crate::limiters::{rules, DistributedBucketExternal, TokenBucketLimiter};
use crate::node::{NodeAddress, NodeId, NodeName};
use crate::settings::{RateLimitSettings, RunMode};

// Controllers and Nodes use queuable messages to send/receive messages internally.
/// To receive a response, provide a oneshot sender along with the message.
///
pub type ClusterMessageResult = Result<ClusterMessageResponse>;

pub struct Queueable {
    pub envelope: MessageEnvelope,
    pub response_tx: oneshot::Sender<ClusterMessageResult>,
}

/// Unified command type for controller queues
pub struct Command {
    pub envelope: MessageEnvelopeV2,
    pub response_tx: oneshot::Sender<Result<Message>>,
}

impl Command {
    /// Create a new command with a Message
    pub fn new(
        from: NodeName,
        to: NodeName,
        message: Message,
        request_id: u64,
    ) -> (Self, oneshot::Receiver<Result<Message>>) {
        let envelope = MessageEnvelopeV2 {
            from,
            to,
            message,
            request_id,
        };

        let (tx, rx) = oneshot::channel();

        (
            Self {
                envelope,
                response_tx: tx,
            },
            rx,
        )
    }
}

/// Serialize using postcard
pub fn serialize<T: Serialize>(msg: &T) -> Result<bytes::Bytes> {
    to_allocvec(msg)
        .map(bytes::Bytes::from)
        .map_err(|e| ColibriError::RateLimit(format!("Failed to serialize request: {}", e)))
}

/// Deserialize using postcard
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    from_bytes(data).map_err(ColibriError::from)
}

/// Maximum forwarding depth to prevent loops in case of topology inconsistency
pub const MAX_FORWARDING_DEPTH: u8 = 3;

/// Request message for rate limiting over internal cluster transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckCallsRequest {
    pub client_id: String,                  // key we rate limit against
    pub rule_name: Option<rules::RuleName>, // None = default rule
    pub consume_token: bool,                // true for rate_limit, false for check_limit
    pub forwarding_depth: u8,               // track forwarding hops to prevent loops
}

/// Response message for rate limiting.
/// Serialized to API clients as JSON as well as used internally so derive Serialize/Deserialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub client_id: String,                  // key we rate limit against
    pub rule_name: Option<rules::RuleName>, // None = default rule
    pub calls_remaining: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Status {
    Healthy,
    Unhealthy(String), // reason
}

/// Cluster health and status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub node_name: NodeName,
    pub node_type: RunMode,
    pub status: Status,
    pub bucket_count: Option<usize>,
    pub last_topology_change: Option<u64>,
    pub errors: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyChangeRequest(pub HashMap<NodeName, NodeAddress>);

/// Cluster health and status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyResponse {
    pub status: StatusResponse,
    pub topology: HashMap<NodeName, NodeAddress>,
}

/// Cluster health and status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRateLimitRule {
    pub rule_name: String,
    pub settings: RateLimitSettings,
}

/// Administrative commands
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminCommand {
    /// Add a new node to cluster membership
    AddNode { name: NodeName, address: SocketAddr },
    /// Remove a node from cluster membership
    RemoveNode { name: NodeName, address: SocketAddr },
    /// Export all rate limiting data (for cluster migration)
    ExportBuckets,
    /// Import rate limiting data (for cluster migration)
    ImportBuckets { filename: String, run_mode: RunMode },
    /// Get current topology information
    GetTopology,
}

/// Gossip message types for production delta-state protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    AdminCommand(AdminCommand),
    StatusRequest,
    GetTopology,
    TopologyChangeRequest(TopologyChangeRequest),
    // Rate limit configuration synchronization messages
    RateLimitConfigCreate(CreateRateLimitRule),
    RateLimitConfigDelete(String), // rule name
    RateLimitConfigGet(String),    // rule name
    RateLimitConfigList,           // rule name
    RateLimitRequest(CheckCallsRequest),
    // Request for specific state
    StateRequest(Vec<String>), // keys requested
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessageResponse {
    // Node + cluster info
    AdminResponse(StatusResponse),
    StatusResponse(StatusResponse),
    TopologyResponse(TopologyResponse),
    // Rate limit responses
    RateLimitResponse(CheckCallsResponse),
    RateLimitConfigCreateResponse,
    RateLimitConfigDeleteResponse,
    RateLimitConfigGetResponse(Option<rules::SerializableRule>),
    RateLimitConfigListResponse(Vec<rules::SerializableRule>),
    /// Response to state request with missing data
    TokenBucketStateResponse(Vec<TokenBucketLimiter>),
    DistributedBucketStateResponse(Vec<DistributedBucketExternal>),
}

/// Message routing and delivery information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// Who sent this message
    pub from: NodeName,
    /// Who should receive this message
    pub to: NodeName,
    /// The actual message
    pub message: ClusterMessage,
    /// Simple lamport-clock timestamp for ordering
    pub request_id: u64,
}

impl MessageEnvelope {
    /// Create a new message envelope
    pub fn new(from: NodeName, to: NodeName, message: ClusterMessage, request_id: u64) -> Self {
        Self {
            from,
            to,
            message,
            request_id,
        }
    }
    pub fn from_incoming_message(data: bytes::Bytes) -> Result<Self> {
        deserialize(&data)
    }
}

// ============================================================================
// MESSAGE ENVELOPE
// ============================================================================

/// Message routing and delivery information using new Message enum
/// This is the V2 envelope that uses the unified Message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelopeV2 {
    /// Who sent this message
    pub from: NodeName,
    /// Who should receive this message
    pub to: NodeName,
    /// The actual message (unified Message enum)
    pub message: Message,
    /// Lamport clock timestamp for ordering
    pub request_id: u64,
}

impl MessageEnvelopeV2 {
    /// Create a new message envelope
    pub fn new(from: NodeName, to: NodeName, message: Message, request_id: u64) -> Self {
        Self {
            from,
            to,
            message,
            request_id,
        }
    }

    /// Deserialize from incoming bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        deserialize(data)
    }

    /// Serialize for sending
    pub fn to_bytes(&self) -> Result<bytes::Bytes> {
        serialize(self)
    }
}

// ============================================================================
// MESSAGE TYPES
// ============================================================================

/// Unified message type for all cluster communication.
///
/// Design: Single enum covering all communication patterns:
/// - Rate limiting operations (both gossip and hashring modes)
/// - Configuration operations (both modes)
/// - Cluster operations (both modes)
/// - Gossip-specific operations (gossip mode only)
/// - Admin operations (all modes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // ===== Rate limiting operations =====
    /// Request to check or consume rate limit tokens
    RateLimitRequest(CheckCallsRequest),
    /// Response with remaining tokens
    RateLimitResponse(CheckCallsResponse),

    ExpireKeys, // Internal message to trigger key expiry
    // ===== Configuration operations =====
    /// Create a new named rate limit rule
    CreateRateLimitRule(rules::SerializableRule),
    /// Acknowledge rule creation
    CreateRateLimitRuleResponse,

    /// Delete a named rate limit rule
    DeleteRateLimitRule {
        rule_name: rules::RuleName,
    },
    /// Acknowledge rule deletion
    DeleteRateLimitRuleResponse,

    /// Get a specific named rate limit rule
    GetRateLimitRule {
        rule_name: rules::RuleName,
    },
    /// Response with rule details (or None if not found)
    GetRateLimitRuleResponse(Option<rules::SerializableRule>),

    /// List all named rate limit rules
    ListRateLimitRules,
    /// Response with all rules
    ListRateLimitRulesResponse(rules::RuleList),

    // ===== Cluster operations (both modes) =====
    /// Request current cluster topology
    GetTopology,
    /// Response with topology information
    TopologyResponse(TopologyResponse),

    /// Request node status/health
    GetStatus,
    /// Response with status information
    StatusResponse(StatusResponse),

    // ===== Gossip-specific operations (gossip mode only) =====
    /// Delta-state synchronization with recent updates
    GossipDeltaSync {
        updates: Vec<DistributedBucketExternal>,
        propagation_factor: u8,
    },

    /// Request for missing state data (anti-entropy)
    GossipStateRequest {
        missing_keys: Option<Vec<String>>,
    },

    /// Response with requested state data
    GossipStateResponse {
        data: Vec<DistributedBucketExternal>,
    },

    /// Heartbeat with vector clock for anti-entropy
    GossipHeartbeat {
        timestamp: u64,
        vclock: crdts::VClock<NodeId>,
    },

    // ===== Admin operations (both modes) =====
    /// Add a new node to cluster topology
    AddNode {
        name: NodeName,
        address: SocketAddr,
    },
    /// Acknowledge node addition
    AddNodeResponse,

    /// Remove a node from cluster topology
    RemoveNode {
        name: NodeName,
        address: SocketAddr,
    },
    /// Acknowledge node removal
    RemoveNodeResponse,

    /// Export all rate limiting data (for migration)
    ExportBuckets,
    /// Response with exported data
    ExportBucketsResponse {
        data: Vec<u8>,
    },

    /// Import rate limiting data (for migration)
    ImportBuckets {
        data: Vec<u8>,
    },
    /// Acknowledge import completion
    ImportBucketsResponse,

    // ===== Generic responses =====
    /// Generic acknowledgment
    Ack,
    /// Error response with message
    Error {
        message: String,
    },
}

impl Message {
    /// Serialize message using postcard binary format
    pub fn serialize(&self) -> Result<bytes::Bytes> {
        serialize(self)
    }

    /// Deserialize message using postcard binary format
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        deserialize(data)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod message_tests {
    use super::*;

    #[test]
    fn test_message_serialization_roundtrip() {
        let messages = vec![
            Message::GetTopology,
            Message::GetStatus,
            Message::Ack,
            Message::Error {
                message: "test error".to_string(),
            },
            Message::RateLimitRequest(CheckCallsRequest {
                client_id: "test_client".to_string(),
                rule_name: None,
                consume_token: false,
                forwarding_depth: 0,
            }),
            Message::CreateRateLimitRule(rules::SerializableRule {
                name: "test_rule".into(),
                settings: RateLimitSettings::default(),
            }),
            Message::ListRateLimitRules,
            Message::AddNode {
                name: NodeName::new("test_node".to_string()),
                address: "127.0.0.1:8000".parse().unwrap(),
            },
        ];

        for msg in messages {
            let serialized = msg.serialize().expect("Failed to serialize");
            let deserialized = Message::deserialize(&serialized).expect("Failed to deserialize");

            // Verify successful roundtrip by checking discriminant
            assert_eq!(
                std::mem::discriminant(&msg),
                std::mem::discriminant(&deserialized)
            );
        }
    }

    #[test]
    fn test_message_serialization_size() {
        // Verify messages serialize to reasonable sizes
        let msg = Message::RateLimitRequest(CheckCallsRequest {
            client_id: "user123".to_string(),
            rule_name: None,
            consume_token: true,
            forwarding_depth: 0,
        });

        let serialized = msg.serialize().unwrap();
        // Should be compact (postcard is efficient)
        assert!(serialized.len() < 100);
    }

    // ============================================================================
    // COMMAND AND MESSAGE ENVELOPE V2 TESTS
    // ============================================================================

    #[test]
    fn test_command_creation() {
        let message = Message::GetStatus;
        let (command, rx) = Command::new(
            NodeName::from("node-a"),
            NodeName::from("node-b"),
            message,
            42,
        );

        assert_eq!(command.envelope.from, NodeName::from("node-a"));
        assert_eq!(command.envelope.to, NodeName::from("node-b"));
        assert_eq!(command.envelope.request_id, 42);
        assert!(matches!(command.envelope.message, Message::GetStatus));

        // Receiver should be available
        drop(rx); // Just verify it exists
    }

    #[test]
    fn test_message_envelope_v2_creation() {
        let message = Message::RateLimitRequest(CheckCallsRequest {
            client_id: "test".to_string(),
            rule_name: None,
            consume_token: true,
            forwarding_depth: 0,
        });

        let envelope = MessageEnvelopeV2::new(
            NodeName::from("sender"),
            NodeName::from("receiver"),
            message.clone(),
            100,
        );

        assert_eq!(envelope.from, NodeName::from("sender"));
        assert_eq!(envelope.to, NodeName::from("receiver"));
        assert_eq!(envelope.request_id, 100);
        assert!(matches!(envelope.message, Message::RateLimitRequest(_)));
    }

    #[test]
    fn test_message_envelope_v2_serialization() {
        let message = Message::GetTopology;
        let envelope = MessageEnvelopeV2::new(
            NodeName::from("node-x"),
            NodeName::from("node-y"),
            message,
            200,
        );

        let serialized = envelope.to_bytes();
        assert!(serialized.is_ok());

        let bytes = serialized.unwrap();
        let deserialized = MessageEnvelopeV2::from_bytes(&bytes);
        assert!(deserialized.is_ok());

        let restored = deserialized.unwrap();
        assert_eq!(restored.from, NodeName::from("node-x"));
        assert_eq!(restored.to, NodeName::from("node-y"));
        assert_eq!(restored.request_id, 200);
        assert!(matches!(restored.message, Message::GetTopology));
    }

    #[test]
    fn test_command_with_response_channel() {
        let message = Message::RateLimitRequest(CheckCallsRequest {
            client_id: "test-key".to_string(),
            rule_name: None,
            consume_token: false,
            forwarding_depth: 0,
        });

        let (command, mut rx) = Command::new(
            NodeName::from("requester"),
            NodeName::from("responder"),
            message,
            500,
        );

        // Simulate sending a response
        let response = Message::RateLimitResponse(CheckCallsResponse {
            client_id: "test-key".to_string(),
            rule_name: None,
            calls_remaining: 100,
        });

        command.response_tx.send(Ok(response)).unwrap();

        // Verify we can receive the response
        let received = rx.try_recv();
        assert!(received.is_ok());
        assert!(matches!(
            received.unwrap().unwrap(),
            Message::RateLimitResponse(_)
        ));
    }
}

// ============================================================================
// ADDITIONAL TYPES
// ============================================================================

/// Admin command responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    Ack,
    Error { message: String },
    Topology(TopologyResponse),
    Export(BucketExport),
    Status(StatusResponse),
}

/// Bucket export data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketExport {
    pub client_data: Vec<u8>,
    pub metadata: ExportMetadata,
}

/// Metadata for bucket exports
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportMetadata {
    pub node_name: String,
    pub export_timestamp: u64,
    pub node_type: RunMode,
    pub bucket_count: usize,
}
