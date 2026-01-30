/// Cluster messages are sent over internal transport only.
use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use postcard::{from_bytes, to_allocvec};
use tokio::sync::oneshot;

use crate::error::{ColibriError, Result};
use crate::limiters::{DistributedBucketExternal, NamedRateLimitRule, TokenBucketLimiter};
use crate::node::{NodeName, NodeAddress};
use crate::settings::{RateLimitSettings, RunMode};

// Controllers and Nodes use queuable messages to send/receive messages internally.
/// To receive a response, provide a oneshot sender along with the message.
///
pub type ClusterMessageResult = Result<ClusterMessageResponse>;

pub struct Queueable {
    pub envelope: MessageEnvelope,
    pub response_tx: oneshot::Sender<ClusterMessageResult>,
}

// ============================================================================
// UNIFIED COMMAND TYPE (Phase 1)
// ============================================================================

/// Unified command type for controller queues
/// This replaces both Queueable and raw ClusterMessage in channels.
/// Uses the new Message enum for type-safe communication.
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

    /// Convert from Queueable (for backward compatibility)
    pub fn from_queueable(queueable: Queueable) -> Result<(Self, oneshot::Receiver<Result<Message>>)> {
        let message = Message::from(queueable.envelope.message);
        Ok(Self::new(
            queueable.envelope.from,
            queueable.envelope.to,
            message,
            queueable.envelope.request_id,
        ))
    }
}

/// Serialize using postcard
pub fn serialize<T: Serialize>(msg: &T) -> Result<bytes::Bytes> {
    to_allocvec(msg)
        .map(bytes::Bytes::from)
        .map_err(|e| {
            ColibriError::RateLimit(format!("Failed to serialize request: {}", e))
        })
}

/// Deserialize using postcard
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    from_bytes(data).map_err(|e| ColibriError::from(e))
}


/// Request message for rate limiting over internal cluster transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckCallsRequest {
    pub client_id: String,  // key we rate limit against
    pub rule_name: Option<String>, // None = default rule
    pub consume_token: bool, // true for rate_limit, false for check_limit
}

/// Response message for rate limiting.
/// Serialized to API clients as JSON as well as used internally so derive Serialize/Deserialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub client_id: String,  // key we rate limit against
    pub rule_name: Option<String>, // None = default rule
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
pub struct CreateRateLimitRule{
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
    RateLimitConfigGet(String), // rule name
    RateLimitConfigList, // rule name
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
    RateLimitConfigGetResponse(Option<NamedRateLimitRule>),
    RateLimitConfigListResponse(Vec<NamedRateLimitRule>),
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
// MESSAGE ENVELOPE V2 (Phase 1)
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

    /// Convert from old MessageEnvelope
    pub fn from_v1(v1: MessageEnvelope) -> Self {
        Self {
            from: v1.from,
            to: v1.to,
            message: Message::from(v1.message),
            request_id: v1.request_id,
        }
    }

    /// Convert to old MessageEnvelope (for backward compatibility)
    /// Returns None if the message cannot be converted back
    pub fn to_v1(&self) -> Option<MessageEnvelope> {
        // This is a simplified conversion - in practice you'd need
        // to handle all Message variants and convert them back
        // For Phase 1, we'll just return None for now
        None
    }
}

// ============================================================================
// MESSAGE TYPES
// ============================================================================

/// Unified message type for all cluster communication.
/// This will eventually replace both ClusterMessage and GossipMessage.
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
    CreateRateLimitRule {
        rule_name: String,
        settings: RateLimitSettings
    },
    /// Acknowledge rule creation
    CreateRateLimitRuleResponse,

    /// Delete a named rate limit rule
    DeleteRateLimitRule {
        rule_name: String
    },
    /// Acknowledge rule deletion
    DeleteRateLimitRuleResponse,

    /// Get a specific named rate limit rule
    GetRateLimitRule {
        rule_name: String
    },
    /// Response with rule details (or None if not found)
    GetRateLimitRuleResponse(Option<NamedRateLimitRule>),

    /// List all named rate limit rules
    ListRateLimitRules,
    /// Response with all rules
    ListRateLimitRulesResponse(Vec<NamedRateLimitRule>),

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
        missing_keys: Option<Vec<String>>
    },

    /// Response with requested state data
    GossipStateResponse {
        data: Vec<DistributedBucketExternal>
    },

    /// Heartbeat with vector clock for anti-entropy
    GossipHeartbeat {
        timestamp: u64,
        vclock: crdts::VClock<crate::node::NodeId>
    },

    // ===== Admin operations (both modes) =====

    /// Add a new node to cluster topology
    AddNode {
        name: NodeName,
        address: SocketAddr
    },
    /// Acknowledge node addition
    AddNodeResponse,

    /// Remove a node from cluster topology
    RemoveNode {
        name: NodeName,
        address: SocketAddr
    },
    /// Acknowledge node removal
    RemoveNodeResponse,

    /// Export all rate limiting data (for migration)
    ExportBuckets,
    /// Response with exported data
    ExportBucketsResponse { data: Vec<u8> },

    /// Import rate limiting data (for migration)
    ImportBuckets {
        data: Vec<u8>
    },
    /// Acknowledge import completion
    ImportBucketsResponse,

    // ===== Generic responses =====

    /// Generic acknowledgment
    Ack,
    /// Error response with message
    Error { message: String },
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
// CONVERSION FROM OLD TYPES (for gradual migration)
// ============================================================================

/// Convert old ClusterMessage to new unified Message
impl From<ClusterMessage> for Message {
    fn from(msg: ClusterMessage) -> Self {
        match msg {
            ClusterMessage::RateLimitRequest(req) => Message::RateLimitRequest(req),
            ClusterMessage::StatusRequest => Message::GetStatus,
            ClusterMessage::GetTopology => Message::GetTopology,
            ClusterMessage::TopologyChangeRequest(_req) => {
                // TopologyChangeRequest doesn't have a direct equivalent yet
                // For now, map to GetTopology (which will return current state)
                Message::GetTopology
            },
            ClusterMessage::RateLimitConfigCreate(req) => {
                Message::CreateRateLimitRule {
                    rule_name: req.rule_name,
                    settings: req.settings,
                }
            },
            ClusterMessage::RateLimitConfigDelete(name) => {
                Message::DeleteRateLimitRule { rule_name: name }
            },
            ClusterMessage::RateLimitConfigGet(name) => {
                Message::GetRateLimitRule { rule_name: name }
            },
            ClusterMessage::RateLimitConfigList => Message::ListRateLimitRules,
            ClusterMessage::AdminCommand(cmd) => {
                match cmd {
                    AdminCommand::AddNode { name, address } => {
                        Message::AddNode {
                            name,
                            address
                        }
                    },
                    AdminCommand::RemoveNode { name, address } => {
                        Message::RemoveNode {
                            name,
                            address
                        }
                    },
                    AdminCommand::GetTopology => Message::GetTopology,
                    AdminCommand::ExportBuckets => Message::ExportBuckets,
                    AdminCommand::ImportBuckets { .. } => {
                        // No direct data access in ImportBuckets variant
                        Message::ImportBuckets { data: vec![] }
                    },
                }
            },
            ClusterMessage::StateRequest(_keys) => {
                // Map state request to gossip state request
                Message::GossipStateRequest { missing_keys: None }
            },
        }
    }
}

/// Convert old ClusterMessageResponse to new unified Message
impl From<ClusterMessageResponse> for Message {
    fn from(resp: ClusterMessageResponse) -> Self {
        match resp {
            ClusterMessageResponse::RateLimitResponse(r) => Message::RateLimitResponse(r),
            ClusterMessageResponse::StatusResponse(r) => Message::StatusResponse(r),
            ClusterMessageResponse::TopologyResponse(r) => Message::TopologyResponse(r),
            ClusterMessageResponse::RateLimitConfigCreateResponse => {
                Message::CreateRateLimitRuleResponse
            },
            ClusterMessageResponse::RateLimitConfigDeleteResponse => {
                Message::DeleteRateLimitRuleResponse
            },
            ClusterMessageResponse::RateLimitConfigGetResponse(rule) => {
                Message::GetRateLimitRuleResponse(rule)
            },
            ClusterMessageResponse::RateLimitConfigListResponse(rules) => {
                Message::ListRateLimitRulesResponse(rules)
            },
            ClusterMessageResponse::AdminResponse(status) => {
                Message::StatusResponse(status)
            },
            ClusterMessageResponse::TokenBucketStateResponse(_limiters) => {
                // No direct equivalent, acknowledge receipt
                Message::Ack
            },
            ClusterMessageResponse::DistributedBucketStateResponse(buckets) => {
                // Map to gossip state response
                Message::GossipStateResponse { data: buckets }
            },
        }
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
            Message::Error { message: "test error".to_string() },
            Message::RateLimitRequest(CheckCallsRequest {
                client_id: "test_client".to_string(),
                rule_name: None,
                consume_token: false,
            }),
            Message::CreateRateLimitRule {
                rule_name: "test_rule".to_string(),
                settings: RateLimitSettings::default(),
            },
            Message::ListRateLimitRules,
            Message::AddNode {
                name: NodeName::new("test_node".to_string()),
                address: "127.0.0.1:8000".parse().unwrap(),
            },
        ];

        for msg in messages {
            let serialized = msg.serialize().expect("Failed to serialize");
            let deserialized = Message::deserialize(&serialized)
                .expect("Failed to deserialize");

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
        });

        let serialized = msg.serialize().unwrap();
        // Should be compact (postcard is efficient)
        assert!(serialized.len() < 100);
    }

    #[test]
    fn test_conversion_from_cluster_message() {
        let old_msg = ClusterMessage::GetTopology;
        let new_msg: Message = old_msg.into();
        assert!(matches!(new_msg, Message::GetTopology));

        let old_msg = ClusterMessage::StatusRequest;
        let new_msg: Message = old_msg.into();
        assert!(matches!(new_msg, Message::GetStatus));

        let old_msg = ClusterMessage::RateLimitConfigList;
        let new_msg: Message = old_msg.into();
        assert!(matches!(new_msg, Message::ListRateLimitRules));
    }

    #[test]
    fn test_conversion_from_cluster_message_response() {
        let old_resp = ClusterMessageResponse::RateLimitConfigCreateResponse;
        let new_msg: Message = old_resp.into();
        assert!(matches!(new_msg, Message::CreateRateLimitRuleResponse));

        let old_resp = ClusterMessageResponse::RateLimitConfigDeleteResponse;
        let new_msg: Message = old_resp.into();
        assert!(matches!(new_msg, Message::DeleteRateLimitRuleResponse));
    }

    #[test]
    fn test_admin_command_conversion() {
        let admin_cmd = ClusterMessage::AdminCommand(AdminCommand::AddNode {
            name: "node1".into(),
            address: "127.0.0.1:8000".parse().unwrap(),
        });

        let new_msg: Message = admin_cmd.into();
        assert!(matches!(new_msg, Message::AddNode { .. }));
    }

    #[test]
    fn test_rate_limit_request_conversion() {
        let request = CheckCallsRequest {
            client_id: "user456".to_string(),
            rule_name: Some("api_limit".to_string()),
            consume_token: true,
        };

        let old_msg = ClusterMessage::RateLimitRequest(request.clone());
        let new_msg: Message = old_msg.into();

        match new_msg {
            Message::RateLimitRequest(req) => {
                assert_eq!(req.client_id, "user456");
                assert_eq!(req.rule_name, Some("api_limit".to_string()));
                assert_eq!(req.consume_token, true);
            },
            _ => panic!("Expected RateLimitRequest variant"),
        }
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
    fn test_message_envelope_v2_from_v1() {
        let old_envelope = MessageEnvelope::new(
            NodeName::from("old-sender"),
            NodeName::from("old-receiver"),
            ClusterMessage::StatusRequest,
            300,
        );

        let new_envelope = MessageEnvelopeV2::from_v1(old_envelope);

        assert_eq!(new_envelope.from, NodeName::from("old-sender"));
        assert_eq!(new_envelope.to, NodeName::from("old-receiver"));
        assert_eq!(new_envelope.request_id, 300);
        assert!(matches!(new_envelope.message, Message::GetStatus));
    }

    #[test]
    fn test_command_with_response_channel() {
        let message = Message::RateLimitRequest(CheckCallsRequest {
            client_id: "test-key".to_string(),
            rule_name: None,
            consume_token: false,
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
/// TODO: Implement proper admin response handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    Ack,
    Error { message: String },
    Topology(TopologyResponse),
    Export(BucketExport),
    Status(StatusResponse),
}

/// Bucket export data structure
/// TODO: Define proper bucket export format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketExport {
    pub client_data: Vec<u8>, // TODO: Define proper structure
    pub metadata: ExportMetadata,
}

/// Metadata for bucket exports
/// TODO: Add more metadata fields as needed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportMetadata {
    pub node_name: String,
    pub export_timestamp: u64,
    pub node_type: RunMode,
    pub bucket_count: usize,
}

