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
    AddNode { name: String, address: SocketAddr },
    /// Remove a node from cluster membership
    RemoveNode { name: String, address: SocketAddr },
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
// NEW UNIFIED MESSAGE TYPES (Phase 1)
// ============================================================================

/// Unified message type for all cluster communication.
/// This will eventually replace both ClusterMessage and GossipMessage.
///
/// Design: Single enum covering all communication patterns:
/// - Rate limiting operations (both gossip and hashring modes)
/// - Configuration operations (both modes)
/// - Cluster operations (both modes)
/// - Gossip-specific operations (gossip mode only)
/// - Admin operations (both modes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // ===== Rate limiting operations (both modes) =====

    /// Request to check or consume rate limit tokens
    RateLimitRequest(CheckCallsRequest),
    /// Response with remaining tokens
    RateLimitResponse(CheckCallsResponse),

    // ===== Configuration operations (both modes) =====

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
                            name: NodeName::new(name),
                            address
                        }
                    },
                    AdminCommand::RemoveNode { name, address } => {
                        Message::RemoveNode {
                            name: NodeName::new(name),
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
            name: "node1".to_string(),
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
}
