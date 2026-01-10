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
