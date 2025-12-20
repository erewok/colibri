/// Cluster messages are sent over internal transport only.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use postcard::{from_bytes, to_allocvec};

use crate::error::{ColibriError, Result};
use crate::node::NodeName;
use crate::settings::RunMode;


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
    from_bytes(data).map_err(|e| {
        ColibriError::RateLimit(format!("Failed to deserialize request: {}", e))
    })
}


/// Request message for rate limiting over internal cluster transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckCallsRequest {
    pub request_id: u64,
    pub client_id: String,  // key we rate limit against
    pub rule_name: Option<String>, // None = default rule
    pub consume_token: bool, // true for rate_limit, false for check_limit
}

/// Response message for rate limiting.
/// Serialized to API clients as JSON as well as used internally so derive Serialize/Deserialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub request_id: u64,
    pub client_id: String,  // key we rate limit against
    pub rule_name: Option<String>, // None = default rule
    pub calls_remaining: u32,
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
pub enum Status {
    Healthy,
    Unhealthy(String), // reason
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyChangeRequest {
    pub request_id: u64,
    pub topology: HashMap<String, String>,
}


/// Cluster health and status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyResponse {
    pub request_id: u64,
    pub status: StatusResponse,
    pub topology: HashMap<String, String>,
}
