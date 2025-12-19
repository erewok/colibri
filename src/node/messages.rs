/// Cluster messages are sent over internal transport only.
use std::collections::HashMap;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::error::{ColibriError, Result};
use crate::node::NodeName;
use crate::settings::RunMode;


/// Serialize using bincode
pub fn serialize<T: Encode>(msg: &T) -> Result<bytes::Bytes> {
    let config = bincode::config::standard().with_big_endian();
    bincode::encode_to_vec(msg, config)
        .map(bytes::Bytes::from)
        .map_err(|e| {
            ColibriError::RateLimit(format!("Failed to serialize request: {}", e))
        })
}

/// Deserialize using bincode
pub fn deserialize<T: Decode<()>>(data: &[u8]) -> Result<T> {
    let config = bincode::config::standard().with_big_endian();
    let (result, _) = bincode::decode_from_slice(data, config).map_err(|e| {
        ColibriError::RateLimit(format!("Failed to deserialize request: {}", e))
    })?;
    Ok(result)
}


/// Request message for rate limiting over internal cluster transport
#[derive(Debug, Clone, Decode, Encode)]
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
#[derive(Debug, Clone, Encode, Decode)]
pub struct StatusResponse {
    #[bincode(with_serde)]
    pub node_name: NodeName,
    #[bincode(with_serde)]
    pub node_type: RunMode,
    pub status: Status,
    pub bucket_count: Option<usize>,
    pub last_topology_change: Option<u64>,
    pub errors: Option<Vec<String>>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Status {
    Healthy,
    Unhealthy(String), // reason
}


#[derive(Debug, Clone, Encode, Decode)]
pub struct TopologyChangeRequest {
    pub request_id: u64,
    pub topology: HashMap<String, String>,
}


/// Cluster health and status information
#[derive(Debug, Clone, Encode, Decode)]
pub struct TopologyResponse {
    pub request_id: u64,
    pub status: StatusResponse,
    pub topology: HashMap<String, String>,
}
