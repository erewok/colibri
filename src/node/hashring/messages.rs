use bincode::{Decode, Encode};

use crate::error::{ColibriError, Result};
use crate::node::CheckCallsResponse;

/// Request message for hashring rate limiting over TCP
#[derive(Debug, Clone, Decode, Encode)]
pub struct HashringRequest {
    pub request_id: String,
    pub client_id: String,
    pub consume_token: bool, // true for rate_limit, false for check_limit
}

impl HashringRequest {
    /// Serialize for TCP cluster communication using bincode
    pub fn serialize(&self) -> Result<bytes::Bytes> {
        let config = bincode::config::standard().with_big_endian();
        bincode::encode_to_vec(self, config)
            .map(bytes::Bytes::from)
            .map_err(|e| {
                ColibriError::RateLimit(format!("Failed to serialize hashring request: {}", e))
            })
    }

    /// Deserialize from TCP cluster communication using bincode
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let config = bincode::config::standard().with_big_endian();
        let (result, _) = bincode::decode_from_slice(data, config).map_err(|e| {
            ColibriError::RateLimit(format!("Failed to deserialize hashring request: {}", e))
        })?;
        Ok(result)
    }
}

/// Response message for hashring rate limiting over TCP
#[derive(Debug, Clone, Decode, Encode)]
pub struct HashringResponse {
    pub request_id: String,
    #[bincode(with_serde)]
    pub result: std::result::Result<CheckCallsResponse, String>,
}

impl HashringResponse {
    /// Serialize for TCP cluster communication using bincode
    pub fn serialize(&self) -> Result<bytes::Bytes> {
        let config = bincode::config::standard().with_big_endian();
        bincode::encode_to_vec(self, config)
            .map(bytes::Bytes::from)
            .map_err(|e| {
                ColibriError::RateLimit(format!("Failed to serialize hashring response: {}", e))
            })
    }

    /// Deserialize from TCP cluster communication using bincode
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let config = bincode::config::standard().with_big_endian();
        let (result, _) = bincode::decode_from_slice(data, config).map_err(|e| {
            ColibriError::RateLimit(format!("Failed to deserialize hashring response: {}", e))
        })?;
        Ok(result)
    }
}
