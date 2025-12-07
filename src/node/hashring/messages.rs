use std::net::SocketAddr;

use bincode::{Decode, Encode};
use tokio::sync::oneshot;

use crate::cluster::{AdminCommand, AdminResponse};
use crate::error::{ColibriError, Result};
use crate::node::CheckCallsResponse;
use crate::settings;

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

/// Commands that can be sent to the HashringController
#[derive(Debug)]
pub enum HashringCommand {
    /// Check remaining calls for a client
    CheckLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<CheckCallsResponse>>,
    },
    /// Apply rate limiting for a client
    RateLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Expire old keys
    ExpireKeys,
    /// Create a named rate limiting rule
    CreateNamedRule {
        rule_name: String,
        settings: settings::RateLimitSettings,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Delete a named rate limiting rule
    DeleteNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// List all named rate limiting rules
    ListNamedRules {
        resp_chan: oneshot::Sender<Result<Vec<settings::NamedRateLimitRule>>>,
    },
    /// Get a specific named rule
    GetNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<Option<settings::NamedRateLimitRule>>>,
    },
    /// Apply custom rate limiting with a named rule
    RateLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Check custom rate limiting with a named rule
    CheckLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<Result<CheckCallsResponse>>,
    },
    /// Handle admin commands from cluster
    AdminCommand {
        command: AdminCommand,
        source: SocketAddr,
        resp_chan: oneshot::Sender<Result<AdminResponse>>,
    },
    /// Export bucket data for cluster migration
    ExportBuckets {
        resp_chan: oneshot::Sender<Result<crate::cluster::BucketExport>>,
    },
    /// Import bucket data from cluster migration
    ImportBuckets {
        import_data: crate::cluster::BucketExport,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Get cluster health status
    ClusterHealth {
        resp_chan: oneshot::Sender<Result<crate::cluster::StatusResponse>>,
    },
    /// Get current topology
    GetTopology {
        resp_chan: oneshot::Sender<Result<crate::cluster::TopologyResponse>>,
    },
    /// Handle topology change
    NewTopology {
        request: crate::cluster::TopologyChangeRequest,
        resp_chan: oneshot::Sender<Result<crate::cluster::TopologyResponse>>,
    },
}
