use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{event, info, Level};

use crate::error::Result;
use crate::gossip::PureGossipNode;
use crate::{cli, rate_limit};

#[derive(Clone, Debug)]
pub enum NodeWrapper {
    Single(SingleNode),
    PureGossip(PureGossipNode),
}

impl NodeWrapper {
    pub fn new(settings: cli::Cli) -> Result<Self> {
        // A rate_limiter holds rate-limiting data in memory
        let rate_limiter = Arc::new(RwLock::new(rate_limit::RateLimiter::new(
            settings.rate_limit_settings(),
        )));

        // Filter out empty strings from topology
        let valid_topology: Vec<String> = settings
            .topology
            .iter()
            .filter(|host| !host.trim().is_empty())
            .map(|host| host.to_string())
            .collect();

        if valid_topology.is_empty() {
            info!("Starting in single-node mode (no other nodes specified)");
            Ok(Self::Single(SingleNode { rate_limiter }))
        } else {
            info!(
                "Starting in pure gossip mode with {} other nodes: {:?}",
                valid_topology.len(),
                valid_topology
            );
            // Use PureGossipNode instead of broken MultiNode
            let gossip_node = PureGossipNode::new(valid_topology, rate_limiter)?;
            Ok(Self::PureGossip(gossip_node))
        }
    }

    pub fn expire_keys(&self) {
        match self {
            Self::Single(node) => node.expire_keys(),
            Self::PureGossip(node) => node.expire_keys(),
        }
    }

    pub async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        match self {
            Self::Single(node) => node.check_limit(client_id).await,
            Self::PureGossip(node) => node.check_limit(client_id).await,
        }
    }
    pub async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        match self {
            Self::Single(node) => node.rate_limit(client_id).await,
            Self::PureGossip(node) => node.rate_limit(client_id).await,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub client_id: String,
    pub calls_remaining: u32,
}

#[async_trait]
pub trait Node {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse>;
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>>;
    fn expire_keys(&self);
}

#[derive(Clone, Debug)]
pub struct SingleNode {
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        local_check_limit(client_id, self.rate_limiter.clone())
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        local_rate_limit(client_id, self.rate_limiter.clone())
    }

    fn expire_keys(&self) {
        match self.rate_limiter.write() {
            Ok(mut rate_limiter) => {
                rate_limiter.expire_keys();
            }
            Err(err) => {
                event!(
                    Level::ERROR,
                    message = "Failed expiring keys",
                    err = format!("{:?}", err)
                );
            }
        }
    }
}

fn local_check_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<CheckCallsResponse> {
    match rate_limiter.read() {
        Ok(rate_limiter) => {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(client_id.as_str());
            Ok(CheckCallsResponse {
                client_id,
                calls_remaining,
            })
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed checking limit",
                err = format!("{:?}", err)
            );
            Err(crate::concurrency_error!("Failed to access rate_limiter"))
        }
    }
}

fn local_rate_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<Option<CheckCallsResponse>> {
    match rate_limiter.write() {
        Ok(mut rate_limiter) => {
            let calls_left = rate_limiter.limit_calls_for_client(client_id.to_string());
            if let Some(calls_remaining) = calls_left {
                Ok(Some(CheckCallsResponse {
                    client_id,
                    calls_remaining,
                }))
            } else {
                Ok(None)
            }
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed applying rate limit",
                err = format!("{:?}", err)
            );
            Err(crate::concurrency_error!("Failed to access rate_limiter"))
        }
    }
}
