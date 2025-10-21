use std::sync::Arc;
use tokio::sync::RwLock;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

pub mod cluster;
pub mod gossip;
pub mod hashring;
pub mod node_id;
pub mod single_node;

use crate::error::Result;
use crate::limiters::rate_limit;
use crate::limiters::token_bucket::TokenBucket;
use crate::limiters::versioned_bucket::VersionedTokenBucket;
use crate::settings;
pub use gossip::GossipNode;
pub use hashring::HashringNode;
pub use node_id::{
    generate_node_id, generate_node_id_from_socket_addr, generate_node_id_from_url,
    validate_node_id,
};
pub use single_node::SingleNode;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub client_id: String,
    pub calls_remaining: u32,
}

#[async_trait]
pub trait Node {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse>;
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>>;
    async fn expire_keys(&self);
}

#[derive(Clone, Debug)]
pub enum NodeWrapper {
    Single(SingleNode),
    Gossip(GossipNode),
    Hashring(HashringNode),
}

impl NodeWrapper {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let rl_settings = settings.rate_limit_settings();
        if settings.topology.is_empty() {
            info!("Starting in single-node mode (no other nodes specified)");
            // A rate_limiter holds rate-limiting data in memory
            let rate_limiter: Arc<RwLock<rate_limit::RateLimiter<TokenBucket>>> =
                Arc::new(RwLock::new(rate_limit::RateLimiter::new(rl_settings)));
            Ok(Self::Single(SingleNode { rate_limiter }))
        } else {
            match settings.run_mode {
                settings::RunMode::Single => {
                    info!("Starting in single-node mode (ignoring specified topology)");
                    let rate_limiter: Arc<RwLock<rate_limit::RateLimiter<TokenBucket>>> =
                        Arc::new(RwLock::new(rate_limit::RateLimiter::new(rl_settings)));
                    Ok(Self::Single(SingleNode { rate_limiter }))
                }
                settings::RunMode::Gossip => {
                    info!(
                        "Starting in gossip mode with {} other nodes: {:?}",
                        settings.topology.len(),
                        settings.topology
                    );
                    let rate_limiter: Arc<RwLock<rate_limit::RateLimiter<VersionedTokenBucket>>> =
                        Arc::new(RwLock::new(rate_limit::RateLimiter::new(rl_settings)));
                    // Use GossipNode instead of broken MultiNode
                    let gossip_node = GossipNode::new(settings, rate_limiter).await?;
                    Ok(Self::Gossip(gossip_node))
                }
                settings::RunMode::Hashring => {
                    info!(
                        "Starting in hashring mode with {} other nodes: {:?}",
                        settings.topology.len(),
                        settings.topology
                    );
                    // Build hashring node
                    let rate_limiter: Arc<RwLock<rate_limit::RateLimiter<TokenBucket>>> =
                        Arc::new(RwLock::new(rate_limit::RateLimiter::new(rl_settings)));
                    let hashring_node = HashringNode::new(settings, rate_limiter)?;
                    Ok(Self::Hashring(hashring_node))
                }
            }
        }
    }

    pub async fn expire_keys(&self) {
        match self {
            Self::Single(node) => node.expire_keys().await,
            Self::Gossip(node) => node.expire_keys().await,
            Self::Hashring(node) => node.expire_keys().await,
        }
    }

    pub async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        match self {
            Self::Single(node) => node.check_limit(client_id).await,
            Self::Gossip(node) => node.check_limit(client_id).await,
            Self::Hashring(node) => node.check_limit(client_id).await,
        }
    }
    pub async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        match self {
            Self::Single(node) => node.rate_limit(client_id).await,
            Self::Gossip(node) => node.rate_limit(client_id).await,
            Self::Hashring(node) => node.rate_limit(client_id).await,
        }
    }
}
