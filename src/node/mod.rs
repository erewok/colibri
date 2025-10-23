use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

pub mod cluster;
pub mod gossip;
pub mod hashring;
pub mod node_id;
pub mod single_node;

use crate::error::Result;
use crate::limiters::epoch_bucket::EpochTokenBucket;
use crate::limiters::rate_limit;
use crate::limiters::token_bucket::{Bucket, TokenBucket};
use crate::settings;
pub use gossip::GossipNode;
pub use hashring::HashringNode;
pub use node_id::{
    generate_node_id, generate_node_id_from_socket_addr, generate_node_id_from_url,
    validate_node_id, NodeId,
};
pub use single_node::SingleNode;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    pub client_id: String,
    pub calls_remaining: u32,
}

#[async_trait]
pub trait Node<T: Bucket + Clone> {
    async fn new(
        settings: settings::Settings,
        rate_limiter: rate_limit::RateLimiter<T>,
    ) -> Result<Self>
    where
        Self: Sized;
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse>;
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>>;
    async fn expire_keys(&self) -> Result<()>;
}

#[derive(Clone, Debug)]
pub enum NodeWrapper {
    Single(Arc<SingleNode>),
    Gossip(Arc<GossipNode>),
    Hashring(Arc<HashringNode>),
}

impl NodeWrapper {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_id = settings.node_id();
        let rl_settings = settings.rate_limit_settings();
        if settings.topology.is_empty() {
            info!(
                "[Node<{}>] Starting in single-node mode (ignoring specified topology)",
                node_id
            );
            // A rate_limiter holds rate-limiting data in memory
            let rate_limiter: rate_limit::RateLimiter<TokenBucket> =
                rate_limit::RateLimiter::new(node_id, rl_settings);
            Ok(Self::Single(Arc::new(
                SingleNode::new(settings, rate_limiter).await?,
            )))
        } else {
            match settings.run_mode {
                settings::RunMode::Single => {
                    info!(
                        "[Node<{}>] Starting in single-node mode (ignoring specified topology)",
                        node_id
                    );
                    let rate_limiter: rate_limit::RateLimiter<TokenBucket> =
                        rate_limit::RateLimiter::new(node_id, rl_settings);
                    Ok(Self::Single(Arc::new(
                        SingleNode::new(settings, rate_limiter).await?,
                    )))
                }
                settings::RunMode::Gossip => {
                    info!(
                        "[Node<{}>] Starting in gossip mode with {} other nodes: {:?}",
                        node_id,
                        settings.topology.len(),
                        settings.topology
                    );
                    let rate_limiter: rate_limit::RateLimiter<EpochTokenBucket> =
                        rate_limit::RateLimiter::new(node_id, rl_settings);
                    // Use GossipNode instead of broken MultiNode
                    let gossip_node = Arc::new(GossipNode::new(settings, rate_limiter).await?);
                    Ok(Self::Gossip(gossip_node))
                }
                settings::RunMode::Hashring => {
                    info!(
                        "[Node<{}>] Starting in hashring mode with {} other nodes: {:?}",
                        node_id,
                        settings.topology.len(),
                        settings.topology
                    );
                    // Build hashring node
                    let rate_limiter: rate_limit::RateLimiter<TokenBucket> =
                        rate_limit::RateLimiter::new(node_id, rl_settings);
                    let hashring_node = HashringNode::new(settings, rate_limiter).await?;
                    Ok(Self::Hashring(Arc::new(hashring_node)))
                }
            }
        }
    }

    pub async fn expire_keys(&self) -> Result<()> {
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
