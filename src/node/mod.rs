use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

pub mod cluster;
pub mod gossip;
pub mod hashring;
pub mod node_id;
pub mod single_node;

use crate::error::Result;
use crate::{rate_limit, settings};
pub use gossip::GossipNode;
pub use hashring::HashringNode;
pub use node_id::{generate_node_id, generate_node_id_from_url, validate_node_id};
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
    fn expire_keys(&self);
}

#[derive(Clone, Debug)]
pub enum NodeWrapper {
    Single(SingleNode),
    Gossip(GossipNode),
    Hashring(HashringNode),
}

impl NodeWrapper {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        // A rate_limiter holds rate-limiting data in memory
        let rate_limiter = Arc::new(RwLock::new(rate_limit::RateLimiter::new(
            settings.rate_limit_settings(),
        )));

        if settings.topology.is_empty() {
            info!("Starting in single-node mode (no other nodes specified)");
            Ok(Self::Single(SingleNode { rate_limiter }))
        } else {
            match settings.run_mode {
                settings::RunMode::Single => {
                    info!("Starting in single-node mode (ignoring specified topology)");
                    Ok(Self::Single(SingleNode { rate_limiter }))
                }
                settings::RunMode::Gossip => {
                    info!(
                        "Starting in gossip mode with {} other nodes: {:?}",
                        settings.topology.len(),
                        settings.topology
                    );
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
                    let hashring_node = HashringNode::new(settings, rate_limiter)?;
                    Ok(Self::Hashring(hashring_node))
                }
            }
        }
    }

    pub fn expire_keys(&self) {
        match self {
            Self::Single(node) => node.expire_keys(),
            Self::Gossip(node) => node.expire_keys(),
            Self::Hashring(node) => node.expire_keys(),
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
