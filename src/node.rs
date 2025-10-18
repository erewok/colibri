use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::Result;
use crate::gossip::GossipNode;
use crate::hashring::HashringNode;
use crate::single_node::SingleNode;
use crate::{cli, rate_limit};

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
    pub async fn new(settings: cli::Cli) -> Result<Self> {
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
            match settings.multi_mode {
                cli::MultiMode::Gossip => {
                    info!(
                        "Starting in pure gossip mode with {} other nodes: {:?}",
                        valid_topology.len(),
                        valid_topology
                    );
                    // Use GossipNode instead of broken MultiNode
                    let gossip_node =
                        GossipNode::new(settings.listen_port, valid_topology, rate_limiter).await?;
                    Ok(Self::Gossip(gossip_node))
                }
                cli::MultiMode::Hashring => {
                    info!(
                        "Starting in hashring mode with {} other nodes: {:?}",
                        valid_topology.len(),
                        valid_topology
                    );
                    // Build hashring topology
                    let mut topology_map: std::collections::HashMap<u32, String> =
                        std::collections::HashMap::new();
                    for (index, host) in valid_topology.iter().enumerate() {
                        topology_map.insert(index as u32, host.clone());
                    }
                    // Assume this node's ID is the last in the list
                    let node_id = (valid_topology.len() - 1) as u32;
                    let hashring_node = HashringNode {
                        topology: topology_map,
                        node_id,
                        rate_limiter,
                    };
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
