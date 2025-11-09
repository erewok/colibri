use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

pub mod cluster;
pub mod gossip;
pub mod hashring;
pub mod node_id;
pub mod single_node;

use crate::error::Result;
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
pub trait Node {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self>
    where
        Self: Sized;
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse>;
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>>;
    async fn expire_keys(&self) -> Result<()>;

    // New methods for named rules
    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()>;
    async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>>;
    async fn delete_named_rule(&self, rule_name: String) -> Result<()>;
    async fn get_named_rule(&self, rule_name: String) -> Result<settings::NamedRateLimitRule>;
    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>>;
    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse>;
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
        // A rate_limiter holds all rate-limiting data in memory: no data persisted to disk!
        if settings.topology.is_empty() {
            Ok(Self::Single(Arc::new(
                SingleNode::new(node_id, settings).await?,
            )))
        } else {
            match settings.run_mode {
                settings::RunMode::Single => Ok(Self::Single(Arc::new(
                    SingleNode::new(node_id, settings).await?,
                ))),
                settings::RunMode::Gossip => {
                    warn!(
                        "[Node<{}>] Gossip mode is experimental and likely does not work correctly!",
                        node_id
                    );
                    // Use GossipNode instead of broken MultiNode
                    let gossip_node = Arc::new(GossipNode::new(node_id, settings).await?);
                    Ok(Self::Gossip(gossip_node))
                }
                settings::RunMode::Hashring => {
                    // Build hashring node
                    let hashring_node = HashringNode::new(node_id, settings).await?;
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

    pub async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        match self {
            Self::Single(node) => node.create_named_rule(rule_name, settings).await,
            Self::Gossip(node) => node.create_named_rule(rule_name, settings).await,
            Self::Hashring(node) => node.create_named_rule(rule_name, settings).await,
        }
    }

    pub async fn get_named_rule(&self, rule_name: String) -> Result<settings::NamedRateLimitRule> {
        match self {
            Self::Single(node) => node.get_named_rule(rule_name).await,
            Self::Gossip(node) => node.get_named_rule(rule_name).await,
            Self::Hashring(node) => node.get_named_rule(rule_name).await,
        }
    }

    pub async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        match self {
            Self::Single(node) => node.delete_named_rule(rule_name).await,
            Self::Gossip(node) => node.delete_named_rule(rule_name).await,
            Self::Hashring(node) => node.delete_named_rule(rule_name).await,
        }
    }

    pub async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        match self {
            Self::Single(node) => node.list_named_rules().await,
            Self::Gossip(node) => node.list_named_rules().await,
            Self::Hashring(node) => node.list_named_rules().await,
        }
    }

    pub async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        match self {
            Self::Single(node) => node.rate_limit_custom(rule_name, key).await,
            Self::Gossip(node) => node.rate_limit_custom(rule_name, key).await,
            Self::Hashring(node) => node.rate_limit_custom(rule_name, key).await,
        }
    }

    pub async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        match self {
            Self::Single(node) => node.check_limit_custom(rule_name, key).await,
            Self::Gossip(node) => node.check_limit_custom(rule_name, key).await,
            Self::Hashring(node) => node.check_limit_custom(rule_name, key).await,
        }
    }
}
