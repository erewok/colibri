use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

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
    async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>>;
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
                    let hashring_arc = Arc::new(hashring_node);

                    // Start background replication sync
                    hashring_arc.clone().start_replication_sync();

                    Ok(Self::Hashring(hashring_arc))
                }
            }
        }
    }
    pub fn is_cluster_participant(&self) -> bool {
        match self {
            Self::Single(_) => false,
            Self::Gossip(_) => true,
            Self::Hashring(_) => true,
        }
    }

    fn get_node_ref(&self) -> &dyn Node {
        match self {
            Self::Single(node) => node.as_ref(),
            Self::Gossip(node) => node.as_ref(),
            Self::Hashring(node) => node.as_ref(),
        }
    }

    pub async fn expire_keys(&self) -> Result<()> {
        self.get_node_ref().expire_keys().await
    }

    pub async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        self.get_node_ref().check_limit(client_id).await
    }

    pub async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        self.get_node_ref().rate_limit(client_id).await
    }

    pub async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        self.get_node_ref().create_named_rule(rule_name, settings).await
    }

    pub async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        self.get_node_ref().get_named_rule(rule_name).await
    }

    pub async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        self.get_node_ref().delete_named_rule(rule_name).await
    }

    pub async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        self.get_node_ref().list_named_rules().await
    }

    pub async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        self.get_node_ref().rate_limit_custom(rule_name, key).await
    }

    pub async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        self.get_node_ref().check_limit_custom(rule_name, key).await
    }

    // Cluster-specific methods (only for gossip and hashring nodes)
    pub async fn handle_export_buckets(&self) -> Result<crate::cluster::BucketExport> {
        match self {
            Self::Single(_) => Err(crate::error::ColibriError::Api("Single nodes don't support bucket export".to_string())),
            Self::Gossip(node) => node.handle_export_buckets().await,
            Self::Hashring(node) => node.handle_export_buckets().await,
        }
    }

    pub async fn handle_import_buckets(&self, import_data: crate::cluster::BucketExport) -> Result<()> {
        match self {
            Self::Single(_) => Err(crate::error::ColibriError::Api("Single nodes don't support bucket import".to_string())),
            Self::Gossip(node) => node.handle_import_buckets(import_data).await,
            Self::Hashring(node) => node.handle_import_buckets(import_data).await,
        }
    }

    pub async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        match self {
            Self::Single(_) => Err(crate::error::ColibriError::Api("Single nodes don't provide cluster health".to_string())),
            Self::Gossip(node) => node.handle_cluster_health().await,
            Self::Hashring(node) => node.handle_cluster_health().await,
        }
    }

    pub async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        match self {
            Self::Single(_) => Err(crate::error::ColibriError::Api("Single nodes don't have topology".to_string())),
            Self::Gossip(node) => node.handle_get_topology().await,
            Self::Hashring(node) => node.handle_get_topology().await,
        }
    }

    pub async fn handle_new_topology(&self, request: crate::cluster::TopologyChangeRequest) -> Result<crate::cluster::TopologyResponse> {
        match self {
            Self::Single(_) => Err(crate::error::ColibriError::Api("Single nodes don't support topology changes".to_string())),
            Self::Gossip(node) => node.handle_new_topology(request).await,
            Self::Hashring(node) => node.handle_new_topology(request).await,
        }
    }
}
