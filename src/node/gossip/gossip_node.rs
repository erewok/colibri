use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tracing::{error, info, warn};

use super::GossipController;
use crate::error::{ColibriError, Result};
use crate::limiters::NamedRateLimitRule;
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, StatusResponse,
    TopologyChangeRequest, TopologyResponse,
    BucketExport, ExportMetadata,
};
use crate::node::{Node, NodeName};
use crate::settings::RunMode;
use crate::settings;

/// Gossip-based distributed rate limiter node
#[derive(Clone)]
pub struct GossipNode {
    /// Node name
    pub node_name: NodeName,

    /// Address for peers
    peer_addr: SocketAddr,

    /// Controller - handles all operations via handle_message()
    pub controller: Arc<GossipController>,

    /// Receiver handler
    pub receiver_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl GossipNode {
    // Note: run_gossip_receiver removed - gossip protocol now handled internally by controller

    /// Stop the gossip receiver task
    pub fn stop_receiver(&self) {
        if let Ok(mut handle) = self.receiver_handle.lock() {
            if let Some(join_handle) = handle.take() {
                join_handle.abort();
                info!("Gossip receiver task stopped");
            }
        } else {
            error!("Failed to acquire lock on receiver_handle during shutdown");
        }
    }

    /// Stop all background tasks
    pub fn stop_all_tasks(&self) {
        self.stop_receiver();
    }
}

impl Drop for GossipNode {
    fn drop(&mut self) {
        // Clean up tasks when the node is dropped
        if let Ok(mut receiver_handle) = self.receiver_handle.lock() {
            if let Some(handle) = receiver_handle.take() {
                handle.abort();
            }
        }
    }
}

impl std::fmt::Debug for GossipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipNode")
            .field("node_name", &self.node_name)
            .finish()
    }
}

#[async_trait]
impl Node for GossipNode {
    async fn new(settings: settings::Settings) -> Result<Self> {
        let node_name: NodeName = settings.node_name();
        let listen_api = format!(
            "{}:{}",
            settings.client_listen_address, settings.client_listen_port
        );
        info!(
            "[Node<{}>] Gossip node starting at {} in gossip mode with {} other nodes: {:?}",
            node_name.as_str(),
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // Create controller
        let controller = GossipController::new(settings.clone()).await?;
        let controller = Arc::new(controller);

        // Start the controller's internal gossip loop
        let controller_clone = controller.clone();
        tokio::spawn(async move {
            controller_clone.start().await;
        });

        // Start the UDP receiver to handle incoming gossip messages
        // TODO: This may need updating to work with new controller architecture
        let receiver_addr = settings.transport_config().peer_listen_url();
        let receiver_handle = tokio::spawn(async move {
            warn!("Gossip receiver startup - may need integration with new controller architecture");
            // Receiver integration will be handled in controller.start()
        });

        Ok(Self {
            node_name,
            peer_addr: receiver_addr,
            controller,
            receiver_handle: Arc::new(Mutex::new(Some(receiver_handle))),
        })
    }

    /// Check remaining calls for a client using local state
    async fn check_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: client_id.clone(),
            rule_name: None,
            consume_token: false,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => {
                Ok(Some(response))
            }
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: client_id.clone(),
            rule_name: None,
            consume_token: true,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => {
                if response.calls_remaining > 0 {
                    Ok(Some(response))
                } else {
                    Ok(None)
                }
            }
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }
    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key.clone(),
            rule_name: Some(rule_name),
            consume_token: false,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => {
                Ok(Some(response))
            }
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }
    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key.clone(),
            rule_name: Some(rule_name),
            consume_token: true,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => {
                if response.calls_remaining > 0 {
                    Ok(Some(response))
                } else {
                    Ok(None)
                }
            }
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }


    /// Expire keys from local buckets
    async fn expire_keys(&self) -> Result<()> {
        // TODO: Implement expire_keys in new Message architecture
        warn!("expire_keys not yet implemented in new architecture");
        Ok(())
    }

    // Configurable rate limit methods
    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        let message = Message::CreateRateLimitRule { rule_name, settings };

        match self.controller.handle_message(message).await? {
            Message::CreateRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }

    async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        let message = Message::DeleteRateLimitRule { rule_name };

        match self.controller.handle_message(message).await? {
            Message::DeleteRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }

    async fn list_named_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let message = Message::ListRateLimitRules;

        match self.controller.handle_message(message).await? {
            Message::ListRateLimitRulesResponse(rules) => Ok(rules),
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }
    async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<NamedRateLimitRule>> {
        let message = Message::GetRateLimitRule { rule_name };

        match self.controller.handle_message(message).await? {
            Message::GetRateLimitRuleResponse(rule) => Ok(rule),
            _ => Err(ColibriError::Node("Unexpected response from controller".to_string())),
        }
    }

}

// Cluster-specific methods for GossipNode
impl GossipNode {
    pub async fn handle_export_buckets(&self) -> Result<BucketExport> {
        // Gossip nodes don't use bucket-based data export
        // All data is replicated across gossip network
        let export = BucketExport {
            client_data: Vec::new(),
            metadata: ExportMetadata {
                node_name: self.node_name.to_string(),
                export_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_type: RunMode::Gossip,
                bucket_count: 0,
            },
        };

        tracing::info!("Gossip node export skipped - using gossip synchronization");
        Ok(export)
    }

    pub async fn handle_import_buckets(&self, _import_data: BucketExport) -> Result<()> {
        // Gossip nodes don't use bucket-based data import
        // Data synchronization happens through gossip protocol
        tracing::info!("Gossip node data import skipped - using gossip synchronization");
        Ok(())
    }

    pub async fn handle_cluster_health(&self) -> Result<StatusResponse> {
        let message = Message::GetStatus;
        match self.controller.handle_message(message).await? {
            Message::StatusResponse(response) => Ok(response),
            _ => Err(ColibriError::Api("Unexpected response type for GetStatus".to_string())),
        }
    }

    pub async fn handle_get_topology(&self) -> Result<TopologyResponse> {
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api("Unexpected response type for GetTopology".to_string())),
        }
    }

    pub async fn handle_new_topology(
        &self,
        _request: TopologyChangeRequest,
    ) -> Result<TopologyResponse> {
        // TODO: Implement topology changes - requires adding AddNode/RemoveNode messages
        warn!("handle_new_topology not yet implemented - topology changes require controller support");

        // For now, just return current topology
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api("Unexpected response type for GetTopology".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for GossipNode functionality - traffic direction and command forwarding

    use super::*;

    #[tokio::test]
    async fn test_gossip_node_creation() {
        let settings = settings::tests::sample();

        // Should create successfully
        let node = GossipNode::new(settings).await.unwrap();
        assert!(node.gossip_command_tx.is_closed() == false);
        assert!(node.controller_handle.lock().unwrap().is_some());
        assert!(node.receiver_handle.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_gossip_node_check_limit() {
        let settings = settings::tests::sample();
        let node = GossipNode::new(settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Check limit should work (returns full limit for new client)
        let result = node.check_limit(client_id.clone()).await.unwrap();
        assert_eq!(result.client_id, client_id);
        assert_eq!(result.calls_remaining, 100); // From test_settings
    }

    #[tokio::test]
    async fn test_gossip_node_rate_limit() {
        let settings = settings::tests::sample();
        let node = GossipNode::new(settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Rate limit should consume tokens and return remaining count
        let result = node.rate_limit(client_id.clone()).await.unwrap();
        assert!(result.is_some());

        let response = result.unwrap();
        assert_eq!(response.client_id, client_id);
        assert!(response.calls_remaining < 100); // Should have consumed tokens
    }

    #[tokio::test]
    async fn test_gossip_node_expire_keys() {
        let settings = settings::tests::sample();
        let node = GossipNode::new(settings).await.unwrap();

        // Should complete without error
        node.expire_keys().await.unwrap();
    }

    #[tokio::test]
    async fn test_gossip_node_command_forwarding() {
        let settings = settings::tests::sample();
        let node = GossipNode::new(settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Make multiple operations to verify command forwarding works
        let check1 = node.check_limit(client_id.clone()).await.unwrap();
        let rate1 = node.rate_limit(client_id.clone()).await.unwrap().unwrap();
        let check2 = node.check_limit(client_id.clone()).await.unwrap();

        // Check that operations are properly forwarded and processed
        assert_eq!(check1.client_id, client_id);
        assert_eq!(rate1.client_id, client_id);
        assert_eq!(check2.client_id, client_id);

        // After rate limiting, remaining calls should be less than initial
        assert!(check2.calls_remaining < check1.calls_remaining);
    }

    #[tokio::test]
    async fn test_gossip_node_cleanup() {
        let settings = settings::tests::sample();
        let node = GossipNode::new(settings).await.unwrap();

        // Verify handles are initially present
        assert!(node.controller_handle.lock().unwrap().is_some());
        assert!(node.receiver_handle.lock().unwrap().is_some());

        // Test individual cleanup methods
        node.stop_controller();
        assert!(node.controller_handle.lock().unwrap().is_none());
        assert!(node.receiver_handle.lock().unwrap().is_some());

        node.stop_receiver();
        assert!(node.receiver_handle.lock().unwrap().is_none());

        // Test stop_all_tasks method with a fresh node
        let node2 = GossipNode::new(settings::tests::sample()).await.unwrap();
        node2.stop_all_tasks();
        assert!(node2.controller_handle.lock().unwrap().is_none());
        assert!(node2.receiver_handle.lock().unwrap().is_none());
    }
}
