use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use super::HashringController;
use crate::error::{ColibriError, Result};
use crate::limiters::NamedRateLimitRule;
use crate::node::messages::{
    BucketExport, CheckCallsRequest, CheckCallsResponse, ExportMetadata, Message, StatusResponse,
    TopologyChangeRequest, TopologyResponse,
};
use crate::node::{Node, NodeName};
use crate::settings;

/// Replication factor for data distribution
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplicationFactor {
    Zero = 1,
    #[default]
    Two = 2,
    Three = 3,
}

/// Consistent hash ring distributed rate limiter node
#[derive(Clone)]
pub struct HashringNode {
    pub node_name: NodeName,

    /// The hashring controller (manages all TCP communication)
    pub controller: Arc<HashringController>,
}

impl HashringNode {
    /// Stop the hashring receiver task
    pub fn stop_receiver(&self) {
        self.controller.stop_receiver();
    }

    /// Stop all background tasks
    pub fn stop_all_tasks(&self) {
        self.stop_receiver();
    }
}

impl Drop for HashringNode {
    fn drop(&mut self) {
        // Clean up receiver task via controller when the node is dropped
        self.controller.stop_receiver();
    }
}

impl std::fmt::Debug for HashringNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringNode")
            .field("node_name", &self.node_name)
            .finish()
    }
}

#[async_trait]
impl Node for HashringNode {
    async fn new(settings: settings::Settings) -> Result<Self> {
        let node_name: NodeName = settings.node_name();
        let listen_api = format!(
            "{}:{}",
            settings.client_listen_address, settings.client_listen_port
        );
        info!(
            "[Node<{}>] Hashring node starting at {} with {} nodes in topology: {:?}",
            &node_name,
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // Create controller
        let controller = Arc::new(HashringController::new(settings.clone()).await?);

        // Start the controller's TCP receiver
        controller.start_receiver().await?;

        Ok(Self {
            node_name,
            controller,
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id,
            rule_name: None,
            consume_token: false,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(Some(response)),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CheckLimit".to_string(),
            )),
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id,
            rule_name: None,
            consume_token: true,
            forwarding_depth: 0,
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
            _ => Err(ColibriError::Api(
                "Unexpected response type for RateLimit".to_string(),
            )),
        }
    }

    async fn expire_keys(&self) -> Result<()> {
        // warn!("expire_keys not yet implemented");
        Ok(())
    }

    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        let message = Message::CreateRateLimitRule {
            rule_name,
            settings,
        };
        match self.controller.handle_message(message).await? {
            Message::CreateRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CreateRule".to_string(),
            )),
        }
    }

    async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        let message = Message::DeleteRateLimitRule { rule_name };
        match self.controller.handle_message(message).await? {
            Message::DeleteRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Api(
                "Unexpected response type for DeleteRule".to_string(),
            )),
        }
    }

    async fn list_named_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let message = Message::ListRateLimitRules;
        match self.controller.handle_message(message).await? {
            Message::ListRateLimitRulesResponse(rules) => Ok(rules),
            _ => Err(ColibriError::Api(
                "Unexpected response type for ListRules".to_string(),
            )),
        }
    }

    async fn get_named_rule(&self, rule_name: String) -> Result<Option<NamedRateLimitRule>> {
        let message = Message::GetRateLimitRule { rule_name };
        match self.controller.handle_message(message).await? {
            Message::GetRateLimitRuleResponse(rule) => Ok(rule),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetRule".to_string(),
            )),
        }
    }

    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key,
            rule_name: Some(rule_name),
            consume_token: true,
            forwarding_depth: 0,
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
            _ => Err(ColibriError::Api(
                "Unexpected response type for RateLimitCustom".to_string(),
            )),
        }
    }

    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key,
            rule_name: Some(rule_name),
            consume_token: false,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(Some(response)),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CheckLimitCustom".to_string(),
            )),
        }
    }
}

// Cluster-specific methods for HashringNode
impl HashringNode {
    pub async fn handle_export_buckets(&self) -> Result<BucketExport> {
        let export = BucketExport {
            client_data: Vec::new(),
            metadata: ExportMetadata {
                node_name: self.node_name.to_string(),
                export_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_type: settings::RunMode::Hashring,
                bucket_count: 0,
            },
        };
        tracing::info!("Hashring bucket export not yet implemented");
        Ok(export)
    }

    pub async fn handle_import_buckets(&self, _import_data: BucketExport) -> Result<()> {
        tracing::info!("Hashring bucket import not yet implemented");
        Ok(())
    }

    pub async fn handle_cluster_health(&self) -> Result<StatusResponse> {
        let message = Message::GetStatus;
        match self.controller.handle_message(message).await? {
            Message::StatusResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetStatus".to_string(),
            )),
        }
    }

    pub async fn handle_get_topology(&self) -> Result<TopologyResponse> {
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetTopology".to_string(),
            )),
        }
    }

    pub async fn handle_new_topology(
        &self,
        _request: TopologyChangeRequest,
    ) -> Result<TopologyResponse> {
        warn!("Topology changes not yet implemented for hashring nodes");

        // For now, just return current topology
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetTopology".to_string(),
            )),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     //! Simple tests for HashringNode functionality - traffic direction and command forwarding

//     use super::*;
//     use std::collections::HashSet;

//     fn test_settings() -> settings::Settings {
//         let mut topology = HashSet::new();
//         topology.insert("127.0.0.1:8422".to_string()); // Add this node's UDP address to topology
//         let mut conf = settings::tests::sample();
//         conf.topology = topology;
//         conf
//     }

//     #[tokio::test]
//     async fn test_hashring_node_creation() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();

//         // Should create successfully
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         assert_eq!(node.node_id, node_id);
//         assert!(node.cluster_inbox_tx.is_closed() == false);
//         assert!(node.controller_handle.lock().unwrap().is_some());
//     }

//     #[tokio::test]
//     async fn test_hashring_node_check_limit() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         let client_id = "test_client".to_string();

//         // Check limit should work (returns full limit for new client)
//         let result = node.check_limit(client_id.clone()).await.unwrap();
//         assert_eq!(result.client_id, client_id);
//         assert_eq!(result.calls_remaining, 100); // From test_settings
//     }

//     #[tokio::test]
//     async fn test_hashring_node_rate_limit() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         let client_id = "test_client".to_string();

//         // Rate limit should consume tokens and return remaining count
//         let result = node.rate_limit(client_id.clone()).await.unwrap();
//         assert!(result.is_some());

//         let response = result.unwrap();
//         assert_eq!(response.client_id, client_id);
//         assert!(response.calls_remaining < 100); // Should have consumed tokens
//     }

//     #[tokio::test]
//     async fn test_hashring_node_expire_keys() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         // Should complete without error
//         node.expire_keys().await.unwrap();
//     }

//     #[tokio::test]
//     async fn test_hashring_node_command_forwarding() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         let client_id = "test_client".to_string();

//         // Make multiple operations to verify command forwarding works
//         let check1 = node.check_limit(client_id.clone()).await.unwrap();
//         let rate1 = node.rate_limit(client_id.clone()).await.unwrap().unwrap();
//         let check2 = node.check_limit(client_id.clone()).await.unwrap();

//         // Check that operations are properly forwarded and processed
//         assert_eq!(check1.client_id, client_id);
//         assert_eq!(rate1.client_id, client_id);
//         assert_eq!(check2.client_id, client_id);

//         // After rate limiting, remaining calls should be less than initial
//         assert!(check2.calls_remaining < check1.calls_remaining);
//     }

//     #[tokio::test]
//     async fn test_hashring_node_cleanup() {
//         let node_id = NodeId::new(1);
//         let settings = test_settings();
//         let node = HashringNode::new(node_id, settings).await.unwrap();

//         // Verify handle is initially present
//         assert!(node.controller_handle.lock().unwrap().is_some());

//         // Test cleanup method
//         node.stop_controller();
//         assert!(node.controller_handle.lock().unwrap().is_none());
//     }
// }
