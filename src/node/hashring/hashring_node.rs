use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::{error, info};

use super::{messages::HashringCommand, HashringController};
use crate::error::{ColibriError, Result};
use crate::node::{CheckCallsResponse, Node, NodeId};
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
    pub node_id: NodeId,
    /// Command sender to the controller
    pub hashring_command_tx: Arc<tokio::sync::mpsc::Sender<HashringCommand>>,
    /// Controller handle
    pub controller_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl HashringNode {
    /// Stop the hashring controller task
    pub fn stop_controller(&self) {
        if let Ok(mut handle) = self.controller_handle.lock() {
            if let Some(join_handle) = handle.take() {
                join_handle.abort();
                info!("Hashring controller task stopped");
            }
        } else {
            error!("Failed to acquire lock on controller_handle during shutdown");
        }
    }
}

impl Drop for HashringNode {
    fn drop(&mut self) {
        // Clean up tasks when the node is dropped
        if let Ok(mut controller_handle) = self.controller_handle.lock() {
            if let Some(handle) = controller_handle.take() {
                handle.abort();
            }
        }
    }
}

impl std::fmt::Debug for HashringNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringNode")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[async_trait]
impl Node for HashringNode {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self> {
        let listen_api = format!("{}:{}", settings.listen_address, settings.listen_port_api);
        info!(
            "[Node<{}>] Hashring node starting at {} with {} nodes in topology: {:?}",
            node_id,
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // Set up command channel
        let (hashring_command_tx, hashring_command_rx): (
            tokio::sync::mpsc::Sender<HashringCommand>,
            tokio::sync::mpsc::Receiver<HashringCommand>,
        ) = tokio::sync::mpsc::channel(1000);

        let hashring_command_tx = Arc::new(hashring_command_tx);

        // Create controller
        let controller = HashringController::new(settings.clone()).await?;
        let controller_handle = tokio::spawn(async move {
            controller.start(hashring_command_rx).await;
        });

        Ok(Self {
            node_id,
            hashring_command_tx,
            controller_handle: Arc::new(Mutex::new(Some(controller_handle))),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CheckLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed checking rate limit {}", e)))?;

        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed checking rate limit {}",
                e
            )))
        })
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::RateLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed rate limiting {}", e)))?;

        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed rate limiting {}",
                e
            )))
        })
    }

    async fn expire_keys(&self) -> Result<()> {
        self.hashring_command_tx
            .send(HashringCommand::ExpireKeys)
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed expiring keys {}", e)))?;
        Ok(())
    }

    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CreateNamedRule {
                rule_name,
                settings,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending create_named_rule command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving create_named_rule response {}",
                e
            )))
        })
    }

    async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::DeleteNamedRule {
                rule_name,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending delete_named_rule command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving delete_named_rule response {}",
                e
            )))
        })
    }

    async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::ListNamedRules { resp_chan: tx })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending list_named_rules command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving list_named_rules response {}",
                e
            )))
        })
    }

    async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::GetNamedRule {
                rule_name,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending get_named_rule command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving get_named_rule response {}",
                e
            )))
        })
    }

    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::RateLimitCustom {
                rule_name,
                key,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending rate_limit_custom command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving rate_limit_custom response {}",
                e
            )))
        })
    }

    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CheckLimitCustom {
                rule_name,
                key,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending check_limit_custom command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving check_limit_custom response {}",
                e
            )))
        })
    }
}

// Cluster-specific methods for HashringNode
impl HashringNode {
    pub async fn handle_export_buckets(&self) -> Result<crate::cluster::BucketExport> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::ExportBuckets { resp_chan: tx })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending export_buckets command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving export_buckets response {}",
                e
            )))
        })
    }

    pub async fn handle_import_buckets(
        &self,
        import_data: crate::cluster::BucketExport,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::ImportBuckets {
                import_data,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending import_buckets command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving import_buckets response {}",
                e
            )))
        })
    }

    pub async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::ClusterHealth { resp_chan: tx })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending cluster_health command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving cluster_health response {}",
                e
            )))
        })
    }

    pub async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::GetTopology { resp_chan: tx })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending get_topology command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving get_topology response {}",
                e
            )))
        })
    }

    pub async fn handle_new_topology(
        &self,
        request: crate::cluster::TopologyChangeRequest,
    ) -> Result<crate::cluster::TopologyResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::NewTopology {
                request,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending new_topology command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving new_topology response {}",
                e
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for HashringNode functionality - traffic direction and command forwarding

    use super::*;
    use std::collections::HashSet;

    fn test_settings() -> settings::Settings {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8422".to_string()); // Add this node's UDP address to topology

        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8420,
            listen_port_tcp: 8421,
            listen_port_udp: 8422,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Hashring,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology,
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        }
    }

    #[tokio::test]
    async fn test_hashring_node_creation() {
        let node_id = NodeId::new(1);
        let settings = test_settings();

        // Should create successfully
        let node = HashringNode::new(node_id, settings).await.unwrap();

        assert_eq!(node.node_id, node_id);
        assert!(node.hashring_command_tx.is_closed() == false);
        assert!(node.controller_handle.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_hashring_node_check_limit() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Check limit should work (returns full limit for new client)
        let result = node.check_limit(client_id.clone()).await.unwrap();
        assert_eq!(result.client_id, client_id);
        assert_eq!(result.calls_remaining, 100); // From test_settings
    }

    #[tokio::test]
    async fn test_hashring_node_rate_limit() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Rate limit should consume tokens and return remaining count
        let result = node.rate_limit(client_id.clone()).await.unwrap();
        assert!(result.is_some());

        let response = result.unwrap();
        assert_eq!(response.client_id, client_id);
        assert!(response.calls_remaining < 100); // Should have consumed tokens
    }

    #[tokio::test]
    async fn test_hashring_node_expire_keys() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Should complete without error
        node.expire_keys().await.unwrap();
    }

    #[tokio::test]
    async fn test_hashring_node_command_forwarding() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = HashringNode::new(node_id, settings).await.unwrap();

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
    async fn test_hashring_node_cleanup() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Verify handle is initially present
        assert!(node.controller_handle.lock().unwrap().is_some());

        // Test cleanup method
        node.stop_controller();
        assert!(node.controller_handle.lock().unwrap().is_none());
    }
}
