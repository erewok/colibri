use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::{error, info};

use super::HashringController;
use crate::error::{ColibriError, Result};
use crate::node::commands::AdminCommand;
use crate::node::{CheckCallsResponse, Node, NodeName, commands::{BucketExport, ClusterCommand, TopologyChangeRequest, TopologyResponse, StatusResponse}};
use crate::{settings, transport};

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

    /// Command sender to the controller
    pub hashring_command_tx: Arc<tokio::sync::mpsc::Sender<ClusterCommand>>,

    /// Controller handle
    pub controller_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// Receiver handle
    pub receiver_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl HashringNode {
    /// Run the TCP Receiver
    async fn run_receiver(
        listen_tcp: std::net::SocketAddr,
        hashring_command_tx: Arc<tokio::sync::mpsc::Sender<ClusterCommand>>,
    ) -> Result<()> {
        let (tcp_send, mut tcp_recv) = tokio::sync::mpsc::channel(1000);
        let receiver = transport::TcpReceiver::new(listen_tcp, Arc::new(tcp_send)).await?;

        let handle = tokio::spawn(async move {
            receiver.start().await;
        });

        loop {
            // Handle incoming messages from network
            if let Some((data, peer_addr)) = tcp_recv.recv().await {
                todo!();
                // // Turn into ClusterCommand and send to main loop
                // let cmd = ClusterCommand::from_incoming_message(data, peer_addr);
                // if let Err(e) = hashring_command_tx.send(cmd).await {
                //     debug!("Failed to send incoming message to main loop: {}", e);
                // }
            }
        }
    }

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
        self.stop_controller();
        self.stop_receiver();
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
        if let Ok(mut receiver_handle) = self.receiver_handle.lock() {
            if let Some(handle) = receiver_handle.take() {
                handle.abort();
            }
        }
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
        let listen_api = format!("{}:{}", settings.client_listen_address, settings.client_listen_port);
        info!(
            "[Node<{}>] Hashring node starting at {} with {} nodes in topology: {:?}",
            &node_name,
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // Set up command channel
        let (hashring_command_tx, hashring_command_rx): (
            tokio::sync::mpsc::Sender<ClusterCommand>,
            tokio::sync::mpsc::Receiver<ClusterCommand>,
        ) = tokio::sync::mpsc::channel(1000);

        let hashring_command_tx = Arc::new(hashring_command_tx);

        // Start the receiver to handle incoming gossip messages
        let receiver_addr = settings.transport_config().peer_listen_url();
        let tx_clone = hashring_command_tx.clone();
        let receiver_handle = tokio::spawn(async move {
            if let Err(e) = HashringNode::run_receiver(receiver_addr, tx_clone).await {
                error!(
                    "HashringNode receiver encountered an error: {}",
                    e
                );
            }
        });

        // Create controller
        let controller = HashringController::new(settings.clone()).await?;
        let controller_handle = tokio::spawn(async move {
            controller.start(hashring_command_rx).await;
        });

        Ok(Self {
            node_name,
            hashring_command_tx,
            controller_handle: Arc::new(Mutex::new(Some(controller_handle))),
            receiver_handle: Arc::new(Mutex::new(Some(receiver_handle))),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(ClusterCommand::CheckLimit {
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
            .send(ClusterCommand::RateLimit {
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
            .send(ClusterCommand::ExpireKeys)
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
            .send(ClusterCommand::CreateNamedRule {
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
            .send(ClusterCommand::DeleteNamedRule {
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
            .send(ClusterCommand::ListNamedRules { resp_chan: tx })
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
            .send(ClusterCommand::GetNamedRule {
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
            .send(ClusterCommand::RateLimitCustom {
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
            .send(ClusterCommand::CheckLimitCustom {
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
    pub async fn handle_export_buckets(&self) -> Result<BucketExport> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(AdminCommand::ExportBuckets)
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
        import_data: BucketExport,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(AdminCommand::ImportBuckets {
                data: import_data,
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

    pub async fn handle_cluster_health(&self) -> Result<StatusResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(ClusterCommand::ClusterHealth { resp_chan: tx })
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

    pub async fn handle_get_topology(&self) -> Result<TopologyResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(ClusterCommand::GetTopology { resp_chan: tx })
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
        request: TopologyChangeRequest,
    ) -> Result<TopologyResponse> {
        let (tx, rx) = oneshot::channel();
        self.hashring_command_tx
            .send(ClusterCommand::NewTopology {
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
//         assert!(node.hashring_command_tx.is_closed() == false);
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
