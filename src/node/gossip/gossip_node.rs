use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use super::{GossipCommand, GossipController};
use crate::error::{ColibriError, Result};
use crate::node::{CheckCallsResponse, Node, NodeId};
use crate::{settings, transport};

/// Gossip-based distributed rate limiter node
#[derive(Clone)]
pub struct GossipNode {
    // rate-limit settings
    pub node_id: NodeId,

    /// Local rate limiter - handles all bucket operations
    pub gossip_command_tx: Arc<mpsc::Sender<GossipCommand>>,

    /// Controller handle
    pub controller_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// Receiver handler
    pub receiver_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// Cluster membership management - uses same UDP transport as gossip controller
    pub cluster_member: Arc<dyn crate::cluster::ClusterMember>,
}

impl GossipNode {
    async fn run_gossip_receiver(
        listen_udp: SocketAddr,
        gossip_command_tx: Arc<mpsc::Sender<GossipCommand>>,
    ) -> Result<()> {
        let (udp_send, mut udp_recv) = mpsc::channel(1000);
        let receiver =
            transport::receiver::UdpReceiver::new(listen_udp, Arc::new(udp_send)).await?;

        tokio::spawn(async move {
            receiver.start().await;
        });

        loop {
            // Handle incoming messages from network
            if let Some((data, peer_addr)) = udp_recv.recv().await {
                // Turn into GossipCommand and send to main loop
                let cmd = GossipCommand::from_incoming_message(data, peer_addr);
                if let Err(e) = gossip_command_tx.send(cmd).await {
                    debug!("Failed to send incoming message to main loop: {}", e);
                }
            }
        }
    }

    /// Stop the gossip controller task
    pub fn stop_controller(&self) {
        if let Ok(mut handle) = self.controller_handle.lock() {
            if let Some(join_handle) = handle.take() {
                join_handle.abort();
                info!("Gossip controller task stopped");
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

impl Drop for GossipNode {
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

impl std::fmt::Debug for GossipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipNode")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[async_trait]
impl Node for GossipNode {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self> {
        let listen_api = format!("{}:{}", settings.listen_address, settings.listen_port_api);
        info!(
            "[Node<{}>] Gossip node starting at {} in gossip mode with {} other nodes: {:?}",
            node_id,
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // The the receive channel is only used in this loop
        let (gossip_command_tx, gossip_command_rx): (
            mpsc::Sender<GossipCommand>,
            mpsc::Receiver<GossipCommand>,
        ) = mpsc::channel(1000);

        let gossip_command_tx = Arc::new(gossip_command_tx);

        // Create cluster member using factory - clean path from config to cluster membership
        let cluster_member =
            crate::cluster::ClusterFactory::create_from_settings(node_id, &settings).await?;

        // Start the GossipCommand controller loop
        let controller = GossipController::new(settings.clone()).await?;
        let controller_handle = tokio::spawn(async move {
            controller.start(gossip_command_rx).await;
        });

        // Start the UDP receiver to handle incoming gossip messages
        let receiver_addr = settings.transport_config().listen_udp;
        let tx_clone = gossip_command_tx.clone();
        let receiver_handle = tokio::spawn(async move {
            if let Err(e) = GossipNode::run_gossip_receiver(receiver_addr, tx_clone).await {
                error!("[{}] Gossip receiver encountered an error: {}", node_id, e);
            }
        });

        Ok(Self {
            node_id,
            gossip_command_tx,
            controller_handle: Arc::new(Mutex::new(Some(controller_handle))),
            receiver_handle: Arc::new(Mutex::new(Some(receiver_handle))),
            cluster_member,
        })
    }

    /// Check remaining calls for a client using local state
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let (tx, rx) = oneshot::channel();
        self.gossip_command_tx
            .send(GossipCommand::CheckLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed checking rate limit {}", e)))?;
        // Await the response
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed checking rate limit {}",
                e
            )))
        })
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let (tx, rx) = oneshot::channel();
        self.gossip_command_tx
            .send(GossipCommand::RateLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed checking rate limit {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed rate limiting {}",
                e
            )))
        })
    }

    /// Expire keys from local buckets
    async fn expire_keys(&self) -> Result<()> {
        self.gossip_command_tx
            .send(GossipCommand::ExpireKeys)
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed expiring keys {}", e)))?;
        Ok(())
    }

    // Configurable rate limit methods
    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.gossip_command_tx
            .send(GossipCommand::CreateNamedRule {
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
        self.gossip_command_tx
            .send(GossipCommand::DeleteNamedRule {
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
        self.gossip_command_tx
            .send(GossipCommand::ListNamedRules { resp_chan: tx })
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
        self.gossip_command_tx
            .send(GossipCommand::GetNamedRule {
                rule_name,
                resp_chan: tx,
            })
            .await
            .map_err(|e| {
                ColibriError::Transport(format!("Failed sending get_named_rule command {}", e))
            })?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving get_named_rules response {}",
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
        self.gossip_command_tx
            .send(GossipCommand::RateLimitCustom {
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
        self.gossip_command_tx
            .send(GossipCommand::CheckLimitCustom {
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

// Cluster-specific methods for GossipNode
impl GossipNode {
    pub async fn handle_export_buckets(&self) -> Result<crate::cluster::BucketExport> {
        use crate::cluster::BucketExport;

        // Gossip nodes don't use bucket-based data export
        // All data is replicated across gossip network
        let export = BucketExport {
            client_data: Vec::new(),
            metadata: crate::cluster::ExportMetadata {
                node_id: self.node_id.to_string(),
                export_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_type: "gossip".to_string(),
                bucket_count: 0,
            },
        };

        tracing::info!("Gossip node export skipped - using gossip synchronization");
        Ok(export)
    }

    pub async fn handle_import_buckets(
        &self,
        _import_data: crate::cluster::BucketExport,
    ) -> Result<()> {
        // Gossip nodes don't use bucket-based data import
        // Data synchronization happens through gossip protocol
        tracing::info!("Gossip node data import skipped - using gossip synchronization");
        Ok(())
    }

    pub async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        use crate::cluster::{ClusterStatus, StatusResponse};

        Ok(StatusResponse {
            node_id: self.node_id.to_string(),
            node_type: "gossip".to_string(),
            status: ClusterStatus::Healthy,
            active_clients: 0,          // TODO: implement client key counting
            last_topology_change: None, // TODO: track topology changes
        })
    }

    pub async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        use crate::cluster::TopologyResponse;

        let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        let peer_nodes: Vec<String> = cluster_nodes.iter().map(|addr| addr.to_string()).collect();

        Ok(TopologyResponse {
            node_id: self.node_id.to_string(),
            node_type: "gossip".to_string(),
            owned_bucket: None,
            replica_buckets: vec![],
            cluster_nodes,
            peer_nodes,
            errors: None,
        })
    }

    pub async fn handle_new_topology(
        &self,
        request: crate::cluster::TopologyChangeRequest,
    ) -> Result<crate::cluster::TopologyResponse> {
        use crate::cluster::TopologyResponse;

        tracing::info!(
            "Gossip node topology change acknowledged with {} new nodes",
            request.new_topology.len()
        );

        // Update cluster membership with new topology
        // First get current nodes, then compute differences
        let current_nodes = self.cluster_member.get_cluster_nodes().await;
        let new_nodes: std::collections::HashSet<_> =
            request.new_topology.iter().cloned().collect();
        let current_nodes_set: std::collections::HashSet<_> =
            current_nodes.iter().cloned().collect();

        // Add new nodes
        for addr in new_nodes.difference(&current_nodes_set) {
            self.cluster_member.add_node(*addr).await;
        }

        // Remove nodes that are no longer in topology
        for addr in current_nodes_set.difference(&new_nodes) {
            self.cluster_member.remove_node(*addr).await;
        }

        // Return updated topology
        let updated_cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        let peer_nodes: Vec<String> = updated_cluster_nodes
            .iter()
            .map(|addr| addr.to_string())
            .collect();

        Ok(TopologyResponse {
            node_id: self.node_id.to_string(),
            node_type: "gossip".to_string(),
            owned_bucket: None,
            replica_buckets: vec![],
            cluster_nodes: updated_cluster_nodes,
            peer_nodes,
            errors: None,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for GossipNode functionality - traffic direction and command forwarding

    use super::*;
    use std::collections::HashSet;

    fn test_settings() -> settings::Settings {
        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8410,
            listen_port_tcp: 8411,
            listen_port_udp: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Gossip,
            gossip_interval_ms: 1000, // Longer for testing
            gossip_fanout: 3,
            topology: HashSet::new(), // Empty topology for simple tests
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        }
    }

    #[tokio::test]
    async fn test_gossip_node_creation() {
        let node_id = NodeId::new(1);
        let settings = test_settings();

        // Should create successfully
        let node = GossipNode::new(node_id, settings).await.unwrap();

        assert_eq!(node.node_id, node_id);
        assert!(node.gossip_command_tx.is_closed() == false);
        assert!(node.controller_handle.lock().unwrap().is_some());
        assert!(node.receiver_handle.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_gossip_node_check_limit() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = GossipNode::new(node_id, settings).await.unwrap();

        let client_id = "test_client".to_string();

        // Check limit should work (returns full limit for new client)
        let result = node.check_limit(client_id.clone()).await.unwrap();
        assert_eq!(result.client_id, client_id);
        assert_eq!(result.calls_remaining, 100); // From test_settings
    }

    #[tokio::test]
    async fn test_gossip_node_rate_limit() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = GossipNode::new(node_id, settings).await.unwrap();

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
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = GossipNode::new(node_id, settings).await.unwrap();

        // Should complete without error
        node.expire_keys().await.unwrap();
    }

    #[tokio::test]
    async fn test_gossip_node_command_forwarding() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = GossipNode::new(node_id, settings).await.unwrap();

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
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let node = GossipNode::new(node_id, settings).await.unwrap();

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
        let node2 = GossipNode::new(node_id, test_settings()).await.unwrap();
        node2.stop_all_tasks();
        assert!(node2.controller_handle.lock().unwrap().is_none());
        assert!(node2.receiver_handle.lock().unwrap().is_none());
    }

    // Helper functions (keeping for compatibility but marked as used)
    #[allow(dead_code)]
    pub fn gen_settings() -> settings::Settings {
        settings::Settings {
            listen_address: "0.0.0.0".to_string(),
            listen_port_api: settings::STANDARD_PORT_HTTP,
            listen_port_tcp: settings::STANDARD_PORT_TCP,
            listen_port_udp: settings::STANDARD_PORT_UDP,
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Single,
            gossip_interval_ms: 25,
            gossip_fanout: 4,
            topology: HashSet::new(),
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        }
    }

    #[allow(dead_code)]
    pub fn rl_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }
}
