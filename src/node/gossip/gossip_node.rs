use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use super::{GossipCommand, GossipController};
use crate::error::{ColibriError, Result};
use crate::node::{CheckCallsResponse, Node, NodeId};
use crate::{settings, transport};

#[derive(Clone)]
pub struct GossipNode {
    // rate-limit settings
    pub node_id: NodeId,

    /// Local rate limiter - handles all bucket operations
    pub gossip_command_tx: Arc<mpsc::Sender<GossipCommand>>,

    /// Controller handle
    pub controller_handle: Option<Arc<tokio::task::JoinHandle<()>>>,

    /// Receiver handler
    pub receiver_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
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
            controller_handle: Some(Arc::new(controller_handle)),
            receiver_handle: Some(Arc::new(receiver_handle)),
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
        assert!(node.controller_handle.is_some());
        assert!(node.receiver_handle.is_some());
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
        }
    }

    #[allow(dead_code)]
    pub fn rl_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            cluster_participant_count: 1,
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }
}
