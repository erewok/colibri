use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use super::{GossipCommand, GossipController};
use crate::error::{ColibriError, Result};
use crate::limiters::{epoch_bucket::EpochTokenBucket, rate_limit::RateLimiter};
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
impl Node<EpochTokenBucket> for GossipNode {
    async fn new(
        settings: settings::Settings,
        rate_limiter: RateLimiter<EpochTokenBucket>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );

        // The the receive channel is only used in this loop
        let (gossip_command_tx, gossip_command_rx): (
            mpsc::Sender<GossipCommand>,
            mpsc::Receiver<GossipCommand>,
        ) = mpsc::channel(1000);

        let gossip_command_tx = Arc::new(gossip_command_tx);

        // Start the GossipCommand controller loop
        let controller = GossipController::new(settings.clone(), rate_limiter).await?;
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
                "Failed checking rate limit {}",
                e
            )))
        })
    }

    /// Expire keys from local buckets
    async fn expire_keys(&self) -> Result<()> {
        self.gossip_command_tx
            .send(GossipCommand::ExpireKeys)
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed checking rate limit {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

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

    pub fn rl_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }

    pub fn new_rate_limiter() -> RateLimiter<EpochTokenBucket> {
        RateLimiter::new(NodeId::new(1), rl_settings())
    }
}
