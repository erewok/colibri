use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use super::{GossipCommand, GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::distributed_bucket::{DistributedBucketExternal, DistributedBucketLimiter};
use crate::node::{CheckCallsResponse, NodeId};
use crate::settings;
use crate::transport::UdpTransport;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_id: NodeId,
    // rate-limit settings
    pub rate_limit_settings: settings::RateLimitSettings,
    /// Local rate limiter - handles all bucket operations
    pub rate_limiter: Arc<Mutex<DistributedBucketLimiter>>,
    /// transport_config
    pub transport_config: settings::TransportConfig,
    /// Transport layer for sending UDP unicast gossip messages
    pub transport: Option<Arc<UdpTransport>>,
    pub response_addr: SocketAddr,
    /// Gossip configuration
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
}

impl std::fmt::Debug for GossipController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipController")
            .field("node_id", &self.node_id)
            .field("has_transport", &self.transport.is_some())
            .field("gossip_interval_ms", &self.gossip_interval_ms)
            .field("gossip_fanout", &self.gossip_fanout)
            .finish()
    }
}

impl GossipController {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = DistributedBucketLimiter::new(node_id, rl_settings.clone());

        // Build transport
        let transport_config = settings.transport_config();
        // cache this for repsonse-addr in messages we send
        let response_addr = transport_config.listen_udp;
        let transport = if !settings.topology.is_empty() {
            info!(
                "Initializing gossip transport at {} with {} peers: {:?}",
                transport_config.listen_udp,
                settings.topology.len(),
                settings.topology
            );
            Some(Arc::new(
                UdpTransport::new(node_id, &transport_config)
                    .await
                    .map_err(|e| {
                        ColibriError::Transport(format!("Failed to create transport: {}", e))
                    })?,
            ))
        } else {
            None
        };

        Ok(Self {
            node_id,
            rate_limit_settings: rl_settings,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            transport: transport.clone(),
            transport_config,
            response_addr,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
        })
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.transport.is_some()
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.rate_limiter.lock().map(|rl| rl.len()).unwrap_or(0);
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_id, bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self, mut gossip_command_rx: mpsc::Receiver<GossipCommand>) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            self.node_id, self.gossip_interval_ms
        );

        let mut gossip_timer = time::interval(time::Duration::from_millis(self.gossip_interval_ms));
        let node_id = self.node_id;
        // We are taking a single reference to this mutex here
        loop {
            tokio::select! {
                // Handle incoming messages from network
                Some(cmd) = gossip_command_rx.recv() => {
                    if let Err(e) = self.handle_command(cmd).await {
                        debug!("[{}] Error processing incoming message: {}", node_id, e);
                    }
                }
                // Gossip timer - send delta updates
                _ = gossip_timer.tick() => {
                    if let Err(e) = self.handle_gossip_tick().await {
                        debug!("[{}] Error during gossip tick: {}", node_id, e);
                    }
                }

                // Exit if inbound channel is closed
                else => {
                    info!("[{}] IO loop inbound channel closed, exiting", node_id);
                    break;
                }
            }
        }
    }

    async fn handle_gossip_tick(&self) -> Result<()> {
        // Send delta updates for buckets that have changed
        let updates = self.collect_gossip_updates().await?;
        if !updates.is_empty() {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id,
                response_addr: self.response_addr,
                propagation_factor: 1,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        } else {
            // Send heartbeat if no updates to share
            let message = GossipMessage::Heartbeat {
                node_id: self.node_id,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                vclock: self
                    .rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?
                    .get_latest_updated_vclock(),
                response_addr: self.response_addr,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Collect buckets that have been updated and should be gossiped
    async fn collect_gossip_updates(&self) -> Result<Vec<DistributedBucketExternal>> {
        self.rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
            .map(|rl| rl.gossip_delta_state())
    }
    /// Handle incoming gossip cmd from our channel
    async fn handle_command(&self, cmd: GossipCommand) -> Result<()> {
        match cmd {
            GossipCommand::ExpireKeys => {
                self.rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
                    .map(|mut rl| rl.expire_keys())?;
                Ok(())
            }
            GossipCommand::GossipMessageReceived { data, peer_addr } => {
                self.process_gossip_packet(data, peer_addr).await
            }
            GossipCommand::CheckLimit {
                client_id,
                resp_chan,
            } => {
                let rl = self
                    .rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;
                let calls_remaining = rl.check_calls_remaining_for_client(&client_id);
                debug!(
                    "Check limit for client '{}': {} tokens remaining",
                    client_id, calls_remaining
                );

                if resp_chan
                    .send(Ok(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                    }))
                    .is_err()
                {
                    error!(
                        "[{}] Failed sending oneshot check_limit response",
                        self.node_id
                    );
                }
                Ok(())
            }
            GossipCommand::RateLimit {
                client_id,
                resp_chan,
            } => {
                let dstate: Option<DistributedBucketExternal>;
                // let mutable result with MutexGuard drop after this block
                {
                    let mut result = self
                        .rate_limiter
                        .lock()
                        .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;

                    let calls_left = result.limit_calls_for_client(client_id.to_string());
                    let response: Option<CheckCallsResponse>;
                    if let Some(calls_remaining) = calls_left {
                        response = Some(CheckCallsResponse {
                            client_id: client_id.clone(),
                            calls_remaining,
                        });
                    } else {
                        response = None;
                    }
                    if resp_chan.send(Ok(response)).is_err() {
                        error!(
                            "[{}] Failed sending oneshot rate_limit response",
                            self.node_id
                        );
                    }
                    dstate = result.client_delta_state_for_gossip(&client_id);
                }
                // Send immediate gossip update after rate limiting
                if let Some(dstate) = dstate {
                    let message = GossipMessage::DeltaStateSync {
                        updates: vec![dstate],
                        sender_node_id: self.node_id,
                        response_addr: self.response_addr,
                        propagation_factor: 3, // Limit spread
                    };
                    let packet = GossipPacket::new(message);
                    self.send_gossip_packet(packet).await?;
                }
                Ok(())
            }
        }
    }
    /// Process an incoming gossip packet
    async fn process_gossip_packet(&self, data: Bytes, peer_addr: SocketAddr) -> Result<()> {
        match GossipPacket::deserialize(&data) {
            Ok(packet) => {
                match packet.message {
                    GossipMessage::StateRequest { .. } => {
                        debug!(
                            "[{}] Received StateRequest - not yet implemented",
                            self.node_id
                        );
                        // TODO: Implement state request handling
                    }
                    GossipMessage::DeltaStateSync {
                        updates,
                        sender_node_id,
                        response_addr: _,
                        propagation_factor,
                    } => {
                        // Don't process our own messages
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing DeltaStateSync from node {} with {} updates",
                            self.node_id,
                            sender_node_id,
                            updates.len()
                        );

                        self.merge_gossip_state(&updates).await;
                        // Propagate further if propagation_factor > 0
                        if propagation_factor > 0 {
                            let message = GossipMessage::DeltaStateSync {
                                updates, // propagate the same updates to other nodes
                                sender_node_id,
                                response_addr: self.response_addr,
                                propagation_factor: propagation_factor - 1,
                            };
                            let packet = GossipPacket::new(message);
                            self.send_gossip_packet(packet).await?;
                        }
                    }
                    GossipMessage::StateResponse {
                        responding_node_id,
                        requested_data,
                        response_addr: _,
                    } => {
                        // Don't process our own messages
                        if responding_node_id == self.node_id {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing StateResponse from node {} for key {}",
                            self.node_id, responding_node_id, requested_data.client_id
                        );
                        let req = vec![requested_data];
                        // Merge each incoming update
                        self.merge_gossip_state(&req).await;
                    }
                    GossipMessage::Heartbeat {
                        node_id: heartbeat_node_id,
                        timestamp: _,
                        vclock: _,
                        response_addr: _,
                    } => {
                        // Don't process our own heartbeat
                        if heartbeat_node_id == self.node_id {
                            return Ok(());
                        }
                    }
                };
                Ok(())
            }
            Err(e) => {
                debug!(
                    "[{}] Failed to deserialize packet: {} from {}",
                    self.node_id, e, peer_addr
                );
                Err(ColibriError::Transport(format!(
                    "Deserialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Send a gossip packet to a random peer
    async fn send_gossip_packet(&self, packet: GossipPacket) -> Result<()> {
        if self.transport.is_none() {
            debug!(
                "[{}] Gossip transport not configured, skipping packet send",
                self.node_id
            );
            return Ok(());
        }
        match packet.serialize() {
            Ok(data) => {
                let _peer = self
                    .transport
                    .as_ref()
                    .unwrap()
                    .clone()
                    .send_to_random_peers(&data, self.gossip_fanout)
                    .await
                    .map_err(|e| ColibriError::Transport(format!("Send failed: {}", e)))?;
                Ok(())
            }
            Err(e) => {
                debug!("[{}] Failed to serialize packet: {}", self.node_id, e);
                Err(ColibriError::Transport(format!(
                    "Serialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub async fn merge_gossip_state(&self, entries: &[DistributedBucketExternal]) {
        let node_id = self.node_id;
        let rate_limiter = Arc::clone(&self.rate_limiter);
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );
        let _ = rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
            .map(|mut rl| rl.accept_delta_state(entries));
        debug!("[{}] merge_gossip_state_static completed", node_id);
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for GossipController to verify core functionality

    use super::*;
    use std::collections::HashSet;
    use tokio::sync::oneshot;

    /// Create a test settings instance for gossip controller
    fn create_test_settings() -> settings::Settings {
        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8410,
            listen_port_tcp: 8411,
            listen_port_udp: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Gossip,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology: HashSet::new(), // Empty topology for simple tests
            failure_timeout_secs: 30,
        }
    }

    #[tokio::test]
    async fn test_controller_creation() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();

        // Basic checks that controller is properly initialized
        assert!(!controller.has_gossip()); // No peers in topology
        assert_eq!(controller.gossip_interval_ms, 1000);
        assert_eq!(controller.gossip_fanout, 3);
    }

    #[tokio::test]
    async fn test_rate_limit_command() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = GossipCommand::RateLimit {
            client_id: "test_client".to_string(),
            resp_chan: tx,
        };

        // Handle the rate limit command
        controller.handle_command(cmd).await.unwrap();

        // Check response
        let response = rx.await.unwrap().unwrap();
        assert!(response.is_some());
        let check_result = response.unwrap();
        assert_eq!(check_result.client_id, "test_client");
        assert!(check_result.calls_remaining < 100); // Should consume tokens
    }

    #[tokio::test]
    async fn test_check_limit_command() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = GossipCommand::CheckLimit {
            client_id: "test_client".to_string(),
            resp_chan: tx,
        };

        // Handle the check limit command
        controller.handle_command(cmd).await.unwrap();

        // Check response
        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.client_id, "test_client");
        // For a new client, should have full token bucket
        assert_eq!(response.calls_remaining, 100);
    }

    #[tokio::test]
    async fn test_expire_keys_command() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();

        let cmd = GossipCommand::ExpireKeys;

        // Should not panic or error
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_gossip_message_processing() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();

        // Create a heartbeat message
        let message = GossipMessage::Heartbeat {
            node_id: NodeId::new(999), // Different node ID
            timestamp: 1234567890,
            vclock: crdts::VClock::new(),
            response_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        let packet = GossipPacket::new(message);
        let data = packet.serialize().unwrap();

        let cmd = GossipCommand::GossipMessageReceived {
            data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should process without error
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_self_message_filtering() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();
        let our_node_id = controller.node_id;

        // Create a heartbeat from our own node
        let message = GossipMessage::Heartbeat {
            node_id: our_node_id, // Same node ID as controller
            timestamp: 1234567890,
            vclock: crdts::VClock::new(),
            response_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        let packet = GossipPacket::new(message);
        let data = packet.serialize().unwrap();

        let cmd = GossipCommand::GossipMessageReceived {
            data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should ignore our own message
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_rate_limiting_exhaustion() {
        let mut settings = create_test_settings();
        settings.rate_limit_max_calls_allowed = 3; // Very small limit

        let controller = GossipController::new(settings).await.unwrap();
        let client_id = "heavy_client".to_string();

        // Make multiple calls to exhaust the rate limit
        let mut successful_calls = 0;
        let mut failed_calls = 0;

        for _i in 0..5 {
            let (tx, rx) = oneshot::channel();
            let cmd = GossipCommand::RateLimit {
                client_id: client_id.clone(),
                resp_chan: tx,
            };

            controller.handle_command(cmd).await.unwrap();
            let response = rx.await.unwrap().unwrap();

            if response.is_some() {
                successful_calls += 1;
            } else {
                failed_calls += 1;
            }
        }

        // We should have some successful calls (at least 1) and some failed ones
        assert!(
            successful_calls > 0,
            "Should have at least one successful call"
        );
        assert!(
            failed_calls > 0,
            "Should have at least one rate-limited call"
        );
        assert!(successful_calls <= 3, "Should not exceed the rate limit");
    }

    #[tokio::test]
    async fn test_malformed_gossip_message() {
        let settings = create_test_settings();
        let controller = GossipController::new(settings).await.unwrap();

        // Send garbage data
        let bad_data = bytes::Bytes::from_static(b"this is not a valid gossip packet");

        let cmd = GossipCommand::GossipMessageReceived {
            data: bad_data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should handle gracefully and return an error
        let result = controller.handle_command(cmd).await;
        assert!(result.is_err());
    }
}
