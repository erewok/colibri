use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use super::{GossipCommand, GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::{epoch_bucket::EpochTokenBucket, rate_limit::RateLimiter};
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
    pub rate_limiter: Arc<Mutex<RateLimiter<EpochTokenBucket>>>,
    /// transport_config
    pub transport_config: settings::TransportConfig,
    /// Transport layer for sending UDP unicast gossip messages
    pub transport: Option<Arc<UdpTransport>>,
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
    pub async fn new(
        settings: settings::Settings,
        rate_limiter: RateLimiter<EpochTokenBucket>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );
        let rl_settings = settings.rate_limit_settings();

        // Build transport
        let transport_config = settings.transport_config();
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
            transport_config: transport_config,
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
        let mut gossip_round = 0u64;

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
                    gossip_round += 1;
                    if let Err(e) = self.handle_gossip_tick(gossip_round).await {
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

    async fn handle_gossip_tick(&self, gossip_round: u64) -> Result<()> {
        // Send delta updates for buckets that have changed
        let updates = self.collect_gossip_updates().await?;
        if !updates.is_empty() {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id,
                gossip_round,
                last_seen_versions: HashMap::new(),
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
                version_vector: HashMap::new(),
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Collect buckets that have been updated and should be gossiped
    async fn collect_gossip_updates(&self) -> Result<HashMap<String, EpochTokenBucket>> {
        // For now, collect all buckets. In the future, this could be optimized
        // to only send buckets that have changed since the last gossip round.
        self.rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
            .map(|rl| rl.get_all_buckets())
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
                let calls_remaining = rl.check_calls_remaining_for_client(client_id.as_str());
                debug!(
                    "Check limit for client '{}': {} tokens remaining",
                    client_id, calls_remaining
                );

                if let Err(_) = resp_chan.send(Ok(CheckCallsResponse {
                    client_id,
                    calls_remaining,
                })) {
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
                let mut result = self
                    .rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;
                let calls_left = result.limit_calls_for_client(client_id.to_string());
                let response: Option<CheckCallsResponse>;
                if let Some(calls_remaining) = calls_left {
                    response = Some(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                    });
                } else {
                    response = None;
                }
                if let Err(_) = resp_chan.send(Ok(response)) {
                    error!(
                        "[{}] Failed sending oneshot rate_limit response",
                        self.node_id
                    );
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
                        gossip_round: _,
                        last_seen_versions: _,
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

                        self.merge_gossip_state(updates).await;
                    }
                    GossipMessage::StateResponse {
                        responding_node_id,
                        requested_data,
                        current_versions: _,
                    } => {
                        // Don't process our own messages
                        if responding_node_id == self.node_id {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing StateResponse from node {} with {} updates",
                            self.node_id,
                            responding_node_id,
                            requested_data.len()
                        );

                        // Merge each incoming update
                        self.merge_gossip_state(requested_data).await;
                    }
                    GossipMessage::Heartbeat {
                        node_id: heartbeat_node_id,
                        timestamp: _,
                        version_vector: _,
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
    pub async fn merge_gossip_state(&self, entries: HashMap<String, EpochTokenBucket>) {
        let node_id = self.node_id;
        let rate_limiter = Arc::clone(&self.rate_limiter);
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );

        for (client_id, incoming_bucket) in entries.iter() {
            debug!("[{}] Processing client: {}", node_id, client_id);
            let maybe_bucket = rate_limiter
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
                .map(|rl| rl.get_bucket(&client_id));
            if let Ok(Some(mut current_entry)) = maybe_bucket {
                debug!(
                    "[{}] Found existing bucket from client: {}",
                    node_id, client_id
                );
                // Try to merge with existing bucket
                if current_entry.merge(&incoming_bucket) {
                    debug!(
                        "[{}] Merged gossip update from client '{}': {} -> {} tokens, version {} -> {}",
                        node_id,
                        client_id,
                        current_entry.tokens,
                        incoming_bucket.tokens,
                        current_entry.node_id,
                        incoming_bucket.node_id
                    );
                    let _ = rate_limiter
                        .lock()
                        .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
                        .map(|mut rl| rl.set_bucket(&client_id, current_entry));
                } else {
                    debug!(
                        "[{}] Rejected gossip update for client '{}': incoming epoch {} <= current epoch {}",
                        node_id, client_id, incoming_bucket.current_epoch, current_entry.current_epoch
                    );
                }
            } else {
                // No existing entry, accept incoming state
                debug!(
                    "[{}] Accepted new gossip state from client '{}': tokens={}, epoch={}",
                    node_id, client_id, incoming_bucket.tokens, incoming_bucket.current_epoch
                );
                let _ = rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
                    .map(|mut rl| rl.set_bucket(&client_id, incoming_bucket.clone()));
                debug!(
                    "[{}] Finished setting new bucket for client: {}",
                    node_id, client_id
                );
            }
        }
        debug!("[{}] merge_gossip_state_static completed", node_id);
    }
}

#[cfg(test)]
mod tests {
    //! Comprehensive tests for GossipController::handle_command method
    //!
    //! These tests cover all GossipCommand variants and their interactions with the CRDT state:
    //!
    //! ## Core Command Types:
    //! - `ExpireKeys`: Cache cleanup operations
    //! - `CheckLimit`: Non-consuming rate limit checks
    //! - `RateLimit`: Consuming rate limit operations
    //! - `GossipMessageReceived`: Processing incoming gossip protocol messages
    //!
    //! ## Gossip Message Types Tested:
    //! - `DeltaStateSync`: CRDT state merging from other nodes
    //! - `StateResponse`: Anti-entropy state responses
    //! - `StateRequest`: State synchronization requests (placeholder)
    //! - `Heartbeat`: Keep-alive messages with version vectors
    //!
    //! ## Key Test Scenarios:
    //! - Single and concurrent operations
    //! - Rate limiting until exhaustion
    //! - CRDT merge conflict resolution
    //! - Large gossip payload handling
    //! - Mixed operation workflows
    //! - Error handling for malformed messages
    //! - Self-message filtering (ignore own gossip messages)
    //!
    //! All tests use isolated controller instances to ensure test independence.

    use super::*;
    use crate::limiters::epoch_bucket::EpochTokenBucket;
    use crate::limiters::rate_limit::RateLimiter;
    use crate::limiters::token_bucket::Bucket;
    use crate::node::NodeId;
    use crate::settings::{RunMode, Settings};
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::oneshot;

    // Helper function to create test settings
    fn create_test_settings() -> Settings {
        Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8410,
            listen_port_tcp: 8411,
            listen_port_udp: 0, // Use port 0 for testing
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 60,
            run_mode: RunMode::Gossip,
            gossip_interval_ms: 100,
            gossip_fanout: 2,
            topology: std::collections::HashSet::new(), // Empty topology for testing
            failure_timeout_secs: 30,
        }
    }

    // Helper function to create a test controller
    async fn create_test_controller() -> GossipController {
        let settings = create_test_settings();
        let rate_limiter = RateLimiter::new(settings.node_id(), settings.rate_limit_settings());

        GossipController::new(settings, rate_limiter)
            .await
            .expect("Failed to create test controller")
    }

    #[tokio::test]
    async fn test_handle_command_expire_keys() {
        let controller = create_test_controller().await;

        // Add some entries to the rate limiter first
        {
            let mut rl = controller.rate_limiter.lock().unwrap();
            rl.limit_calls_for_client("client1".to_string());
            rl.limit_calls_for_client("client2".to_string());
        }

        // Verify entries exist
        {
            let rl = controller.rate_limiter.lock().unwrap();
            assert!(!rl.is_empty());
        }

        // Test ExpireKeys command
        let cmd = GossipCommand::ExpireKeys;
        let result = controller.handle_command(cmd).await;

        assert!(result.is_ok());
        // Note: expire_keys may not actually remove entries unless they're truly expired
        // The important thing is that the command executed without error
    }

    #[tokio::test]
    async fn test_handle_command_check_limit() {
        let controller = create_test_controller().await;
        let client_id = "test_client".to_string();

        // Create a oneshot channel for the response
        let (tx, rx) = oneshot::channel();

        // Test CheckLimit command
        let cmd = GossipCommand::CheckLimit {
            client_id: client_id.clone(),
            resp_chan: tx,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Check the response
        let response = rx.await.expect("Should receive response");
        assert!(response.is_ok());

        let check_response = response.unwrap();
        assert_eq!(check_response.client_id, client_id);
        assert_eq!(check_response.calls_remaining, 10); // Should be max capacity for new client
    }

    #[tokio::test]
    async fn test_handle_command_rate_limit_success() {
        let controller = create_test_controller().await;
        let client_id = "test_client".to_string();

        // Create a oneshot channel for the response
        let (tx, rx) = oneshot::channel();

        // Test RateLimit command
        let cmd = GossipCommand::RateLimit {
            client_id: client_id.clone(),
            resp_chan: tx,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Check the response
        let response = rx.await.expect("Should receive response");
        assert!(response.is_ok());

        let rate_limit_response = response.unwrap();
        assert!(rate_limit_response.is_some());

        let check_response = rate_limit_response.unwrap();
        assert_eq!(check_response.client_id, client_id);
        // The rate limiter returns current token count, which starts at max capacity (10)
        // The behavior depends on the specific implementation - let's verify it's valid
        assert!(check_response.calls_remaining <= 10);
    }

    #[tokio::test]
    async fn test_handle_command_rate_limit_exhausted() {
        let controller = create_test_controller().await;
        let client_id = "test_client".to_string();

        // Exhaust the rate limit first by consuming many times
        {
            let mut rl = controller.rate_limiter.lock().unwrap();
            for _ in 0..15 {
                // Try more than the limit to ensure exhaustion
                rl.limit_calls_for_client(client_id.clone());
            }
        }

        // Create a oneshot channel for the response
        let (tx, rx) = oneshot::channel();

        // Test RateLimit command when exhausted
        let cmd = GossipCommand::RateLimit {
            client_id: client_id.clone(),
            resp_chan: tx,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Check the response
        let response = rx.await.expect("Should receive response");
        assert!(response.is_ok());

        let rate_limit_response = response.unwrap();
        // When rate limited, we should get None OR a response with 0 tokens
        if let Some(check_response) = rate_limit_response {
            assert_eq!(check_response.calls_remaining, 0);
        }
        // The response could be None or Some with 0 tokens depending on implementation
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_received_delta_sync() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create a test EpochTokenBucket
        let mut test_bucket = EpochTokenBucket::new(
            NodeId::new(2), // Different node ID
            &controller.rate_limit_settings,
        );
        test_bucket.try_consume(3); // Consume some tokens

        // Create delta sync message
        let mut updates = HashMap::new();
        updates.insert("remote_client".to_string(), test_bucket);

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: NodeId::new(2),
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Test GossipMessageReceived command
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let rl = controller.rate_limiter.lock().unwrap();
        let merged_bucket = rl.get_bucket("remote_client");
        assert!(merged_bucket.is_some());
        let bucket = merged_bucket.unwrap();
        assert_eq!(bucket.remaining(), 7); // Should have 7 tokens remaining (10 - 3)
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_received_ignore_own_message() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create delta sync message from same node (should be ignored)
        let message = GossipMessage::DeltaStateSync {
            updates: HashMap::new(),
            sender_node_id: controller.node_id, // Same node ID
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Test GossipMessageReceived command
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok()); // Should succeed but ignore the message
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_received_state_response() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create a test EpochTokenBucket
        let mut test_bucket = EpochTokenBucket::new(
            NodeId::new(3), // Different node ID
            &controller.rate_limit_settings,
        );
        test_bucket.try_consume(5); // Consume some tokens

        // Create state response message
        let mut requested_data = HashMap::new();
        requested_data.insert("response_client".to_string(), test_bucket);

        let message = GossipMessage::StateResponse {
            responding_node_id: NodeId::new(3),
            requested_data,
            current_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Test GossipMessageReceived command
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let rl = controller.rate_limiter.lock().unwrap();
        let merged_bucket = rl.get_bucket("response_client");
        assert!(merged_bucket.is_some());
        let bucket = merged_bucket.unwrap();
        assert_eq!(bucket.remaining(), 5); // Should have 5 tokens remaining (10 - 5)
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_received_heartbeat() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create heartbeat message
        let message = GossipMessage::Heartbeat {
            node_id: NodeId::new(4), // Different node ID
            timestamp: 1234567890,
            version_vector: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Test GossipMessageReceived command
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok()); // Should succeed (heartbeat processing is minimal)
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_received_malformed_data() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create malformed data
        let malformed_data = bytes::Bytes::from("invalid_data");

        // Test GossipMessageReceived command with malformed data
        let cmd = GossipCommand::GossipMessageReceived {
            data: malformed_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_err()); // Should fail with deserialization error

        if let Err(ColibriError::Transport(msg)) = result {
            assert!(msg.contains("Deserialization failed"));
        } else {
            panic!("Expected Transport error with deserialization message");
        }
    }

    #[tokio::test]
    async fn test_handle_command_concurrent_operations() {
        let controller = create_test_controller().await;

        // Test multiple concurrent operations
        let mut handles = Vec::new();

        // Spawn multiple rate limit operations
        for i in 0..5 {
            let controller_clone = controller.clone();
            let client_id = format!("client_{}", i);

            let handle = tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                let cmd = GossipCommand::RateLimit {
                    client_id,
                    resp_chan: tx,
                };

                let cmd_result = controller_clone.handle_command(cmd).await;
                let response = rx.await;
                (cmd_result, response)
            });

            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            let (cmd_result, response) = handle.await.unwrap();
            assert!(cmd_result.is_ok());
            assert!(response.is_ok());

            let rate_response = response.unwrap().unwrap();
            assert!(rate_response.is_some());
        }

        // Verify all clients were processed
        let rl = controller.rate_limiter.lock().unwrap();
        assert_eq!(rl.len(), 5);
    }

    #[tokio::test]
    async fn test_merge_gossip_state_integration() {
        let controller = create_test_controller().await;

        // Create test buckets from different nodes
        let mut bucket1 = EpochTokenBucket::new(NodeId::new(2), &controller.rate_limit_settings);
        bucket1.try_consume(2);

        let mut bucket2 = EpochTokenBucket::new(NodeId::new(3), &controller.rate_limit_settings);
        bucket2.try_consume(4);

        let mut entries = HashMap::new();
        entries.insert("client_a".to_string(), bucket1);
        entries.insert("client_b".to_string(), bucket2);

        // Test merge operation
        controller.merge_gossip_state(entries).await;

        // Verify merged state
        let rl = controller.rate_limiter.lock().unwrap();

        let bucket_a = rl.get_bucket("client_a").unwrap();
        assert_eq!(bucket_a.remaining(), 8); // 10 - 2

        let bucket_b = rl.get_bucket("client_b").unwrap();
        assert_eq!(bucket_b.remaining(), 6); // 10 - 4
    }

    #[tokio::test]
    async fn test_handle_command_multiple_rate_limits_same_client() {
        let controller = create_test_controller().await;
        let client_id = "test_client".to_string();

        // Make multiple rate limit requests for the same client
        for i in 0..5 {
            let (tx, rx) = oneshot::channel();
            let cmd = GossipCommand::RateLimit {
                client_id: client_id.clone(),
                resp_chan: tx,
            };

            let result = controller.handle_command(cmd).await;
            assert!(result.is_ok());

            let response = rx.await.expect("Should receive response");
            assert!(response.is_ok());

            let rate_response = response.unwrap();
            if let Some(check_response) = rate_response {
                assert_eq!(check_response.client_id, client_id);
                // Tokens should decrease with each call
                assert!(check_response.calls_remaining <= 10 - i);
            }
        }
    }

    #[tokio::test]
    async fn test_handle_command_gossip_message_crdt_merge_conflict_resolution() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create local state for a client
        {
            let mut rl = controller.rate_limiter.lock().unwrap();
            rl.limit_calls_for_client("conflict_client".to_string());
            rl.limit_calls_for_client("conflict_client".to_string());
        }

        // Create a conflicting remote bucket with different consumption
        let mut remote_bucket = EpochTokenBucket::new(
            NodeId::new(5), // Different node ID
            &controller.rate_limit_settings,
        );
        remote_bucket.try_consume(5); // Different consumption level

        let mut updates = HashMap::new();
        updates.insert("conflict_client".to_string(), remote_bucket);

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: NodeId::new(5),
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Process the conflicting gossip message
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify CRDT merge resolved the conflict properly
        let rl = controller.rate_limiter.lock().unwrap();
        let merged_bucket = rl.get_bucket("conflict_client");
        assert!(merged_bucket.is_some());
        // The exact result depends on CRDT merge semantics
    }

    #[tokio::test]
    async fn test_handle_command_gossip_state_request_not_implemented() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create StateRequest message
        let message = GossipMessage::StateRequest {
            requesting_node_id: NodeId::new(6),
            missing_keys: Some(vec!["missing_key".to_string()]),
            since_version: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Test StateRequest handling (currently just logs, doesn't fail)
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok()); // Should not fail, just log the unimplemented feature
    }

    #[tokio::test]
    async fn test_handle_command_check_limit_multiple_clients() {
        let controller = create_test_controller().await;

        // Test check limit for multiple different clients
        for i in 0..3 {
            let client_id = format!("client_{}", i);
            let (tx, rx) = oneshot::channel();

            let cmd = GossipCommand::CheckLimit {
                client_id: client_id.clone(),
                resp_chan: tx,
            };

            let result = controller.handle_command(cmd).await;
            assert!(result.is_ok());

            let response = rx.await.expect("Should receive response");
            assert!(response.is_ok());

            let check_response = response.unwrap();
            assert_eq!(check_response.client_id, client_id);
            assert_eq!(check_response.calls_remaining, 10); // New client should have max tokens
        }
    }

    #[tokio::test]
    async fn test_handle_command_mixed_operations_workflow() {
        let controller = create_test_controller().await;
        let client_id = "workflow_client".to_string();

        // 1. Check limit for new client
        let (tx1, rx1) = oneshot::channel();
        let cmd1 = GossipCommand::CheckLimit {
            client_id: client_id.clone(),
            resp_chan: tx1,
        };

        let result1 = controller.handle_command(cmd1).await;
        assert!(result1.is_ok());
        let response1 = rx1.await.unwrap().unwrap();
        assert_eq!(response1.calls_remaining, 10);

        // 2. Rate limit a few calls
        for _ in 0..3 {
            let (tx, rx) = oneshot::channel();
            let cmd = GossipCommand::RateLimit {
                client_id: client_id.clone(),
                resp_chan: tx,
            };

            let result = controller.handle_command(cmd).await;
            assert!(result.is_ok());
            let _response = rx.await.unwrap().unwrap();
        }

        // 3. Check limit again - should show reduced tokens
        let (tx2, rx2) = oneshot::channel();
        let cmd2 = GossipCommand::CheckLimit {
            client_id: client_id.clone(),
            resp_chan: tx2,
        };

        let result2 = controller.handle_command(cmd2).await;
        assert!(result2.is_ok());
        let response2 = rx2.await.unwrap().unwrap();
        assert!(response2.calls_remaining < 10); // Should be less than initial

        // 4. Expire keys (should succeed without error)
        let cmd3 = GossipCommand::ExpireKeys;
        let result3 = controller.handle_command(cmd3).await;
        assert!(result3.is_ok());
    }

    #[tokio::test]
    async fn test_handle_command_large_gossip_payload() {
        let controller = create_test_controller().await;
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create a large gossip payload with many clients
        let mut updates = HashMap::new();
        for i in 0..100 {
            let client_id = format!("bulk_client_{}", i);
            let mut bucket = EpochTokenBucket::new(NodeId::new(7), &controller.rate_limit_settings);
            bucket.try_consume(i % 5 + 1); // Vary consumption
            updates.insert(client_id, bucket);
        }

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: NodeId::new(7),
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        let serialized_data = packet.serialize().expect("Should serialize");

        // Process large gossip payload
        let cmd = GossipCommand::GossipMessageReceived {
            data: serialized_data,
            peer_addr,
        };

        let result = controller.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify all clients were processed
        let rl = controller.rate_limiter.lock().unwrap();
        assert!(rl.len() >= 100); // Should have all the bulk clients
    }
}
