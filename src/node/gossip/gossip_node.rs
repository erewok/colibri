use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};

use async_trait::async_trait;
use tracing::{debug, info};

use super::{GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::{rate_limit::RateLimiter, versioned_bucket::VersionedTokenBucket};
use crate::node::{CheckCallsResponse, Node};
use crate::settings;
use crate::transport::UdpTransport;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipNode {
    // rate-limit settings
    rate_limit_settings: settings::RateLimitSettings,
    /// Local rate limiter - handles all bucket operations
    rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
    /// Channel for receiving incoming gossip messages from network
    inbound_msg_tx: mpsc::Sender<Vec<u8>>,
    /// Transport layer for network communication
    transport: Option<Arc<UdpTransport>>,
    /// Gossip configuration
    gossip_interval_ms: u64,
    gossip_fanout: usize,
}

impl std::fmt::Debug for GossipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipNode")
            .field("node_id", &self.rate_limit_settings.node_id)
            .field("has_transport", &self.transport.is_some())
            .finish()
    }
}

impl GossipNode {
    pub async fn new(
        settings: settings::Settings,
        rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );
        let rl_settings = settings.rate_limit_settings();

        // Create transport - use port 0 for tests to avoid conflicts
        let gossip_port = if cfg!(test) {
            0
        } else {
            settings.listen_port_udp
        };
        // Build transport
        let transport = if !settings.topology.is_empty() {
            info!(
                "Initializing gossip transport with {} peers: {:?}",
                settings.topology.len(),
                settings.topology
            );
            Some(Arc::new(
                UdpTransport::new(node_id, gossip_port, settings.topology.clone())
                    .await
                    .map_err(|e| {
                        ColibriError::Transport(format!("Failed to create transport: {}", e))
                    })?,
            ))
        } else {
            None
        };

        // Create channel for incoming gossip messages from network
        let (inbound_msg_tx, inbound_msg_rx) = mpsc::channel(1000);

        let node_id = rl_settings.node_id;
        let node = Self {
            rate_limit_settings: rl_settings,
            rate_limiter: rate_limiter.clone(),
            inbound_msg_tx: inbound_msg_tx.clone(),
            transport: transport.clone(),
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
        };

        // Start the central IO processing loop if we have transport
        if let Some(transport) = transport {
            let rate_limiter_for_io = rate_limiter.clone();
            let gossip_interval = std::time::Duration::from_millis(settings.gossip_interval_ms);
            tokio::spawn(async move {
                Self::start_io_loop(
                    node_id,
                    rate_limiter_for_io,
                    inbound_msg_rx,
                    transport,
                    gossip_interval,
                )
                .await;
            });
        }

        Ok(node)
    }
    pub fn node_id(&self) -> u32 {
        self.rate_limit_settings.node_id
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub async fn merge_gossip_state(&self, entries: HashMap<String, VersionedTokenBucket>) {
        Self::merge_gossip_state_static(self.node_id(), &self.rate_limiter, entries).await;
    }

    /// Static version of merge_gossip_state for use in IO loop
    async fn merge_gossip_state_static(
        node_id: u32,
        rate_limiter: &Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        entries: HashMap<String, VersionedTokenBucket>,
    ) {
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );

        // Add explicit debug to track progress
        eprintln!(
            "[DEBUG] merge_gossip_state_static starting with {} entries",
            entries.len()
        );

        for (client_id, incoming_bucket) in entries.into_iter() {
            eprintln!("[DEBUG] Processing client: {}", client_id);
            if let Some(mut current_entry) = rate_limiter.read().await.get_bucket(&client_id) {
                eprintln!("[DEBUG] Found existing bucket for client: {}", client_id);
                let current_version = current_entry
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);
                let incoming_version = incoming_bucket
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);

                // Try to merge with existing bucket
                if current_entry.merge(incoming_bucket.clone(), node_id) {
                    debug!(
                        "[{}] Merged gossip update for client '{}': {} -> {} tokens, version {} -> {}",
                        node_id,
                        client_id,
                        current_entry.bucket.tokens,
                        incoming_bucket.bucket.tokens,
                        current_version,
                        incoming_version
                    );
                    rate_limiter
                        .write()
                        .await
                        .set_bucket(&client_id, current_entry);
                } else {
                    debug!(
                        "[{}] Rejected gossip update for client '{}': incoming version {} <= current version {}",
                        node_id, client_id, incoming_version, current_version
                    );
                }
            } else {
                eprintln!(
                    "[DEBUG] No existing bucket, creating new one for client: {}",
                    client_id
                );
                // No existing entry, accept incoming state
                debug!(
                    "[{}] Accepted new gossip state for client '{}': tokens={}, version={}",
                    node_id,
                    client_id,
                    incoming_bucket.bucket.tokens,
                    incoming_bucket
                        .vector_clock
                        .get_timestamp(incoming_bucket.last_updated_by)
                );
                rate_limiter
                    .write()
                    .await
                    .set_bucket(&client_id, incoming_bucket);
                eprintln!(
                    "                eprintln!("[DEBUG] Finished setting new bucket for client: {}", client_id);
            }
        }
        eprintln!("[DEBUG] merge_gossip_state_static completed");
    }",
                    client_id
                );
            }
        }
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.transport.is_some()
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.rate_limiter.read().await.len();
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_id(),
            bucket_count
        );
    }

    /// Get channel sender for incoming messages (used by transport layer)
    pub fn inbound_message_sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.inbound_msg_tx.clone()
    }

    /// Central IO loop - handles all network communication and gossip timing
    /// This eliminates async contention by processing all IO in one place
    ///
    /// Static method that takes only the shared state it needs, avoiding
    /// the need to clone the entire GossipNode
    async fn start_io_loop(
        node_id: u32,
        rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        mut inbound_rx: mpsc::Receiver<Vec<u8>>,
        transport: Arc<UdpTransport>,
        gossip_interval: std::time::Duration,
    ) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            node_id,
            gossip_interval.as_millis()
        );

        let mut gossip_timer = tokio::time::interval(gossip_interval);
        let mut last_gossip_buckets: HashMap<String, Instant> = HashMap::new();
        let mut gossip_round = 0u64;

        loop {
            tokio::select! {
                // Handle incoming messages from network
                Some(data) = inbound_rx.recv() => {
                    if let Err(e) = Self::handle_incoming_message(node_id, &rate_limiter, data).await {
                        debug!("[{}] Error processing incoming message: {}", node_id, e);
                    }
                }

                // Gossip timer - send delta updates
                _ = gossip_timer.tick() => {
                    if let Err(e) = Self::handle_gossip_tick(
                        node_id,
                        &rate_limiter,
                        &transport,
                        &mut last_gossip_buckets,
                        &mut gossip_round,
                    ).await {
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

    /// Handle incoming gossip message from network
    async fn handle_incoming_message(
        node_id: u32,
        rate_limiter: &Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        data: Vec<u8>,
    ) -> Result<()> {
        match GossipPacket::deserialize(&data) {
            Ok(packet) => {
                debug!("[{}] Received gossip packet", node_id);
                Self::process_incoming_message(node_id, rate_limiter, packet).await
            }
            Err(e) => {
                debug!("[{}] Failed to deserialize packet: {}", node_id, e);
                Err(ColibriError::Transport(format!(
                    "Deserialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Handle gossip tick - for now just send a heartbeat
    /// TODO: Implement proper delta-state gossip when we have change tracking
    async fn handle_gossip_tick(
        node_id: u32,
        _rate_limiter: &Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        transport: &UdpTransport,
        _last_gossip_buckets: &mut HashMap<String, Instant>,
        gossip_round: &mut u64,
    ) -> Result<()> {
        // For now, just send a heartbeat to keep connections alive
        let message = GossipMessage::Heartbeat {
            node_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            version_vector: HashMap::new(),
        };

        let packet = GossipPacket::new(message);
        Self::send_gossip_packet(node_id, packet, transport).await?;

        *gossip_round += 1;
        debug!(
            "[{}] Sent heartbeat in gossip round {}",
            node_id, *gossip_round
        );

        Ok(())
    }

    /// Send a gossip packet to a random peer
    async fn send_gossip_packet(
        node_id: u32,
        packet: GossipPacket,
        transport: &UdpTransport,
    ) -> Result<()> {
        match packet.serialize() {
            Ok(data) => {
                debug!("[{}] Sending gossip packet", node_id);
                let _peer = transport
                    .send_to_random_peer(&data)
                    .await
                    .map_err(|e| ColibriError::Transport(format!("Send failed: {}", e)))?;
                Ok(())
            }
            Err(e) => {
                debug!("[{}] Failed to serialize packet: {}", node_id, e);
                Err(ColibriError::Transport(format!(
                    "Serialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Process a single incoming gossip message
    async fn process_incoming_message(
        node_id: u32,
        rate_limiter: &Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        packet: GossipPacket,
    ) -> Result<()> {
        match packet.message {
            GossipMessage::StateRequest { .. } => {
                debug!("[{}] Received StateRequest - not yet implemented", node_id);
                // TODO: Implement state request handling
            }
            GossipMessage::DeltaStateSync {
                updates,
                sender_node_id,
                gossip_round: _,
                last_seen_versions: _,
            } => {
                // Don't process our own messages
                if sender_node_id == node_id {
                    debug!("[{}] Ignoring our own delta gossip message", node_id);
                    return Ok(());
                }

                debug!(
                    "[{}] Processing DeltaStateSync from node {} with {} updates",
                    node_id,
                    sender_node_id,
                    updates.len()
                );

                Self::merge_gossip_state_static(node_id, rate_limiter, updates).await;
            }
            GossipMessage::StateResponse {
                responding_node_id,
                requested_data,
                current_versions: _,
            } => {
                // Don't process our own messages
                if responding_node_id == node_id {
                    return Ok(());
                }

                debug!(
                    "[{}] Processing StateResponse from node {} with {} updates",
                    node_id,
                    responding_node_id,
                    requested_data.len()
                );

                // Merge each incoming update
                Self::merge_gossip_state_static(node_id, rate_limiter, requested_data).await;
            }
            GossipMessage::Heartbeat {
                node_id: heartbeat_node_id,
                timestamp: _,
                version_vector: _,
            } => {
                // Don't process our own heartbeat
                if heartbeat_node_id == node_id {
                    return Ok(());
                }

                debug!(
                    "[{}] Received heartbeat from node {}",
                    node_id, heartbeat_node_id
                );
                // TODO: Update peer liveness information
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Node for GossipNode {
    /// Check remaining calls for a client using local state
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let rate_limiter = self.rate_limiter.read().await;
        let calls_remaining = rate_limiter
            .async_check_calls_remaining_for_client(client_id.as_str())
            .await;
        debug!(
            "Check limit for client '{}': {} tokens remaining",
            client_id, calls_remaining
        );

        Ok(CheckCallsResponse {
            client_id,
            calls_remaining,
        })
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let mut rate_limiter = self.rate_limiter.write().await;
        let calls_left = rate_limiter.limit_calls_for_client(client_id.to_string());
        if let Some(calls_remaining) = calls_left {
            Ok(Some(CheckCallsResponse {
                client_id,
                calls_remaining,
            }))
        } else {
            Ok(None)
        }
    }

    /// Expire keys from local buckets
    async fn expire_keys(&self) {
        // Expire from the legacy rate limiter
        let mut rate_limiter = self.rate_limiter.write().await;
        rate_limiter.expire_keys();

        // TODO: Implement expiry for local_buckets based on TokenBucket's last_refill time
        // For now, we'll rely on the existing rate_limiter for expiry logic
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;
    use crate::limiters::token_bucket::Bucket;

    pub fn gen_settings() -> settings::Settings {
        settings::Settings {
            listen_address: "0.0.0.0".to_string(),
            listen_port: settings::STANDARD_PORT_HTTP,
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
            node_id: 1,
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }

    pub fn new_rate_limiter() -> RateLimiter<VersionedTokenBucket> {
        RateLimiter::new(rl_settings())
    }

    #[tokio::test]
    async fn test_gossip_node_single_mode() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        // Create node with no topology (single mode even though Gossip specified)
        let mut conf = gen_settings();
        conf.topology = HashSet::new();
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();
        assert!(!node.has_gossip());
        assert!(node.transport.is_none());
    }

    #[tokio::test]
    async fn test_gossip_node_gossip_mode() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        // Create node with topology (gossip mode) - use unique port
        let topology: HashSet<SocketAddr> =
            HashSet::from(["http://127.0.0.1:7949".parse().unwrap()]);
        // Create node with topology (gossip mode)
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();
        assert!(node.has_gossip());
        assert!(node.transport.is_some());

        // Test that we can get gossip stats
    }

    #[tokio::test]
    async fn test_rate_limit_triggers_gossip() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        // Create node with topology to enable gossip - use unique port
        let mut conf = gen_settings();
        let topology: HashSet<SocketAddr> =
            HashSet::from(["http://127.0.0.1:7950".parse().unwrap()]);
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Perform rate limiting operation
        let result = node.rate_limit("test_client".to_string()).await.unwrap();
        assert!(result.is_some());

        // Check that the bucket was created locally
        assert!(node
            .rate_limiter
            .read()
            .await
            .get_bucket("test_client")
            .is_some());

        // Check that gossip stats show activity
    }

    #[tokio::test]
    async fn test_merge_gossip_state() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let mut conf = gen_settings();
        conf.listen_port = 8084;
        let topology: HashSet<SocketAddr> =
            HashSet::from(["http://127.0.0.1:7951".parse().unwrap()]);
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Create some fake gossip state
        let mut entries = std::collections::HashMap::new();
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 500,
            rate_limit_interval_seconds: 60,
            node_id: 999,
        };
        let versioned_bucket = VersionedTokenBucket::new(&rl_settings); // Different node ID
        entries.insert("remote_client".to_string(), versioned_bucket);

        // Merge the state
        node.merge_gossip_state(entries).await;

        // Check that the state was merged
        let stored_bucket = node.rate_limiter.read().await.get_bucket("remote_client");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 999);
    }

    #[tokio::test]
    async fn test_message_processing_delta_state_sync() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let node = create_gossip_node(rate_limiter, 1).await;

        // Create a delta state sync message from another node
        let mut updates = HashMap::new();
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 200,
            rate_limit_interval_seconds: 60,
            node_id: 2, // Different node ID
        };
        let versioned_bucket = VersionedTokenBucket::new(&rl_settings);
        updates.insert("client_from_node_2".to_string(), versioned_bucket);

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: 2,
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);

        // Process the message
        let result =
            GossipNode::process_incoming_message(node.node_id(), &node.rate_limiter, packet).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let stored_bucket = node
            .rate_limiter
            .read()
            .await
            .get_bucket("client_from_node_2");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 2);
    }

    #[tokio::test]
    async fn test_message_processing_ignores_own_messages() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let node = create_gossip_node(rate_limiter, 123).await;

        // Create a message from our own node
        let mut updates = HashMap::new();
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 200,
            rate_limit_interval_seconds: 60,
            node_id: 123, // Same as our node ID
        };
        let versioned_bucket = VersionedTokenBucket::new(&rl_settings);
        updates.insert("self_client".to_string(), versioned_bucket);

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: 123, // Same as local node
            gossip_round: 1,
            last_seen_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);

        // Process the message
        let result =
            GossipNode::process_incoming_message(node.node_id(), &node.rate_limiter, packet).await;
        assert!(result.is_ok());

        // Verify our own message was ignored
        assert!(node
            .rate_limiter
            .read()
            .await
            .get_bucket("self_client")
            .is_none());
    }

    #[tokio::test]
    async fn test_message_processing_state_response() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let node = create_gossip_node(rate_limiter, 1).await;

        // Create a state response message
        let mut requested_data = HashMap::new();
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 300,
            rate_limit_interval_seconds: 60,
            node_id: 3,
        };
        let versioned_bucket = VersionedTokenBucket::new(&rl_settings);
        requested_data.insert("response_client".to_string(), versioned_bucket);

        let message = GossipMessage::StateResponse {
            responding_node_id: 3,
            requested_data,
            current_versions: HashMap::new(),
        };

        let packet = GossipPacket::new(message);

        // Process the message
        let result =
            GossipNode::process_incoming_message(node.node_id(), &node.rate_limiter, packet).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let stored_bucket = node.rate_limiter.read().await.get_bucket("response_client");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 3);
    }

    #[tokio::test]
    async fn test_message_processing_heartbeat() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let node = create_gossip_node(rate_limiter, 1).await;

        // Create a heartbeat message
        let message = GossipMessage::Heartbeat {
            node_id: 4,
            timestamp: 1234567890,
            version_vector: HashMap::new(),
        };

        let packet = GossipPacket::new(message);

        // Process the message - should not error but currently does nothing
        let result =
            GossipNode::process_incoming_message(node.node_id(), &node.rate_limiter, packet).await;
        assert!(result.is_ok());

        // No state changes expected from heartbeat (TODO: track liveness)
    }

    #[tokio::test]
    async fn test_channel_senders_available() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let node = create_gossip_node(rate_limiter, 1).await;

        // Verify we can get inbound channel sender
        let inbound_sender = node.inbound_message_sender();

        // Verify channel is not closed
        assert!(!inbound_sender.is_closed());
    }

    #[tokio::test]
    async fn test_serialize_deserialize_round_trip() {
        // Test that our packet serialization works correctly
        let mut updates = HashMap::new();
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            node_id: 5,
        };
        let versioned_bucket = VersionedTokenBucket::new(&rl_settings);
        updates.insert("test_client".to_string(), versioned_bucket);

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: 5,
            gossip_round: 42,
            last_seen_versions: HashMap::new(),
        };

        let original_packet = GossipPacket::new(message);

        // Serialize and deserialize
        let serialized = original_packet.serialize().expect("Failed to serialize");
        let deserialized = GossipPacket::deserialize(&serialized).expect("Failed to deserialize");

        // Verify packet ID is preserved
        assert_eq!(original_packet.packet_id, deserialized.packet_id);

        // Verify message content
        match deserialized.message {
            GossipMessage::DeltaStateSync {
                sender_node_id,
                gossip_round,
                updates,
                ..
            } => {
                assert_eq!(sender_node_id, 5);
                assert_eq!(gossip_round, 42);
                assert!(updates.contains_key("test_client"));
            }
            _ => panic!("Wrong message type after deserialization"),
        }
    }

    // Helper function to create a gossip node for testing
    async fn create_gossip_node(
        rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>>,
        node_id: u32,
    ) -> GossipNode {
        let mut conf = gen_settings();
        conf.topology = HashSet::new(); // No transport needed for message processing tests
        conf.run_mode = settings::RunMode::Single;
        conf.rate_limit_max_calls_allowed = 1000;
        conf.rate_limit_interval_seconds = 60;

        // Override the rate limiter settings to use custom node ID
        let mut node = GossipNode::new(conf, rate_limiter).await.unwrap();
        node.rate_limit_settings.node_id = node_id;
        node
    }

    #[tokio::test]
    async fn test_topology_parsing_with_http_urls() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        // Test with HTTP URLs (like the justfile uses)
        let topology = [
            "http://localhost:8002",
            "https://localhost:8003",
            "http://127.0.0.1:8004", // With HTTP prefix
            "http://localhost:8005", // localhost with HTTP
        ]
        .iter()
        .map(|s| s.parse::<SocketAddr>().unwrap())
        .collect::<HashSet<SocketAddr>>();
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port_udp = 8081;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Verify gossip is enabled (meaning topology was parsed successfully)
        assert!(node.has_gossip());
        assert!(node.transport.is_some());

        // Verify node has unique ID based on port
        assert_ne!(node.node_id(), 0);
    }

    #[tokio::test]
    async fn test_different_ports_generate_different_node_ids() {
        let rate_limiter1: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        let rate_limiter2 = rate_limiter1.clone();

        // Create nodes with different ports - same topology to avoid differences there
        let topology1: HashSet<SocketAddr> =
            HashSet::from(["http://127.0.0.1:9000".parse().unwrap()]); // Different topology
        let topology2: HashSet<SocketAddr> =
            HashSet::from(["http://127.0.0.1:9001".parse().unwrap()]); // Different topology

        let mut conf = gen_settings();
        conf.topology = topology1;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port = 8001; // Different HTTP port for node ID generation
        conf.listen_port_udp = 8401; // Different UDP port

        let node1 = GossipNode::new(conf, rate_limiter1).await.unwrap();

        let mut conf = gen_settings();
        conf.topology = topology2;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port = 8002; // Different HTTP port for node ID generation
        conf.listen_port_udp = 8402; // Different UDP port

        let node2 = GossipNode::new(conf, rate_limiter2).await.unwrap();

        // Verify nodes have different IDs
        assert_ne!(node1.node_id(), node2.node_id());
        assert_ne!(node1.node_id(), 0);
        assert_ne!(node2.node_id(), 0);

        info!(
            "Node 1 (HTTP port 8001, UDP port 8401) has ID: {}",
            node1.node_id()
        );
        info!(
            "Node 2 (HTTP port 8002, UDP port 8402) has ID: {}",
            node2.node_id()
        );
    }

    #[tokio::test]
    async fn test_central_io_design_channels_created() {
        let rate_limiter: Arc<RwLock<RateLimiter<VersionedTokenBucket>>> =
            Arc::new(RwLock::new(new_rate_limiter()));

        // Create node with gossip enabled to verify IO loop starts
        let topology: HashSet<SocketAddr> =
            HashSet::from(["http://localhost:9005".parse().unwrap()]);
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port = 8005;
        conf.listen_port_udp = 8405;
        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Verify the central IO design: channels are available and IO loop should be running
        assert!(node.has_gossip());
        assert!(!node.inbound_message_sender().is_closed());

        // Give IO loop a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}
