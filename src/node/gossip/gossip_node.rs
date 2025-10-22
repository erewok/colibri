use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::net::unix::pipe::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug, error, info};

use super::{GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::{rate_limit::RateLimiter, versioned_bucket::VersionedTokenBucket};
use crate::node::{CheckCallsResponse, Node};
use crate::transport::UdpTransport;
use crate::{settings, transport};

pub enum GossipCommand {
    CheckLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<CheckCallsResponse>>,
    },
    RateLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    GossipMessageReceived {
        data: bytes::Bytes,
        peer_addr: SocketAddr,
    },
}

impl GossipCommand {
    fn from_incoming_message(data: bytes::Bytes, peer_addr: SocketAddr) -> Self {
        GossipCommand::GossipMessageReceived { data, peer_addr }
    }
}

async fn run_gossip_receiver(
    listen_udp: SocketAddr,
    receive_message_msg_tx: Arc<mpsc::Sender<GossipCommand>>,
) -> Result<()> {
    // TODO: Implement the gossip receiver loop

    let (udp_send, mut udp_recv) = mpsc::channel(1000);
    let receiver = transport::receiver::UdpReceiver::new(listen_udp, Arc::new(udp_send)).await?;
    tokio::spawn(async move {
        receiver.start().await;
    });

    loop {
        // Handle incoming messages from network
        if let Some((data, peer_addr)) = udp_recv.recv().await {
            // Turn into GossipCommand and send to main loop
            let cmd = GossipCommand::from_incoming_message(data, peer_addr);
            if let Err(e) = receive_message_msg_tx.send(cmd).await {
                debug!("Failed to send incoming message to main loop: {}", e);
            }
        }
    }
}

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipNode {
    // rate-limit settings
    pub rate_limit_settings: settings::RateLimitSettings,
    /// Local rate limiter - handles all bucket operations
    pub rate_limiter: Arc<Mutex<RateLimiter<VersionedTokenBucket>>>,
    pub inbound_message_chan: Arc<mpsc::Sender<GossipCommand>>,
    pub udp_listener_chan: Arc<Mutex<mpsc::Receiver<GossipCommand>>>,
    /// transport_config
    pub transport_config: settings::TransportConfig,
    /// Transport layer for sending UDP unicast gossip messages
    pub transport: Option<Arc<UdpTransport>>,
    /// Gossip configuration
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
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
    pub fn node_id(&self) -> u32 {
        self.rate_limit_settings.node_id
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.transport.is_some()
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.rate_limiter.lock().unwrap().len();
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_id(),
            bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            self.node_id(),
            self.gossip_interval_ms
        );

        let receiver_addr = self.transport_config.listen_udp;
        let inbound_tx = self.inbound_message_chan.clone();
        tokio::spawn(async move {
            if let Err(e) = run_gossip_receiver(receiver_addr, inbound_tx).await {
                error!("Gossip receiver error: {}", e);
            }
        });

        let mut gossip_timer = time::interval(time::Duration::from_millis(self.gossip_interval_ms));
        let mut gossip_round = 0u64;

        let node_id = self.node_id();
        // We are taking a single reference to this mutex here
        let mut receiver  = self.udp_listener_chan.lock().unwrap();
        loop {
            tokio::select! {
                // Handle incoming messages from network
                Some(cmd) = receiver.recv() => {
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
        debug!("[{}] Gossip tick round {}", self.node_id(), gossip_round);

        // Send delta updates for buckets that have changed
        let updates = self.collect_gossip_updates();
        if !updates.is_empty() {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id(),
                gossip_round,
                last_seen_versions: HashMap::new(),
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        } else {
            // Send heartbeat if no updates to share
            let message = GossipMessage::Heartbeat {
                node_id: self.node_id(),
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
    fn collect_gossip_updates(&self) -> HashMap<String, VersionedTokenBucket> {
        // For now, collect all buckets. In the future, this could be optimized
        // to only send buckets that have changed since the last gossip round.
        match self.rate_limiter.lock() {
            Ok(rl) => rl.get_all_buckets(),
            Err(e) => {
                debug!(
                    "[{}] Failed to acquire lock for gossip updates: {}",
                    self.node_id(),
                    e
                );
                HashMap::new()
            }
        }
    }
    /// Handle incoming gossip cmd from our channel
    async fn handle_command(&self, cmd: GossipCommand) -> Result<()> {
        match cmd {
            GossipCommand::GossipMessageReceived { data, peer_addr } => {
                self.process_gossip_packet(data, peer_addr).await
            }
            GossipCommand::CheckLimit {
                client_id,
                resp_chan,
            } => {
                let result = match self.rate_limiter.lock() {
                    Ok(rl) => {
                        let calls_remaining =
                            rl.check_calls_remaining_for_client(client_id.as_str());
                        debug!(
                            "Check limit for client '{}': {} tokens remaining",
                            client_id, calls_remaining
                        );
                        Ok(CheckCallsResponse {
                            client_id,
                            calls_remaining,
                        })
                    }
                    Err(e) => Err(ColibriError::Concurrency(format!(
                        "[{}] Failed to acquire lock on rate_limiter: {}",
                        self.node_id(),
                        e
                    ))),
                };
                if let Err(_) = resp_chan.send(result) {
                    error!(
                        "[{}] Failed sending oneshot check_limit response",
                        self.node_id()
                    );
                }
                Ok(())
            }
            GossipCommand::RateLimit {
                client_id,
                resp_chan,
            } => {
                let result = match self.rate_limiter.lock() {
                    Ok(mut rl) => {
                        let calls_left = rl.limit_calls_for_client(client_id.to_string());
                        if let Some(calls_remaining) = calls_left {
                            let response = CheckCallsResponse {
                                client_id,
                                calls_remaining,
                            };
                            Ok(Some(response))
                        } else {
                            Ok(None)
                        }
                    }
                    Err(e) => Err(ColibriError::Concurrency(format!(
                        "[{}] Failed to acquire lock on rate_limiter: {}",
                        self.node_id(),
                        e
                    ))),
                };
                if let Err(_) = resp_chan.send(result) {
                    error!(
                        "[{}] Failed sending oneshot rate_limit response",
                        self.node_id()
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
                debug!(
                    "[{}] Received gossip packet from {}",
                    self.node_id(),
                    peer_addr
                );
                match packet.message {
                    GossipMessage::StateRequest { .. } => {
                        debug!(
                            "[{}] Received StateRequest - not yet implemented",
                            self.node_id()
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
                        if sender_node_id == self.node_id() {
                            debug!("[{}] Ignoring our own delta gossip message", self.node_id());
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing DeltaStateSync from node {} with {} updates",
                            self.node_id(),
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
                        if responding_node_id == self.node_id() {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing StateResponse from node {} with {} updates",
                            self.node_id(),
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
                        if heartbeat_node_id == self.node_id() {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Received heartbeat from node {}",
                            self.node_id(),
                            heartbeat_node_id
                        );
                        // TODO: Update peer liveness information
                    }
                };
                Ok(())
            }
            Err(e) => {
                debug!(
                    "[{}] Failed to deserialize packet: {} from {}",
                    self.node_id(),
                    e,
                    peer_addr
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
                self.node_id()
            );
            return Ok(());
        }
        match packet.serialize() {
            Ok(data) => {
                debug!("[{}] Sending gossip packet", self.node_id());
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
                debug!("[{}] Failed to serialize packet: {}", self.node_id(), e);
                Err(ColibriError::Transport(format!(
                    "Serialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub async fn merge_gossip_state(&self, entries: HashMap<String, VersionedTokenBucket>) {
        let node_id = self.node_id();
        let rate_limiter = Arc::clone(&self.rate_limiter);
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );

        for (client_id, incoming_bucket) in entries.into_iter() {
            debug!("[{}] Processing client: {}", node_id, client_id);
            if let Some(mut current_entry) = rate_limiter.lock().unwrap().get_bucket(&client_id) {
                debug!(
                    "[{}] Found existing bucket for client: {}",
                    node_id, client_id
                );
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
                        .lock()
                        .unwrap()
                        .set_bucket(&client_id, current_entry);
                } else {
                    debug!(
                        "[{}] Rejected gossip update for client '{}': incoming version {} <= current version {}",
                        node_id, client_id, incoming_version, current_version
                    );
                }
            } else {
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
                    .lock()
                    .unwrap()
                    .set_bucket(&client_id, incoming_bucket);
                debug!(
                    "[{}] Finished setting new bucket for client: {}",
                    node_id, client_id
                );
            }
        }
        debug!("[{}] merge_gossip_state_static completed", node_id);
    }
}

#[async_trait]
impl Node<VersionedTokenBucket> for GossipNode {
    async fn new(
        settings: settings::Settings,
        rate_limiter: RateLimiter<VersionedTokenBucket>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );
        let rl_settings = settings.rate_limit_settings();

        let (inbound_msg_tx, inbound_msg_rx): (
            mpsc::Sender<GossipCommand>,
            mpsc::Receiver<GossipCommand>,
        ) = mpsc::channel(1000);

        let inbound_message_chan = Arc::new(inbound_msg_tx);
        let udp_listener_chan = Arc::new(Mutex::new(inbound_msg_rx));

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
            rate_limit_settings: rl_settings,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            inbound_message_chan,
            udp_listener_chan,
            transport: transport.clone(),
            transport_config: transport_config,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
        })
    }

    /// Check remaining calls for a client using local state
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let (tx, rx) = oneshot::channel();
        self.inbound_message_chan.send(GossipCommand::CheckLimit { client_id: client_id, resp_chan: tx }).await;
        // Await the response
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!("Failed checking rate limit {}", e)))
        })
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let (tx, rx) = oneshot::channel();
        self.inbound_message_chan.send(GossipCommand::RateLimit { client_id: client_id, resp_chan: tx }).await;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!("Failed checking rate limit {}", e)))
        })
    }

    /// Expire keys from local buckets
    async fn expire_keys(&self) {
        match self.rate_limiter.lock() {
            Ok(mut rl) => {
                rl.expire_keys();
            }
            Err(e) => debug!(
                "[{}] Failed to acquire lock on rate_limiter: {}",
                self.node_id(),
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
        let rate_limiter = new_rate_limiter();

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
        let rate_limiter = new_rate_limiter();

        // Create node with topology (gossip mode) - use unique port
        let topology: HashSet<String> = HashSet::from(["127.0.0.1:7949".to_string()]);
        // Create node with topology (gossip mode)
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();
        assert!(node.has_gossip());
        assert!(node.transport.is_some());

        // Test that we can get gossip stats
        node.log_stats().await;
    }

    #[tokio::test]
    async fn test_rate_limit_triggers_gossip() {
        let rate_limiter = new_rate_limiter();

        // Create node with topology to enable gossip - use unique port
        let mut conf = gen_settings();
        let topology: HashSet<String> = HashSet::from(["127.0.0.1:7950".to_string()]);
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Perform rate limiting operation
        let result = node.rate_limit("test_client".to_string()).await.unwrap();
        assert!(result.is_some());

        // Check that the bucket was created locally
        assert!(node
            .rate_limiter
            .lock()
            .unwrap()
            .get_bucket("test_client")
            .is_some());

        // Check that gossip stats show activity
        node.log_stats().await;
    }

    #[tokio::test]
    async fn test_merge_gossip_state() {
        let rate_limiter = new_rate_limiter();

        let mut conf = gen_settings();
        conf.listen_port = 8084;
        let topology: HashSet<String> = HashSet::from(["127.0.0.1:7951".to_string()]);
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
        let stored_bucket = node
            .rate_limiter
            .lock()
            .unwrap()
            .get_bucket("remote_client");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 999);
    }

    #[tokio::test]
    async fn test_message_processing_delta_state_sync() {
        let node = create_gossip_node_with_id(1).await;

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

        // Simulate processing the gossip message through the command interface
        let cmd = GossipCommand::GossipMessageReceived {
            data: packet.serialize().unwrap(),
            peer_addr: "127.0.0.1:8000".parse().unwrap(),
        };

        // Process the message
        let result = node.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let stored_bucket = node
            .rate_limiter
            .lock()
            .unwrap()
            .get_bucket("client_from_node_2");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 2);
    }

    #[tokio::test]
    async fn test_message_processing_ignores_own_messages() {
        let node = create_gossip_node_with_id(123).await;

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

        // Simulate processing the gossip message through the command interface
        let cmd = GossipCommand::GossipMessageReceived {
            data: packet.serialize().unwrap(),
            peer_addr: "127.0.0.1:8000".parse().unwrap(),
        };

        // Process the message
        let result = node.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify our own message was ignored
        assert!(node
            .rate_limiter
            .lock()
            .unwrap()
            .get_bucket("self_client")
            .is_none());
    }

    #[tokio::test]
    async fn test_message_processing_state_response() {
        let node = create_gossip_node_with_id(1).await;

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

        // Simulate processing the gossip message through the command interface
        let cmd = GossipCommand::GossipMessageReceived {
            data: packet.serialize().unwrap(),
            peer_addr: "127.0.0.1:8000".parse().unwrap(),
        };

        // Process the message
        let result = node.handle_command(cmd).await;
        assert!(result.is_ok());

        // Verify the state was merged
        let stored_bucket = node
            .rate_limiter
            .lock()
            .unwrap()
            .get_bucket("response_client");
        assert!(stored_bucket.is_some());
        assert_eq!(stored_bucket.unwrap().last_updated_by, 3);
    }

    #[tokio::test]
    async fn test_message_processing_heartbeat() {
        let node = create_gossip_node_with_id(1).await;

        // Create a heartbeat message
        let message = GossipMessage::Heartbeat {
            node_id: 4,
            timestamp: 1234567890,
            version_vector: HashMap::new(),
        };

        let packet = GossipPacket::new(message);

        // Simulate processing the gossip message through the command interface
        let cmd = GossipCommand::GossipMessageReceived {
            data: packet.serialize().unwrap(),
            peer_addr: "127.0.0.1:8000".parse().unwrap(),
        };

        // Process the message - should not error but currently does nothing
        let result = node.handle_command(cmd).await;
        assert!(result.is_ok());

        // No state changes expected from heartbeat (TODO: track liveness)
    }

    #[tokio::test]
    async fn test_channel_senders_available() {
        let rate_limiter = new_rate_limiter();

        let node = create_gossip_node_with_id(1).await;

        // Verify we can get inbound channel sender
        // let inbound_sender = node.inbound_message_sender();

        // Verify channel is not closed
        // assert!(!inbound_sender.is_closed());
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

    // Helper function to create a gossip node for testing with custom node ID
    async fn create_gossip_node_with_id(node_id: u32) -> GossipNode {
        let mut conf = gen_settings();
        conf.topology = HashSet::new(); // No transport needed for message processing tests
        conf.run_mode = settings::RunMode::Single;
        conf.rate_limit_max_calls_allowed = 1000;
        conf.rate_limit_interval_seconds = 60;

        // Create rate limiter with custom node ID
        let rl_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
            node_id,
        };
        let rate_limiter = RateLimiter::new(rl_settings);

        // Create node
        let mut node = GossipNode::new(conf, rate_limiter).await.unwrap();
        node.rate_limit_settings.node_id = node_id;
        node
    }

    #[tokio::test]
    async fn test_topology_parsing_with_http_urls() {
        let rate_limiter = new_rate_limiter();

        // Test with HTTP URLs (like the justfile uses)
        let topology = HashSet::from([
            "http://localhost:8002".to_string(),
            "https://localhost:8003".to_string(),
            "http://127.0.0.1:8004".to_string(), // With HTTP prefix
            "http://localhost:8005".to_string(), // localhost with HTTP
        ]);
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
        let rate_limiter1 = new_rate_limiter();

        let rate_limiter2 = rate_limiter1.clone();

        // Create nodes with different ports - same topology to avoid differences there
        let topology1: HashSet<String> = HashSet::from(["http://127.0.0.1:9000".to_string()]); // Different topology
        let topology2: HashSet<String> = HashSet::from(["http://127.0.0.1:9001".to_string()]); // Different topology

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
        let rate_limiter = new_rate_limiter();

        // Create node with gossip enabled to verify IO loop starts
        let topology: HashSet<String> = HashSet::from(["http://localhost:9005".to_string()]);
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port = 8005;
        conf.listen_port_udp = 8405;
        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Verify the central IO design: channels are available and IO loop should be running
        assert!(node.has_gossip());
        // assert!(!node.inbound_message_sender().is_closed());

        // Give IO loop a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}
