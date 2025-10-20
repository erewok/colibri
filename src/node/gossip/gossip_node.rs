#[cfg(test)]
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, event, info, Level};

use super::{GossipMessage, GossipPacket, GossipScheduler};
use crate::error::{ColibriError, Result};
use crate::node::{CheckCallsResponse, Node};
use crate::rate_limit::RateLimiter;
use crate::settings;
use crate::token_bucket::TokenBucket;
use crate::transport::{UdpReceiver, UdpTransport};
use crate::versioned_bucket::VersionedTokenBucket;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipNode {
    node_id: u32,
    /// Local rate limiter
    rate_limiter: Arc<RwLock<RateLimiter>>,
    /// All versioned token buckets maintained locally
    local_buckets: Arc<DashMap<String, VersionedTokenBucket>>,
    /// Gossip scheduler for state propagation
    gossip_scheduler: Option<Arc<GossipScheduler>>,
    /// Transport layer for network communication
    transport: Option<Arc<UdpTransport>>,
    /// Configuration
    default_rate_limit: u32,
    default_window: Duration,
}

impl std::fmt::Debug for GossipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipNode")
            .field("node_id", &self.node_id)
            .field("default_rate_limit", &self.default_rate_limit)
            .field("default_window", &self.default_window)
            .field("has_gossip_scheduler", &self.gossip_scheduler.is_some())
            .field("has_transport", &self.transport.is_some())
            .finish()
    }
}

impl GossipNode {
    pub async fn new(
        settings: settings::Settings,
        rate_limiter: Arc<RwLock<RateLimiter>>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );

        // Create local buckets first (needed for message processing)
        let local_buckets = Arc::new(DashMap::new());

        // Create gossip components if we have peers
        let (gossip_scheduler, transport) = if !settings.topology.is_empty() {
            info!(
                "Initializing gossip transport with {} peers: {:?}",
                settings.topology.len(),
                settings.topology
            );

            // Create transport - use port 0 for tests to avoid conflicts
            let gossip_port = if cfg!(test) {
                0
            } else {
                settings.listen_port_udp
            };
            // Convert peer addresses to URLs for UdpTransport
            let transport = Arc::new(
                UdpTransport::new(node_id, gossip_port, settings.topology.clone())
                    .await
                    .map_err(|e| {
                        ColibriError::Transport(format!("Failed to create transport: {}", e))
                    })?,
            );

            // Create gossip scheduler - transport already knows about peers
            let scheduler = Arc::new(GossipScheduler::new(
                Duration::from_millis(settings.gossip_interval_ms),
                settings.gossip_fanout,
                32768, // max payload size (32KB)
                transport.clone(),
                node_id,
            ));

            // Start the gossip scheduler
            scheduler.start().await;
            info!("Gossip scheduler started");

            // Start incoming message processing loop
            GossipNode::start_message_processing_loop(
                transport.clone(),
                node_id,
                local_buckets.clone(),
            )
            .await;
            info!("Gossip message processing loop started");

            (Some(scheduler), Some(transport))
        } else {
            info!("No peers specified, running in single-node mode without gossip");
            (None, None)
        };

        Ok(Self {
            node_id,
            rate_limiter,
            local_buckets,
            gossip_scheduler,
            transport,
            default_rate_limit: 1000, // Default from rate limiter settings
            default_window: Duration::from_secs(60), // Default window
        })
    }

    /// Get or create a versioned token bucket for a client
    fn get_or_create_bucket(&self, client_id: &str) -> VersionedTokenBucket {
        let bucket = self
            .local_buckets
            .entry(client_id.to_string())
            .or_insert_with(|| {
                debug!("Creating new token bucket for client '{}'", client_id);
                let bucket = TokenBucket::new(self.default_rate_limit, self.default_window);
                VersionedTokenBucket::new(bucket, self.node_id)
            })
            .clone();

        debug!(
            "[{}] Retrieved bucket for client '{}': tokens={}, version={}",
            self.node_id,
            client_id,
            bucket.bucket.tokens,
            bucket.vector_clock.get_timestamp(self.node_id)
        );
        bucket
    }

    /// Update a bucket locally and trigger gossip propagation if available
    fn update_bucket(&self, client_id: String, mut bucket: VersionedTokenBucket) {
        let old_version = bucket.vector_clock.get_timestamp(self.node_id);

        // Increment our vector clock
        let new_version = bucket.vector_clock.increment(self.node_id);
        bucket.last_updated_by = self.node_id;

        debug!(
            "[{}] Updated bucket for client '{}': tokens={}, version {} -> {}",
            self.node_id, client_id, bucket.bucket.tokens, old_version, new_version
        );

        // Store the updated bucket
        self.local_buckets.insert(client_id.clone(), bucket.clone());

        // Trigger gossip if available
        if let Some(ref scheduler) = self.gossip_scheduler {
            scheduler.record_change(client_id.clone(), bucket);
            debug!(
                "[{}] Recorded change in gossip scheduler for client '{}'",
                self.node_id, client_id
            );
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub fn merge_gossip_state(
        &self,
        entries: std::collections::HashMap<String, VersionedTokenBucket>,
    ) {
        debug!(
            "[{}] Processing {} gossip entries for merge",
            self.node_id,
            entries.len()
        );

        for (client_id, incoming_bucket) in entries {
            if let Some(mut current_entry) = self.local_buckets.get_mut(&client_id) {
                let current_version = current_entry
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);
                let incoming_version = incoming_bucket
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);

                // Try to merge with existing bucket
                if current_entry.merge(incoming_bucket.clone(), self.node_id) {
                    debug!(
                        "[{}] Merged gossip update for client '{}': {} -> {} tokens, version {} -> {}",
                        self.node_id,
                        client_id,
                        current_entry.bucket.tokens,
                        incoming_bucket.bucket.tokens,
                        current_version,
                        incoming_version
                    );
                } else {
                    debug!(
                        "[{}] Rejected gossip update for client '{}': incoming version {} <= current version {}",
                        self.node_id, client_id, incoming_version, current_version
                    );
                }
            } else {
                // No existing entry, accept incoming state
                debug!(
                    "[{}] Accepted new gossip state for client '{}': tokens={}, version={}",
                    self.node_id,
                    client_id,
                    incoming_bucket.bucket.tokens,
                    incoming_bucket
                        .vector_clock
                        .get_timestamp(incoming_bucket.last_updated_by)
                );
                self.local_buckets
                    .insert(client_id.clone(), incoming_bucket);
            }
        }
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.gossip_scheduler.is_some() && self.transport.is_some()
    }

    /// Get direct access to the UDP receiver for custom message processing
    pub fn get_receiver(&self) -> Option<Arc<UdpReceiver>> {
        self.transport.as_ref().map(|t| t.receiver().clone())
    }

    /// Get direct access to the transport (if available)
    pub fn get_transport(&self) -> Option<Arc<UdpTransport>> {
        self.transport.clone()
    }

    /// Send a gossip message to random peers using the underlying socket pool
    pub async fn send_to_random_peers(
        &self,
        message: &GossipMessage,
        fanout: usize,
    ) -> Result<Vec<SocketAddr>> {
        if let Some(ref scheduler) = self.gossip_scheduler {
            scheduler.gossip_to_random_peers(message, fanout).await
        } else {
            Err(ColibriError::Transport(
                "Gossip not enabled - no transport available".to_string(),
            ))
        }
    }

    /// Send a gossip message to a specific peer using the underlying socket pool
    pub async fn send_to_peer(
        &self,
        message: &GossipMessage,
        target: SocketAddr,
    ) -> Result<SocketAddr> {
        if let Some(ref scheduler) = self.gossip_scheduler {
            scheduler.gossip_to_peer(message, target).await
        } else {
            Err(ColibriError::Transport(
                "Gossip not enabled - no transport available".to_string(),
            ))
        }
    }

    /// Log current statistics about the gossip node state for debugging
    pub fn log_stats(&self) {
        let bucket_count = self.local_buckets.len();
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_id, bucket_count
        );

        if bucket_count > 0 && bucket_count <= 10 {
            // If we have a reasonable number of buckets, log details
            for entry in self.local_buckets.iter() {
                let (client_id, bucket) = (entry.key(), entry.value());
                debug!(
                    "[{}] Client '{}': {} tokens, version={}, last_updated_by={}",
                    self.node_id,
                    client_id,
                    bucket.bucket.tokens,
                    bucket.vector_clock.get_timestamp(bucket.last_updated_by),
                    bucket.last_updated_by
                );
            }
        } else if bucket_count > 10 {
            // If we have many buckets, just log a summary
            let total_tokens: f64 = self
                .local_buckets
                .iter()
                .map(|entry| entry.value().bucket.tokens)
                .sum();
            debug!(
                "[{}]  Total tokens across all clients: {}",
                self.node_id, total_tokens
            );
        }

        if self.gossip_scheduler.is_some() {
            // Note: We could add scheduler stats here if needed
            debug!("[{}]  Gossip scheduler is active", self.node_id);
        } else {
            debug!("[{}]  No gossip scheduler configured", self.node_id);
        }
    }

    /// Start the background task that processes incoming gossip messages
    async fn start_message_processing_loop(
        transport: Arc<UdpTransport>,
        node_id: u32,
        local_buckets: Arc<DashMap<String, VersionedTokenBucket>>,
    ) {
        tokio::spawn(async move {
            info!(
                "Starting gossip message processing loop for node {}",
                node_id
            );

            let mut message_receiver = transport.get_message_receiver().await;

            loop {
                // Try to receive a message with a timeout
                match tokio::time::timeout(Duration::from_millis(100), message_receiver.recv())
                    .await
                {
                    Ok(Some((data, sender_addr))) => {
                        event!(Level::DEBUG, "Received gossip packet from {}", sender_addr);

                        // Deserialize the packet
                        match GossipPacket::deserialize(&data) {
                            Ok(packet) => {
                                // Process the message
                                if let Err(e) = GossipNode::process_incoming_message(
                                    packet,
                                    node_id,
                                    &local_buckets,
                                )
                                .await
                                {
                                    event!(Level::WARN, "Failed to process gossip message: {}", e);
                                }
                            }
                            Err(e) => {
                                event!(Level::WARN, "Failed to deserialize gossip packet: {}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        // Channel closed - break the loop
                        event!(Level::WARN, "Message receiver channel closed");
                        break;
                    }
                    Err(_) => {
                        // Timeout - continue loop
                        continue;
                    }
                }
            }
        });
    }

    /// Process a single incoming gossip message
    async fn process_incoming_message(
        packet: GossipPacket,
        local_node_id: u32,
        local_buckets: &DashMap<String, VersionedTokenBucket>,
    ) -> Result<()> {
        match packet.message {
            GossipMessage::StateSync {
                entries,
                sender_node_id,
                membership_version: _,
            } => {
                // Don't process our own messages
                if sender_node_id == local_node_id {
                    event!(Level::DEBUG, "Ignoring our own gossip message");
                    return Ok(());
                }

                event!(
                    Level::DEBUG,
                    "Processing StateSync from node {} with {} entries",
                    sender_node_id,
                    entries.len()
                );

                // Merge each incoming entry
                for (client_id, incoming_bucket) in entries {
                    if let Some(mut current_entry) = local_buckets.get_mut(&client_id) {
                        // Try to merge with existing bucket
                        if current_entry.merge(incoming_bucket, local_node_id) {
                            event!(
                                Level::DEBUG,
                                "Merged gossip update for client: {} from node {}",
                                client_id,
                                sender_node_id
                            );
                        } else {
                            event!(
                                Level::DEBUG,
                                "Rejected stale gossip update for client: {} from node {}",
                                client_id,
                                sender_node_id
                            );
                        }
                    } else {
                        // No existing entry, accept incoming state
                        local_buckets.insert(client_id.clone(), incoming_bucket);
                        event!(
                            Level::DEBUG,
                            "Accepted new gossip state for client: {} from node {}",
                            client_id,
                            sender_node_id
                        );
                    }
                }

                event!(
                    Level::DEBUG,
                    "Completed processing StateSync from node {}",
                    sender_node_id
                );
            }
            GossipMessage::StateRequest { .. } => {
                event!(Level::DEBUG, "Received StateRequest - not yet implemented");
                // TODO: Implement state request handling
            }
            GossipMessage::DeltaStateSync {
                updates,
                sender_node_id,
                gossip_round: _,
                last_seen_versions: _,
            } => {
                // Don't process our own messages
                if sender_node_id == local_node_id {
                    event!(Level::DEBUG, "Ignoring our own delta gossip message");
                    return Ok(());
                }

                event!(
                    Level::DEBUG,
                    "Processing DeltaStateSync from node {} with {} updates",
                    sender_node_id,
                    updates.len()
                );

                // Merge each incoming update
                for (client_id, incoming_bucket) in updates {
                    if let Some(mut current_entry) = local_buckets.get_mut(&client_id) {
                        // Try to merge with existing bucket
                        if current_entry.merge(incoming_bucket, local_node_id) {
                            event!(
                                Level::DEBUG,
                                "Merged delta update for client: {} from node {}",
                                client_id,
                                sender_node_id
                            );
                        }
                    } else {
                        // No existing entry, accept incoming state
                        local_buckets.insert(client_id.clone(), incoming_bucket);
                        event!(
                            Level::DEBUG,
                            "Accepted new delta state for client: {} from node {}",
                            client_id,
                            sender_node_id
                        );
                    }
                }
            }
            GossipMessage::StateResponse {
                responding_node_id,
                requested_data,
                current_versions: _,
            } => {
                // Don't process our own messages
                if responding_node_id == local_node_id {
                    return Ok(());
                }

                event!(
                    Level::DEBUG,
                    "Processing StateResponse from node {} with {} updates",
                    responding_node_id,
                    requested_data.len()
                );

                // Merge each incoming update
                for (client_id, incoming_bucket) in requested_data {
                    if let Some(mut current_entry) = local_buckets.get_mut(&client_id) {
                        current_entry.merge(incoming_bucket, local_node_id);
                    } else {
                        local_buckets.insert(client_id, incoming_bucket);
                    }
                }
            }
            GossipMessage::Heartbeat {
                node_id,
                timestamp: _,
                version_vector: _,
                is_alive: _,
            } => {
                // Don't process our own heartbeat
                if node_id == local_node_id {
                    return Ok(());
                }

                event!(Level::DEBUG, "Received heartbeat from node {}", node_id);

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
        let bucket = self.get_or_create_bucket(&client_id);
        let calls_remaining = bucket.bucket.tokens_to_u32();

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
        let mut bucket = self.get_or_create_bucket(&client_id);

        // Try to consume one token
        if bucket.bucket.try_consume(1) {
            let calls_remaining = bucket.bucket.tokens_to_u32();

            debug!(
                "[{}] Rate limit SUCCESS for client '{}': {} tokens remaining",
                self.node_id, client_id, calls_remaining
            );

            // Success - update our state and trigger gossip
            let response = CheckCallsResponse {
                client_id: client_id.clone(),
                calls_remaining,
            };

            // Update bucket with new state
            self.update_bucket(client_id, bucket);

            Ok(Some(response))
        } else {
            debug!(
                "Rate limit EXCEEDED for client '{}': no tokens remaining",
                client_id
            );
            // Rate limit exceeded
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
    use url::Url;

    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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

    #[tokio::test]
    async fn test_gossip_node_single_mode() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with no topology (single mode even though Gossip specified)
        let mut conf = gen_settings();
        conf.topology = HashSet::new();
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();
        assert!(!node.has_gossip());
        assert!(node.gossip_scheduler.is_none());
        assert!(node.transport.is_none());
    }

    #[tokio::test]
    async fn test_gossip_node_gossip_mode() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with topology (gossip mode) - use unique port
        let topology: HashSet<SocketAddr> = vec![Url::parse("http://127.0.0.1:7949").unwrap()]
            .into_iter()
            .map(|url| url.socket_addrs(|| None).unwrap()[0])
            .collect();
        // Create node with topology (gossip mode)
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();
        assert!(node.has_gossip());
        assert!(node.gossip_scheduler.is_some());
        assert!(node.transport.is_some());

        // Test that we can get gossip stats
    }

    #[tokio::test]
    async fn test_rate_limit_triggers_gossip() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 10,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with topology to enable gossip - use unique port
        let mut conf = gen_settings();
        let topology: HashSet<SocketAddr> = vec![Url::parse("http://127.0.0.1:7950").unwrap()]
            .into_iter()
            .map(|url| url.socket_addrs(|| None).unwrap()[0])
            .collect();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Perform rate limiting operation
        let result = node.rate_limit("test_client".to_string()).await.unwrap();
        assert!(result.is_some());

        // Check that the bucket was created locally
        assert!(node.local_buckets.contains_key("test_client"));

        // Check that gossip stats show activity
    }

    #[tokio::test]
    async fn test_merge_gossip_state() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));
        let mut conf = gen_settings();
        conf.listen_port = 8084;
        let topology: HashSet<SocketAddr> = vec![Url::parse("http://127.0.0.1:7951").unwrap()]
            .into_iter()
            .map(|url| url.socket_addrs(|| None).unwrap()[0])
            .collect();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Create some fake gossip state
        let mut entries = std::collections::HashMap::new();
        let bucket = TokenBucket::new(500, Duration::from_secs(60));
        let versioned_bucket = VersionedTokenBucket::new(bucket, 999); // Different node ID
        entries.insert("remote_client".to_string(), versioned_bucket);

        // Merge the state
        node.merge_gossip_state(entries);

        // Check that the state was merged
        assert!(node.local_buckets.contains_key("remote_client"));
        let stored_bucket = node.local_buckets.get("remote_client").unwrap();
        assert_eq!(stored_bucket.last_updated_by, 999);
    }

    #[tokio::test]
    async fn test_message_processing_logic() {
        let local_buckets = Arc::new(DashMap::new());
        let local_node_id = 123;

        // Create a fake incoming message
        let mut entries = std::collections::HashMap::new();
        let bucket = TokenBucket::new(750, Duration::from_secs(60));
        let versioned_bucket = VersionedTokenBucket::new(bucket, 456); // Different node ID
        entries.insert("test_client".to_string(), versioned_bucket);

        let message = GossipMessage::StateSync {
            entries,
            sender_node_id: 456,
            membership_version: 1,
        };
        let packet = crate::node::gossip::GossipPacket::new(message);

        // Process the message
        let result =
            GossipNode::process_incoming_message(packet, local_node_id, &local_buckets).await;
        assert!(result.is_ok());

        // Verify the state was merged
        assert!(local_buckets.contains_key("test_client"));
        let stored_bucket = local_buckets.get("test_client").unwrap();
        assert_eq!(stored_bucket.last_updated_by, 456);
    }

    #[tokio::test]
    async fn test_ignore_own_messages() {
        let local_buckets = Arc::new(DashMap::new());
        let local_node_id = 123;

        // Create a message from our own node
        let mut entries = std::collections::HashMap::new();
        let bucket = TokenBucket::new(750, Duration::from_secs(60));
        let versioned_bucket = VersionedTokenBucket::new(bucket, local_node_id);
        entries.insert("self_client".to_string(), versioned_bucket);

        let message = GossipMessage::StateSync {
            entries,
            sender_node_id: local_node_id, // Same as local_node_id
            membership_version: 1,
        };
        let packet = crate::node::gossip::GossipPacket::new(message);

        // Process the message
        let result =
            GossipNode::process_incoming_message(packet, local_node_id, &local_buckets).await;
        assert!(result.is_ok());

        // Verify our own message was ignored
        assert!(!local_buckets.contains_key("self_client"));
    }

    #[tokio::test]
    async fn test_topology_parsing_with_http_urls() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

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
        assert!(node.gossip_scheduler.is_some());
        assert!(node.transport.is_some());

        // Verify node has unique ID based on port
        assert_ne!(node.node_id, 0);
    }

    #[tokio::test]
    async fn test_different_ports_generate_different_node_ids() {
        let rate_limiter1 = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));
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
        assert_ne!(node1.node_id, node2.node_id);
        assert_ne!(node1.node_id, 0);
        assert_ne!(node2.node_id, 0);

        info!(
            "Node 1 (HTTP port 8001, UDP port 8401) has ID: {}",
            node1.node_id
        );
        info!(
            "Node 2 (HTTP port 8002, UDP port 8402) has ID: {}",
            node2.node_id
        );
    }

    #[tokio::test]
    async fn test_transport_integration_methods() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with gossip enabled
        let topology: HashSet<SocketAddr> =
            HashSet::from(["http://localhost:9005".parse().unwrap()]);
        let mut conf = gen_settings();
        conf.topology = topology;
        conf.run_mode = settings::RunMode::Gossip;
        conf.listen_port = 8005;
        conf.listen_port_udp = 8405;

        let node = GossipNode::new(conf, rate_limiter).await.unwrap();

        // Test direct component access methods
        assert!(node.has_gossip());

        let receiver = node.get_receiver();
        assert!(receiver.is_some());

        let transport = node.get_transport();
        assert!(transport.is_some());

        // Test that we can access the receiver
        if let Some(recv) = receiver {
            let local_addr = recv.local_addr();
            // In tests, port 0 is used which gets assigned a random port
            assert!(local_addr.port() > 0, "Receiver should have a valid port");
            debug!("Receiver listening on: {}", local_addr);
        }
    }
}
