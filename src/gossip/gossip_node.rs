use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{event, info, Level};

use crate::error::{ColibriError, GossipError, Result};
use crate::gossip::{
    generate_node_id_from_system, ClusterMembership, DynamicMulticastTransport, GossipMessage,
    GossipPacket, GossipScheduler, VersionedTokenBucket,
};
use crate::node::{CheckCallsResponse, Node};
use crate::rate_limit::RateLimiter;
use crate::token_bucket::TokenBucket;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol. No consistent hashing,
/// no HTTP forwarding - every node can answer every request.
#[derive(Clone)]
pub struct GossipNode {
    node_id: u32,
    /// Local rate limiter for fallback/legacy support
    rate_limiter: Arc<RwLock<RateLimiter>>,
    /// All versioned token buckets maintained locally
    local_buckets: Arc<DashMap<String, VersionedTokenBucket>>,
    /// Gossip scheduler for state propagation
    gossip_scheduler: Option<Arc<GossipScheduler>>,
    /// Transport layer for network communication
    transport: Option<Arc<DynamicMulticastTransport>>,
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
        listen_port: u16,
        topology: Vec<String>,
        rate_limiter: Arc<RwLock<RateLimiter>>,
    ) -> Result<Self> {
        let node_id = generate_node_id_from_system(listen_port)?;
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, listen_port
        );

        // Create local buckets first (needed for message processing)
        let local_buckets = Arc::new(DashMap::new());

        // Parse topology to get peer addresses
        let peer_addresses: Vec<SocketAddr> = topology
            .iter()
            .filter_map(|host_str| {
                let host = host_str.trim();

                // Remove http:// or https:// prefix if present
                let host = if host.starts_with("http://") {
                    host.strip_prefix("http://").unwrap_or(host)
                } else if host.starts_with("https://") {
                    host.strip_prefix("https://").unwrap_or(host)
                } else {
                    host
                };

                // Try to parse as socket address first
                if let Ok(addr) = host.parse::<SocketAddr>() {
                    Some(addr)
                } else {
                    // Try to parse as host:port
                    if let Some((hostname, port_str)) = host.split_once(':') {
                        if let Ok(port) = port_str.parse::<u16>() {
                            // Handle localhost
                            let ip_str = if hostname == "localhost" {
                                "127.0.0.1"
                            } else {
                                hostname
                            };

                            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                                return Some(SocketAddr::new(ip, port));
                            }
                        }
                    }
                    // Default to port 7946 if no port specified
                    let ip_str = if host == "localhost" {
                        "127.0.0.1"
                    } else {
                        host
                    };

                    if let Ok(ip) = ip_str.parse::<IpAddr>() {
                        Some(SocketAddr::new(ip, 7946))
                    } else {
                        event!(
                            Level::WARN,
                            "Failed to parse topology address: {}",
                            host_str
                        );
                        None
                    }
                }
            })
            .collect();

        // Create gossip components if we have peers
        let (gossip_scheduler, transport) = if !peer_addresses.is_empty() {
            info!(
                "Initializing gossip transport with {} peers: {:?}",
                peer_addresses.len(),
                peer_addresses
            );

            // Create transport - use port 0 for tests to avoid conflicts
            let gossip_port = if cfg!(test) { 0 } else { 7946 };
            let multicast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)), 7946);
            let transport = Arc::new(
                DynamicMulticastTransport::new(gossip_port, multicast_addr, peer_addresses.clone())
                    .await
                    .map_err(|e| {
                        ColibriError::Gossip(GossipError::Transport(format!(
                            "Failed to create transport: {}",
                            e
                        )))
                    })?,
            );

            // Create initial membership
            let local_addr = transport.get_local_address();
            let membership = ClusterMembership::new(node_id, local_addr);

            // Create gossip scheduler
            let scheduler = Arc::new(GossipScheduler::new(
                Duration::from_millis(25), // regular interval
                4,                         // fanout
                32768,                     // max payload size (32KB)
                2,                         // urgent fanout
                transport.clone(),
                node_id,
                membership,
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
        self.local_buckets
            .entry(client_id.to_string())
            .or_insert_with(|| {
                let bucket = TokenBucket::new(self.default_rate_limit, self.default_window);
                VersionedTokenBucket::new(bucket, self.node_id)
            })
            .clone()
    }

    /// Update a bucket locally and trigger gossip propagation if available
    fn update_bucket(&self, client_id: String, mut bucket: VersionedTokenBucket) {
        // Increment our vector clock
        bucket.vector_clock.increment(self.node_id);
        bucket.last_updated_by = self.node_id;

        // Store the updated bucket
        self.local_buckets.insert(client_id.clone(), bucket.clone());

        // Trigger gossip if available
        if let Some(ref scheduler) = self.gossip_scheduler {
            // Use urgent gossip for low token counts (critical state)
            if bucket.bucket.tokens_to_u32() < 10 {
                scheduler.add_urgent_update(client_id, bucket);
                event!(
                    Level::DEBUG,
                    "Added urgent gossip update for low token count"
                );
            } else {
                scheduler.add_regular_update(client_id, bucket);
                event!(Level::DEBUG, "Added regular gossip update");
            }
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub fn merge_gossip_state(
        &self,
        entries: std::collections::HashMap<String, VersionedTokenBucket>,
    ) {
        for (client_id, incoming_bucket) in entries {
            if let Some(mut current_entry) = self.local_buckets.get_mut(&client_id) {
                // Try to merge with existing bucket
                if current_entry.merge(incoming_bucket, self.node_id) {
                    event!(
                        Level::DEBUG,
                        "Merged gossip update for client: {}",
                        client_id
                    );
                }
            } else {
                // No existing entry, accept incoming state
                self.local_buckets
                    .insert(client_id.clone(), incoming_bucket);
                event!(
                    Level::DEBUG,
                    "Accepted new gossip state for client: {}",
                    client_id
                );
            }
        }
    }

    /// Get gossip statistics (for monitoring/debugging)
    pub async fn get_gossip_stats(&self) -> Option<crate::gossip::scheduler::GossipStats> {
        if let Some(ref scheduler) = self.gossip_scheduler {
            Some(scheduler.get_stats().await)
        } else {
            None
        }
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.gossip_scheduler.is_some() && self.transport.is_some()
    }

    /// Start the background task that processes incoming gossip messages
    async fn start_message_processing_loop(
        transport: Arc<DynamicMulticastTransport>,
        node_id: u32,
        local_buckets: Arc<DashMap<String, VersionedTokenBucket>>,
    ) {
        tokio::spawn(async move {
            info!(
                "Starting gossip message processing loop for node {}",
                node_id
            );

            loop {
                // Try to receive a message with a timeout
                match transport
                    .receive_with_timeout(Duration::from_millis(100))
                    .await
                {
                    Ok(Some((packet, sender_addr))) => {
                        event!(Level::DEBUG, "Received gossip packet from {}", sender_addr);

                        // Process the message
                        if let Err(e) =
                            GossipNode::process_incoming_message(packet, node_id, &local_buckets)
                                .await
                        {
                            event!(Level::WARN, "Failed to process gossip message: {}", e);
                        }
                    }
                    Ok(None) => {
                        // Timeout - continue loop
                        continue;
                    }
                    Err(e) => {
                        event!(Level::ERROR, "Error receiving gossip message: {}", e);
                        // Brief pause before retrying to avoid spinning
                        tokio::time::sleep(Duration::from_millis(100)).await;
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
            GossipMessage::MembershipUpdate { .. } => {
                event!(
                    Level::DEBUG,
                    "Received MembershipUpdate - not yet implemented"
                );
                // TODO: Implement membership update handling
            }
            GossipMessage::NodeJoinRequest { .. } => {
                event!(
                    Level::DEBUG,
                    "Received NodeJoinRequest - not yet implemented"
                );
                // TODO: Implement node join handling
            }
            GossipMessage::NodeJoinResponse { .. } => {
                event!(
                    Level::DEBUG,
                    "Received NodeJoinResponse - not yet implemented"
                );
                // TODO: Implement join response handling
            }
            GossipMessage::NodeLeaveNotification { .. } => {
                event!(
                    Level::DEBUG,
                    "Received NodeLeaveNotification - not yet implemented"
                );
                // TODO: Implement node leave handling
            }
            GossipMessage::NodeFailureDetected { .. } => {
                event!(
                    Level::DEBUG,
                    "Received NodeFailureDetected - not yet implemented"
                );
                // TODO: Implement failure detection handling
            }
            GossipMessage::Heartbeat { .. } => {
                event!(Level::DEBUG, "Received Heartbeat - not yet implemented");
                // TODO: Implement heartbeat handling
            }
            GossipMessage::MembershipSync { .. } => {
                event!(
                    Level::DEBUG,
                    "Received MembershipSync - not yet implemented"
                );
                // TODO: Implement membership sync handling
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
        Ok(CheckCallsResponse {
            client_id,
            calls_remaining: bucket.bucket.tokens_to_u32(),
        })
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let mut bucket = self.get_or_create_bucket(&client_id);

        // Try to consume one token
        if bucket.bucket.try_consume(1) {
            // Success - update our state and trigger gossip
            let response = CheckCallsResponse {
                client_id: client_id.clone(),
                calls_remaining: bucket.bucket.tokens_to_u32(),
            };

            // Update bucket with new state
            self.update_bucket(client_id, bucket);

            Ok(Some(response))
        } else {
            // Rate limit exceeded
            Ok(None)
        }
    }

    /// Expire keys from local buckets
    fn expire_keys(&self) {
        // Expire from the legacy rate limiter
        match self.rate_limiter.write() {
            Ok(mut rate_limiter) => {
                rate_limiter.expire_keys();
            }
            Err(err) => {
                event!(
                    Level::ERROR,
                    message = "Failed expiring keys from rate_limiter",
                    err = format!("{:?}", err)
                );
            }
        }

        // TODO: Implement expiry for local_buckets based on TokenBucket's last_refill time
        // For now, we'll rely on the existing rate_limiter for expiry logic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    #[tokio::test]
    async fn test_pure_gossip_node_single_mode() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with no topology (single mode)
        let node = GossipNode::new(8080, vec![], rate_limiter).await.unwrap();

        assert!(!node.has_gossip());
        assert!(node.gossip_scheduler.is_none());
        assert!(node.transport.is_none());
    }

    #[tokio::test]
    async fn test_pure_gossip_node_gossip_mode() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with topology (gossip mode) - use unique port
        let topology = vec!["127.0.0.1:7949".to_string()];
        let node = GossipNode::new(8081, topology, rate_limiter).await.unwrap();

        assert!(node.has_gossip());
        assert!(node.gossip_scheduler.is_some());
        assert!(node.transport.is_some());

        // Test that we can get gossip stats
        let stats = node.get_gossip_stats().await;
        assert!(stats.is_some());
    }

    #[tokio::test]
    async fn test_rate_limit_triggers_gossip() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 10,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Create node with topology to enable gossip - use unique port
        let topology = vec!["127.0.0.1:7950".to_string()];
        let node = GossipNode::new(8082, topology, rate_limiter).await.unwrap();

        // Perform rate limiting operation
        let result = node.rate_limit("test_client".to_string()).await.unwrap();
        assert!(result.is_some());

        // Check that the bucket was created locally
        assert!(node.local_buckets.contains_key("test_client"));

        // Check that gossip stats show activity
        if let Some(stats) = node.get_gossip_stats().await {
            assert!(stats.total_updates_processed > 0);
        }
    }

    #[tokio::test]
    async fn test_merge_gossip_state() {
        let rate_limiter = Arc::new(RwLock::new(crate::rate_limit::RateLimiter::new(
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        let node = GossipNode::new(8083, vec![], rate_limiter).await.unwrap();

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
        let packet = crate::gossip::GossipPacket::new(message);

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
        let packet = crate::gossip::GossipPacket::new(message);

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
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));

        // Test with HTTP URLs (like the justfile uses)
        let topology = vec![
            "http://localhost:8002".to_string(),
            "https://localhost:8003".to_string(),
            "127.0.0.1:8004".to_string(), // Without HTTP prefix
            "localhost:8005".to_string(), // localhost without HTTP
        ];

        let node = GossipNode::new(8001, topology, rate_limiter).await.unwrap();

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
            crate::cli::RateLimitSettings {
                rate_limit_max_calls_allowed: 1000,
                rate_limit_interval_seconds: 60,
            },
        )));
        let rate_limiter2 = rate_limiter1.clone();

        // Create nodes with different ports - same topology to avoid differences there
        let topology = vec!["http://localhost:9000".to_string()];

        let node1 = GossipNode::new(8001, topology.clone(), rate_limiter1)
            .await
            .unwrap();
        let node2 = GossipNode::new(8002, topology, rate_limiter2)
            .await
            .unwrap();

        // Verify nodes have different IDs
        assert_ne!(node1.node_id, node2.node_id);
        assert_ne!(node1.node_id, 0);
        assert_ne!(node2.node_id, 0);

        info!("Node 1 (port 8001) has ID: {}", node1.node_id);
        info!("Node 2 (port 8002) has ID: {}", node2.node_id);
    }
}
