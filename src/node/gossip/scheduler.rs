//! Gossip Scheduler for Intelligent Message Distribution
//!
//! Provides layered gossip timing with membership awareness and payload optimization.
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use rand::prelude::IndexedRandom;
use tokio::sync::RwLock;

use super::messages::{GossipMessage, GossipPacket};
use crate::transport::{UdpReceiver, UdpSocketPool, UdpTransport};
use crate::versioned_bucket::VersionedTokenBucket;

/// Change record for tracking when keys were modified for delta-state gossip
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    pub bucket: VersionedTokenBucket,
    pub last_changed: Instant,
    pub gossip_attempts: u32,
    pub last_gossiped: Option<Instant>,
}

/// Gossip scheduler with delta-state propagation
///
/// Only sends recently changed keys, not full state. Implements patterns from
/// production systems like Cassandra and Consul.
pub struct GossipScheduler {
    // Configuration
    gossip_interval: Duration,
    gossip_fanout: usize,
    max_gossip_payload_size: usize,
    anti_entropy_interval: Duration,

    // Transport and cluster state
    transport: Arc<UdpTransport>,
    node_id: u32,
    cluster_peers: Arc<RwLock<HashSet<SocketAddr>>>,

    // Change tracking for delta-state gossip
    recent_changes: Arc<DashMap<String, ChangeRecord>>,
    version_vector: Arc<DashMap<u32, u64>>, // node_id -> last_known_version
    gossip_round: Arc<AtomicU64>,
    packet_id_counter: Arc<AtomicU64>,

    // Runtime statistics
    stats: Arc<RwLock<GossipStats>>,
}

/// Statistics for gossip scheduler performance monitoring
#[derive(Debug, Clone, Default)]
pub struct GossipStats {
    pub urgent_messages_sent: u64,
    pub regular_messages_sent: u64,
    pub total_updates_processed: u64,
    pub payload_bytes_sent: u64,
    pub last_urgent_gossip: Option<Instant>,
    pub last_regular_gossip: Option<Instant>,
    pub queue_sizes: QueueSizes,
}

#[derive(Debug, Clone, Default)]
pub struct QueueSizes {
    pub urgent_queue_size: usize,
    pub regular_queue_size: usize,
}

impl GossipScheduler {
    /// Create a new production gossip scheduler with delta-state support
    ///
    /// # Arguments
    /// * `gossip_interval` - Interval for regular gossip (recommended: 100ms)
    /// * `gossip_fanout` - Number of peers for regular gossip (recommended: 3)
    /// * `max_gossip_payload_size` - Maximum payload size in bytes (recommended: 32KB)
    /// * `transport` - Transport layer for network communication
    /// * `node_id` - Local node identifier
    /// * `initial_peers` - Initial cluster peer addresses
    pub fn new(
        gossip_interval: Duration,
        gossip_fanout: usize,
        max_gossip_payload_size: usize,
        transport: Arc<UdpTransport>,
        node_id: u32,
        initial_peers: HashSet<SocketAddr>,
    ) -> Self {
        Self {
            gossip_interval,
            gossip_fanout,
            max_gossip_payload_size,
            anti_entropy_interval: Duration::from_secs(10),
            transport,
            node_id,
            cluster_peers: Arc::new(RwLock::new(initial_peers)),
            recent_changes: Arc::new(DashMap::new()),
            version_vector: Arc::new(DashMap::new()),
            gossip_round: Arc::new(AtomicU64::new(0)),
            packet_id_counter: Arc::new(AtomicU64::new(0)),
            stats: Arc::new(RwLock::new(GossipStats::default())),
        }
    }

    /// Update cluster peers for gossip
    pub async fn update_cluster_peers(&self, new_peers: HashSet<SocketAddr>) {
        let mut peers = self.cluster_peers.write().await;
        *peers = new_peers;
        println!("Updated cluster peers to {} members", peers.len());
    }

    /// Record a state change for delta-state gossip
    pub fn record_change(&self, client_id: String, bucket: VersionedTokenBucket) {
        let change_record = ChangeRecord {
            bucket,
            last_changed: Instant::now(),
            gossip_attempts: 0,
            last_gossiped: None,
        };

        self.recent_changes.insert(client_id, change_record);
    }

    /// Update version vector for anti-entropy
    pub fn update_version_vector(&self, node_id: u32, version: u64) {
        self.version_vector.insert(node_id, version);
    }

    /// Start the gossip scheduler with delta-state support
    ///
    /// This spawns multiple async tasks:
    /// 1. Delta-state gossip processor (configurable interval, default 100ms)
    /// 2. Anti-entropy process (every 10 seconds)
    /// 3. Change cleanup (every 30 seconds)
    /// 4. Legacy urgent/regular processors (for backward compatibility)
    pub async fn start(&self) {
        // TODO: Re-enable async tasks once Send issues are resolved
        // For now, just log that the scheduler is starting

        println!(
            "Production gossip scheduler starting with {}ms delta-state interval",
            self.gossip_interval.as_millis()
        );
        println!("Note: Async gossip loops temporarily disabled pending Send trait fixes");

        // Legacy processors can be enabled for basic functionality
        // self.start_legacy_processors().await;
    }

    /// Start delta-state gossip loop (DISABLED - TODO: Fix Send trait issues)
    #[allow(dead_code)]
    async fn start_delta_gossip_loop(&self) {
        let _recent_changes = self.recent_changes.clone();
        let _transport = self.transport.clone();
        let _cluster_peers = self.cluster_peers.clone();
        let _gossip_interval = self.gossip_interval;
        let _gossip_fanout = self.gossip_fanout;
        let _max_payload_size = self.max_gossip_payload_size;
        let _node_id = self.node_id;
        let _gossip_round = self.gossip_round.clone();
        let _packet_id_counter = self.packet_id_counter.clone();
        let _version_vector = self.version_vector.clone();
        let _stats = self.stats.clone();

        // TODO: Re-implement without Send trait issues
        println!("Delta-state gossip loop would start here");

        /*
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(gossip_interval);

            loop {
                interval.tick().await;

                // Get recent changes for delta-state gossip
                let changes = Self::collect_recent_changes(
                    &recent_changes,
                    Duration::from_millis(100), // Changes in last 100ms
                );

                if changes.is_empty() {
                    continue;
                }

                // Select random peers for this gossip round
                let peers = cluster_peers.read().await;
                let selected_peers = Self::select_random_peers(&peers, gossip_fanout);

                if selected_peers.is_empty() {
                    continue;
                }

                // Create version vector for anti-entropy
                let last_seen_versions: HashMap<u32, u64> = version_vector
                    .iter()
                    .map(|entry| (*entry.key(), *entry.value()))
                    .collect();

                // Split changes into chunks if needed
                let change_chunks = Self::chunk_changes(changes, max_payload_size);

                for chunk in change_chunks {
                    let message = GossipMessage::DeltaStateSync {
                        updates: chunk,
                        sender_node_id: node_id,
                        gossip_round: gossip_round.fetch_add(1, Ordering::Relaxed),
                        last_seen_versions: last_seen_versions.clone(),
                    };

                    let packet = GossipPacket::new_with_id(
                        message,
                        packet_id_counter.fetch_add(1, Ordering::Relaxed),
                    );

                    if let Ok(bytes) = packet.serialize() {
                        // Send to selected peers
                        for &peer_addr in &selected_peers {
                            if let Err(e) = transport.send_to_peer(peer_addr, &bytes).await {
                                eprintln!("Failed to send delta gossip to {}: {}", peer_addr, e);
                            }
                        }

                        // Update stats
                        let mut stats_guard = stats.write().await;
                        stats_guard.regular_messages_sent += 1;
                        stats_guard.last_regular_gossip = Some(Instant::now());
                    }
                }
            }
        });
        */
    }

    /// Start anti-entropy process for missed updates (DISABLED - TODO: Fix Send trait issues)
    #[allow(dead_code)]
    async fn start_anti_entropy_loop(&self) {
        let _transport = self.transport.clone();
        let _cluster_peers = self.cluster_peers.clone();
        let _anti_entropy_interval = self.anti_entropy_interval;
        let _node_id = self.node_id;
        let _version_vector = self.version_vector.clone();
        let _packet_id_counter = self.packet_id_counter.clone();

        println!("Anti-entropy loop would start here");

        /*
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(anti_entropy_interval);

            loop {
                interval.tick().await;

                let peers = cluster_peers.read().await;
                if peers.is_empty() {
                    continue;
                }

                // Select one random peer for anti-entropy
                let mut rng = rand::rng();
                if let Some(&peer_addr) = peers.choose(&mut rng) {
                    // Send our version vector to detect missed updates
                    let since_version: HashMap<u32, u64> = version_vector
                        .iter()
                        .map(|entry| (*entry.key(), *entry.value()))
                        .collect();

                    let message = GossipMessage::StateRequest {
                        requesting_node_id: node_id,
                        missing_keys: None, // Request everything we might be missing
                        since_version,
                    };

                    let packet = GossipPacket::new_with_id(
                        message,
                        packet_id_counter.fetch_add(1, Ordering::Relaxed),
                    );

                    if let Ok(bytes) = packet.serialize() {
                        if let Err(e) = transport.send_to_peer(peer_addr, &bytes).await {
                            eprintln!(
                                "Failed to send anti-entropy request to {}: {}",
                                peer_addr, e
                            );
                        }
                    }
                }
            }
        });
        */
    }

    /// Clean up old changes that have been sufficiently gossiped (DISABLED - TODO: Fix Send trait issues)
    #[allow(dead_code)]
    async fn start_change_cleanup_loop(&self) {
        let _recent_changes = self.recent_changes.clone();

        println!("Change cleanup loop would start here");

        /*
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                let cleanup_cutoff = Instant::now() - Duration::from_secs(60);
                let mut keys_to_remove = Vec::new();

                for entry in recent_changes.iter() {
                    let record = entry.value();

                    // Remove if old and well-gossiped
                    if record.last_changed < cleanup_cutoff && record.gossip_attempts >= 3 {
                        keys_to_remove.push(entry.key().clone());
                    }
                }

                for key in keys_to_remove {
                    recent_changes.remove(&key);
                }
            }
        });
        */
    }

    // Helper methods for delta-state gossip
    fn select_random_peers(all_peers: &[SocketAddr], count: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        let selected_count = count.min(all_peers.len());

        all_peers
            .choose_multiple(&mut rng, selected_count)
            .cloned()
            .collect()
    }

    fn collect_recent_changes(
        recent_changes: &DashMap<String, ChangeRecord>,
        max_age: Duration,
    ) -> HashMap<String, VersionedTokenBucket> {
        let cutoff = Instant::now() - max_age;
        let mut changes = HashMap::new();

        for entry in recent_changes.iter() {
            let record = entry.value();
            if record.last_changed >= cutoff {
                changes.insert(entry.key().clone(), record.bucket.clone());
            }
        }

        changes
    }

    fn chunk_changes(
        changes: HashMap<String, VersionedTokenBucket>,
        _max_payload_size: usize,
    ) -> Vec<HashMap<String, VersionedTokenBucket>> {
        // Simple chunking by count for now
        // In production, you'd estimate actual serialized size
        const MAX_ITEMS_PER_CHUNK: usize = 100;

        let mut chunks = Vec::new();
        let mut current_chunk = HashMap::new();
        let mut current_count = 0;

        for (key, bucket) in changes {
            current_chunk.insert(key, bucket);
            current_count += 1;

            if current_count >= MAX_ITEMS_PER_CHUNK {
                chunks.push(current_chunk);
                current_chunk = HashMap::new();
                current_count = 0;
            }
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        if chunks.is_empty() {
            vec![HashMap::new()]
        } else {
            chunks
        }
    }

    /// Track a key change for delta-state gossip
    pub async fn track_change(&self, key: String, bucket: VersionedTokenBucket) {
        let record = ChangeRecord {
            bucket,
            last_changed: Instant::now(),
            gossip_attempts: 0,
            last_gossiped: None,
        };
        self.recent_changes.insert(key, record);
    }

    /// Get direct access to the UDP socket pool for custom gossip operations
    pub fn get_socket_pool(&self) -> Arc<UdpSocketPool> {
        self.transport.socket_pool().clone()
    }

    /// Get direct access to the UDP receiver for custom message processing
    pub fn get_receiver(&self) -> Arc<UdpReceiver> {
        self.transport.receiver().clone()
    }

    /// Send gossip message to random peers using socket pool directly
    pub async fn gossip_to_random_peers(
        &self,
        message: &GossipMessage,
        fanout: usize,
    ) -> crate::error::Result<Vec<std::net::SocketAddr>> {
        let packet = GossipPacket::new_with_id(
            message.clone(),
            self.packet_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );

        if let Ok(bytes) = packet.serialize() {
            self.transport.send_to_random_peers(&bytes, fanout).await
        } else {
            Err(crate::error::ColibriError::Gossip(
                crate::error::GossipError::Message("Failed to serialize gossip packet".to_string()),
            ))
        }
    }

    /// Send gossip message to specific peer using socket pool directly
    pub async fn gossip_to_peer(
        &self,
        message: &GossipMessage,
        target: std::net::SocketAddr,
    ) -> crate::error::Result<()> {
        let packet = GossipPacket::new_with_id(
            message.clone(),
            self.packet_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );

        if let Ok(bytes) = packet.serialize() {
            self.transport.send_to_peer(target, &bytes).await
        } else {
            Err(crate::error::ColibriError::Gossip(
                crate::error::GossipError::Message("Failed to serialize gossip packet".to_string()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token_bucket::TokenBucket;
    use std::collections::HashSet;
    use std::time::Duration;

    async fn create_test_scheduler() -> GossipScheduler {
        let cluster_urls = HashSet::new(); // Empty for test
        let transport = Arc::new(
            UdpTransport::new(0, cluster_urls, 1)
                .await
                .expect("Failed to create transport"),
        );

        let initial_peers = HashSet::new();
        let node_id = 1;

        GossipScheduler::new(
            Duration::from_millis(25), // gossip_interval
            3,                         // gossip_fanout
            32768,                     // max_gossip_payload_size
            transport,
            node_id,
            initial_peers,
        )
    }

    fn create_test_bucket(node_id: u32) -> VersionedTokenBucket {
        let bucket = TokenBucket::new(100, Duration::from_secs(60));
        VersionedTokenBucket::new(bucket, node_id)
    }

    #[tokio::test]
    async fn test_change_tracking() {
        let scheduler = create_test_scheduler().await;
        let bucket = create_test_bucket(1);

        // Test that changes are tracked
        scheduler
            .track_change("test_key".to_string(), bucket.clone())
            .await;

        // Verify change was recorded
        assert!(scheduler.recent_changes.contains_key("test_key"));
        let change_record = scheduler.recent_changes.get("test_key").unwrap();
        assert_eq!(change_record.bucket.last_updated_by, bucket.last_updated_by);
        assert_eq!(change_record.gossip_attempts, 0);
    }
}
