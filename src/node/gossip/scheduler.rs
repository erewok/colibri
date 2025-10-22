//! Gossip Scheduler for Intelligent Message Distribution
//!
//! Provides layered gossip timing with membership awareness and payload optimization.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use papaya::HashMap;
use tokio::sync::mpsc;
use tracing::debug;

use super::messages::{GossipMessage, GossipPacket};
use crate::limiters::versioned_bucket::VersionedTokenBucket;

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
    node_id: u32,
    gossip_interval: Duration,
    gossip_fanout: u32,
    max_gossip_payload_size: usize,

    // Communication with parent gossip_node via channel
    gossip_channel: mpsc::Sender<GossipPacket>,

    // Change tracking for delta-state gossip
    recent_changes: Arc<HashMap<String, ChangeRecord>>,
    version_vector: Arc<HashMap<u32, u64>>, // node_id -> last_known_version
    gossip_round: Arc<AtomicU64>,
    packet_id_counter: Arc<AtomicU64>,
}

/// Statistics for gossip scheduler performance monitoring
#[derive(Debug, Clone, Default)]
pub struct GossipStats {
    pub messages_sent: u64,
    pub total_updates_processed: u64,
    pub payload_bytes_sent: u64,
    pub last_gossip: Option<Instant>,
}

impl GossipScheduler {
    /// Create a new gossip scheduler
    pub fn new(
        gossip_interval: Duration,
        gossip_fanout: u32,
        gossip_channel: mpsc::Sender<GossipPacket>,
        max_gossip_payload_size: usize,
        node_id: u32,
    ) -> Self {
        Self {
            node_id,
            gossip_interval,
            gossip_fanout,
            max_gossip_payload_size,
            gossip_channel,
            recent_changes: Arc::new(HashMap::new()),
            version_vector: Arc::new(HashMap::new()),
            gossip_round: Arc::new(AtomicU64::new(0)),
            packet_id_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a state change for delta-state gossip
    pub fn record_change(&self, client_id: String, bucket: VersionedTokenBucket) {
        let change_record = ChangeRecord {
            bucket: bucket.clone(),
            last_changed: Instant::now(),
            gossip_attempts: 0,
            last_gossiped: None,
        };

        self.recent_changes
            .pin_owned()
            .insert(client_id.clone(), change_record);
        debug!(
            "[{}] Recorded state change for client '{}' with tokens={} version={}",
            self.node_id,
            client_id,
            bucket.bucket.tokens,
            bucket.vector_clock.get_timestamp(self.node_id)
        );
    }

    /// Update version vector for anti-entropy
    pub fn update_version_vector(&self, node_id: u32, version: u64) {
        self.version_vector.pin_owned().insert(node_id, version);
        debug!(
            "Updated version vector: node {} -> version {}",
            node_id, version
        );
    }

    /// Send a gossip packet to the node (non-blocking)
    pub async fn send_gossip_packet(
        &self,
        message: GossipMessage,
    ) -> Result<(), mpsc::error::SendError<GossipPacket>> {
        let packet_id = self
            .packet_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let packet = GossipPacket::new_with_id(message, packet_id);

        self.gossip_channel.send(packet).await
    }

    /// Create a delta state sync message from recent changes
    pub fn create_delta_state_sync(
        &self,
        updates: std::collections::HashMap<String, VersionedTokenBucket>,
    ) -> GossipMessage {
        let gossip_round = self
            .gossip_round
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Convert version vector to std::collections::HashMap for the message
        let last_seen_versions: std::collections::HashMap<u32, u64> = self
            .version_vector
            .pin_owned()
            .iter()
            .map(|(node_id, version)| (*node_id, *version))
            .collect();

        GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: self.node_id,
            gossip_round,
            last_seen_versions,
        }
    }

    // /// Start the gossip scheduler with delta-state support
    // ///
    // /// This spawns multiple async tasks:
    // /// 1. Delta-state gossip processor (configurable interval, default 100ms)
    // /// 2. Anti-entropy process (every 10 seconds)
    // /// 3. Change cleanup (every 30 seconds)
    // pub async fn start(&self) {
    //     info!(
    //         "Production gossip scheduler starting with {}ms delta-state interval",
    //         self.gossip_interval.as_millis()
    //     );

    //     // Start all background tasks
    //     self.start_delta_gossip_loop().await;
    //     self.start_anti_entropy_loop().await;
    //     self.start_change_cleanup_loop().await;

    //     info!("All gossip scheduler background tasks started successfully");
    // }

    // /// Start delta-state gossip loop - sends recent changes to random peers via transport
    // async fn start_delta_gossip_loop(&self) {
    //     // Clone all necessary data for the async task
    //     let recent_changes = self.recent_changes.clone();
    //     let gossip_interval = self.gossip_interval;
    //     let gossip_fanout = self.gossip_fanout;
    //     let send_chan = self.gossip_channel.clone();
    //     let max_payload_size = self.max_gossip_payload_size;
    //     let node_id = self.node_id;
    //     let gossip_round = self.gossip_round.clone();
    //     let packet_id_counter = self.packet_id_counter.clone();
    //     let version_vector = self.version_vector.clone();

    //     tokio::spawn(async move {
    //         let mut interval = tokio::time::interval(gossip_interval);
    //         debug!(
    //             "[{}] Delta-state gossip loop started with {}ms interval",
    //             node_id,
    //             gossip_interval.as_millis()
    //         );

    //         loop {
    //             interval.tick().await;

    //             // Get recent changes for delta-state gossip
    //             let changes = Self::collect_recent_changes(
    //                 &recent_changes,
    //                 Duration::from_millis(500), // Changes in last 500ms
    //             );

    //             if changes.is_empty() {
    //                 continue;
    //             }

    //             debug!(
    //                 "[{}] Found {} recent changes to gossip",
    //                 node_id,
    //                 changes.len()
    //             );
    //             // Split changes into chunks if needed
    //             let change_chunks = Self::chunk_changes(changes, max_payload_size);

    //             for chunk in change_chunks {
    //                 let round = gossip_round.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    //                 let message = GossipMessage::DeltaStateSync {
    //                     updates: chunk,
    //                     sender_node_id: node_id,
    //                     gossip_round: round,
    //                     last_seen_versions: version_vector.clone(),
    //                 };

    //                 let packet = GossipPacket::new_with_id(
    //                     message,
    //                     packet_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    //                 );

    //                 if let Ok(bytes) = packet.serialize() {
    //                     send_chan.send(bytes).await.unwrap_or_else(|e| {
    //                         warn!(
    //                             "[{}] Failed to send gossip message to parent node: {}",
    //                             node_id, e
    //                         );
    //                     });
    //                 } else {
    //                     warn!("[{}] Failed to serialize gossip packet", node_id);
    //                 }
    //             }

    //             // Mark changes as gossiped
    //             for entry in recent_changes.iter() {
    //                 let key = entry.key();
    //                 if let Some(mut record) = recent_changes.get_mut(key.as_str()).await {
    //                     record.gossip_attempts += 1;
    //                     record.last_gossiped = Some(Instant::now());
    //                 }
    //             }
    //         }
    //     });
    // }

    // /// Start anti-entropy process for missed updates via transport
    // async fn start_anti_entropy_loop(&self) {
    //     let anti_entropy_interval = self.anti_entropy_interval;
    //     let node_id = self.node_id;
    //     let version_vector = self.version_vector.clone();
    //     let packet_id_counter = self.packet_id_counter.clone();
    //     let send_chan = self.gossip_channel.clone();

    //     tokio::spawn(async move {
    //         let mut interval = tokio::time::interval(anti_entropy_interval);
    //         debug!(
    //             "[{}] Anti-entropy loop started with {}s interval",
    //             node_id,
    //             anti_entropy_interval.as_secs()
    //         );

    //         loop {
    //             interval.tick().await;

    //             // Send our version vector to detect missed updates
    //             let since_version: HashMap<u32, u64> = version_vector
    //                 .iter()
    //                 .map(|entry| (*entry.key(), *entry.value()))
    //                 .collect();

    //             let message = GossipMessage::StateRequest {
    //                 requesting_node_id: node_id,
    //                 missing_keys: None, // Request everything we might be missing
    //                 since_version,
    //             };

    //             let packet = GossipPacket::new_with_id(
    //                 message,
    //                 packet_id_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    //             );

    //             if let Ok(bytes) = packet.serialize() {
    //                 // Let transport select a random peer and send to it
    //                 match send_chan.send(bytes).await {
    //                     Ok(_) => {
    //                         debug!("[{}] Sent anti-entropy request", node_id);
    //                     }
    //                     Err(e) => {
    //                         warn!("[{}] Failed to send anti-entropy request: {}", node_id, e);
    //                     }
    //                 }
    //             }
    //         }
    //     });
    // }

    // /// Clean up old changes that have been sufficiently gossiped
    // async fn start_change_cleanup_loop(&self) {
    //     let recent_changes = self.recent_changes.clone();

    //     tokio::spawn(async move {
    //         let mut interval = tokio::time::interval(Duration::from_secs(30));
    //         debug!("Change cleanup loop started with 30s interval");

    //         loop {
    //             interval.tick().await;

    //             let cleanup_cutoff = Instant::now() - Duration::from_secs(60);
    //             let mut keys_to_remove = Vec::new();

    //             for entry in recent_changes.iter() {
    //                 let record = entry.value();

    //                 // Remove if old and well-gossiped
    //                 if record.last_changed < cleanup_cutoff && record.gossip_attempts >= 3 {
    //                     keys_to_remove.push(entry.key().clone());
    //                 }
    //             }

    //             if !keys_to_remove.is_empty() {
    //                 debug!("Cleaning up {} old change records", keys_to_remove.len());
    //                 for key in keys_to_remove {
    //                     recent_changes.remove(&key);
    //                 }
    //             }
    //         }
    //     });
    // }

    // // Helper methods for delta-state gossip
    // fn collect_recent_changes(
    //     recent_changes: &HashMap<String, ChangeRecord>,
    //     max_age: Duration,
    // ) -> HashMap<String, VersionedTokenBucket> {
    //     let cutoff = Instant::now() - max_age;
    //     let mut changes = HashMap::new();

    //     for entry in recent_changes.pin_owned().iter() {
    //         let record = entry.value();
    //         if record.last_changed >= cutoff {
    //             changes.insert(entry.key().clone(), record.bucket.clone());
    //         }
    //     }

    //     changes
    // }

    // fn chunk_changes(
    //     changes: HashMap<String, VersionedTokenBucket>,
    //     _max_payload_size: usize,
    // ) -> Vec<HashMap<String, VersionedTokenBucket>> {
    //     // Simple chunking by count for now
    //     // In production, you'd estimate actual serialized size
    //     const MAX_ITEMS_PER_CHUNK: usize = 100;

    //     let mut chunks = Vec::new();
    //     let mut current_chunk = HashMap::new();
    //     let mut current_count = 0;

    //     for (key, bucket) in changes {
    //         current_chunk.insert(key, bucket);
    //         current_count += 1;

    //         if current_count >= MAX_ITEMS_PER_CHUNK {
    //             chunks.push(current_chunk);
    //             current_chunk = HashMap::new();
    //             current_count = 0;
    //         }
    //     }

    //     if !current_chunk.is_empty() {
    //         chunks.push(current_chunk);
    //     }

    //     if chunks.is_empty() {
    //         vec![HashMap::new()]
    //     } else {
    //         chunks
    //     }
    // }

    // /// Track a key change for delta-state gossip
    // pub async fn track_change(&self, key: String, bucket: VersionedTokenBucket) {
    //     let record = ChangeRecord {
    //         bucket,
    //         last_changed: Instant::now(),
    //         gossip_attempts: 0,
    //         last_gossiped: None,
    //     };
    //     self.recent_changes.insert(key, record);
    // }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn create_test_scheduler() -> GossipScheduler {
        let node_id = 1;
        let send_chan = mpsc::channel(100).0;
        GossipScheduler::new(
            Duration::from_millis(25), // gossip_interval
            3,                         // gossip_fanout
            send_chan,
            32768, // max_gossip_payload_size
            node_id,
        )
    }

    fn create_test_bucket(node_id: u32) -> VersionedTokenBucket {
        use crate::limiters::token_bucket::Bucket;
        use crate::settings::RateLimitSettings;
        let settings = RateLimitSettings {
            node_id,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
        };
        VersionedTokenBucket::new(&settings)
    }

    // #[tokio::test]
    // async fn test_change_tracking() {
    //     let scheduler = create_test_scheduler().await;
    //     let bucket = create_test_bucket(1);

    //     // Test that changes are tracked
    //     scheduler
    //         .track_change("test_key".to_string(), bucket.clone())
    //         .await;

    //     // Verify change was recorded
    //     assert!(scheduler.recent_changes.contains_key("test_key"));
    //     let change_record = scheduler.recent_changes.get("test_key").unwrap();
    //     assert_eq!(change_record.bucket.last_updated_by, bucket.last_updated_by);
    //     assert_eq!(change_record.gossip_attempts, 0);
    // }

    #[tokio::test]
    async fn test_scheduler_initialization() {
        let scheduler = create_test_scheduler().await;

        // Test basic scheduler functionality
        let bucket = create_test_bucket(1);
        scheduler.record_change("test_key".to_string(), bucket);

        // Verify scheduler can track changes
        assert!(scheduler
            .recent_changes
            .pin_owned()
            .contains_key("test_key"));
    }

    #[tokio::test]
    async fn test_scheduler_delta_state_sync_creation() {
        let scheduler = create_test_scheduler().await;

        // Create some test data
        let mut updates = std::collections::HashMap::new();
        let bucket = create_test_bucket(1);
        updates.insert("client_1".to_string(), bucket);

        // Create a delta state sync message
        let message = scheduler.create_delta_state_sync(updates.clone());

        match message {
            GossipMessage::DeltaStateSync {
                sender_node_id,
                gossip_round,
                updates: msg_updates,
                ..
            } => {
                assert_eq!(sender_node_id, 1);
                assert_eq!(gossip_round, 0); // First message
                assert_eq!(msg_updates.len(), 1);
                assert!(msg_updates.contains_key("client_1"));
            }
            _ => panic!("Expected DeltaStateSync message"),
        }
    }

    #[tokio::test]
    async fn test_scheduler_send_packet() {
        let (tx, mut rx) = mpsc::channel(10);
        let scheduler = GossipScheduler::new(Duration::from_millis(25), 3, tx, 32768, 1);

        // Create and send a delta state message
        let mut updates = std::collections::HashMap::new();
        let bucket = create_test_bucket(1);
        updates.insert("test_client".to_string(), bucket);

        let message = scheduler.create_delta_state_sync(updates);
        let send_result = scheduler.send_gossip_packet(message).await;

        assert!(send_result.is_ok());

        // Verify message was received
        let received_packet = rx.recv().await.unwrap();
        match received_packet.message {
            GossipMessage::DeltaStateSync { sender_node_id, .. } => {
                assert_eq!(sender_node_id, 1);
            }
            _ => panic!("Expected DeltaStateSync message"),
        }
    }
}
