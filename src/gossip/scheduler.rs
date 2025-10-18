//! Gossip Scheduler for Intelligent Message Distribution
//!
//! Provides layered gossip timing with membership awareness and payload optimization.
//! The scheduler manages two distinct gossip queues for different urgency levels:
//!
//! - **Urgent Queue**: 5ms intervals for critical updates (low token buckets)
//! - **Regular Queue**: 25ms intervals for normal state synchronization
//!
//! Key Features:
//! - Dynamic payload batching to respect network MTU limits
//! - Membership-aware peer selection and fanout
//! - Automatic queue management and size limits
//! - Performance-optimized gossip patterns for sub-10ms propagation
//!
//! Example usage:
//! ```rust,no_run
//! use colibri::gossip::{GossipScheduler, DynamicMulticastTransport, ClusterMembership};
//! use colibri::gossip::versioned_bucket::VersionedTokenBucket;
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create transport, membership, and other dependencies
//! let transport = Arc::new(DynamicMulticastTransport::new(
//!     7946,
//!     "224.0.1.1:7946".parse()?,
//!     vec!["192.168.1.10:7946".parse()?],
//! ).await?);
//! let membership = ClusterMembership::new(1, "127.0.0.1:8001".parse()?);
//! let node_id = 1;
//!
//! let scheduler = GossipScheduler::new(
//!     Duration::from_millis(25), // regular interval
//!     4,                        // fanout
//!     32768,                    // max payload size
//!     2,                        // urgent fanout
//!     transport,
//!     node_id,
//!     membership,
//! );
//!
//! // Example bucket and client ID
//! let client_id = "example_client".to_string();
//! let token_bucket = colibri::token_bucket::TokenBucket::default();
//! let bucket = VersionedTokenBucket::new(token_bucket, 1);
//!
//! // Add urgent update for critical state
//! scheduler.add_urgent_update(client_id.clone(), bucket.clone());
//!
//! // Add regular update for normal sync
//! scheduler.add_regular_update(client_id, bucket);
//! # Ok(())
//! # }
//! ```
//! ```

use crate::gossip::messages::{ClusterMembership, GossipMessage, GossipPacket};
use crate::gossip::transport::DynamicMulticastTransport;
use crate::gossip::versioned_bucket::VersionedTokenBucket;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;

/// Represents an urgent update that needs immediate propagation
#[derive(Debug, Clone)]
pub struct UrgentUpdate {
    pub client_id: String,
    pub bucket: VersionedTokenBucket,
    pub timestamp: Instant,
}

/// Intelligent gossip scheduler with layered timing and membership awareness
///
/// Manages both urgent and regular gossip queues with optimized batching and
/// peer selection for maximum efficiency and minimum latency.
pub struct GossipScheduler {
    // Configuration
    gossip_interval: Duration,
    gossip_fanout: usize,
    max_gossip_payload_size: usize,
    urgent_gossip_fanout: usize,

    // Transport and membership
    transport: Arc<DynamicMulticastTransport>,
    node_id: u32,
    current_membership: Arc<RwLock<ClusterMembership>>,

    // Queues for different gossip priorities
    urgent_updates: Arc<DashMap<String, UrgentUpdate>>,
    pending_updates: Arc<DashMap<String, VersionedTokenBucket>>,

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
    /// Create a new gossip scheduler
    ///
    /// # Arguments
    /// * `gossip_interval` - Interval for regular gossip (recommended: 25ms)
    /// * `gossip_fanout` - Number of peers for regular gossip (recommended: 4)
    /// * `max_gossip_payload_size` - Maximum payload size in bytes (recommended: 32KB)
    /// * `urgent_gossip_fanout` - Number of peers for urgent gossip (recommended: 2)
    /// * `transport` - Transport layer for network communication
    /// * `node_id` - Local node identifier
    /// * `initial_membership` - Initial cluster membership state
    pub fn new(
        gossip_interval: Duration,
        gossip_fanout: usize,
        max_gossip_payload_size: usize,
        urgent_gossip_fanout: usize,
        transport: Arc<DynamicMulticastTransport>,
        node_id: u32,
        initial_membership: ClusterMembership,
    ) -> Self {
        Self {
            gossip_interval,
            gossip_fanout,
            max_gossip_payload_size,
            urgent_gossip_fanout,
            transport,
            node_id,
            current_membership: Arc::new(RwLock::new(initial_membership)),
            urgent_updates: Arc::new(DashMap::new()),
            pending_updates: Arc::new(DashMap::new()),
            stats: Arc::new(RwLock::new(GossipStats::default())),
        }
    }

    /// Update cluster membership and adjust peer selection accordingly
    pub async fn update_membership(&self, new_membership: ClusterMembership) {
        let mut membership = self.current_membership.write().await;
        *membership = new_membership.clone();

        // Update transport peers based on new membership
        self.transport
            .update_peers_from_membership(&new_membership, self.node_id);

        println!(
            "Gossip scheduler updated membership to version {}",
            new_membership.membership_version
        );
    }

    /// Start the gossip scheduler background tasks
    ///
    /// This spawns two async tasks:
    /// 1. Urgent gossip processor (5ms intervals)
    /// 2. Regular gossip processor (configurable interval, default 25ms)
    pub async fn start(&self) {
        // Start urgent gossip processor
        let urgent_updates = self.urgent_updates.clone();
        let transport_urgent = self.transport.clone();
        let urgent_fanout = self.urgent_gossip_fanout;
        let node_id = self.node_id;
        let stats_urgent = self.stats.clone();
        let membership_urgent = self.current_membership.clone();

        tokio::spawn(async move {
            let mut urgent_interval = interval(Duration::from_millis(5)); // 5ms for urgent updates
            loop {
                urgent_interval.tick().await;
                Self::process_urgent_updates(
                    &urgent_updates,
                    &transport_urgent,
                    urgent_fanout,
                    node_id,
                    &stats_urgent,
                    &membership_urgent,
                )
                .await;
            }
        });

        // Start regular gossip processor
        let pending_updates = self.pending_updates.clone();
        let transport_regular = self.transport.clone();
        let regular_interval = self.gossip_interval;
        let regular_fanout = self.gossip_fanout;
        let max_payload_size = self.max_gossip_payload_size;
        let stats_regular = self.stats.clone();
        let membership_regular = self.current_membership.clone();

        tokio::spawn(async move {
            let mut regular_interval = interval(regular_interval);
            loop {
                regular_interval.tick().await;
                Self::process_regular_gossip(
                    &pending_updates,
                    &transport_regular,
                    regular_fanout,
                    max_payload_size,
                    node_id,
                    &stats_regular,
                    &membership_regular,
                )
                .await;
            }
        });

        println!(
            "Gossip scheduler started with {}ms regular interval",
            self.gossip_interval.as_millis()
        );
    }

    /// Add an urgent update for immediate propagation
    ///
    /// Urgent updates are processed every 5ms and are used for critical state
    /// changes like low token buckets that need immediate cluster awareness.
    pub fn add_urgent_update(&self, client_id: String, bucket: VersionedTokenBucket) {
        let update = UrgentUpdate {
            client_id: client_id.clone(),
            bucket,
            timestamp: Instant::now(),
        };

        self.urgent_updates.insert(client_id, update);

        // Update stats synchronously for consistent testing
        if let Ok(mut stats_guard) = self.stats.try_write() {
            stats_guard.total_updates_processed += 1;
        }
    }

    /// Add a regular update for normal gossip propagation
    ///
    /// Regular updates are processed at the configured gossip interval (default 25ms)
    /// and are used for normal state synchronization.
    pub fn add_regular_update(&self, client_id: String, bucket: VersionedTokenBucket) {
        self.pending_updates.insert(client_id, bucket);

        // Update stats synchronously for consistent testing
        if let Ok(mut stats_guard) = self.stats.try_write() {
            stats_guard.total_updates_processed += 1;
        }
    }

    /// Process urgent updates with immediate propagation
    async fn process_urgent_updates(
        urgent_updates: &DashMap<String, UrgentUpdate>,
        transport: &DynamicMulticastTransport,
        fanout: usize,
        node_id: u32,
        stats: &Arc<RwLock<GossipStats>>,
        membership: &Arc<RwLock<ClusterMembership>>,
    ) {
        if urgent_updates.is_empty() {
            return;
        }

        // Collect updates older than 1ms (batching window)
        let cutoff = Instant::now() - Duration::from_millis(1);
        let mut updates_to_send = HashMap::new();
        let mut keys_to_remove = Vec::new();

        for entry in urgent_updates.iter() {
            if entry.value().timestamp <= cutoff {
                updates_to_send.insert(entry.key().clone(), entry.value().bucket.clone());
                keys_to_remove.push(entry.key().clone());
            }
        }

        // Remove processed updates
        for key in &keys_to_remove {
            urgent_updates.remove(key);
        }

        if updates_to_send.is_empty() {
            return;
        }

        // Get current membership version for consistency
        let membership_version = {
            let membership_guard = membership.read().await;
            membership_guard.membership_version
        };

        // Send to subset of peers (urgent fanout is smaller for speed)
        let peers = transport.get_peers();
        let selected_peers: Vec<_> = peers
            .iter()
            .take(fanout.min(peers.len()))
            .cloned()
            .collect();

        if !selected_peers.is_empty() {
            let message = GossipMessage::StateSync {
                entries: updates_to_send,
                sender_node_id: node_id,
                membership_version,
            };
            let packet = GossipPacket::new(message);

            match transport.send_to_peers(&packet, &selected_peers).await {
                Ok(successful_sends) => {
                    // Update statistics
                    let mut stats_guard = stats.write().await;
                    stats_guard.urgent_messages_sent += 1;
                    stats_guard.last_urgent_gossip = Some(Instant::now());
                    stats_guard.queue_sizes.urgent_queue_size = stats_guard
                        .queue_sizes
                        .urgent_queue_size
                        .saturating_sub(keys_to_remove.len());

                    if successful_sends < selected_peers.len() {
                        eprintln!(
                            "Urgent gossip: only {}/{} sends successful",
                            successful_sends,
                            selected_peers.len()
                        );
                    }
                }
                Err(e) => {
                    eprintln!("Failed to send urgent gossip: {}", e);
                }
            }
        }
    }

    /// Process regular gossip with payload batching and size limits
    async fn process_regular_gossip(
        pending_updates: &DashMap<String, VersionedTokenBucket>,
        transport: &DynamicMulticastTransport,
        _fanout: usize,
        max_payload_size: usize,
        node_id: u32,
        stats: &Arc<RwLock<GossipStats>>,
        membership: &Arc<RwLock<ClusterMembership>>,
    ) {
        if pending_updates.is_empty() {
            return;
        }

        // Collect updates for regular gossip with size batching
        let mut updates_to_send = HashMap::new();
        let mut current_size = 0;
        let mut keys_to_remove = Vec::new();

        // Estimate base message overhead (rough approximation)
        let base_overhead = 100; // bytes for message envelope
        let keys: Vec<_> = pending_updates
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((_, bucket)) = pending_updates.remove(&key) {
                // Estimate size (rough approximation for payload planning)
                let estimated_entry_size = key.len() + 200; // rough estimate for bucket size

                if current_size + estimated_entry_size + base_overhead > max_payload_size
                    && !updates_to_send.is_empty()
                {
                    // Would exceed size limit, process what we have
                    break;
                }

                updates_to_send.insert(key.clone(), bucket);
                keys_to_remove.push(key);
                current_size += estimated_entry_size;
            }
        }

        if updates_to_send.is_empty() {
            return;
        }

        // Get current membership version
        let membership_version = {
            let membership_guard = membership.read().await;
            membership_guard.membership_version
        };

        // Save entries for potential retry
        let entries_backup = updates_to_send.clone();

        // Use multicast for regular gossip (more efficient for cluster-wide sync)
        let message = GossipMessage::StateSync {
            entries: updates_to_send,
            sender_node_id: node_id,
            membership_version,
        };
        let packet = GossipPacket::new(message);

        match transport.multicast(&packet).await {
            Ok(()) => {
                // Update statistics
                let mut stats_guard = stats.write().await;
                stats_guard.regular_messages_sent += 1;
                stats_guard.last_regular_gossip = Some(Instant::now());
                stats_guard.payload_bytes_sent += current_size as u64;
                stats_guard.queue_sizes.regular_queue_size = stats_guard
                    .queue_sizes
                    .regular_queue_size
                    .saturating_sub(keys_to_remove.len());
            }
            Err(e) => {
                eprintln!("Failed to send regular gossip: {}", e);

                // Re-add updates on failure for retry
                for key in keys_to_remove {
                    if let Some(bucket) = entries_backup.get(&key) {
                        pending_updates.insert(key, bucket.clone());
                    }
                }
            }
        }
    }

    /// Get current gossip scheduler statistics
    pub async fn get_stats(&self) -> GossipStats {
        let mut stats = self.stats.read().await.clone();

        // Update current queue sizes
        stats.queue_sizes.urgent_queue_size = self.urgent_updates.len();
        stats.queue_sizes.regular_queue_size = self.pending_updates.len();

        stats
    }

    /// Get current queue sizes (lightweight operation)
    pub fn get_queue_sizes(&self) -> QueueSizes {
        QueueSizes {
            urgent_queue_size: self.urgent_updates.len(),
            regular_queue_size: self.pending_updates.len(),
        }
    }

    /// Clear all pending updates (useful for testing or reset scenarios)
    pub fn clear_all_queues(&self) {
        let urgent_count = self.urgent_updates.len();
        let regular_count = self.pending_updates.len();

        self.urgent_updates.clear();
        self.pending_updates.clear();

        println!(
            "Cleared {} urgent and {} regular updates",
            urgent_count, regular_count
        );
    }

    /// Check if scheduler has any pending work
    pub fn has_pending_updates(&self) -> bool {
        !self.urgent_updates.is_empty() || !self.pending_updates.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::messages::{NodeCapabilities, NodeInfo, NodeStatus};
    use crate::token_bucket::TokenBucket;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    async fn create_test_scheduler() -> GossipScheduler {
        let multicast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)), 7946);
        let transport = Arc::new(
            DynamicMulticastTransport::new(0, multicast_addr, vec![])
                .await
                .expect("Failed to create transport"),
        );

        let local_addr = transport.get_local_address();
        let membership = ClusterMembership::new(1, local_addr);

        GossipScheduler::new(
            Duration::from_millis(25), // gossip_interval
            4,                         // gossip_fanout
            32768,                     // max_payload_size (32KB)
            2,                         // urgent_fanout
            transport,
            1, // node_id
            membership,
        )
    }

    fn create_test_bucket(node_id: u32) -> VersionedTokenBucket {
        let bucket = TokenBucket::new(100, Duration::from_secs(60));
        VersionedTokenBucket::new(bucket, node_id)
    }

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = create_test_scheduler().await;
        let stats = scheduler.get_stats().await;
        let queue_sizes = scheduler.get_queue_sizes();

        assert_eq!(stats.urgent_messages_sent, 0);
        assert_eq!(stats.regular_messages_sent, 0);
        assert_eq!(queue_sizes.urgent_queue_size, 0);
        assert_eq!(queue_sizes.regular_queue_size, 0);
        assert!(!scheduler.has_pending_updates());
    }

    #[tokio::test]
    async fn test_add_urgent_update() {
        let scheduler = create_test_scheduler().await;
        let bucket = create_test_bucket(1);

        scheduler.add_urgent_update("client1".to_string(), bucket);

        let queue_sizes = scheduler.get_queue_sizes();
        assert_eq!(queue_sizes.urgent_queue_size, 1);
        assert_eq!(queue_sizes.regular_queue_size, 0);
        assert!(scheduler.has_pending_updates());
    }

    #[tokio::test]
    async fn test_add_regular_update() {
        let scheduler = create_test_scheduler().await;
        let bucket = create_test_bucket(1);

        scheduler.add_regular_update("client1".to_string(), bucket);

        let queue_sizes = scheduler.get_queue_sizes();
        assert_eq!(queue_sizes.urgent_queue_size, 0);
        assert_eq!(queue_sizes.regular_queue_size, 1);
        assert!(scheduler.has_pending_updates());
    }

    #[tokio::test]
    async fn test_mixed_updates() {
        let scheduler = create_test_scheduler().await;
        let bucket1 = create_test_bucket(1);
        let bucket2 = create_test_bucket(1);

        scheduler.add_urgent_update("urgent_client".to_string(), bucket1);
        scheduler.add_regular_update("regular_client".to_string(), bucket2);

        let queue_sizes = scheduler.get_queue_sizes();
        assert_eq!(queue_sizes.urgent_queue_size, 1);
        assert_eq!(queue_sizes.regular_queue_size, 1);
        assert!(scheduler.has_pending_updates());
    }

    #[tokio::test]
    async fn test_clear_queues() {
        let scheduler = create_test_scheduler().await;
        let bucket1 = create_test_bucket(1);
        let bucket2 = create_test_bucket(1);

        scheduler.add_urgent_update("urgent_client".to_string(), bucket1);
        scheduler.add_regular_update("regular_client".to_string(), bucket2);

        // Verify updates were added
        assert!(scheduler.has_pending_updates());

        // Clear and verify
        scheduler.clear_all_queues();

        let queue_sizes = scheduler.get_queue_sizes();
        assert_eq!(queue_sizes.urgent_queue_size, 0);
        assert_eq!(queue_sizes.regular_queue_size, 0);
        assert!(!scheduler.has_pending_updates());
    }

    #[tokio::test]
    async fn test_membership_update() {
        let scheduler = create_test_scheduler().await;

        // Create updated membership with additional node
        let addr1 = scheduler.transport.get_local_address();
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7947);

        let mut new_membership = ClusterMembership::new(1, addr1);
        let node2 = NodeInfo {
            node_id: 2,
            address: addr2,
            status: NodeStatus::Active,
            joined_at: 1234567890,
            last_seen: 1234567890,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 50_000,
                gossip_protocol_version: 1,
            },
        };
        new_membership.add_node(node2);

        // Update membership
        scheduler.update_membership(new_membership.clone()).await;

        // Verify membership was updated
        let current_membership = scheduler.current_membership.read().await;
        assert_eq!(
            current_membership.membership_version,
            new_membership.membership_version
        );
        assert_eq!(current_membership.nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let scheduler = create_test_scheduler().await;
        let bucket = create_test_bucket(1);

        // Add some updates
        scheduler.add_urgent_update("client1".to_string(), bucket.clone());
        scheduler.add_regular_update("client2".to_string(), bucket);

        let stats = scheduler.get_stats().await;

        // Should track total updates processed
        assert_eq!(stats.total_updates_processed, 2);
        assert_eq!(stats.queue_sizes.urgent_queue_size, 1);
        assert_eq!(stats.queue_sizes.regular_queue_size, 1);

        // Initially no messages sent
        assert_eq!(stats.urgent_messages_sent, 0);
        assert_eq!(stats.regular_messages_sent, 0);
        assert!(stats.last_urgent_gossip.is_none());
        assert!(stats.last_regular_gossip.is_none());
    }

    #[tokio::test]
    async fn test_urgent_update_timing() {
        let scheduler = create_test_scheduler().await;
        let bucket = create_test_bucket(1);

        // Add urgent update
        scheduler.add_urgent_update("client1".to_string(), bucket);

        // Check that update has recent timestamp
        let urgent_entry = scheduler
            .urgent_updates
            .get("client1")
            .expect("Update should exist");
        let age = Instant::now().duration_since(urgent_entry.timestamp);

        // Should be very recent (< 100ms)
        assert!(age < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_queue_size_consistency() {
        let scheduler = create_test_scheduler().await;

        // Add multiple updates
        for i in 0..5 {
            let bucket = create_test_bucket(1);
            scheduler.add_urgent_update(format!("urgent_{}", i), bucket);
        }

        for i in 0..3 {
            let bucket = create_test_bucket(1);
            scheduler.add_regular_update(format!("regular_{}", i), bucket);
        }

        let queue_sizes = scheduler.get_queue_sizes();
        let stats = scheduler.get_stats().await;

        // Queue sizes should be consistent between methods
        assert_eq!(queue_sizes.urgent_queue_size, 5);
        assert_eq!(queue_sizes.regular_queue_size, 3);
        assert_eq!(stats.queue_sizes.urgent_queue_size, 5);
        assert_eq!(stats.queue_sizes.regular_queue_size, 3);
        assert_eq!(stats.total_updates_processed, 8);
    }
}
