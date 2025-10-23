/// Distributed token bucket using epoch-based G-counter for gossip protocol
/// Each node tracks per-node consumption counts with epoch numbers to handle resets
use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use chrono::Utc;

use super::token_bucket::Bucket;
use crate::node::NodeId;
use crate::settings;

/// Per-node counter with epoch for reset detection
#[derive(Clone, Debug, Decode, Encode, PartialEq)]
pub struct NodeCounter {
    pub epoch: u64,
    pub consumed: u32,
}

impl NodeCounter {
    pub fn new() -> Self {
        Self {
            epoch: 0,
            consumed: 0,
        }
    }
}

/// Distributed token bucket that can be gossiped and merged across cluster nodes
#[derive(Clone, Debug, Decode, Encode)]
pub struct EpochTokenBucket {
    pub node_id: NodeId,

    /// Per-node consumption counters with epochs
    pub node_counters: HashMap<NodeId, NodeCounter>,

    /// Current available tokens (like TokenBucket)
    pub tokens: f64,

    /// Timestamp in unix milliseconds of last token addition
    last_call: i64,

    /// Maximum token capacity (bucket size)
    max_capacity: u32,

    /// Rate limit settings for token replenishment
    rate_limit_settings: settings::RateLimitSettings,

    /// Current epoch number for this node
    pub current_epoch: u64,

    /// Last epoch reset time (unix milliseconds)
    pub last_epoch_reset: i64,
}

impl Bucket for EpochTokenBucket {
    /// Create a new distributed token bucket
    fn new(node_id: NodeId, rate_limit_settings: &settings::RateLimitSettings) -> Self {
        let mut node_counters = HashMap::new();
        node_counters.insert(node_id.clone(), NodeCounter::new());

        let now = Utc::now().timestamp_millis();
        Self {
            node_id,
            node_counters,
            tokens: rate_limit_settings.rate_limit_max_calls_allowed as f64,
            last_call: now,
            max_capacity: rate_limit_settings.rate_limit_max_calls_allowed,
            rate_limit_settings: rate_limit_settings.clone(),
            current_epoch: 0,
            last_epoch_reset: now,
        }
    }

    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self {
        // Same token replenishment logic as TokenBucket
        let diff_ms: i64 = Utc::now().timestamp_millis() - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        let diff_ms: i32 = diff_ms as i32;
        if diff_ms < 5i32 {
            return self;
        }
        // Tokens are added at the token rate
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * f64::from(diff_ms);
        // Max calls is limited to: rate_limit_settings.rate_limit_max_calls_allowed
        self.tokens = (self.tokens + tokens_to_add).clamp(0.0, f64::from(self.max_capacity));
        self.last_call = Utc::now().timestamp_millis();
        self
    }

    fn check_if_allowed(&self) -> bool {
        self.tokens >= 1.0
    }

    fn decrement(&mut self) -> &mut Self {
        self.tokens -= 1.0;
        self.last_call = Utc::now().timestamp_millis();
        self
    }

    fn last_call(&self) -> i64 {
        self.last_call
    }

    fn tokens_to_u32(&self) -> u32 {
        self.tokens.trunc().clamp(0.0, u32::MAX.into()) as u32
    }
    /// Try to consume tokens (for local requests)
    fn try_consume(&mut self, tokens_requested: u32) -> bool {
        // First add tokens based on time elapsed (like TokenBucket)
        self.add_tokens_to_bucket(&self.rate_limit_settings.clone());

        let requested = tokens_requested as f64;
        if self.tokens >= requested {
            // Consume tokens from the bucket
            self.tokens -= requested;
            self.last_call = Utc::now().timestamp_millis();

            // Also track consumption in node counter for gossip merging
            self.check_and_reset_epoch();
            if let Some(counter) = self.node_counters.get_mut(&self.node_id) {
                counter.consumed += tokens_requested;
            }

            true
        } else {
            false
        }
    }
}

impl EpochTokenBucket {
    /// Check if epoch has expired and reset if needed
    fn check_and_reset_epoch(&mut self) {
        let now = Utc::now().timestamp_millis();
        let epoch_duration_ms = self.rate_limit_settings.rate_limit_interval_seconds as i64 * 1000;

        if now - self.last_epoch_reset >= epoch_duration_ms {
            // Epoch expired - increment epoch and reset consumption counters
            self.current_epoch += 1;
            self.last_epoch_reset = now;

            // Reset our own consumption counter but keep the new epoch
            if let Some(counter) = self.node_counters.get_mut(&self.node_id) {
                counter.epoch = self.current_epoch;
                counter.consumed = 0;
            }

            // Clean up old epoch data from other nodes
            self.cleanup_old_epochs();
        }
    }

    /// Get total consumed tokens across all nodes in current epoch
    pub fn total_consumed(&self) -> u32 {
        self.node_counters
            .values()
            .map(|counter| counter.consumed)
            .sum()
    }

    /// Get remaining tokens available (from the token bucket perspective)
    pub fn remaining(&self) -> u32 {
        self.tokens.trunc().clamp(0.0, u32::MAX.into()) as u32
    }

    /// Check if request would be allowed without consuming
    pub fn check_if_allowed_without_consuming(&mut self, tokens_requested: u32) -> bool {
        // First add tokens based on time elapsed
        self.add_tokens_to_bucket(&self.rate_limit_settings.clone());
        self.tokens >= tokens_requested as f64
    }

    /// Merge gossip state from another node (CRDT merge operation)
    pub fn merge(&mut self, other: &EpochTokenBucket) -> bool {
        // First check if we need to reset our epoch
        self.check_and_reset_epoch();

        // Update our epoch if the other node has a newer one
        self.current_epoch = self.current_epoch.max(other.current_epoch);

        // Merge each node's counter
        let mut modifying_node_ids: HashSet<&NodeId> = HashSet::new();
        for (node_id, remote_counter) in &other.node_counters {
            match self.node_counters.get_mut(node_id) {
                Some(local_counter) => {
                    if remote_counter.epoch > local_counter.epoch {
                        // Remote has newer epoch, take it entirely
                        *local_counter = remote_counter.clone();
                        modifying_node_ids.insert(node_id);
                    } else if remote_counter.epoch == local_counter.epoch {
                        // Same epoch - take max consumed (G-counter property)
                        local_counter.consumed =
                            local_counter.consumed.max(remote_counter.consumed);
                        modifying_node_ids.insert(node_id);
                    }
                    // else: remote epoch is older, ignore it
                }
                None => {
                    // New node we haven't seen before
                    self.node_counters
                        .insert(node_id.clone(), remote_counter.clone());
                    modifying_node_ids.insert(node_id);
                }
            }
        }

        // After merging, we might need to adjust our token count based on
        // total consumption in current epoch (for rate limiting across cluster)
        self.adjust_tokens_after_merge();

        modifying_node_ids.len() > 0
    }

    /// Get max capacity
    pub fn capacity(&self) -> u32 {
        self.max_capacity
    }

    /// Create state suitable for gossiping to other nodes
    pub fn gossip_state(&self) -> EpochTokenBucket {
        self.clone()
    }

    /// Clean up old epoch data from other nodes (keep only current and recent epochs)
    fn cleanup_old_epochs(&mut self) {
        let current_epoch = self.current_epoch;
        self.node_counters.retain(|node_id, counter| {
            // Keep our own node always, and keep others if they're in current epoch or 1 behind
            node_id == &self.node_id || counter.epoch >= current_epoch.saturating_sub(1)
        });
    }

    /// Adjust token count after merging gossip data
    /// This ensures distributed rate limiting - if we've learned about more consumption
    /// in the current epoch, we may need to reduce available tokens
    fn adjust_tokens_after_merge(&mut self) {
        let total_consumed = self.total_consumed();
        let max_tokens = self.max_capacity as f64;

        // If total cluster consumption in this epoch exceeds our available tokens,
        // we need to be more conservative
        if total_consumed as f64 > max_tokens - self.tokens {
            // Reduce our available tokens to account for cluster-wide consumption
            let adjusted_tokens = max_tokens - total_consumed as f64;
            self.tokens = self.tokens.min(adjusted_tokens.max(0.0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_rl_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 60,
        }
    }

    #[test]
    fn test_basic_consume() {
        let mut bucket = EpochTokenBucket::new(1u32.into(), &get_rl_settings());

        assert!(bucket.try_consume(3));
        assert_eq!(bucket.total_consumed(), 3);
        assert_eq!(bucket.remaining(), 7);

        assert!(bucket.try_consume(5));
        assert_eq!(bucket.total_consumed(), 8);

        // Should fail - not enough tokens
        assert!(!bucket.try_consume(3));
        assert_eq!(bucket.total_consumed(), 8);
    }

    #[test]
    fn test_merge_same_epoch() {
        let mut bucket1 = EpochTokenBucket::new(1u32.into(), &get_rl_settings());
        let mut bucket2 = EpochTokenBucket::new(2u32.into(), &get_rl_settings());

        // Each node consumes locally
        bucket1.try_consume(10);
        bucket2.try_consume(20);

        // Merge node2's state into node1
        bucket1.merge(&bucket2);

        // bucket1 should now know about both consumptions
        assert_eq!(bucket1.total_consumed(), 30);
        assert_eq!(
            bucket1.node_counters.get(&1u32.into()).unwrap().consumed,
            10
        );
        assert_eq!(
            bucket1.node_counters.get(&2u32.into()).unwrap().consumed,
            20
        );
    }

    #[test]
    fn test_merge_with_epoch_reset() {
        let mut bucket1 = EpochTokenBucket::new(1u32.into(), &get_rl_settings());
        let mut bucket2 = EpochTokenBucket::new(2u32.into(), &get_rl_settings());

        // Node1 consumes in epoch 0
        bucket1.try_consume(30);

        // Node2 resets to epoch 1
        bucket2.node_counters.get_mut(&2u32.into()).unwrap().epoch = 1;
        bucket2
            .node_counters
            .get_mut(&2u32.into())
            .unwrap()
            .consumed = 5;

        // Merge - node2's newer epoch should win
        bucket1.merge(&bucket2);

        // bucket1 should accept node2's epoch 1 state
        assert_eq!(bucket1.node_counters.get(&2u32.into()).unwrap().epoch, 1);
        assert_eq!(bucket1.node_counters.get(&2u32.into()).unwrap().consumed, 5);
        assert_eq!(bucket1.node_counters.get(&1u32.into()).unwrap().epoch, 0);
        assert_eq!(bucket1.total_consumed(), 35);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut bucket1 = EpochTokenBucket::new(1u32.into(), &get_rl_settings());
        let bucket2 = EpochTokenBucket::new(2u32.into(), &get_rl_settings());

        bucket1.try_consume(10);

        // Merge multiple times - should be idempotent
        bucket1.merge(&bucket2);
        let first_total = bucket1.total_consumed();

        bucket1.merge(&bucket2);
        let second_total = bucket1.total_consumed();

        assert_eq!(first_total, second_total);
    }

    #[test]
    fn test_g_counter_property() {
        let mut bucket1 = EpochTokenBucket::new(1u32.into(), &get_rl_settings());
        let mut bucket2 = bucket1.clone();

        // Simulate concurrent updates on same node
        bucket1.try_consume(5);
        bucket2.try_consume(8);

        // Both think node1 consumed different amounts
        // Merge should take max
        bucket1.merge(&bucket2);

        assert_eq!(bucket1.node_counters.get(&1u32.into()).unwrap().consumed, 8);
    }

    #[test]
    fn test_three_way_merge() {
        let mut bucket1 = EpochTokenBucket::new(1u32.into(), &get_rl_settings());
        let mut bucket2 = EpochTokenBucket::new(2u32.into(), &get_rl_settings());
        let mut bucket3 = EpochTokenBucket::new(3u32.into(), &get_rl_settings());

        bucket1.try_consume(10);
        bucket2.try_consume(20);
        bucket3.try_consume(30);

        // Merge in different orders - should converge
        let mut bucket_a = bucket1.clone();
        bucket_a.merge(&bucket2);
        bucket_a.merge(&bucket3);

        let mut bucket_b = bucket1.clone();
        bucket_b.merge(&bucket3);
        bucket_b.merge(&bucket2);

        assert_eq!(bucket_a.total_consumed(), 60);
        assert_eq!(bucket_b.total_consumed(), 60);
    }
}
