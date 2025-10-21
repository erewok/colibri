use bincode::{Decode, Encode};

use super::token_bucket::{Bucket, TokenBucket};
use super::vector_clock::VectorClock;
use crate::settings::RateLimitSettings;

/// A TokenBucket wrapped with vector clock for distributed conflict resolution
#[derive(Clone, Debug, Decode, Encode)]
pub struct VersionedTokenBucket {
    pub bucket: TokenBucket,
    pub vector_clock: VectorClock,
    pub last_updated_by: u32, // node_id that made the last update
}

impl VersionedTokenBucket {
    /// Check if we should accept an incoming update based on vector clock causality
    pub fn should_accept_update(&self, incoming: &VersionedTokenBucket) -> bool {
        incoming.vector_clock.is_newer_than(&self.vector_clock)
    }
    /// Merge an incoming versioned bucket using optimistic conflict resolution
    /// Returns true if the update was accepted, false if rejected
    pub fn merge(&mut self, incoming: VersionedTokenBucket, local_node_id: u32) -> bool {
        if incoming.vector_clock.is_newer_than(&self.vector_clock) {
            // Accept the incoming update - it's causally newer
            *self = incoming;
            true
        } else if self.vector_clock.is_newer_than(&incoming.vector_clock) {
            // Reject - our version is causally newer
            false
        } else {
            // Concurrent updates - use optimistic conflict resolution
            // Higher token count wins (optimistic strategy for rate limiting)
            if incoming.bucket.tokens >= self.bucket.tokens {
                // Update our vector clock to include the incoming update
                self.vector_clock.update(&incoming.vector_clock);
                self.vector_clock.increment(local_node_id);

                // Accept the higher token count
                self.bucket = incoming.bucket;
                self.last_updated_by = local_node_id; // We resolved the conflict
                true
            } else {
                // Keep our higher token count but update vector clock
                self.vector_clock.update(&incoming.vector_clock);
                self.vector_clock.increment(local_node_id);
                self.last_updated_by = local_node_id; // We resolved the conflict
                false
            }
        }
    }

    /// Update the bucket locally and increment our vector clock
    pub fn update_locally(&mut self, node_id: u32) {
        self.vector_clock.increment(node_id);
        self.last_updated_by = node_id;
    }

    /// Check if this bucket has concurrent updates with another
    pub fn is_concurrent_with(&self, other: &VersionedTokenBucket) -> bool {
        self.vector_clock.is_concurrent_with(&other.vector_clock)
    }
}

impl Bucket for VersionedTokenBucket {
    /// Create a new versioned token bucket with initial vector clock entry
    fn new(rate_limit_settings: &RateLimitSettings) -> Self {
        let bucket = TokenBucket::new(rate_limit_settings);
        let node_id = rate_limit_settings.node_id;
        let mut vector_clock = VectorClock::new();
        vector_clock.increment(node_id);

        Self {
            bucket,
            vector_clock,
            last_updated_by: node_id,
        }
    }

    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &crate::settings::RateLimitSettings,
    ) -> &mut Self {
        self.bucket.add_tokens_to_bucket(rate_limit_settings);
        self.vector_clock.increment(self.last_updated_by);
        self
    }
    /// Check if a request is allowed (at least 1 token available)
    fn check_if_allowed(&self) -> bool {
        self.bucket.check_if_allowed()
    }

    fn decrement(&mut self) -> &mut Self {
        self.bucket.decrement();
        self.vector_clock.increment(self.last_updated_by);
        self
    }

    fn last_call(&self) -> i64 {
        self.bucket.last_call()
    }
    /// Get the current token count as u32
    fn tokens_to_u32(&self) -> u32 {
        self.bucket.tokens_to_u32()
    }

    /// Consume tokens by decrementing, updating vector clock if successful
    fn try_consume(&mut self, tokens_requested: u32) -> bool {
        let result = self.bucket.try_consume(tokens_requested);
        if result {
            // Update vector clock on successful consumption
            self.vector_clock.increment(self.last_updated_by);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings;

    fn get_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
            node_id: 1,
        }
    }

    fn create_bucket_with_tokens(tokens: f64) -> TokenBucket {
        let mut bucket = TokenBucket::default();
        bucket.tokens = tokens;
        bucket
    }

    #[test]
    fn test_new_versioned_bucket() {
        let versioned = VersionedTokenBucket::new(&get_settings());

        assert_eq!(versioned.tokens_to_u32(), 1); // Default starts with 1 token
        assert_eq!(versioned.last_updated_by, 1);
        assert_eq!(versioned.vector_clock.get_timestamp(1), 1); // Should be incremented once
    }

    #[test]
    fn test_should_accept_newer_update() {
        let bucket1 = create_bucket_with_tokens(100.0);
        let mut versioned1 = VersionedTokenBucket::new(&get_settings());
        versioned1.bucket = bucket1;

        let bucket2 = create_bucket_with_tokens(80.0);
        let mut versioned2 = VersionedTokenBucket::new(&get_settings()); // Same node ID
        versioned2.bucket = bucket2;

        // Make versioned2 newer by incrementing again
        versioned2.vector_clock.increment(1);

        assert!(versioned1.should_accept_update(&versioned2));
        assert!(!versioned2.should_accept_update(&versioned1));
    }

    #[test]
    fn test_merge_newer_update_accepted() {
        let bucket1 = create_bucket_with_tokens(100.0);
        let mut versioned1 = VersionedTokenBucket::new(&get_settings());
        versioned1.bucket = bucket1;

        let bucket2 = create_bucket_with_tokens(80.0);
        let mut versioned2 = VersionedTokenBucket::new(&get_settings()); // Same node ID
        versioned2.bucket = bucket2;
        versioned2.vector_clock.increment(1); // Make it newer

        let result = versioned1.merge(versioned2.clone(), 1);

        assert!(result); // Update should be accepted
        assert_eq!(versioned1.tokens_to_u32(), 80); // Should have new token count
        assert_eq!(versioned1.last_updated_by, 1); // Should track original updater
    }

    #[test]
    fn test_merge_concurrent_higher_tokens_wins() {
        // Create two concurrent updates
        let bucket1 = create_bucket_with_tokens(50.0); // Lower tokens
        let mut versioned1 = VersionedTokenBucket::new(&get_settings());
        versioned1.bucket = bucket1;

        let bucket2 = create_bucket_with_tokens(80.0); // Higher tokens
        let mut versioned2 = VersionedTokenBucket::new(&get_settings());
        versioned2.bucket = bucket2;

        // Make them concurrent by having both nodes increment once
        versioned1.vector_clock.increment(1); // Node 1 at [1:2, 2:0]
        versioned2.vector_clock.increment(2); // Node 2 at [1:0, 2:2]

        let result = versioned1.merge(versioned2, 1);

        assert!(result); // Higher tokens should win
        assert_eq!(versioned1.tokens_to_u32(), 80); // Should accept higher token count
        assert_eq!(versioned1.last_updated_by, 1); // We resolved the conflict
    }

    #[test]
    fn test_try_consume_success() {
        let bucket = create_bucket_with_tokens(10.0);
        let mut versioned = VersionedTokenBucket::new(&get_settings());
        versioned.bucket = bucket;

        let original_timestamp = versioned.vector_clock.get_timestamp(1);

        let result = versioned.try_consume(1);

        assert!(result);
        assert_eq!(versioned.tokens_to_u32(), 9); // Should be decremented

        let new_timestamp = versioned.vector_clock.get_timestamp(1);
        assert!(new_timestamp > original_timestamp);
    }

    #[test]
    fn test_try_consume_failure() {
        let bucket = create_bucket_with_tokens(0.5); // Less than 1 token
        let mut versioned = VersionedTokenBucket::new(&get_settings());
        versioned.bucket = bucket;

        let original_timestamp = versioned.vector_clock.get_timestamp(1);

        let result = versioned.try_consume(1);

        assert!(!result);
        assert!(!versioned.check_if_allowed()); // Should not be allowed

        let timestamp_after = versioned.vector_clock.get_timestamp(1);
        assert_eq!(timestamp_after, original_timestamp); // Vector clock unchanged on failure
    }
}
