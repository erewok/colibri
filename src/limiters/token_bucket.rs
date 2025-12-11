//! Token bucket rate limiting algorithm
use bincode::{Decode, Encode};
use chrono::Utc;
use papaya::HashMap;

use crate::node::NodeId;
use crate::settings;

/// Rate limiter bucket interface
pub trait Bucket {
    fn new(max_calls: u32, node_id: NodeId) -> Self;
    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self;
    fn check_if_allowed(&self) -> bool;
    fn decrement(&mut self) -> &mut Self;
    fn tokens_to_u32(&self) -> u32;
}

/// Token bucket for rate limiting
#[derive(Clone, Debug, Decode, Encode)]
pub struct TokenBucket {
    pub tokens: f64,
    pub last_call: i64,
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self {
            tokens: 1f64,
            last_call: Utc::now().timestamp_millis(),
        }
    }
}

impl Bucket for TokenBucket {
    fn new(max_calls: u32, _: NodeId) -> Self {
        TokenBucket {
            tokens: max_calls as f64,
            last_call: Utc::now().timestamp_millis(),
        }
    }
    /// Function for adding tokens to the bucket.
    /// Tokens are added at the rate of token_rate * time_since_last_request
    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self {
        // epoch time in milliseconds here from `chrono` is an i64
        let diff_ms: i64 = Utc::now().timestamp_millis() - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        let diff_ms: i32 = diff_ms as i32;
        if diff_ms < 5i32 {
            return self;
        }
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * f64::from(diff_ms);
        self.tokens = (self.tokens + tokens_to_add).clamp(
            0.0,
            f64::from(rate_limit_settings.rate_limit_max_calls_allowed),
        );
        self.last_call = Utc::now().timestamp_millis();
        self
    }

    /// Subtract a full token (represents a request)
    fn decrement(&mut self) -> &mut Self {
        self.tokens -= 1f64;
        self
    }
    /// Check if rate-limited.
    /// Must have at least 1 full token
    fn check_if_allowed(&self) -> bool {
        self.tokens >= 1f64
    }

    /// Return number of tokens as u32, clamped to u32 range
    fn tokens_to_u32(&self) -> u32 {
        self.tokens.trunc().clamp(0.0, u32::MAX.into()) as u32
    }
}

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
/// Rate limiter managing multiple token buckets
#[derive(Clone, Debug)]
pub struct TokenBucketLimiter {
    settings: settings::RateLimitSettings,
    cache: HashMap<String, TokenBucket>,
}

impl TokenBucketLimiter {
    pub fn new(rate_limit_settings: settings::RateLimitSettings) -> Self {
        Self {
            settings: rate_limit_settings,
            cache: HashMap::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn new_bucket(&self) -> TokenBucket {
        TokenBucket::new(self.settings.rate_limit_max_calls_allowed, NodeId::default())
    }

    // It's possible that updates happen around this object
    pub fn get_bucket(&self, key: &str) -> Option<TokenBucket> {
        self.cache.pin().get(key).cloned()
    }

    // It's possible that updates happen around this object
    pub fn set_bucket(&mut self, key: &str, bucket: TokenBucket) -> bool {
        self.cache.pin().insert(key.to_string(), bucket).is_some()
    }

    pub fn get_settings(&self) -> &settings::RateLimitSettings {
        &self.settings
    }

    /// Return actual calls remaining or a default value
    pub fn check_calls_remaining_for_client(&self, key: &str) -> u32 {
        self.cache
            .pin()
            .get(key)
            .map(|b| b.tokens_to_u32())
            .unwrap_or(self.settings.rate_limit_max_calls_allowed)
    }

    /// Expire all keys older than a threshold to keep cache from growing endlessly
    pub fn expire_keys(&mut self) {
        let threshold = (self.settings.rate_limit_interval_seconds * 1000) as i64 * 2;
        let cutoff = Utc::now().timestamp_millis() - threshold;
        // pin_owned is expensive but we need to mutate potentially the whole map
        self.cache.pin_owned().retain(|_k, bucket| {
            // check that last call was within cutoff
            bucket.last_call > cutoff
        });
    }

    /// Check rate limit and decrement if call is allowed
    /// Returns Some(remaining_tokens) if allowed, None if rate limited
    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        if let Some(bucket) = self.cache.pin().get(&key) {
            // Update existing bucket
            let mut updated_bucket = bucket.clone();
            updated_bucket.add_tokens_to_bucket(&self.settings);

            if updated_bucket.check_if_allowed() {
                // Call is allowed - decrement and update
                updated_bucket.decrement();
                let remaining = updated_bucket.tokens_to_u32();
                self.cache.pin().insert(key, updated_bucket);
                Some(remaining)
            } else {
                // Call not allowed - update bucket state but don't decrement
                self.cache.pin().insert(key, updated_bucket);
                None
            }
        } else {
            // Create new bucket - first call is always allowed
            let mut new_bucket = self.new_bucket();
            new_bucket.decrement(); // Use one token
                                    // if negative here, we'll have an unreliable value.
                                    // so we need to safe-check *first* before returning `remaining`
            let is_allowed = new_bucket.check_if_allowed();
            let remaining = Some(new_bucket.tokens_to_u32());
            self.cache.pin().insert(key, new_bucket);
            if is_allowed {
                remaining
            } else {
                None
            }
        }
    }

    /// Check rate limit and decrement if call is allowed using provided settings
    /// Returns Some(remaining_tokens) if allowed, None if rate limited
    pub fn limit_calls_for_client_with_settings(
        &mut self,
        key: String,
        settings: &settings::RateLimitSettings,
    ) -> Option<u32> {
        if let Some(bucket) = self.cache.pin().get(&key) {
            // Update existing bucket with provided settings
            let mut updated_bucket = bucket.clone();
            updated_bucket.add_tokens_to_bucket(settings);

            if updated_bucket.check_if_allowed() {
                // Call is allowed - decrement and update
                updated_bucket.decrement();
                let remaining = updated_bucket.tokens_to_u32();
                self.cache.pin().insert(key, updated_bucket);
                Some(remaining)
            } else {
                // Call not allowed - update bucket state but don't decrement
                self.cache.pin().insert(key, updated_bucket);
                None
            }
        } else {
            // Create new bucket - first call is always allowed
            let mut new_bucket =
                TokenBucket::new(settings.rate_limit_max_calls_allowed, NodeId::default());
            // Set tokens to max for the provided settings
            new_bucket.tokens = f64::from(settings.rate_limit_max_calls_allowed);
            new_bucket.decrement(); // Use one token
            let is_allowed = new_bucket.check_if_allowed();
            let remaining = Some(new_bucket.tokens_to_u32());
            self.cache.pin().insert(key, new_bucket);
            if is_allowed {
                remaining
            } else {
                None
            }
        }
    }

    /// Export all token buckets for cluster migration
    pub fn export_all_buckets(&self) -> std::collections::HashMap<String, TokenBucket> {
        let guard = self.cache.pin();
        let mut exported = std::collections::HashMap::new();

        // Iterate through all entries in the papaya HashMap
        for (key, bucket) in guard.iter() {
            exported.insert(key.clone(), bucket.clone());
        }

        exported
    }

    /// Import token buckets from another node during cluster migration
    pub fn import_buckets(&self, buckets: std::collections::HashMap<String, TokenBucket>) {
        let guard = self.cache.pin();

        for (key, bucket) in buckets {
            // Only import if the bucket doesn't already exist or if the imported bucket
            // has more recent activity (higher last_call timestamp)
            match guard.get(&key) {
                Some(existing) => {
                    if bucket.last_call > existing.last_call {
                        guard.insert(key, bucket);
                    }
                }
                None => {
                    guard.insert(key, bucket);
                }
            }
        }
    }

    /// Get all client keys currently in the cache (for debugging/monitoring)
    pub fn get_all_client_keys(&self) -> Vec<String> {
        let guard = self.cache.pin();
        guard.iter().map(|(key, _)| key.clone()).collect()
    }

    /// Get bucket count for monitoring
    pub fn bucket_count(&self) -> usize {
        self.cache.pin().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, Duration};

    #[test]
    fn check_allowed() {
        let mut bucket = TokenBucket::default();
        bucket.tokens = 2.0;
        bucket.decrement();
        assert!(bucket.check_if_allowed());
        bucket.decrement();
        assert_eq!(bucket.check_if_allowed(), false);
    }

    #[test]
    fn tokens_to_u32() {
        let bucket = TokenBucket {
            tokens: 4294967297.0,
            last_call: Utc::now().timestamp_millis(),
        };
        assert_eq!(bucket.tokens_to_u32(), u32::MAX);

        let bucket = TokenBucket {
            tokens: -4294967297.0,
            last_call: Utc::now().timestamp_millis(),
        };
        assert_eq!(bucket.tokens_to_u32(), 0);

        let bucket = TokenBucket {
            tokens: 3.333333333333333,
            last_call: Utc::now().timestamp_millis(),
        };
        assert_eq!(bucket.tokens_to_u32(), 3);

        let bucket = TokenBucket {
            tokens: 1203.9999999999,
            last_call: Utc::now().timestamp_millis(),
        };
        assert_eq!(bucket.tokens_to_u32(), 1203);
    }

    #[tokio::test]
    async fn add_tokens_check_math() {
        let mut bucket = TokenBucket {
            tokens: 5.0,
            last_call: Utc::now().timestamp_millis(),
        };

        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
        };

        // Consume all tokens first
        for _n in 0..5 {
            assert!(bucket.tokens > 0.0);
            bucket.decrement();
        }
        // tokens should be exhausted now
        assert!(bucket.tokens <= 0.0);
        assert!(!bucket.check_if_allowed());

        // sleep and let tokens refill
        time::sleep(Duration::from_secs(2)).await;

        // Set last_call to trigger token addition
        bucket.last_call = Utc::now().timestamp_millis() - 1000; // 1 second ago
        bucket.add_tokens_to_bucket(&settings);

        // Should have tokens again after refill
        assert!(bucket.tokens > 0.0);
        assert!(bucket.tokens <= 5.0);
        assert!(bucket.check_if_allowed());
    }

    #[test]
    fn test_token_bucket_depletion() {
        let mut bucket = TokenBucket::default();
        bucket.tokens = 2.0;

        // First request should succeed
        assert!(bucket.check_if_allowed());
        bucket.decrement();

        // Second request should succeed
        assert!(bucket.check_if_allowed());
        bucket.decrement();

        // Third request should fail
        assert!(!bucket.check_if_allowed());
    }

    #[test]
    fn test_token_bucket_overflow_protection() {
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
        };

        let mut bucket = TokenBucket::default();
        bucket.tokens = 10.0; // Above maximum
                              // Set timestamp to trigger token addition
        bucket.last_call = Utc::now().timestamp_millis() - 1000; // 1 second ago

        bucket.add_tokens_to_bucket(&settings);

        // Should be clamped to maximum (5.0)
        assert!(bucket.tokens <= 5.0);
        assert_eq!(bucket.tokens_to_u32(), 5);
    }

    #[test]
    fn test_token_bucket_minimum_time_diff() {
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 1,
        };
        let mut bucket = TokenBucket::default();
        let initial_tokens = bucket.tokens;

        // Simulate a very small time difference (less than 5ms)
        bucket.last_call = Utc::now().timestamp_millis() - 1;

        bucket.add_tokens_to_bucket(&settings);

        // Tokens should remain the same for intervals < 5ms
        assert_eq!(bucket.tokens, initial_tokens);
    }

    #[test]
    fn test_tokens_to_u32_edge_cases() {
        // Test negative values
        let mut bucket = TokenBucket::default();
        bucket.tokens = -5.0;
        assert_eq!(bucket.tokens_to_u32(), 0);

        // Test very large values
        bucket.tokens = (u32::MAX as f64) + 1000.0;
        assert_eq!(bucket.tokens_to_u32(), u32::MAX);

        // Test fractional values
        bucket.tokens = 3.7;
        assert_eq!(bucket.tokens_to_u32(), 3);

        // Test zero
        bucket.tokens = 0.0;
        assert_eq!(bucket.tokens_to_u32(), 0);
    }

    #[test]
    fn test_token_rate_calculation() {
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
        };

        // Should be 100 calls / 60 seconds = 1.666... calls per second
        let rate_per_second = settings.token_rate_seconds();
        assert!((rate_per_second - (100.0 / 60.0)).abs() < 0.001);

        // Should be much smaller per millisecond
        let rate_per_ms = settings.token_rate_milliseconds();
        assert!(rate_per_ms > 0.0);
        assert!(rate_per_ms < rate_per_second);
    }

    #[tokio::test]
    async fn test_token_refill_over_time() {
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 1,
        };

        let mut bucket = TokenBucket::default();
        bucket.tokens = 0.0;

        // Simulate 500ms passage
        bucket.last_call = Utc::now().timestamp_millis() - 500;
        bucket.add_tokens_to_bucket(&settings);

        let tokens_after_500ms = bucket.tokens;
        assert!(tokens_after_500ms > 0.0);

        // Simulate another 500ms passage (1 second total)
        bucket.last_call = Utc::now().timestamp_millis() - 500;
        bucket.add_tokens_to_bucket(&settings);

        // Should have more tokens now, but not exceed maximum
        assert!(bucket.tokens >= tokens_after_500ms);
        assert!(bucket.tokens <= 10.0);
    }

    fn get_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
        }
    }

    fn new_rate_limiter() -> TokenBucketLimiter {
        TokenBucketLimiter::new(get_settings())
    }

    #[tokio::test]
    async fn expire_keys_works() {
        let mut rl = new_rate_limiter();
        assert!((rl.limit_calls_for_client("a1".to_string()).is_some()));
        assert!((rl.limit_calls_for_client("a1".to_string()).is_some()));
        let calls_remain = rl.check_calls_remaining_for_client("a1");
        // sanity check (duplicated below) before we call expire
        assert!(calls_remain > 0);
        assert!(calls_remain < 5);
        // sleep and expire
        time::sleep(Duration::from_secs(2)).await;
        assert!((rl.limit_calls_for_client("b1".to_string()).is_some()));
        assert!((rl.limit_calls_for_client("b1".to_string()).is_some()));
        rl.expire_keys();
        // this one should be expired
        let calls_remain2 = rl.check_calls_remaining_for_client("a1");
        assert_eq!(calls_remain2, 5);
        // this one not expired
        let calls_remain3 = rl.check_calls_remaining_for_client("b1");
        // sanity check (duplicated below) before we call expire
        assert!(calls_remain3 > 0);
        assert!(calls_remain3 < 5);
    }

    #[test]
    fn check_calls_remaining() {
        let mut rl = new_rate_limiter();
        assert!((rl.limit_calls_for_client("a1".to_string()).is_some()));
        assert!((rl.limit_calls_for_client("a1".to_string()).is_some()));
        let calls_remain = rl.check_calls_remaining_for_client("a1");
        // more than 0 left but fewer than max
        assert!(calls_remain > 0);
        assert!(calls_remain < 5);
    }

    #[test]
    fn limit_calls_will_limit() {
        let mut rl = new_rate_limiter();
        for _n in 0..5 {
            assert!(rl.check_calls_remaining_for_client("a1") > 0);
            assert!((rl.limit_calls_for_client("a1".to_string()).is_some()));
        }
        // calls are blocked now
        assert_eq!(rl.check_calls_remaining_for_client("a1"), 0);
        assert!((rl.limit_calls_for_client("a1".to_string()).is_none()));
        assert!((rl.limit_calls_for_client("a1".to_string()).is_none()));
        // calls allowed still zero until expired
        assert_eq!(rl.check_calls_remaining_for_client("a1"), 0);
    }

    #[test]
    fn test_rate_limiter_per_client_isolation() {
        let mut limiter = new_rate_limiter();

        // Exhaust tokens for client1
        for _ in 0..5 {
            assert!(limiter
                .limit_calls_for_client("client1".to_string())
                .is_some());
        }
        // client1 should now be blocked
        assert!(limiter
            .limit_calls_for_client("client1".to_string())
            .is_none());
        assert_eq!(limiter.check_calls_remaining_for_client("client1"), 0);

        // client2 should still have full quota
        assert_eq!(limiter.check_calls_remaining_for_client("client2"), 5);
        assert!(limiter
            .limit_calls_for_client("client2".to_string())
            .is_some());
    }

    #[test]
    fn test_rate_limiter_quota_tracking() {
        let mut limiter = new_rate_limiter();
        let client_id = "quota_test_client";

        // Check initial quota
        assert_eq!(limiter.check_calls_remaining_for_client(client_id), 5);

        // Make some requests and verify quota decreases
        let remaining1 = limiter
            .limit_calls_for_client(client_id.to_string())
            .unwrap();
        assert_eq!(remaining1, 4);
        assert_eq!(limiter.check_calls_remaining_for_client(client_id), 4);

        let remaining2 = limiter
            .limit_calls_for_client(client_id.to_string())
            .unwrap();
        assert_eq!(remaining2, 3);
        assert_eq!(limiter.check_calls_remaining_for_client(client_id), 3);
    }

    #[test]
    fn test_multiple_clients_different_patterns() {
        let mut limiter = new_rate_limiter();

        // Client A makes 3 requests
        for _ in 0..3 {
            assert!(limiter
                .limit_calls_for_client("clientA".to_string())
                .is_some());
        }
        assert_eq!(limiter.check_calls_remaining_for_client("clientA"), 2);

        // Client B makes 1 request
        assert!(limiter
            .limit_calls_for_client("clientB".to_string())
            .is_some());
        assert_eq!(limiter.check_calls_remaining_for_client("clientB"), 4);

        // Client C makes no requests - should have full quota
        assert_eq!(limiter.check_calls_remaining_for_client("clientC"), 5);

        // Client A should still have 2 remaining
        assert_eq!(limiter.check_calls_remaining_for_client("clientA"), 2);
    }

    #[test]
    fn test_rate_limiter_with_zero_tokens() {
        let zero_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 0,
            rate_limit_interval_seconds: 1,
        };

        let mut limiter: TokenBucketLimiter =
            TokenBucketLimiter::new(zero_settings);

        // default is 0
        assert_eq!(limiter.check_calls_remaining_for_client("test_client"), 0);
        // Should immediately deny any requests
        assert!(limiter
            .limit_calls_for_client("test_client".to_string())
            .is_none());
        assert_eq!(limiter.check_calls_remaining_for_client("test_client"), 0);
        // calculating refills should also result in 0 remaining
        assert!(limiter
            .limit_calls_for_client("test_client".to_string())
            .is_none());
    }

    #[test]
    fn test_rate_limiter_settings_impact() {
        // Test with high limits
        let high_limit = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
        };

        let mut limiter: TokenBucketLimiter = TokenBucketLimiter::new(high_limit);
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 100);

        // Make 10 requests
        for _ in 0..10 {
            assert!(limiter.limit_calls_for_client("test".to_string()).is_some());
        }
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 90);
    }
}
