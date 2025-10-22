// use bincode::{Decode, Encode};
use chrono::Utc;
use papaya::HashMap;

use super::token_bucket::Bucket;
use crate::settings;

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
/// This data structure offers a garbage collection
/// method for expired items. Otherwise, it will store
/// and offer access to TokenBucket instances.
/// Access to this *whole* data structure requires mutability
/// so it should happen inside something like a RwLock.
#[derive(Clone, Debug)]
pub struct RateLimiter<T: Bucket + Clone> {
    settings: settings::RateLimitSettings,
    /// Cache defines a Lazy-TTL based HashMap.
    cache: HashMap<String, T>,
}

impl<T: Bucket + Clone> RateLimiter<T> {
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

    pub fn new_bucket(&self) -> T {
        T::new(&self.settings)
    }

    pub fn node_id(&self) -> u32 {
        self.settings.node_id
    }

    // It's possible that updates happen around this object
    pub fn get_bucket(&self, key: &str) -> Option<T> {
        self.cache.pin().get(key).cloned()
    }

    // It's possible that updates happen around this object
    pub fn set_bucket(&mut self, key: &str, bucket: T) -> bool {
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
            bucket.last_call() > cutoff
        });
    }

    /// Check rate limit and decrement if call is allowed
    /// None -> not allowed
    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        let limit_checker = |bucket: &T| {
            let mut bucket = bucket.to_owned();
            // Add more tokens at token bucket rate
            bucket.add_tokens_to_bucket(&self.settings);
            if bucket.check_if_allowed() {
                // We only count this call if client is allowed to proceed
                bucket.decrement();
                bucket
            } else {
                // This result means no more calls allowed
                bucket
            }
        };
        // Clobber entry in cache for this bucket if update
        self.cache
            .pin()
            .update_or_insert_with(key.clone(), limit_checker, || self.new_bucket());

        // Return result showing this call allowed and tokens remaining
        self.cache.pin().get(&key).map(|b| b.tokens_to_u32())
    }

    /// Get all current buckets
    pub fn get_all_buckets(&self) -> std::collections::HashMap<String, T> {
        let mut result = std::collections::HashMap::new();
        let pin = self.cache.pin();
        for (key, value) in pin.iter() {
            result.insert(key.clone(), value.clone());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration};

    use super::*;
    use crate::limiters::token_bucket::TokenBucket;
    use crate::limiters::versioned_bucket::VersionedTokenBucket;

    fn get_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
            node_id: 1,
        }
    }

    fn new_rate_limiter() -> RateLimiter<TokenBucket> {
        RateLimiter::new(get_settings())
    }

    fn new_rate_limiter_versioned() -> RateLimiter<VersionedTokenBucket> {
        RateLimiter::new(get_settings())
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
            node_id: 1,
            rate_limit_max_calls_allowed: 0,
            rate_limit_interval_seconds: 1,
        };

        let mut limiter: RateLimiter<TokenBucket> = RateLimiter::new(zero_settings);

        // Should immediately deny any requests
        assert!(limiter
            .limit_calls_for_client("test_client".to_string())
            .is_none());
        assert_eq!(limiter.check_calls_remaining_for_client("test_client"), 0);
    }

    #[test]
    fn test_rate_limiter_settings_impact() {
        // Test with high limits
        let high_limit = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            node_id: 1,
        };

        let mut limiter: RateLimiter<TokenBucket> = RateLimiter::new(high_limit);
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 100);

        // Make 10 requests
        for _ in 0..10 {
            assert!(limiter.limit_calls_for_client("test".to_string()).is_some());
        }
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 90);
    }
}
