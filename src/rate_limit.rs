/// Cache defines a Lazy-TTL based HashMap.
/// This data structure offers a garbage collection
/// method for expired items. Otherwise, it will store
/// and offer access to TokenBucket instances.
/// Access to this data structure requires mutability
/// so it should happen inside something like a RwLock.
///
use std::collections::HashMap;

use bincode::{Decode, Encode};
use chrono::Utc;

use crate::cli;
use crate::token_bucket;
use crate::token_bucket::TokenBucket;

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
#[derive(Clone, Debug, Decode, Encode)]
pub struct RateLimiter {
    settings: cli::RateLimitSettings,
    cache: HashMap<String, token_bucket::TokenBucket>,
}

impl RateLimiter {
    pub fn new(rate_limit_settings: cli::RateLimitSettings) -> Self {
        Self {
            settings: rate_limit_settings,
            cache: HashMap::new(),
        }
    }

    /// Return actual calls remaining or a default value
    pub fn check_calls_remaining_for_client(&self, key: &str) -> u32 {
        let bucket = self.cache.get(key);
        bucket
            .map(|b| b.tokens_to_u32())
            .unwrap_or(self.settings.rate_limit_max_calls_allowed)
    }

    /// Expire all keys older than a threshold to keep cache from growing endlessly
    pub fn expire_keys(&mut self) {
        let threshold = (self.settings.rate_limit_interval_seconds * 1000) as i64 * 2;
        let cutoff = Utc::now().timestamp_millis() - threshold;
        let after_buckets_expired: HashMap<String, token_bucket::TokenBucket> = self
            .cache
            .extract_if(|_k, bucket| {
                // check that last call was within cutoff
                bucket.last_call > cutoff
            })
            .collect();
        self.cache = after_buckets_expired;
    }

    /// Check rate limit and decrement if call is allowed
    /// None -> not allowed
    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        let mut bucket = match self.cache.get(&key) {
            // We are going to modify and insert this bucket back into cache
            Some(mut _bucket) => _bucket.to_owned(),
            None => TokenBucket {
                tokens: self.settings.rate_limit_max_calls_allowed.into(),
                ..Default::default()
            },
        };
        // Add more tokens at token bucket rate
        bucket.add_tokens_to_bucket(&self.settings);
        let result = if bucket.check_if_allowed() {
            // We only count this call if client is allowed to proceed
            bucket.decrement();
            // Return result showing this call allowed and tokens remaining
            Some(bucket.tokens_to_u32())
        } else {
            // This result means no more calls allowed
            None
        };
        // Clobber entry in cache for this bucket
        self.cache.insert(key, bucket);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, Duration};

    fn get_settings() -> cli::RateLimitSettings {
        cli::RateLimitSettings {
            rate_limit_max_calls_allowed: 5,
            rate_limit_interval_seconds: 1,
        }
    }

    fn new_rate_limiter() -> RateLimiter {
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
        let mut limiter = RateLimiter::new(get_settings());

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
        let zero_settings = cli::RateLimitSettings {
            rate_limit_max_calls_allowed: 0,
            rate_limit_interval_seconds: 1,
        };

        let mut limiter = RateLimiter::new(zero_settings);

        // Should immediately deny any requests
        assert!(limiter
            .limit_calls_for_client("test_client".to_string())
            .is_none());
        assert_eq!(limiter.check_calls_remaining_for_client("test_client"), 0);
    }

    #[test]
    fn test_rate_limiter_settings_impact() {
        // Test with high limits
        let high_limit = cli::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
        };

        let mut limiter = RateLimiter::new(high_limit);
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 100);

        // Make 10 requests
        for _ in 0..10 {
            assert!(limiter.limit_calls_for_client("test".to_string()).is_some());
        }
        assert_eq!(limiter.check_calls_remaining_for_client("test"), 90);
    }
}
