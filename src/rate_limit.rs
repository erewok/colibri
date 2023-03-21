/// Cache defines a Lazy-TTL based HashMap.
/// This data structure offers a garbage collection
/// method for expired items. Otherwise, it will store
/// and offer access to TokenBucket instances.
/// Access to this data structure requires mutability
/// so it should happen inside something like a RwLock.
///
use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::cli;
use crate::token_bucket;
use crate::token_bucket::TokenBucket;

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
#[derive(Clone, Debug, Deserialize, Serialize)]
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
        let threshold = self.settings.rate_limit_interval_seconds as i64 * 2;
        let cutoff = Utc::now().timestamp_millis() - threshold;
        let after_buckets_expired: HashMap<String, token_bucket::TokenBucket> = self
            .cache
            .drain_filter(|_k, bucket| {
                // check that last call was within cutoff
                bucket.last_call > cutoff
            })
            .collect();
        self.cache = after_buckets_expired;
    }

    /// Check rate limit and decrement if call is allowed
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
    fn limit_calls_will_block() {
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
}
