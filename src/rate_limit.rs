use serde::{Deserialize, Serialize};
/// Cache defines a Lazy-TTL based HashMap.
/// This data structure offers a garbage collection
/// method for expired items. Otherwise, it will store
/// and offer access to TokenBucket instances.
/// Access to this data structure requires mutability
/// so it should happen inside something like a RwLock.
///
use std::collections::HashMap;

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
    pub fn new(settings: cli::Cli) -> Self {
        Self {
            settings: settings.rate_limit_settings(),
            cache: HashMap::new(),
        }
    }
    pub fn expire_keys(&mut self)  {
        let after_buckets_expired: HashMap<String, token_bucket::TokenBucket> = self
            .cache
            .drain_filter(|_k, bucket| {
                bucket.last_call > self.settings.rate_limit_interval_seconds as i64 * 2
            })
            .collect();
        self.cache = after_buckets_expired;
    }

    pub fn check_calls_remaining_for_client(&self, key: &str) -> u32 {
        let bucket = self.cache.get(key);
        bucket
            .map(|b| b.tokens_to_u32())
            .unwrap_or(self.settings.rate_limit_max_calls_allowed)
    }

    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        let mut bucket = match self.cache.get(&key) {
            Some(mut _bucket) => _bucket.to_owned(),
            None => TokenBucket::default(),
        };
        // Add more tokens at rate
        bucket.add_tokens_to_bucket(&self.settings);
        let result = if bucket.check_if_allowed() {
            bucket.decrement();
            Some(bucket.tokens_to_u32())
        } else {
            None
        };
        self.cache.insert(key, bucket);
        result
    }
}
