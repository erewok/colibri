//! This module includes the "Token Bucket" rate-limiting algorithm.
//! For inspiration see: https://en.wikipedia.org/wiki/Token_bucket
use bincode::{Decode, Encode};
use chrono::Utc;

use crate::node::NodeId;
use crate::settings;

pub trait Bucket {
    fn new(node_id: NodeId, rate_limit_settings: &settings::RateLimitSettings) -> Self;
    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self;
    fn check_if_allowed(&self) -> bool;
    fn decrement(&mut self) -> &mut Self;
    fn last_call(&self) -> i64;
    fn tokens_to_u32(&self) -> u32;
    fn try_consume(&mut self, tokens_requested: u32) -> bool;
}

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
#[derive(Clone, Debug, Decode, Encode)]
pub struct TokenBucket {
    // Count of tokens
    pub tokens: f64,
    // timestamp in unix milliseconds
    last_call: i64,
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self {
            // greater than zero, probably fewer than whatever max will be for this app
            tokens: 1f64,
            last_call: Utc::now().timestamp_millis(),
        }
    }
}

impl Bucket for TokenBucket {
    /// Create a new TokenBucket with full capacity
    fn new(_node_id: NodeId, rate_limit_settings: &settings::RateLimitSettings) -> Self {
        Self {
            tokens: rate_limit_settings.rate_limit_max_calls_allowed as f64,
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
        // Tokens are added at the token rate
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * f64::from(diff_ms);
        // Max calls is limited to: rate_limit_settings.rate_limit_max_calls_allowed
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
    /// Return timestamp of last call
    fn last_call(&self) -> i64 {
        self.last_call
    }
    /// Return number of tokens as u32, clamped to u32 range
    fn tokens_to_u32(&self) -> u32 {
        self.tokens.trunc().clamp(0.0, u32::MAX.into()) as u32
    }
    /// Try to consume tokens
    fn try_consume(&mut self, tokens_requested: u32) -> bool {
        let requested = tokens_requested as f64;
        if self.tokens >= requested {
            self.tokens -= requested;
            self.last_call = Utc::now().timestamp_millis();
            true
        } else {
            false
        }
    }
}

/// Rate limiting is implemented by a single bucket for each limited object.
impl TokenBucket {}

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

        for _n in 0..5 {
            assert!(bucket.tokens > 0.0);
            assert!(bucket.tokens < 6.0);
            bucket.add_tokens_to_bucket(&settings);
        }
        // exhausted tokens now
        assert!((1.0 - bucket.tokens).is_sign_negative());
        // sleep and expire
        time::sleep(Duration::from_secs(2)).await;

        assert!(bucket.tokens > 0.0);
        assert!(bucket.tokens < 6.0);
        bucket.add_tokens_to_bucket(&settings);
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
}
