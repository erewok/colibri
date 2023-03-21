/// This module includes the "Token Bucket" rate-limiting algorithm.
/// For inspiration see: https://en.wikipedia.org/wiki/Token_bucket
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::cli;

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct TokenBucket {
    // Count of tokens
    pub tokens: f64,
    // timestamp in unix milliseconds
    pub last_call: i64,
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

/// Rate limiting is implemented by a single bucket for each limited object.
impl TokenBucket {
    pub fn tokens_to_u32(&self) -> u32 {
        self.tokens.trunc().clamp(0.0, u32::MAX.into()) as u32
    }

    /// Function for adding tokens to the bucket.
    /// Tokens are added at the rate of token_rate * time_since_last_request
    pub fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &cli::RateLimitSettings,
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
    pub fn decrement(&mut self) -> &mut Self {
        self.tokens -= 1f64;
        self
    }

    /// Check if rate-limited.
    pub fn check_if_allowed(&self) -> bool {
        self.tokens > 0f64
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

        let settings = cli::RateLimitSettings {
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
}
