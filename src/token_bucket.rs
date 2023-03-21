/// This module includes the "Token Bucket" rate-limiting algorithm.
/// For inspiration see: https://en.wikipedia.org/wiki/Token_bucket
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::cli;

/// Each rate-limited item will be stored in here.
/// To check if a limit has been exceeded we will ask an instance of `TokenBucket`
#[derive(Clone, Debug, Deserialize, Serialize)]
pub (crate) struct TokenBucket {
    // Count of tokens
    pub tokens: f64,
    // timestamp in unix milliseconds
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
        self.tokens = (self.tokens + tokens_to_add).clamp(0.0, f64::from(rate_limit_settings.rate_limit_max_calls_allowed));
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
