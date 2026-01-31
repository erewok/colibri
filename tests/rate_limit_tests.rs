//! Integration tests for rate limiting functionality
use colibri::limiters::token_bucket::TokenBucketLimiter;
use colibri::settings::RateLimitSettings;

#[test]
fn test_token_bucket_basic_rate_limiting() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 10,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = TokenBucketLimiter::new(settings);
    let client_id = "test_client";

    // Initial check should show full limit
    let remaining = limiter.check_calls_remaining_for_client(client_id);
    assert_eq!(remaining, 10);

    // Consume one token
    let result = limiter.limit_calls_for_client(client_id.to_string());
    assert_eq!(result, Some(9));

    // Check remaining
    let remaining = limiter.check_calls_remaining_for_client(client_id);
    assert_eq!(remaining, 9);
}

#[test]
fn test_token_bucket_exhaustion() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 3,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = TokenBucketLimiter::new(settings);
    let client_id = "exhaustion_client";

    // Exhaust the tokens
    assert_eq!(
        limiter.limit_calls_for_client(client_id.to_string()),
        Some(2)
    );
    assert_eq!(
        limiter.limit_calls_for_client(client_id.to_string()),
        Some(1)
    );
    assert_eq!(
        limiter.limit_calls_for_client(client_id.to_string()),
        Some(0)
    );

    // Next call should be denied
    assert_eq!(limiter.limit_calls_for_client(client_id.to_string()), None);
}

#[test]
fn test_token_bucket_multiple_clients() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 5,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = TokenBucketLimiter::new(settings);

    // Different clients have independent limits
    assert_eq!(
        limiter.limit_calls_for_client("client1".to_string()),
        Some(4)
    );
    assert_eq!(
        limiter.limit_calls_for_client("client2".to_string()),
        Some(4)
    );
    assert_eq!(
        limiter.limit_calls_for_client("client1".to_string()),
        Some(3)
    );

    // Check they're tracked separately
    assert_eq!(limiter.check_calls_remaining_for_client("client1"), 3);
    assert_eq!(limiter.check_calls_remaining_for_client("client2"), 4);
}

#[test]
fn test_export_import() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = TokenBucketLimiter::new(settings.clone());

    // Create some state
    limiter.limit_calls_for_client("user1".to_string());
    limiter.limit_calls_for_client("user2".to_string());

    // Export
    let exported = limiter.export_all_buckets();
    assert_eq!(exported.len(), 2);

    // Import into new limiter
    let new_limiter = TokenBucketLimiter::new(settings);
    new_limiter.import_buckets(exported);

    assert_eq!(new_limiter.bucket_count(), 2);
    let keys = new_limiter.get_all_client_keys();
    assert!(keys.contains(&"user1".to_string()));
    assert!(keys.contains(&"user2".to_string()));
}
