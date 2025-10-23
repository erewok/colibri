use colibri::limiters::epoch_bucket::EpochTokenBucket;
use colibri::limiters::rate_limit::RateLimiter;
use colibri::node::NodeId;
use colibri::settings::RateLimitSettings;

#[test]
fn test_rate_limiter_integration() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 5,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter: RateLimiter<EpochTokenBucket> = RateLimiter::new(NodeId::new(1), settings);

    // Test basic rate limiting workflow
    let client_id = "integration_test_client";

    // Should start with full quota
    assert_eq!(limiter.check_calls_remaining_for_client(client_id), 5);

    // Make some requests
    for expected_remaining in (0..5).rev() {
        let result = limiter.limit_calls_for_client(client_id.to_string());
        assert!(result.is_some());
        assert_eq!(result.unwrap(), expected_remaining);
    }

    // Should be blocked now
    assert!(limiter
        .limit_calls_for_client(client_id.to_string())
        .is_none());
    assert_eq!(limiter.check_calls_remaining_for_client(client_id), 0);
}

#[test]
fn test_rate_limiter_multiple_clients() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 3,
        rate_limit_interval_seconds: 1,
    };

    let mut limiter: RateLimiter<EpochTokenBucket> = RateLimiter::new(NodeId::new(1), settings);

    // Test that different clients have independent limits
    assert!(limiter
        .limit_calls_for_client("client_a".to_string())
        .is_some());
    assert!(limiter
        .limit_calls_for_client("client_b".to_string())
        .is_some());

    assert_eq!(limiter.check_calls_remaining_for_client("client_a"), 2);
    assert_eq!(limiter.check_calls_remaining_for_client("client_b"), 2);

    // Exhaust one client
    for _ in 0..2 {
        assert!(limiter
            .limit_calls_for_client("client_a".to_string())
            .is_some());
    }

    // Client A should be blocked, Client B should still work
    assert!(limiter
        .limit_calls_for_client("client_a".to_string())
        .is_none());
    assert!(limiter
        .limit_calls_for_client("client_b".to_string())
        .is_some());
}

#[test]
fn test_rate_limiter_expire_functionality() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 10,
        rate_limit_interval_seconds: 1,
    };

    let mut limiter: RateLimiter<EpochTokenBucket> = RateLimiter::new(NodeId::new(1), settings);

    // Make some requests to populate the cache
    limiter.limit_calls_for_client("test_client".to_string());
    assert_eq!(limiter.check_calls_remaining_for_client("test_client"), 9);

    // Call expire_keys - should not panic and basic functionality should continue
    limiter.expire_keys();

    // Should still be able to make requests
    limiter.limit_calls_for_client("another_client".to_string());
    assert_eq!(
        limiter.check_calls_remaining_for_client("another_client"),
        9
    );
}
