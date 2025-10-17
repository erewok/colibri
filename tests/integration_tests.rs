use colibri::cli::RateLimitSettings;
use colibri::consistent_hashing::{get_neighbor_bucket, jump_consistent_hash};
use colibri::rate_limit::RateLimiter;

#[test]
fn test_rate_limiter_integration() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 5,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = RateLimiter::new(settings);

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
fn test_consistent_hashing_integration() {
    // Test basic functionality
    let key = "test_integration_key";
    let bucket_count = 10;

    let hash_result = jump_consistent_hash(key, bucket_count);
    assert!(hash_result < bucket_count);

    // Test neighbor calculation
    let (left, right) = get_neighbor_bucket(hash_result, bucket_count);
    assert!(left < bucket_count);
    assert!(right < bucket_count);

    // Test deterministic behavior
    let hash_result2 = jump_consistent_hash(key, bucket_count);
    assert_eq!(hash_result, hash_result2);
}

#[test]
fn test_rate_limiter_multiple_clients() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 3,
        rate_limit_interval_seconds: 1,
    };

    let mut limiter = RateLimiter::new(settings);

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
fn test_consistent_hashing_distribution_basic() {
    let bucket_count = 5;
    let mut bucket_usage = vec![0; bucket_count as usize];

    // Hash 100 different keys
    for i in 0..100 {
        let key = format!("key_{}", i);
        let bucket = jump_consistent_hash(&key, bucket_count);
        bucket_usage[bucket as usize] += 1;
    }

    // Each bucket should be used at least once (very likely with 100 keys and 5 buckets)
    for (bucket_id, usage_count) in bucket_usage.iter().enumerate() {
        assert!(*usage_count > 0, "Bucket {} was never used", bucket_id);
    }
}

#[test]
fn test_rate_limiter_expire_functionality() {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 10,
        rate_limit_interval_seconds: 1,
    };

    let mut limiter = RateLimiter::new(settings);

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
