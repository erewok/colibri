use colibri::{
    limiters::token_bucket::{TokenBucket, TokenBucketLimiter},
    node::NodeId,
    settings::RateLimitSettings,
};
use std::collections::HashMap;

#[test]
fn test_basic_export_import() {
    let node_id = NodeId::new(1);
    let rate_limit_settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
    };

    let mut limiter = TokenBucketLimiter::new(node_id, rate_limit_settings.clone());

    // Create some token buckets by making rate limit calls
    let _ = limiter.limit_calls_for_client("user1".to_string());
    let _ = limiter.limit_calls_for_client("user2".to_string());

    // Export the data
    let exported = limiter.export_all_buckets();

    // Should have 2 clients
    assert_eq!(exported.len(), 2);

    // Create a new limiter and import the data
    let new_limiter = TokenBucketLimiter::new(node_id, rate_limit_settings);
    new_limiter.import_buckets(exported);

    // Check that the data was imported
    assert_eq!(new_limiter.bucket_count(), 2);

    let client_keys = new_limiter.get_all_client_keys();
    assert!(client_keys.contains(&"user1".to_string()));
    assert!(client_keys.contains(&"user2".to_string()));
}

#[test]
fn test_manual_bucket_import() {
    let node_id = NodeId::new(1);
    let rate_limit_settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
    };

    let limiter = TokenBucketLimiter::new(node_id, rate_limit_settings);

    // Manually create some token buckets
    let mut buckets = HashMap::new();
    buckets.insert("test_client".to_string(), TokenBucket {
        tokens: 95.0,
        last_call: 1234567890,
    });

    // Import them
    limiter.import_buckets(buckets);

    // Verify
    assert_eq!(limiter.bucket_count(), 1);
    let exported = limiter.export_all_buckets();
    let bucket = exported.get("test_client").unwrap();
    assert_eq!(bucket.tokens, 95.0);
    assert_eq!(bucket.last_call, 1234567890);
}