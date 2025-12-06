use colibri::{
    cluster::{BucketExport, ClientBucketData, ExportMetadata},
    limiters::token_bucket::{TokenBucket, TokenBucketLimiter},
    node::NodeId,
    settings::RateLimitSettings,
};
use std::collections::HashMap;

#[tokio::test]
async fn test_token_bucket_export_import() {
    let node_id = NodeId::new(1);
    let rate_limit_settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
    };

    // Create a token bucket limiter with some test data
    let mut limiter = TokenBucketLimiter::new(node_id, rate_limit_settings.clone());

    // Add some client data by making rate limit calls (this creates buckets)
    let _ = limiter.limit_calls_for_client("client1".to_string());
    let _ = limiter.limit_calls_for_client("client2".to_string());
    let _ = limiter.limit_calls_for_client("client3".to_string());

    // Export all buckets
    let exported = limiter.export_all_buckets();

    // Verify we have 3 clients
    assert_eq!(exported.len(), 3);
    assert!(exported.contains_key("client1"));
    assert!(exported.contains_key("client2"));
    assert!(exported.contains_key("client3"));

    // Create a new limiter and import the data
    let new_limiter = TokenBucketLimiter::new(node_id, rate_limit_settings);
    new_limiter.import_buckets(exported.clone());

    // Verify the data was imported
    let reimported = new_limiter.export_all_buckets();
    assert_eq!(reimported.len(), 3);
    assert!(reimported.contains_key("client1"));
    assert!(reimported.contains_key("client2"));
    assert!(reimported.contains_key("client3"));

    // Verify bucket count method
    assert_eq!(new_limiter.bucket_count(), 3);

    // Verify client keys method
    let keys = new_limiter.get_all_client_keys();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"client1".to_string()));
    assert!(keys.contains(&"client2".to_string()));
    assert!(keys.contains(&"client3".to_string()));
}

#[test]
fn test_bucket_export_serialization() {
    let export = BucketExport {
        client_data: vec![ClientBucketData {
            client_id: "test-client".to_string(),
            remaining_tokens: 95,
            last_refill: 1234567890,
            bucket_id: Some(0),
        }],
        metadata: ExportMetadata {
            node_id: "test-node".to_string(),
            export_timestamp: 1234567890,
            node_type: "single".to_string(),
            bucket_count: 1,
        },
    };

    // Test JSON serialization/deserialization
    let json = serde_json::to_string(&export).expect("Should serialize");
    let deserialized: BucketExport = serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.client_data.len(), 1);
    assert_eq!(deserialized.client_data[0].client_id, "test-client");
    assert_eq!(deserialized.client_data[0].remaining_tokens, 95);
    assert_eq!(deserialized.client_data[0].last_refill, 1234567890);
}

#[tokio::test]
async fn test_import_with_conflict_resolution() {
    let node_id = NodeId::new(1);
    let rate_limit_settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
    };

    let limiter = TokenBucketLimiter::new(node_id, rate_limit_settings);

    // Create initial data with older timestamp
    let mut initial_data = HashMap::new();
    initial_data.insert(
        "client1".to_string(),
        TokenBucket {
            tokens: 50.0,
            last_call: 1000, // Older timestamp
        },
    );
    limiter.import_buckets(initial_data);

    // Try to import newer data for the same client
    let mut newer_data = HashMap::new();
    newer_data.insert(
        "client1".to_string(),
        TokenBucket {
            tokens: 75.0,
            last_call: 2000, // Newer timestamp
        },
    );
    limiter.import_buckets(newer_data);

    // Verify the newer data won (conflict resolution)
    let exported = limiter.export_all_buckets();
    let client1_bucket = exported.get("client1").unwrap();
    assert_eq!(client1_bucket.tokens, 75.0);
    assert_eq!(client1_bucket.last_call, 2000);

    // Try to import older data - should be ignored
    let mut older_data = HashMap::new();
    older_data.insert(
        "client1".to_string(),
        TokenBucket {
            tokens: 25.0,
            last_call: 500, // Much older timestamp
        },
    );
    limiter.import_buckets(older_data);

    // Verify the data didn't change
    let final_exported = limiter.export_all_buckets();
    let final_client1_bucket = final_exported.get("client1").unwrap();
    assert_eq!(final_client1_bucket.tokens, 75.0);
    assert_eq!(final_client1_bucket.last_call, 2000);
}
