use proptest::prelude::*;

use colibri::limiters::token_bucket::TokenBucketLimiter;
use colibri::node::NodeId;
use colibri::settings::RateLimitSettings;

proptest! {
    #[test]
    fn test_rate_limiter_monotonic_decrease_property(
        client_id in "[a-zA-Z0-9_]+",
        max_calls in 1u32..100,
        interval in 1u32..60
    ) {
        let settings = RateLimitSettings {
            cluster_participant_count: 1,
            rate_limit_max_calls_allowed: max_calls,
            rate_limit_interval_seconds: interval,
        };

        let mut limiter: TokenBucketLimiter = TokenBucketLimiter::new(NodeId::new(1), settings);

        let initial_remaining = limiter.check_calls_remaining_for_client(&client_id);
        prop_assert_eq!(initial_remaining, max_calls);

        // Make a request
        if let Some(remaining_after_request) = limiter.limit_calls_for_client(client_id.clone()) {
            let check_remaining = limiter.check_calls_remaining_for_client(&client_id);
            prop_assert_eq!(remaining_after_request, check_remaining);
            prop_assert!(check_remaining < initial_remaining);
        }
    }

    #[test]
    fn test_rate_limiter_never_negative_property(
        requests in 1usize..200,
        max_calls in 1u32..50
    ) {
        let settings = RateLimitSettings {
            cluster_participant_count: 1,
            rate_limit_max_calls_allowed: max_calls,
            rate_limit_interval_seconds: 1,
        };

        let mut limiter: TokenBucketLimiter = TokenBucketLimiter::new(NodeId::new(1), settings);
        let client_id = "property_test_client";

        for _ in 0..requests {
            limiter.limit_calls_for_client(client_id.to_string());
            let remaining = limiter.check_calls_remaining_for_client(client_id);
            // Should never go negative or exceed the maximum
            prop_assert!(remaining <= max_calls);
        }
    }
}
