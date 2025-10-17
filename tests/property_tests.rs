use colibri::cli::RateLimitSettings;
use colibri::consistent_hashing::jump_consistent_hash;
use colibri::rate_limit::RateLimiter;
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_consistent_hashing_bounds_property(
        key in "[a-zA-Z0-9]+",
        bucket_count in 1u32..100
    ) {
        let result = jump_consistent_hash(&key, bucket_count);
        prop_assert!(result < bucket_count);
    }

    #[test]
    fn test_consistent_hashing_deterministic_property(
        key in "[a-zA-Z0-9]+",
        bucket_count in 1u32..100
    ) {
        let result1 = jump_consistent_hash(&key, bucket_count);
        let result2 = jump_consistent_hash(&key, bucket_count);
        prop_assert_eq!(result1, result2);
    }

    #[test]
    fn test_rate_limiter_monotonic_decrease_property(
        client_id in "[a-zA-Z0-9_]+",
        max_calls in 1u32..100,
        interval in 1u32..60
    ) {
        let settings = RateLimitSettings {
            rate_limit_max_calls_allowed: max_calls,
            rate_limit_interval_seconds: interval,
        };

        let mut limiter = RateLimiter::new(settings);

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
            rate_limit_max_calls_allowed: max_calls,
            rate_limit_interval_seconds: 1,
        };

        let mut limiter = RateLimiter::new(settings);
        let client_id = "property_test_client";

        for _ in 0..requests {
            limiter.limit_calls_for_client(client_id.to_string());
            let remaining = limiter.check_calls_remaining_for_client(client_id);
            // Should never go negative or exceed the maximum
            prop_assert!(remaining <= max_calls);
        }
    }
}
