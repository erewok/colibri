//! Comprehensive integration tests for the configurable rate limits feature.
//! These tests focus on behavior rather than implementation details, testing
//! the feature from the API level down to actual rate limiting enforcement.

use colibri::api;
use colibri::node::NodeWrapper;
use colibri::settings::{NamedRateLimitRule, RunMode, Settings};
use std::collections::HashSet;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt; // for `call`, `oneshot`, etc.

/// Helper to create test settings for a single node
fn create_test_settings() -> Settings {
    Settings {
        listen_address: "127.0.0.1".to_string(),
        listen_port_api: 8410,
        listen_port_tcp: 8411,
        listen_port_udp: 8412,
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 60,
        run_mode: RunMode::Single,
        gossip_interval_ms: 1000,
        gossip_fanout: 3,
        topology: HashSet::new(), // Empty topology for single node
        failure_timeout_secs: 30,
    }
}

/// Helper to create a node wrapper and API for testing
async fn setup_test_node() -> (NodeWrapper, axum::Router) {
    let settings = create_test_settings();
    let node = NodeWrapper::new(settings).await.unwrap();
    let api = api::api(node.clone()).await.unwrap();
    (node, api)
}

/// Helper to create a JSON request body for rate limit settings
fn create_rate_limit_rule_body(
    rule_name: &str,
    max_calls: u32,
    interval_seconds: u32,
) -> serde_json::Value {
    json!({
        "name": rule_name,
        "settings": {
            "rate_limit_max_calls_allowed": max_calls,
            "rate_limit_interval_seconds": interval_seconds
        }
    })
}

#[cfg(test)]
mod configuration_management_tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_list_named_rules() {
        let (_node, app) = setup_test_node().await;

        // Create a rule
        let rule_body = create_rate_limit_rule_body("test-rule", 500, 30);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // List rules to verify it was created
        let list_request = Request::builder()
            .method(Method::GET)
            .uri("/rl-config")
            .body(Body::empty())
            .unwrap();

        let list_response = app.clone().oneshot(list_request).await.unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let rules: Vec<NamedRateLimitRule> = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "test-rule");
        assert_eq!(rules[0].settings.rate_limit_max_calls_allowed, 500);
        assert_eq!(rules[0].settings.rate_limit_interval_seconds, 30);
    }

    #[tokio::test]
    async fn test_get_specific_rule() {
        let (_node, app) = setup_test_node().await;

        // Create a rule first
        let rule_body = create_rate_limit_rule_body("test-rule", 200, 15);
        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let _create_response = app.clone().oneshot(create_request).await.unwrap();

        // Get the specific rule
        let get_request = Request::builder()
            .method(Method::GET)
            .uri("/rl-config/test-rule")
            .body(Body::empty())
            .unwrap();

        let get_response = app.clone().oneshot(get_request).await.unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let rule: NamedRateLimitRule = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(rule.name, "test-rule");
        assert_eq!(rule.settings.rate_limit_max_calls_allowed, 200);
        assert_eq!(rule.settings.rate_limit_interval_seconds, 15);
    }

    #[tokio::test]
    async fn test_delete_rule() {
        let (_node, app) = setup_test_node().await;

        // Create a rule first
        let rule_body = create_rate_limit_rule_body("test-rule", 300, 45);
        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let _create_response = app.clone().oneshot(create_request).await.unwrap();

        // Delete the rule
        let delete_request = Request::builder()
            .method(Method::DELETE)
            .uri("/rl-config/test-rule")
            .body(Body::empty())
            .unwrap();

        let delete_response = app.clone().oneshot(delete_request).await.unwrap();
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        // Verify it's gone by listing rules
        let list_request = Request::builder()
            .method(Method::GET)
            .uri("/rl-config")
            .body(Body::empty())
            .unwrap();

        let list_response = app.clone().oneshot(list_request).await.unwrap();
        let body_bytes = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let rules: Vec<NamedRateLimitRule> = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(rules.len(), 0);
    }

    #[tokio::test]
    async fn test_get_nonexistent_rule() {
        let (_node, app) = setup_test_node().await;

        let get_request = Request::builder()
            .method(Method::GET)
            .uri("/rl-config/nonexistent-rule")
            .body(Body::empty())
            .unwrap();

        let get_response = app.clone().oneshot(get_request).await.unwrap();
        // Should return an error status (likely 404 or 500)
        assert_ne!(get_response.status(), StatusCode::OK);
    }
}

#[cfg(test)]
mod custom_rate_limiting_behavior_tests {
    use super::*;

    #[tokio::test]
    async fn test_custom_rate_limiting_enforces_limits() {
        let (_node, app) = setup_test_node().await;

        // Create a strict rule: 3 calls per 60 seconds
        let rule_body = json!({
            "name": "strict-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 3,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let create_response = app.clone().oneshot(create_request).await.unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let test_key = "test-user-123";

        // First request should succeed (uses 1 of 3 tokens)
        let limit_request1 = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/strict-rule/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response1 = app.clone().oneshot(limit_request1).await.unwrap();
        assert_eq!(response1.status(), StatusCode::OK);

        // Second request should succeed (uses 2 of 3 tokens)
        let limit_request2 = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/strict-rule/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response2 = app.clone().oneshot(limit_request2).await.unwrap();
        assert_eq!(response2.status(), StatusCode::OK);

        // Third request should succeed (uses 3 of 3 tokens)
        let limit_request3 = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/strict-rule/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response3 = app.clone().oneshot(limit_request3).await.unwrap();
        assert_eq!(response3.status(), StatusCode::OK);

        // Fourth request should be rate limited (no tokens remaining)
        let limit_request4 = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/strict-rule/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response4 = app.clone().oneshot(limit_request4).await.unwrap();
        assert_eq!(response4.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_check_limit_custom() {
        let (_node, app) = setup_test_node().await;

        // Create a rule with 10 calls allowed
        let rule_body = json!({
            "name": "check-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 10,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let _create_response = app.clone().oneshot(create_request).await.unwrap();

        let test_key = "check-user";

        // Check initial limit
        let check_request = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/check-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let check_response = app.clone().oneshot(check_request).await.unwrap();
        assert_eq!(check_response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(check_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(result["client_id"], test_key);
        assert_eq!(result["calls_remaining"], 10);

        // Make a rate limit request to consume a token
        let limit_request = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/check-rule/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let _limit_response = app.clone().oneshot(limit_request).await.unwrap();

        // Check limit again - should be decreased
        let check_request2 = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/check-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let check_response2 = app.clone().oneshot(check_request2).await.unwrap();
        let body_bytes2 = axum::body::to_bytes(check_response2.into_body(), usize::MAX)
            .await
            .unwrap();
        let result2: serde_json::Value = serde_json::from_slice(&body_bytes2).unwrap();

        assert_eq!(result2["client_id"], test_key);
        assert_eq!(result2["calls_remaining"], 9);
    }

    #[tokio::test]
    async fn test_different_rules_have_different_limits() {
        let (_node, app) = setup_test_node().await;

        // Create a permissive rule
        let permissive_rule = json!({
            "name": "permissive-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 1000,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request1 = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(permissive_rule.to_string()))
            .unwrap();

        let _create_response1 = app.clone().oneshot(create_request1).await.unwrap();

        // Create a strict rule
        let strict_rule = json!({
            "name": "strict-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 1,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request2 = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(strict_rule.to_string()))
            .unwrap();

        let _create_response2 = app.clone().oneshot(create_request2).await.unwrap();

        let test_key = "same-user";

        // Check permissive rule - should have 1000 calls
        let check_permissive = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/permissive-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let permissive_response = app.clone().oneshot(check_permissive).await.unwrap();
        let permissive_bytes = axum::body::to_bytes(permissive_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let permissive_result: serde_json::Value =
            serde_json::from_slice(&permissive_bytes).unwrap();

        assert_eq!(permissive_result["calls_remaining"], 1000);

        // Check strict rule - should have 1 call
        let check_strict = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/strict-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let strict_response = app.clone().oneshot(check_strict).await.unwrap();
        let strict_bytes = axum::body::to_bytes(strict_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let strict_result: serde_json::Value = serde_json::from_slice(&strict_bytes).unwrap();

        assert_eq!(strict_result["calls_remaining"], 1);
    }
}

#[cfg(test)]
mod rule_isolation_tests {
    use super::*;

    #[tokio::test]
    async fn test_keys_are_isolated_between_rules() {
        let (_node, app) = setup_test_node().await;

        // Create two different rules
        let rule1 = json!({
            "name": "rule-1",
            "settings": {
                "rate_limit_max_calls_allowed": 5,
                "rate_limit_interval_seconds": 60
            }
        });

        let rule2 = json!({
            "name": "rule-2",
            "settings": {
                "rate_limit_max_calls_allowed": 3,
                "rate_limit_interval_seconds": 60
            }
        });

        // Create both rules
        for rule in [&rule1, &rule2] {
            let create_request = Request::builder()
                .method(Method::POST)
                .uri("/rl-config")
                .header("content-type", "application/json")
                .body(Body::from(rule.to_string()))
                .unwrap();

            let _create_response = app.clone().oneshot(create_request).await.unwrap();
        }

        let test_key = "isolated-user";

        // Consume all tokens for rule-1 (5 calls)
        for _ in 0..5 {
            let limit_request = Request::builder()
                .method(Method::POST)
                .uri(&format!("/rl/rule-1/{}", test_key))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();

            let response = app.clone().oneshot(limit_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Verify rule-1 is exhausted
        let limit_request_exhausted = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/rule-1/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let exhausted_response = app.clone().oneshot(limit_request_exhausted).await.unwrap();
        assert_eq!(exhausted_response.status(), StatusCode::TOO_MANY_REQUESTS);

        // But rule-2 should still have all its tokens available for the same key
        let check_rule2 = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/rule-2/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let rule2_response = app.clone().oneshot(check_rule2).await.unwrap();
        let rule2_bytes = axum::body::to_bytes(rule2_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let rule2_result: serde_json::Value = serde_json::from_slice(&rule2_bytes).unwrap();

        assert_eq!(rule2_result["calls_remaining"], 3);

        // And we should still be able to make requests to rule-2
        let limit_request_rule2 = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/rule-2/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let rule2_limit_response = app.clone().oneshot(limit_request_rule2).await.unwrap();
        assert_eq!(rule2_limit_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_different_keys_are_isolated_within_same_rule() {
        let (_node, app) = setup_test_node().await;

        // Create a rule with 2 calls allowed
        let rule_body = json!({
            "name": "isolation-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 2,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let _create_response = app.clone().oneshot(create_request).await.unwrap();

        // Exhaust tokens for user1
        for _ in 0..2 {
            let limit_request = Request::builder()
                .method(Method::POST)
                .uri("/rl/isolation-rule/user1")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();

            let response = app.clone().oneshot(limit_request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Verify user1 is rate limited
        let user1_blocked = Request::builder()
            .method(Method::POST)
            .uri("/rl/isolation-rule/user1")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let blocked_response = app.clone().oneshot(user1_blocked).await.unwrap();
        assert_eq!(blocked_response.status(), StatusCode::TOO_MANY_REQUESTS);

        // But user2 should still have full tokens available
        let check_user2 = Request::builder()
            .method(Method::GET)
            .uri("/rl-check/isolation-rule/user2")
            .body(Body::empty())
            .unwrap();

        let user2_response = app.clone().oneshot(check_user2).await.unwrap();
        let user2_bytes = axum::body::to_bytes(user2_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let user2_result: serde_json::Value = serde_json::from_slice(&user2_bytes).unwrap();

        assert_eq!(user2_result["calls_remaining"], 2);

        // And user2 should be able to make successful requests
        let user2_limit = Request::builder()
            .method(Method::POST)
            .uri("/rl/isolation-rule/user2")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let user2_limit_response = app.clone().oneshot(user2_limit).await.unwrap();
        assert_eq!(user2_limit_response.status(), StatusCode::OK);
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiting_with_nonexistent_rule() {
        let (_node, app) = setup_test_node().await;

        // Try to use a rule that doesn't exist
        let limit_request = Request::builder()
            .method(Method::POST)
            .uri("/rl/nonexistent-rule/some-key")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response = app.clone().oneshot(limit_request).await.unwrap();
        // Should return an error status (not 200 or 429)
        assert_ne!(response.status(), StatusCode::OK);
        assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_check_limit_with_nonexistent_rule() {
        let (_node, app) = setup_test_node().await;

        // Try to check a limit for a rule that doesn't exist
        let check_request = Request::builder()
            .method(Method::GET)
            .uri("/rl-check/nonexistent-rule/some-key")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(check_request).await.unwrap();
        // Should return an error status
        assert_ne!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_rule() {
        let (_node, app) = setup_test_node().await;

        let delete_request = Request::builder()
            .method(Method::DELETE)
            .uri("/rl-config/nonexistent-rule")
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(delete_request).await.unwrap();
        // DELETE is idempotent - should succeed even for non-existent rules
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}

#[cfg(test)]
mod backward_compatibility_tests {
    use super::*;

    #[tokio::test]
    async fn test_default_rate_limiting_still_works() {
        let (_node, app) = setup_test_node().await;

        // Test that the original /rl/{client_id} endpoint still works
        let client_id = "legacy-client";

        // Check initial limit
        let check_request = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/{}", client_id))
            .body(Body::empty())
            .unwrap();

        let check_response = app.clone().oneshot(check_request).await.unwrap();
        assert_eq!(check_response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(check_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(result["client_id"], client_id);
        assert_eq!(result["calls_remaining"], 100); // Default from create_test_settings()

        // Make a rate limit request
        let limit_request = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/{}", client_id))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let limit_response = app.clone().oneshot(limit_request).await.unwrap();
        assert_eq!(limit_response.status(), StatusCode::OK);

        // Verify the limit was consumed
        let check_request2 = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/{}", client_id))
            .body(Body::empty())
            .unwrap();

        let check_response2 = app.clone().oneshot(check_request2).await.unwrap();
        let body_bytes2 = axum::body::to_bytes(check_response2.into_body(), usize::MAX)
            .await
            .unwrap();
        let result2: serde_json::Value = serde_json::from_slice(&body_bytes2).unwrap();

        assert_eq!(result2["calls_remaining"], 99);
    }

    #[tokio::test]
    async fn test_default_and_custom_rules_coexist() {
        let (_node, app) = setup_test_node().await;

        // Create a custom rule
        let rule_body = json!({
            "name": "custom-rule",
            "settings": {
                "rate_limit_max_calls_allowed": 5,
                "rate_limit_interval_seconds": 60
            }
        });

        let create_request = Request::builder()
            .method(Method::POST)
            .uri("/rl-config")
            .header("content-type", "application/json")
            .body(Body::from(rule_body.to_string()))
            .unwrap();

        let _create_response = app.clone().oneshot(create_request).await.unwrap();

        let test_key = "coexistence-user";

        // Use default rate limiting
        let default_check = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let default_response = app.clone().oneshot(default_check).await.unwrap();
        let default_bytes = axum::body::to_bytes(default_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let default_result: serde_json::Value = serde_json::from_slice(&default_bytes).unwrap();

        assert_eq!(default_result["calls_remaining"], 100);

        // Use custom rule
        let custom_check = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/custom-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let custom_response = app.clone().oneshot(custom_check).await.unwrap();
        let custom_bytes = axum::body::to_bytes(custom_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let custom_result: serde_json::Value = serde_json::from_slice(&custom_bytes).unwrap();

        assert_eq!(custom_result["calls_remaining"], 5);

        // Consuming tokens in one should not affect the other
        let default_limit = Request::builder()
            .method(Method::POST)
            .uri(&format!("/rl/{}", test_key))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let _default_limit_response = app.clone().oneshot(default_limit).await.unwrap();

        // Custom rule should still have all its tokens
        let custom_check2 = Request::builder()
            .method(Method::GET)
            .uri(&format!("/rl-check/custom-rule/{}", test_key))
            .body(Body::empty())
            .unwrap();

        let custom_response2 = app.clone().oneshot(custom_check2).await.unwrap();
        let custom_bytes2 = axum::body::to_bytes(custom_response2.into_body(), usize::MAX)
            .await
            .unwrap();
        let custom_result2: serde_json::Value = serde_json::from_slice(&custom_bytes2).unwrap();

        assert_eq!(custom_result2["calls_remaining"], 5);
    }
}
