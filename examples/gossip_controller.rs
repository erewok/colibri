//! GossipController Example
//! This example demonstrates GossipController functionality.
//! We drive the GossipController manually primarily using its handle_command method.
use std::collections::HashSet;

use tokio::sync::oneshot;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use colibri::error::Result;
use colibri::node::gossip::{GossipController, GossipCommand};
use colibri::settings::{Settings, RateLimitSettings, RunMode};


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,colibri=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting GossipController manual functionality test");

    // Create test settings
    let settings = create_test_settings();

    // Create GossipController
    let controller = GossipController::new(settings).await?;
    info!("Created GossipController: {:?}", controller);

    // Test 1: ExpireKeys command
    info!("\n=== Test 1: ExpireKeys Command ===");
    test_expire_keys(&controller).await?;

    // Test 2: Rate limiting commands
    info!("\n=== Test 2: Rate Limiting Commands ===");
    test_rate_limiting(&controller).await?;

    // Test 3: Named rule management
    info!("\n=== Test 3: Named Rule Management ===");
    test_named_rules(&controller).await?;

    // Test 4: Custom rate limiting with named rules
    info!("\n=== Test 4: Custom Rate Limiting ===");
    test_custom_rate_limiting(&controller).await?;

    info!("\nGossipController functional tests completed successfully!");
    Ok(())
}

fn create_test_settings() -> Settings {
    Settings {
        listen_address: "127.0.0.1".to_string(),
        listen_port_api: 8410,
        listen_port_tcp: 8411,
        listen_port_udp: 8412,
        rate_limit_max_calls_allowed: 10,
        rate_limit_interval_seconds: 60,
        run_mode: RunMode::Gossip,
        gossip_interval_ms: 1000,
        gossip_fanout: 3,
        topology: HashSet::new(), // Empty topology for this example
        failure_timeout_secs: 30,
    }
}

async fn test_expire_keys(controller: &GossipController) -> Result<()> {
    info!("Testing ExpireKeys command...");

    let cmd = GossipCommand::ExpireKeys;
    controller.handle_command(cmd).await?;

    info!("✓ ExpireKeys command executed successfully");
    Ok(())
}

async fn test_rate_limiting(controller: &GossipController) -> Result<()> {
    info!("Testing rate limiting commands...");

    let client_id = "test-client-1".to_string();

    // Test CheckLimit
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::CheckLimit {
        client_id: client_id.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(check_response) => {
                info!("✓ CheckLimit response: client '{}' has {} calls remaining",
                      check_response.client_id, check_response.calls_remaining);
            }
            Err(e) => {
                error!("CheckLimit failed: {}", e);
                return Err(e);
            }
        }
    } else {
        error!("Failed to receive CheckLimit response");
    }

    // Test RateLimit (consume a token)
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::RateLimit {
        client_id: client_id.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(Some(rate_response)) => {
                info!("✓ RateLimit consumed token: client '{}' has {} calls remaining",
                      rate_response.client_id, rate_response.calls_remaining);
            }
            Ok(None) => {
                warn!("RateLimit returned None - rate limit may be exceeded");
            }
            Err(e) => {
                error!("RateLimit failed: {}", e);
                return Err(e);
            }
        }
    } else {
        error!("Failed to receive RateLimit response");
    }

    // Check limit again after consuming
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::CheckLimit {
        client_id: client_id.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(check_response) => {
                info!("✓ CheckLimit after consumption: client '{}' has {} calls remaining",
                      check_response.client_id, check_response.calls_remaining);
            }
            Err(e) => {
                error!("Second CheckLimit failed: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn test_named_rules(controller: &GossipController) -> Result<()> {
    info!("Testing named rule management...");

    let rule_name = "test-rule".to_string();
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 5,
        rate_limit_interval_seconds: 30,
    };

    // Create a named rule
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::CreateNamedRule {
        rule_name: rule_name.clone(),
        settings: settings.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(()) => {
                info!("✓ Created named rule '{}'", rule_name);
            }
            Err(e) => {
                error!("Failed to create named rule: {}", e);
                return Err(e);
            }
        }
    }

    // List named rules
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::ListNamedRules { resp_chan: tx };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(rules) => {
                info!("✓ Listed {} named rules:", rules.len());
                for rule in &rules {
                    info!("  - Rule '{}': {} calls per {} seconds",
                          rule.name,
                          rule.settings.rate_limit_max_calls_allowed,
                          rule.settings.rate_limit_interval_seconds);
                }
            }
            Err(e) => {
                error!("Failed to list named rules: {}", e);
                return Err(e);
            }
        }
    }

    // Get specific named rule
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::GetNamedRule {
        rule_name: rule_name.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(Some(rule)) => {
                info!("✓ Retrieved named rule '{}': {} calls per {} seconds",
                      rule.name,
                      rule.settings.rate_limit_max_calls_allowed,
                      rule.settings.rate_limit_interval_seconds);
            }
            Ok(None) => {
                warn!("Named rule '{}' not found", rule_name);
            }
            Err(e) => {
                error!("Failed to get named rule: {}", e);
                return Err(e);
            }
        }
    }

    // Delete the named rule
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::DeleteNamedRule {
        rule_name: rule_name.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(()) => {
                info!("✓ Deleted named rule '{}'", rule_name);
            }
            Err(e) => {
                error!("Failed to delete named rule: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn test_custom_rate_limiting(controller: &GossipController) -> Result<()> {
    info!("Testing custom rate limiting with named rules...");

    let rule_name = "custom-rule".to_string();
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 3,
        rate_limit_interval_seconds: 60,
    };

    // Create custom rule first
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::CreateNamedRule {
        rule_name: rule_name.clone(),
        settings,
        resp_chan: tx,
    };
    controller.handle_command(cmd).await?;
    let _ = rx.await;

    let key = "custom-key-1".to_string();

    // Check custom limit
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::CheckLimitCustom {
        rule_name: rule_name.clone(),
        key: key.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(check_response) => {
                info!("✓ CheckLimitCustom for key '{}' with rule '{}': {} calls remaining",
                      key, rule_name, check_response.calls_remaining);
            }
            Err(e) => {
                error!("CheckLimitCustom failed: {}", e);
                return Err(e);
            }
        }
    }

    // Apply custom rate limit
    let (tx, rx) = oneshot::channel();
    let cmd = GossipCommand::RateLimitCustom {
        rule_name: rule_name.clone(),
        key: key.clone(),
        resp_chan: tx,
    };

    controller.handle_command(cmd).await?;

    if let Ok(response) = rx.await {
        match response {
            Ok(Some(rate_response)) => {
                info!("✓ RateLimitCustom consumed token for key '{}': {} calls remaining",
                      key, rate_response.calls_remaining);
            }
            Ok(None) => {
                warn!("RateLimitCustom returned None - rate limit may be exceeded");
            }
            Err(e) => {
                error!("RateLimitCustom failed: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}
