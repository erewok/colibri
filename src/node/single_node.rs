use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tracing::info;

use crate::error::{ColibriError, Result};
use crate::limiters::{token_bucket, NamedRateLimitRule, RateLimitConfig, DEFAULT_RULE_NAME};
use crate::node::{messages::CheckCallsResponse, Node, NodeName};
use crate::settings::{RateLimitSettings, Settings};

/// Standalone rate limiter node
#[derive(Clone, Debug)]
pub struct SingleNode {
    pub node_name: NodeName,
    pub rate_limit_config: Arc<RwLock<RateLimitConfig>>,
    pub named_rate_limiters:
        Arc<RwLock<HashMap<String, Arc<Mutex<token_bucket::TokenBucketLimiter>>>>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn new(settings: Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let node_name: NodeName = settings.server_name.clone().into();
        let listen_api = format!(
            "{}:{}",
            settings.client_listen_address, settings.client_listen_port
        );
        info!(
            "[Node<{}>] Starting at {} in single-node mode",
            node_name, listen_api
        );
        let rate_limit_config = RateLimitConfig::new(settings.rate_limit_settings());

        let mut named_rules: HashMap<String, Arc<Mutex<token_bucket::TokenBucketLimiter>>> =
            HashMap::new();
        named_rules.insert(
            DEFAULT_RULE_NAME.to_string(),
            Arc::new(Mutex::new(token_bucket::TokenBucketLimiter::new(
                settings.rate_limit_settings(),
            ))),
        );

        Ok(Self {
            node_name,
            rate_limit_config: Arc::new(RwLock::new(rate_limit_config)),
            named_rate_limiters: Arc::new(RwLock::new(named_rules)),
        })
    }
    async fn check_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        self.check_limit_custom(DEFAULT_RULE_NAME.to_string(), client_id)
            .await
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        self.rate_limit_custom(DEFAULT_RULE_NAME.to_string(), client_id)
            .await
    }

    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        // Get the settings for this rule
        let settings = {
            let config = self.rate_limit_config.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            match config.get_named_rule_settings(&rule_name) {
                Some(settings) => settings.clone(),
                None => return Err(ColibriError::Api(format!("Rule '{}' not found", rule_name))),
            }
        };

        // Get the limiter for this rule
        let rate_limiter = {
            let limiters = self.named_rate_limiters.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            match limiters.get(&rule_name) {
                Some(limiter) => limiter.clone(),
                None => {
                    return Err(ColibriError::Api(format!(
                        "Limiter for rule '{}' not found",
                        rule_name
                    )))
                }
            }
        };

        // Use the custom limiter with custom settings
        local_rate_limit_with_settings(key, rule_name, rate_limiter, &settings).await
    }

    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        // Get the limiter for this rule
        let rate_limiter = {
            let limiters = self.named_rate_limiters.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            match limiters.get(&rule_name) {
                Some(limiter) => limiter.clone(),
                None => {
                    return Err(ColibriError::Api(format!(
                        "Limiter for rule '{}' not found",
                        rule_name
                    )))
                }
            }
        };

        local_check_limit(Some(rule_name), key, rate_limiter).await
    }

    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        // check if already exists
        {
            let config = self.rate_limit_config.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            if config.get_named_rule_settings(&rule_name).is_some() {
                return Ok(());
            }
        }

        // Add the rule to configuration
        {
            let mut config = self.rate_limit_config.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            let rule = NamedRateLimitRule {
                name: rule_name.clone(),
                settings: settings.clone(),
            };
            config.add_named_rule(&rule);
        }

        // Create a new rate limiter for this rule
        let limiter = token_bucket::TokenBucketLimiter::new(settings);

        let mut limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        limiters.insert(rule_name.clone(), Arc::new(Mutex::new(limiter)));

        Ok(())
    }

    async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        // Remove from configuration
        {
            let mut config = self.rate_limit_config.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            config.remove_named_rule(&rule_name);
        }

        // Remove the limiter
        let mut limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        limiters.remove(&rule_name);

        Ok(())
    }

    async fn get_named_rule(&self, rule_name: String) -> Result<Option<NamedRateLimitRule>> {
        self.rate_limit_config
            .read()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e)))
            .map(|rlconf| {
                rlconf
                    .get_named_rule_settings(&rule_name)
                    .cloned()
                    .map(|rl_settings| NamedRateLimitRule {
                        name: rule_name,
                        settings: rl_settings,
                    })
            })
    }

    async fn list_named_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let config = self.rate_limit_config.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
        })?;
        Ok(config.list_named_rules())
    }

    async fn expire_keys(&self) -> Result<()> {
        let mut rate_limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
        })?;
        for (rule_name, rate_limiter) in rate_limiters.iter_mut() {
            match rate_limiter.lock() {
                Err(e) => {
                    tracing::error!("Failed to acquire rate_limiter lock: {}", e);
                    return Err(crate::error::ColibriError::Concurrency(
                        "Failed to acquire rate_limiter lock".to_string(),
                    ));
                }
                Ok(mut rl) => {
                    rl.expire_keys();
                    if rule_name == DEFAULT_RULE_NAME {
                        continue;
                    }
                    self.rate_limit_config
                        .write()
                        .map_err(|e| {
                            ColibriError::Concurrency(format!(
                                "Failed to acquire config lock: {}",
                                e
                            ))
                        })?
                        .remove_named_rule(rule_name);
                }
            }
        }
        Ok(())
    }
}

pub async fn local_check_limit(
    rule_name: Option<String>,
    client_id: String,
    rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
) -> Result<Option<CheckCallsResponse>> {
    match rate_limiter.lock() {
        Err(e) => {
            tracing::error!("Failed to acquire rate_limiter lock: {}", e);
            Err(crate::error::ColibriError::Concurrency(
                "Failed to acquire rate_limiter lock".to_string(),
            ))
        }
        Ok(rate_limiter) => {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(client_id.as_str());
            Ok(Some(CheckCallsResponse {
                client_id,
                calls_remaining,
                rule_name,
            }))
        }
    }
}

pub async fn local_rate_limit(
    client_id: String,
    rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
) -> Result<Option<CheckCallsResponse>> {
    match rate_limiter.lock() {
        Err(e) => {
            tracing::error!("Failed to acquire rate_limiter lock: {}", e);
            Err(crate::error::ColibriError::Concurrency(
                "Failed to acquire rate_limiter lock".to_string(),
            ))
        }
        Ok(mut rate_limiter) => {
            let calls_left = rate_limiter.limit_calls_for_client(client_id.to_string());
            if let Some(calls_remaining) = calls_left {
                if calls_remaining == 0 {
                    Ok(None)
                } else {
                    Ok(Some(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                        rule_name: None,
                    }))
                }
            } else {
                Ok(None)
            }
        }
    }
}

pub async fn local_rate_limit_with_settings(
    client_id: String,
    rule_name: String,
    rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
    settings: &RateLimitSettings,
) -> Result<Option<CheckCallsResponse>> {
    match rate_limiter.lock() {
        Err(e) => {
            tracing::error!("Failed to acquire rate_limiter lock: {}", e);
            Err(crate::error::ColibriError::Concurrency(
                "Failed to acquire rate_limiter lock".to_string(),
            ))
        }
        Ok(mut rate_limiter) => {
            let calls_left =
                rate_limiter.limit_calls_for_client_with_settings(client_id.to_string(), settings);
            if let Some(calls_remaining) = calls_left {
                Ok(Some(CheckCallsResponse {
                    calls_remaining,
                    client_id,
                    rule_name: Some(rule_name),
                }))
            } else {
                Ok(None)
            }
        }
    }
}
