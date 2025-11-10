use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tracing::info;

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket;
use crate::node::{CheckCallsResponse, Node, NodeId};
use crate::settings::{NamedRateLimitRule, RateLimitConfig, RateLimitSettings, Settings};

/// Standalone rate limiter node
#[derive(Clone, Debug)]
pub struct SingleNode {
    pub node_id: NodeId,
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
    pub rate_limit_config: Arc<RwLock<RateLimitConfig>>,
    pub named_rate_limiters:
        Arc<RwLock<HashMap<String, Arc<Mutex<token_bucket::TokenBucketLimiter>>>>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn new(node_id: NodeId, settings: Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let listen_api = format!("{}:{}", settings.listen_address, settings.listen_port_api);
        info!(
            "[Node<{}>] Starting at {} in single-node mode",
            node_id, listen_api
        );
        let rate_limiter: token_bucket::TokenBucketLimiter =
            token_bucket::TokenBucketLimiter::new(node_id, settings.rate_limit_settings());
        let rate_limit_config = RateLimitConfig::new(settings.rate_limit_settings());
        Ok(Self {
            node_id,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(RwLock::new(rate_limit_config)),
            named_rate_limiters: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        local_check_limit(client_id, self.rate_limiter.clone()).await
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        local_rate_limit(client_id, self.rate_limiter.clone()).await
    }

    async fn expire_keys(&self) -> Result<()> {
        let mut rate_limiter = self.rate_limiter.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
        })?;
        rate_limiter.expire_keys();
        Ok(())
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
        let limiter = token_bucket::TokenBucketLimiter::new(self.node_id, settings);

        let mut limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        limiters.insert(rule_name, Arc::new(Mutex::new(limiter)));

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
        local_rate_limit_with_settings(key, rate_limiter, &settings).await
    }

    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
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

        local_check_limit(key, rate_limiter).await
    }
}

pub async fn local_check_limit(
    client_id: String,
    rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
) -> Result<CheckCallsResponse> {
    match rate_limiter.lock() {
        Err(e) => {
            tracing::error!("Failed to acquire rate_limiter lock: {}", e);
            Err(crate::error::ColibriError::Concurrency(
                "Failed to acquire rate_limiter lock".to_string(),
            ))
        }
        Ok(rate_limiter) => {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(client_id.as_str());
            Ok(CheckCallsResponse {
                client_id,
                calls_remaining,
            })
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
                    client_id,
                    calls_remaining,
                }))
            } else {
                Ok(None)
            }
        }
    }
}
