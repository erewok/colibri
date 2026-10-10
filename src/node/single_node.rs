use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tracing::info;

use crate::error::{ColibriError, Result};
use crate::limiters::{rules, token_bucket};
use crate::node::{Node, NodeName, messages::CheckCallsResponse};
use crate::settings::Settings;

/// Standalone rate limiter node
#[derive(Clone, Debug)]
pub struct SingleNode {
    pub node_name: NodeName,
    pub named_rate_limiters:
        Arc<RwLock<HashMap<rules::RuleName, Arc<Mutex<token_bucket::TokenBucketLimiter>>>>>,
}

impl SingleNode {
    /// Get a limiter by rule name, returning a cloned Arc
    fn get_limiter(
        &self,
        rule: &rules::RuleName,
    ) -> Result<Arc<Mutex<token_bucket::TokenBucketLimiter>>> {
        let limiters = self.named_rate_limiters.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        limiters
            .get(rule)
            .cloned()
            .ok_or_else(|| ColibriError::Api(format!("Limiter for '{}' not found", rule)))
    }
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

        let mut named_rules: HashMap<
            rules::RuleName,
            Arc<Mutex<token_bucket::TokenBucketLimiter>>,
        > = HashMap::new();
        named_rules.insert(
            rules::RuleName::default(),
            Arc::new(Mutex::new(token_bucket::TokenBucketLimiter::new(
                settings.rate_limit_settings(),
            ))),
        );

        Ok(Self {
            node_name,
            named_rate_limiters: Arc::new(RwLock::new(named_rules)),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        self.check_limit_custom(rules::RuleName::default(), client_id)
            .await
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        self.rate_limit_custom(rules::RuleName::default(), client_id)
            .await
    }

    async fn rate_limit_custom(
        &self,
        rule: rules::RuleName,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let rate_limiter = self.get_limiter(&rule)?;
        local_rate_limit(key, Some(rule), rate_limiter).await
    }

    async fn check_limit_custom(
        &self,
        rule: rules::RuleName,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let rate_limiter = self.get_limiter(&rule)?;
        local_check_limit(Some(rule), key, rate_limiter).await
    }

    async fn create_named_rule(&self, rule: rules::SerializableRule) -> Result<()> {
        let name_str = rule.name.as_str();

        if name_str.is_empty() {
            return Err(ColibriError::Api("Rule name cannot be empty".to_string()));
        }
        if name_str == rules::DEFAULT_RULE_NAME {
            return Err(ColibriError::Api(
                "Cannot create or modify the default rule".to_string(),
            ));
        }

        let mut limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;

        // If already exists, no-op
        if limiters.contains_key(&rule.name) {
            return Ok(());
        }

        let limiter = token_bucket::TokenBucketLimiter::new(rule.settings);
        limiters.insert(rule.name, Arc::new(Mutex::new(limiter)));

        Ok(())
    }

    async fn delete_named_rule(&self, rule_name: rules::RuleName) -> Result<()> {
        let mut limiters = self.named_rate_limiters.write().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        limiters.remove(&rule_name);
        Ok(())
    }

    async fn get_named_rule(
        &self,
        rule_name: rules::RuleName,
    ) -> Result<Option<rules::SerializableRule>> {
        let limiters = self.named_rate_limiters.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;

        match limiters.get(&rule_name) {
            Some(limiter_arc) => {
                let limiter = limiter_arc.lock().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
                })?;
                Ok(Some(rules::SerializableRule {
                    name: rule_name,
                    settings: limiter.get_settings().clone(),
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_named_rules(&self) -> Result<rules::RuleList> {
        let limiters = self.named_rate_limiters.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;

        let mut rules_list = Vec::new();
        for (rule_name, limiter_arc) in limiters.iter() {
            let limiter = limiter_arc.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
            })?;
            rules_list.push(rules::SerializableRule {
                name: rule_name.clone(),
                settings: limiter.get_settings().clone(),
            });
        }
        Ok(rules::RuleList(rules_list))
    }

    async fn expire_keys(&self) -> Result<()> {
        let limiters = self.named_rate_limiters.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
        })?;
        for rate_limiter in limiters.values() {
            let mut rl = rate_limiter.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
            })?;
            rl.expire_keys();
        }
        Ok(())
    }
}

pub async fn local_check_limit(
    rule_name: Option<rules::RuleName>,
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
    rule_name: Option<rules::RuleName>,
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
            // `None` means rate limited. `Some(0)` is an admitted request that
            // took the last token.
            let calls_left = rate_limiter.limit_calls_for_client(client_id.to_string());
            Ok(calls_left.map(|calls_remaining| CheckCallsResponse {
                client_id,
                calls_remaining,
                rule_name,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{self, RunMode};

    /// Regression (found by the demo scripts): the request that takes the last
    /// token is admitted with 0 remaining, not reported as rate limited.
    #[tokio::test]
    async fn test_last_token_is_admitted() {
        let mut settings = settings::tests::sample();
        settings.rate_limit_max_calls_allowed = 1;
        settings.run_mode = RunMode::Single;
        let node = SingleNode::new(settings).await.unwrap();
        let client_id = "last_token_client".to_string();

        let first = node.rate_limit(client_id.clone()).await.unwrap();
        assert_eq!(first.map(|r| r.calls_remaining), Some(0));
        assert!(node.rate_limit(client_id).await.unwrap().is_none());
    }
}
