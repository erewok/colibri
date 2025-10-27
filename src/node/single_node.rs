use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket;
use crate::node::{CheckCallsResponse, Node, NodeId};
use crate::settings;

#[derive(Clone, Debug)]
pub struct SingleNode {
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn new(
        node_id: NodeId,
        settings: settings::Settings,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let rate_limiter: token_bucket::TokenBucketLimiter =
                token_bucket::TokenBucketLimiter::new(node_id, settings.rate_limit_settings());
        Ok(Self {
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
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
