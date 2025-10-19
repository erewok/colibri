use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::node::{CheckCallsResponse, Node};
use crate::rate_limit;

#[derive(Clone, Debug)]
pub struct SingleNode {
    pub rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        local_check_limit(client_id, self.rate_limiter.clone()).await
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        local_rate_limit(client_id, self.rate_limiter.clone()).await
    }

    async fn expire_keys(&self) {
        let mut rate_limiter = self.rate_limiter.write().await;
        rate_limiter.expire_keys();
    }
}

pub async fn local_check_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<CheckCallsResponse> {
    let rate_limiter = rate_limiter.read().await;
    let calls_remaining = rate_limiter.check_calls_remaining_for_client(client_id.as_str());
    Ok(CheckCallsResponse {
        client_id,
        calls_remaining,
    })
}

pub async fn local_rate_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<Option<CheckCallsResponse>> {
    let mut rate_limiter = rate_limiter.write().await;
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
