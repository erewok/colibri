use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tracing::{event, Level};

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
        local_check_limit(client_id, self.rate_limiter.clone())
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        local_rate_limit(client_id, self.rate_limiter.clone())
    }

    fn expire_keys(&self) {
        match self.rate_limiter.write() {
            Ok(mut rate_limiter) => {
                rate_limiter.expire_keys();
            }
            Err(err) => {
                event!(
                    Level::ERROR,
                    message = "Failed expiring keys",
                    err = format!("{:?}", err)
                );
            }
        }
    }
}

pub fn local_check_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<CheckCallsResponse> {
    match rate_limiter.read() {
        Ok(rate_limiter) => {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(client_id.as_str());
            Ok(CheckCallsResponse {
                client_id,
                calls_remaining,
            })
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed checking limit",
                err = format!("{:?}", err)
            );
            Err(crate::concurrency_error!("Failed to access rate_limiter"))
        }
    }
}

pub fn local_rate_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<Option<CheckCallsResponse>> {
    match rate_limiter.write() {
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
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed applying rate limit",
                err = format!("{:?}", err)
            );
            Err(crate::concurrency_error!("Failed to access rate_limiter"))
        }
    }
}
