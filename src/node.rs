use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{event, info, Level};

use crate::{cli, consistent_hashing, rate_limit};

#[derive(Clone, Debug)]
pub enum NodeWrapper {
    Single(SingleNode),
    Multi(MultiNode),
}

impl NodeWrapper {
    pub fn new(settings: cli::Cli) -> Self {
        // A rate_limiter holds rate-limiting data in memory
        let rate_limiter = Arc::new(RwLock::new(rate_limit::RateLimiter::new(
            settings.rate_limit_settings(),
        )));
        if settings.topology.is_empty() {
            info!("Starting in single-node mode");
            Self::Single(SingleNode { rate_limiter })
        } else {
            info!("Starting in multi-node mode");
            Self::Multi(MultiNode {
                node_id: settings.node_id,
                topology: settings
                    .topology
                    .iter()
                    .enumerate()
                    .map(|(node_id, host)| (node_id as u32, host.to_string()))
                    .collect(),
                rate_limiter,
            })
        }
    }

    pub fn expire_keys(&self) {
        match self {
            Self::Single(node) => node.expire_keys(),
            Self::Multi(node) => node.expire_keys(),
        }
    }

    pub async fn check_limit(
        &self,
        client_id: String,
    ) -> Result<CheckCallsResponse, anyhow::Error> {
        match self {
            Self::Single(node) => node.check_limit(client_id).await,
            Self::Multi(node) => node.check_limit(client_id).await,
        }
    }
    pub async fn rate_limit(
        &self,
        client_id: String,
    ) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
        match self {
            Self::Single(node) => node.rate_limit(client_id).await,
            Self::Multi(node) => node.rate_limit(client_id).await,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckCallsResponse {
    client_id: String,
    calls_remaining: u32,
}

#[async_trait]
pub trait Node {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error>;
    async fn rate_limit(
        &self,
        client_id: String,
    ) -> Result<Option<CheckCallsResponse>, anyhow::Error>;
    fn expire_keys(&self);
}

#[derive(Clone, Debug)]
pub struct SingleNode {
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
}

#[async_trait]
impl Node for SingleNode {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error> {
        local_check_limit(client_id, self.rate_limiter.clone())
    }

    async fn rate_limit(
        &self,
        client_id: String,
    ) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
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

pub enum ReplicationFactor {
    One = 1,
    Two = 2,
    Three = 3,
}

#[derive(Clone, Debug)]
pub struct MultiNode {
    topology: HashMap<u32, String>,
    node_id: u32,
    // replication_factor: ReplicationFactor,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
}

#[async_trait]
impl Node for MultiNode {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id {
            local_check_limit(client_id, self.rate_limiter.clone())
        } else {
            info!("Requesting data from bucket {}", bucket);
            // Use bucket to select into the topology HashMap
            match self.topology.get(&bucket) {
                Some(host) => {
                    let url = format!("{}/rl-check/{}", host, client_id);
                    reqwest::Client::new()
                        .post(url)
                        .send()
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?
                        .json()
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                }
                // fallback to self?
                None => local_check_limit(client_id, self.rate_limiter.clone()),
            }
        }
    }

    async fn rate_limit(
        &self,
        client_id: String,
    ) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id {
            local_rate_limit(client_id, self.rate_limiter.clone())
        } else {
            // Use bucket to select into the topology HashMap
            // The problem right now is that if this is a 429, we want to send that back
            info!("Requesting data from bucket {}", bucket);
            match self.topology.get(&bucket) {
                Some(host) => {
                    let url = format!("{}/rl/{}", host, client_id);
                    let resp = reqwest::Client::new()
                        .post(url)
                        .send()
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    let status = resp.status().as_u16();
                    if status == 429 {
                        Ok(None)
                    } else {
                        resp.json() //  won't work for 429s
                            .await
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                }
                // fallback to self?
                None => local_rate_limit(client_id, self.rate_limiter.clone()),
            }
        }
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

fn local_check_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<CheckCallsResponse, anyhow::Error> {
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
            Err(anyhow::anyhow!("Failed to access rate_limiter"))
        }
    }
}

fn local_rate_limit(
    client_id: String,
    rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
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
            Err(anyhow::anyhow!("Failed to access rate_limiter"))
        }
    }
}
