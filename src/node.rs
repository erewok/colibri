use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};

use crate::{cli, rate_limit, consistent_hashing};


pub enum NodeWrapper {
    Single(SingleNode),
    Multi(MultiNode)
}

impl NodeWrapper {
    pub fn new(settings: cli::Cli) -> Self {
        // A rate_limiter holds rate-limiting data in memory
        let rate_limiter = rate_limit::RateLimiter::new(settings.rate_limit_settings());
        if settings.topology.is_empty() {
            Self::Single(SingleNode { rate_limiter })
        } else {
           Self::Multi(
                MultiNode {
                    node_id: settings.node_id,
                    topology: settings.topology.iter().enumerate().map(|(node_id, host)| (node_id as u32, host.to_string())).collect(),
                    rate_limiter: rate_limit::RateLimiter::new(settings.rate_limit_settings()),
                },
            )
        }
    }

    pub fn expire_keys(&mut self) {
        match self {
            Self::Single(node) => node.rate_limiter.expire_keys(),
            Self::Multi(node) => node.rate_limiter.expire_keys()
        }
    }

    pub async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error> {
        match self {
            Self::Single(node) => node.check_limit(client_id).await,
            Self::Multi(node) => node.check_limit(client_id).await,
        }
    }
    pub async fn rate_limit(&mut self, client_id: String) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
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
    fn get_rate_limiter_mut(&mut self) -> &mut rate_limit::RateLimiter;
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error>;
    async fn rate_limit(&mut self, client_id: String) -> Result<Option<CheckCallsResponse>, anyhow::Error>;
}

#[derive(Clone, Debug)]
pub struct SingleNode {
    rate_limiter: rate_limit::RateLimiter
}

#[async_trait]
impl Node for SingleNode {

    fn get_rate_limiter_mut(&mut self) -> &mut rate_limit::RateLimiter {
        &mut self.rate_limiter
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error> {
        let calls_remaining = self.rate_limiter.check_calls_remaining_for_client(client_id.as_str());
        Ok(CheckCallsResponse {
            client_id: client_id,
            calls_remaining,
        })
    }

    async fn rate_limit(&mut self, client_id: String) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
        let calls_left = self.rate_limiter.limit_calls_for_client(client_id.to_string());
        if let Some(calls_remaining) = calls_left {
            Ok(Some(CheckCallsResponse {
                client_id: client_id,
                calls_remaining,
            }))
        } else {
            Ok(None)
        }
    }
}

pub enum ReplicationFactor {
    One = 1,
    Two = 2,
    Three = 3
}

#[derive(Clone, Debug)]
pub struct MultiNode {
    topology: HashMap<u32, String>,
    node_id: u32,
    // replication_factor: ReplicationFactor,
    rate_limiter: rate_limit::RateLimiter
}

#[async_trait]
impl Node for MultiNode {

    fn get_rate_limiter_mut(&mut self) -> &mut rate_limit::RateLimiter {
        &mut self.rate_limiter
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse, anyhow::Error> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket = consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id {
            let calls_remaining = self.rate_limiter.check_calls_remaining_for_client(client_id.as_str());
            Ok(CheckCallsResponse {
                client_id,
                calls_remaining,
            })
        } else {
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
                }, 
                // fallback to self?
                None => {
                    let calls_remaining = self.rate_limiter.check_calls_remaining_for_client(client_id.as_str());
                    Ok(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                    })
                }
            }
        }
    }

    async fn rate_limit(&mut self, client_id: String) -> Result<Option<CheckCallsResponse>, anyhow::Error> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket = consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id {
            let calls_left = self.rate_limiter.limit_calls_for_client(client_id.to_string());
            if let Some(calls_remaining) = calls_left {
                Ok(Some(CheckCallsResponse {
                    client_id,
                    calls_remaining,
                }))
            } else {
                Ok(None)
            }
        } else {
            // Use bucket to select into the topology HashMap
            // The problem right now is that if this is a 429, we want to send that back
            match self.topology.get(&bucket) {
                Some(host) => {
                    let url = format!("{}/rl/{}", host, client_id);
                    reqwest::Client::new()
                        .post(url)
                        .send()
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?
                        .json() //  won't work for 429s
                        .await
                        .map_err(|e| anyhow::anyhow!(e))
                }, 
                // fallback to self?
                None => {
                    let calls_left = self.rate_limiter.limit_calls_for_client(client_id.to_string());
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
    }
}