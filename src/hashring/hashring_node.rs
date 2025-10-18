use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{event, info, Level};

use crate::error::Result;
use crate::hashring::consistent_hashing;
use crate::node::{CheckCallsResponse, Node};
use crate::rate_limit;
use crate::single_node::{local_check_limit, local_rate_limit};
use async_trait::async_trait;

pub enum ReplicationFactor {
    One = 1,
    Two = 2,
    Three = 3,
}

#[derive(Clone, Debug)]
pub struct HashringNode {
    pub topology: HashMap<u32, String>,
    pub node_id: u32,
    // replication_factor: ReplicationFactor,
    pub rate_limiter: Arc<RwLock<rate_limit::RateLimiter>>,
}

#[async_trait]
impl Node for HashringNode {
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
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
                        .get(url)
                        .send()
                        .await?
                        .json()
                        .await
                        .map_err(Into::into)
                }
                // fallback to self?
                None => local_check_limit(client_id, self.rate_limiter.clone()),
            }
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
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
                        .header("content-type", "application/json")
                        .body("{}")
                        .send()
                        .await?;
                    let status = resp.status().as_u16();
                    if status == 429 {
                        Ok(None)
                    } else {
                        resp.json().await.map_err(Into::into)
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
