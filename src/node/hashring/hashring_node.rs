use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket;
use crate::node::{
    hashring::consistent_hashing,
    single_node::{local_check_limit, local_rate_limit},
    CheckCallsResponse, Node, NodeId,
};
use crate::settings;

pub enum ReplicationFactor {
    One = 1,
    Two = 2,
    Three = 3,
}

#[derive(Clone, Debug)]
pub struct HashringNode {
    pub node_id: NodeId,
    bucket: u32,
    number_of_buckets: u32,
    listen_api: String,
    pub topology: HashMap<u32, String>,
    // replication_factor: ReplicationFactor,
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
}

#[async_trait]
impl Node for HashringNode {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self> {
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter: token_bucket::TokenBucketLimiter =
            token_bucket::TokenBucketLimiter::new(node_id, rl_settings);

        let listen_api = format!("{}:{}", settings.listen_address, settings.listen_port_api);

        // get the bucket count to use in consistent hashing calls (assuming self is present in topology)
        let number_of_buckets = settings.topology.len().try_into()?;

        // Use consistent hashing to assign buckets to nodes
        // Every node must use the same identifiers for other nodes.
        // This node must be a listed member of the topology.
        let topology = settings
            .topology
            .iter()
            .map(|host| {
                (
                    consistent_hashing::jump_consistent_hash(host.as_str(), number_of_buckets),
                    host.to_string(),
                )
            })
            .collect::<HashMap<u32, String>>();

        // find the bucket ID that consistently maps to *this* node's listen address
        // fail early if this node's listen address is not in the topology
        let bucket: u32 = topology
            .iter()
            .find_map(|(bucket, host)| {
                if host.contains(&listen_api) {
                    Some(*bucket)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                error!(
                    "[Node<{}>] Critical failure: Listen address for this node {} not found in topology! {:?}",
                    node_id, listen_api, settings.topology
                );
                ColibriError::Config(format!(
                    "Listen address {} not found in topology",
                    listen_api
                ))
            })?;

        info!(
            "[Node<{}>] Hashring node starting at {} with bucket {} out of {} buckets and topology {:?}",
            node_id, listen_api, bucket, number_of_buckets, topology
        );
        Ok(Self {
            node_id,
            bucket,
            number_of_buckets,
            listen_api,
            topology,
            // replication_factor: ReplicationFactor,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
        })
    }
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), self.number_of_buckets);
        if bucket == self.bucket {
            local_check_limit(client_id, self.rate_limiter.clone()).await
        } else {
            info!(
                "[bucket {}] Requesting data from bucket {} {:?}",
                self.bucket, bucket, self.topology
            );
            // Use bucket to select into the topology HashMap
            match self.topology.get(&bucket) {
                Some(host) => {
                    if host.contains(&self.listen_api) {
                        // shouldn't happen, but just in case
                        warn!("Host {} from bucket {} matches our own listen API (but not self.bucket? {}), using local check_limit", host, bucket, self.bucket);
                        return local_check_limit(client_id, self.rate_limiter.clone()).await;
                    }
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
                None => local_check_limit(client_id, self.rate_limiter.clone()).await,
            }
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.bucket {
            local_rate_limit(client_id, self.rate_limiter.clone()).await
        } else {
            // Use bucket to select into the topology HashMap
            // The problem right now is that if this is a 429, we want to send that back
            info!("Requesting data from bucket {}", bucket);
            match self.topology.get(&bucket) {
                Some(host) => {
                    if host.contains(&self.listen_api) {
                        // shouldn't happen, but just in case
                        warn!("Host {} from bucket {} matches our own listen API (but not self.bucket? {}), using local check_limit", host, bucket, self.bucket);
                        return local_rate_limit(client_id, self.rate_limiter.clone()).await;
                    }
                    let url = format!("{}/rl/{}", host, client_id);
                    info!("Sending request to {}", url);
                    let resp = reqwest::Client::new()
                        .post(url)
                        .header("content-type", "application/json")
                        .body("{}")
                        .send()
                        .await?;

                    let status = resp.status().as_u16();
                    info!("Received status code {}", status);
                    if status == 429 {
                        Ok(None)
                    } else {
                        resp.json().await.map_err(Into::into)
                    }
                }
                // fallback to self?
                None => local_rate_limit(client_id, self.rate_limiter.clone()).await,
            }
        }
    }

    async fn expire_keys(&self) -> Result<()> {
        let mut rate_limiter = self.rate_limiter.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
        })?;
        rate_limiter.expire_keys();
        Ok(())
    }
}
