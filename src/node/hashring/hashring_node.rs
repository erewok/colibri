use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket;
use crate::node::{
    generate_node_id_from_socket_addr,
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
    pub topology: HashMap<NodeId, SocketAddr>,
    pub node_id: NodeId,
    // replication_factor: ReplicationFactor,
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
}

#[async_trait]
impl Node for HashringNode {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self> {
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter: token_bucket::TokenBucketLimiter =
            token_bucket::TokenBucketLimiter::new(node_id, rl_settings);

        let topology: HashMap<NodeId, SocketAddr> = settings
            .transport_config()
            .topology
            .into_iter()
            .map(|socket_addr: std::net::SocketAddr| {
                (generate_node_id_from_socket_addr(&socket_addr), socket_addr)
            })
            .collect();

        Ok(Self {
            topology,
            node_id,
            // replication_factor: ReplicationFactor,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
        })
    }
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id.value() {
            local_check_limit(client_id, self.rate_limiter.clone()).await
        } else {
            info!("Requesting data from bucket {}", bucket);
            // Use bucket to select into the topology HashMap
            match self.topology.get(&NodeId::new(bucket)) {
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
                None => local_check_limit(client_id, self.rate_limiter.clone()).await,
            }
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        if bucket == self.node_id.value() {
            local_rate_limit(client_id, self.rate_limiter.clone()).await
        } else {
            // Use bucket to select into the topology HashMap
            // The problem right now is that if this is a 429, we want to send that back
            info!("Requesting data from bucket {}", bucket);
            match self.topology.get(&NodeId::new(bucket)) {
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
