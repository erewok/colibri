use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::api::paths;
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
    // Connection-pooling clients reuse TCP connections to each node
    pub topology: HashMap<u32, (Url, reqwest::Client)>,
    // replication_factor: ReplicationFactor,
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
    pub rate_limit_config: Arc<RwLock<settings::RateLimitConfig>>,
    pub named_rate_limiters: Arc<RwLock<HashMap<String, Arc<Mutex<token_bucket::TokenBucketLimiter>>>>>,
}

#[async_trait]
impl Node for HashringNode {
    async fn new(node_id: NodeId, settings: settings::Settings) -> Result<Self> {
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter: token_bucket::TokenBucketLimiter =
            token_bucket::TokenBucketLimiter::new(node_id, rl_settings);

        let listen_api = if settings.listen_address.contains("http") {
            format!("{}:{}", settings.listen_address, settings.listen_port_api)
        } else {
            format!("http://{}:{}", settings.listen_address, settings.listen_port_api)
        };

        // get the bucket count to use in consistent hashing calls (assuming self is present in topology)
        let number_of_buckets = settings.topology.len().try_into()?;

        // Use consistent hashing to assign buckets to nodes
        // Every node must use the same identifiers for other nodes.
        // This node must be a listed member of the topology.
        let topology = settings
            .topology
            .iter()
            .filter_map(|host| {
                let bucketnum = consistent_hashing::jump_consistent_hash(host.as_str(), number_of_buckets);
                let host = if !host.contains("http") {
                    format!("http://{}", host)
                } else {
                    host.to_string()
                };
                let url = Url::parse(host.as_str()).ok()?;
                let client = reqwest::Client::builder()
                    .pool_idle_timeout(std::time::Duration::from_secs(90))
                    .build()
                    .ok()?;
                Some((
                    bucketnum,
                    (url, client),
                ))
            })
            .collect::<HashMap<u32, (Url, reqwest::Client)>>();

        // find the bucket ID that consistently maps to *this* node's listen address
        // fail early if this node's listen address is not in the topology
        let bucket: u32 = topology
            .iter()
            .find_map(|(bucket, host)| {
                debug!("Checking host {:?} against listen_api {}", host.0.as_str(), listen_api);
                if host.0.as_str().contains(&listen_api) {
                    Some(*bucket)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                error!(
                    "[Node<{}>] Critical failure: Listen address for this node '{}' not found in topology! {:?}",
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
        let rate_limit_config = settings::RateLimitConfig::new(settings.rate_limit_settings());

        Ok(Self {
            node_id,
            bucket,
            number_of_buckets,
            listen_api,
            topology,
            // replication_factor: ReplicationFactor,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(RwLock::new(rate_limit_config)),
            named_rate_limiters: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), self.number_of_buckets);

        // Use bucket to select into the topology HashMap
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                info!("[bucket {}] Requesting data from bucket {} at {}", self.bucket, bucket, host);
                let path = paths::drop_leading_slash(paths::default_rate_limits::CHECK).replace("{client_id}", &client_id);
                let url = format!("{}{}", host, path);
                client
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

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let number_of_buckets = self.topology.len().try_into()?;
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::default_rate_limits::LIMIT).replace("{client_id}", &client_id);
                let url = format!("{}{}", host, path);
                info!("[bucket {}] Requesting data from bucket {} at {}", self.bucket, bucket, url);
                let resp = client
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
            // fallback to self
            None => local_rate_limit(client_id, self.rate_limiter.clone()).await,
        }
    }

    async fn expire_keys(&self) -> Result<()> {
        let mut rate_limiter = self.rate_limiter.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
        })?;
        rate_limiter.expire_keys();
        Ok(())
    }

    async fn create_named_rule(&self, rule_name: String, settings: settings::RateLimitSettings) -> Result<()> {
        // Add the rule to our local configuration
        {
            let mut config = self.rate_limit_config.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            config.add_named_rule(rule_name.clone(), settings.clone());
        }

        // Create a new rate limiter for this rule
        let rate_limiter = token_bucket::TokenBucketLimiter::new(self.node_id, settings.clone());
        {
            let mut limiters = self.named_rate_limiters.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            limiters.insert(rule_name.clone(), Arc::new(Mutex::new(rate_limiter)));
        }

        // Propagate the rule to all other nodes in the topology
        for (_, (node_address, _client)) in &self.topology {
            if !self.topology_address_is_self(node_address) {
                // Send rule to remote node
                let path = paths::drop_leading_slash(paths::custom::RULE_CONFIG).replace("{rule_name}", &rule_name);
                let url = format!("{}{}", node_address, path);
                let client = reqwest::Client::new();
                let response = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .json(&settings)
                    .send()
                    .await;

                if let Err(e) = response {
                    warn!("Failed to sync rule {} to node {}: {}", rule_name, node_address, e);
                    // Continue with other nodes rather than failing completely
                }
            }
        }
        Ok(())
    }

    async fn delete_named_rule(&self, rule_name: String) -> Result<()> {
        // Remove from local configuration
        {
            let mut config = self.rate_limit_config.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            config.remove_named_rule(&rule_name);
        }

        // Remove the rate limiter
        {
            let mut limiters = self.named_rate_limiters.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            limiters.remove(&rule_name);
        }

        // Propagate the deletion to all other nodes in the topology
        for (_, (node_address, client)) in &self.topology {
            if !self.topology_address_is_self(node_address) {
                // Delete rule from remote node
                let path = paths::drop_leading_slash(paths::custom::RULE_CONFIG).replace("{rule_name}", &rule_name);
                let url = format!("{}{}", node_address, path);
                let response = client.delete(&url).send().await;

                if let Err(e) = response {
                    warn!("Failed to delete rule {} from node {}: {}", rule_name, node_address, e);
                    // Continue with other nodes rather than failing completely
                }
            }
        }
        Ok(())
    }

    async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        let config = self.rate_limit_config.read().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
        })?;
        Ok(config.list_named_rules())
    }

    async fn rate_limit_custom(&self, rule_name: String, key: String) -> Result<Option<CheckCallsResponse>> {
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::custom::LIMIT).replace("{rule_name}", &rule_name).replace("{key}", &key);
                let url = format!("{}{}", host, path);
                let response = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body("{}")
                    .send()
                    .await?;
                let status = response.status().as_u16();
                if status == 429 {
                    Ok(None)
                } else if status == 200 {
                    let resp: CheckCallsResponse = response.json().await?;
                    Ok(Some(resp))
                } else {
                    Err(ColibriError::Api(format!("Remote node returned status {}", status)))
                }
            }
            None => {
                // Fallback to local handling
                warn!("No node found for bucket {}, handling locally", bucket);
                self.local_rate_limit_custom(rule_name, key).await
            }
        }
    }

    async fn check_limit_custom(&self, rule_name: String, key: String) -> Result<CheckCallsResponse> {
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::custom::CHECK).replace("{rule_name}", &rule_name).replace("{key}", &key);
                let url = format!("{}{}", host, path);
                let response = client.get(&url).send().await?;
                let resp: CheckCallsResponse = response.json().await?;
                Ok(resp)
            }
            None => {
                // Handle locally
                self.local_check_limit_custom(rule_name, key).await
            }
        }
    }
}

impl HashringNode {
    fn topology_address_is_self(&self, address: &Url) -> bool {
        // confirm that the address matches our own listen_api: we DO NOT WANT TO SEND REQUESTS TO SELF!
        address.as_str().contains(&self.listen_api)
    }

    fn get_topology_address_for_bucket(&self, bucket: u32) -> Option<(&Url, &reqwest::Client)> {
        // Pull the address for the given bucket from the topology
        // If None -> local handling!
        if bucket == self.bucket {
            // Handle locally
            return None;
        }
        match self.topology.get(&bucket) {
            Some((node_address, client)) => if !self.topology_address_is_self(node_address) {
                Some((node_address, client))
            } else {
                None
            },
            None => None,
        }
    }

    async fn local_rate_limit_custom(&self, rule_name: String, key: String) -> Result<Option<CheckCallsResponse>> {
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
                None => return Err(ColibriError::Api(format!("Limiter for rule '{}' not found", rule_name))),
            }
        };

        // Use the custom limiter with custom settings
        crate::node::single_node::local_rate_limit_with_settings(key, rate_limiter, &settings).await
    }

    async fn local_check_limit_custom(&self, rule_name: String, key: String) -> Result<CheckCallsResponse> {
        // Get the limiter for this rule
        let rate_limiter = {
            let limiters = self.named_rate_limiters.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            match limiters.get(&rule_name) {
                Some(limiter) => limiter.clone(),
                None => return Err(ColibriError::Api(format!("Limiter for rule '{}' not found", rule_name))),
            }
        };

        // Check limit without consuming tokens
        local_check_limit(key, rate_limiter).await
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for HashringNode functionality - consistent hashing and request routing

    use super::*;
    use std::collections::HashSet;

    fn test_settings_single_node() -> settings::Settings {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8410".to_string()); // Only this node

        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8410,
            listen_port_tcp: 8411,
            listen_port_udp: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Hashring,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology,
            failure_timeout_secs: 30,
        }
    }

    fn test_settings_multi_node() -> settings::Settings {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8410".to_string()); // This node
        topology.insert("127.0.0.1:8420".to_string()); // Other node 1
        topology.insert("127.0.0.1:8430".to_string()); // Other node 2

        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8410,
            listen_port_tcp: 8411,
            listen_port_udp: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: settings::RunMode::Hashring,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology,
            failure_timeout_secs: 30,
        }
    }

    #[tokio::test]
    async fn test_hashring_node_creation_single() {
        let node_id = NodeId::new(1);
        let settings = test_settings_single_node();

        let node = HashringNode::new(node_id, settings).await.unwrap();

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.number_of_buckets, 1);
        assert_eq!(node.bucket, 0); // Should be bucket 0 with single node
        assert!(node.topology.contains_key(&0));
    }

    #[tokio::test]
    async fn test_hashring_node_creation_multi() {
        let node_id = NodeId::new(1);
        let settings = test_settings_multi_node();

        let node = HashringNode::new(node_id, settings).await.unwrap();

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.number_of_buckets, 3);

        // Due to consistent hashing collisions, topology size may be <= number_of_buckets
        assert!(node.topology.len() <= 3);
        assert!(node.topology.len() > 0); // Should have at least one entry

        // Should have assigned this node to one of the 3 buckets
        assert!(node.bucket < 3);
        assert!(node
            .topology
            .values()
            .any(|host| node.topology_address_is_self(&host.0)));
    }
    #[tokio::test]
    async fn test_hashring_node_local_rate_limiting() {
        let node_id = NodeId::new(1);
        let settings = test_settings_single_node();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        let client_id = "local_client".to_string();

        // With single node, all requests should be handled locally
        let check_result = node.check_limit(client_id.clone()).await.unwrap();
        assert_eq!(check_result.client_id, client_id);
        assert_eq!(check_result.calls_remaining, 100);

        let rate_result = node.rate_limit(client_id.clone()).await.unwrap();
        assert!(rate_result.is_some());

        let response = rate_result.unwrap();
        assert_eq!(response.client_id, client_id);
        assert!(response.calls_remaining < 100); // Should have consumed tokens
    }

    #[tokio::test]
    async fn test_hashring_node_consistent_hashing() {
        let node_id = NodeId::new(1);
        let settings = test_settings_multi_node();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Test that consistent hashing produces stable results
        let client_id = "test_client".to_string();
        let bucket1 = consistent_hashing::jump_consistent_hash(&client_id, node.number_of_buckets);
        let bucket2 = consistent_hashing::jump_consistent_hash(&client_id, node.number_of_buckets);

        assert_eq!(bucket1, bucket2); // Should be deterministic
        assert!(bucket1 < node.number_of_buckets); // Should be within range

        // Different clients should potentially map to different buckets
        let different_client = "different_client".to_string();
        let different_bucket =
            consistent_hashing::jump_consistent_hash(&different_client, node.number_of_buckets);
        assert!(different_bucket < node.number_of_buckets);

        // Note: We can't guarantee they'll be different due to hash collisions,
        // but this tests the hashing function works
    }

    #[tokio::test]
    async fn test_hashring_node_bucket_assignment() {
        let node_id = NodeId::new(1);
        let settings = test_settings_multi_node();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Verify the node knows which bucket it's responsible for
        assert!(node.bucket < node.number_of_buckets);

        // Verify the topology mapping is correct
        let our_host = node.topology.get(&node.bucket).unwrap();
        assert!(node.topology_address_is_self(&our_host.0));
    }

    #[tokio::test]
    async fn test_hashring_node_expire_keys() {
        let node_id = NodeId::new(1);
        let settings = test_settings_single_node();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Should complete without error
        node.expire_keys().await.unwrap();
    }

    #[tokio::test]
    async fn test_hashring_node_missing_topology_error() {
        let node_id = NodeId::new(1);
        let mut settings = test_settings_single_node();

        // Remove our node from topology - should cause error
        settings.topology.clear();
        settings.topology.insert("127.0.0.1:9999".to_string()); // Different port

        let result = HashringNode::new(node_id, settings).await;
        assert!(result.is_err());

        // Should be a config error about missing topology
        match result.unwrap_err() {
            ColibriError::Config(msg) => {
                assert!(msg.contains("not found in topology"));
            }
            _ => panic!("Expected Config error"),
        }
    }

    #[tokio::test]
    async fn test_hashring_node_request_routing_logic() {
        let node_id = NodeId::new(1);
        let settings = test_settings_multi_node();
        let node = HashringNode::new(node_id, settings).await.unwrap();

        // Test with a client that should map to this node's bucket
        let mut local_client = None;
        let mut remote_client = None;

        // Find clients that map to local vs remote buckets
        for i in 0..100 {
            let test_client = format!("test_client_{}", i);
            let bucket =
                consistent_hashing::jump_consistent_hash(&test_client, node.number_of_buckets);

            if bucket == node.bucket && local_client.is_none() {
                local_client = Some(test_client);
            } else if bucket != node.bucket && remote_client.is_none() {
                remote_client = Some(test_client);
            }

            if local_client.is_some() && remote_client.is_some() {
                break;
            }
        }

        // Test local client (should handle locally)
        if let Some(client_id) = local_client {
            let result = node.check_limit(client_id).await.unwrap();
            assert_eq!(result.calls_remaining, 100); // Fresh client
        }

        // For remote clients, we can't easily test HTTP calls in unit tests,
        // but we can verify the routing logic by checking bucket assignment
        if let Some(client_id) = remote_client {
            let bucket =
                consistent_hashing::jump_consistent_hash(&client_id, node.number_of_buckets);
            assert_ne!(bucket, node.bucket); // Should route to different node
            assert!(node.topology.contains_key(&bucket)); // Should have host for that bucket
        }
    }
}
