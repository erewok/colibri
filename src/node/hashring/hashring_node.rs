use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::time::interval;
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

/// Replication factor for data distribution
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplicationFactor {
    Zero = 1,
    #[default]
    Two = 2,
    Three = 3,
}

/// Consistent hash ring distributed rate limiter node
#[derive(Clone, Debug)]
pub struct HashringNode {
    pub node_id: NodeId,
    bucket: u32,
    number_of_buckets: u32,
    listen_api: String,
    // Connection-pooling clients reuse TCP connections to each node
    pub topology: HashMap<u32, (Url, reqwest::Client)>,
    replication_factor: ReplicationFactor,
    replica_buckets: Vec<u32>,
    replication_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    // Replication sync state: track last sync time per bucket we replicate
    sync_state: Arc<RwLock<HashMap<u32, Instant>>>,
    pub rate_limiter: Arc<Mutex<token_bucket::TokenBucketLimiter>>,
    pub rate_limit_config: Arc<RwLock<settings::RateLimitConfig>>,
    pub named_rate_limiters:
        Arc<RwLock<HashMap<String, Arc<Mutex<token_bucket::TokenBucketLimiter>>>>>,
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
            format!(
                "http://{}:{}",
                settings.listen_address, settings.listen_port_api
            )
        };

        let number_of_buckets = settings.topology.len().try_into()?;

        // Assign buckets using consistent hashing
        let topology = settings
            .topology
            .iter()
            .filter_map(|host| {
                let bucketnum =
                    consistent_hashing::jump_consistent_hash(host.as_str(), number_of_buckets);
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
                Some((bucketnum, (url, client)))
            })
            .collect::<HashMap<u32, (Url, reqwest::Client)>>();

        // Find this node's bucket assignment
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

        // Set up replication. For unsupported values, default to ReplicationFactor::default() (currently RF=2).
        let replication_factor = match settings.hash_replication_factor {
            0 | 1 => ReplicationFactor::Zero,
            2 => ReplicationFactor::Two,
            3 => ReplicationFactor::Three,
            other => {
                warn!("Unsupported replication factor {}, defaulting to 2", other);
                ReplicationFactor::default()
            }
        };
        let replica_buckets =
            Self::calculate_replica_buckets(bucket, number_of_buckets, &replication_factor);

        info!(
            "[Node<{}>] Hashring node starting at {} with bucket {} out of {} buckets, replicas {:?}, and topology {:?}",
            node_id, listen_api, bucket, number_of_buckets, replica_buckets, topology
        );
        let rate_limit_config = settings::RateLimitConfig::new(settings.rate_limit_settings());

        Ok(Self {
            node_id,
            bucket,
            number_of_buckets,
            listen_api,
            topology,
            replication_factor,
            replica_buckets,
            replication_task_handle: Arc::new(Mutex::new(None)),
            sync_state: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(RwLock::new(rate_limit_config)),
            named_rate_limiters: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), self.number_of_buckets);

        // Try primary owner first
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                info!(
                    "[bucket {}] Requesting data from bucket {} at {}",
                    self.bucket, bucket, host
                );
                let path = paths::drop_leading_slash(paths::default_rate_limits::CHECK)
                    .replace("{client_id}", &client_id);
                let url = format!("{}{}", host, path);

                match client.get(&url).send().await {
                    Ok(response) => response.json().await.map_err(Into::into),
                    Err(e) => {
                        warn!(
                            "Primary owner failed for bucket {}: {}. Trying replicas...",
                            bucket, e
                        );
                        self.try_replicas_for_check(&client_id, bucket).await
                    }
                }
            }
            // Handle locally if this is our bucket, or try replicas
            None => {
                if bucket == self.bucket {
                    local_check_limit(client_id, self.rate_limiter.clone()).await
                } else {
                    self.try_replicas_for_check(&client_id, bucket).await
                }
            }
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let bucket =
            consistent_hashing::jump_consistent_hash(client_id.as_str(), self.number_of_buckets);

        // Try primary owner first
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::default_rate_limits::LIMIT)
                    .replace("{client_id}", &client_id);
                let url = format!("{}{}", host, path);
                info!(
                    "[bucket {}] Requesting data from bucket {} at {}",
                    self.bucket, bucket, url
                );

                match client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body("{}")
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        info!("Received status code {}", status);
                        if status == 429 {
                            Ok(None)
                        } else {
                            resp.json().await.map_err(Into::into)
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Primary owner failed for bucket {}: {}. Trying replicas...",
                            bucket, e
                        );
                        self.try_replicas_for_rate_limit(&client_id, bucket).await
                    }
                }
            }
            // Handle locally if this is our bucket, or try replicas
            None => {
                if bucket == self.bucket {
                    local_rate_limit(client_id, self.rate_limiter.clone()).await
                } else {
                    self.try_replicas_for_rate_limit(&client_id, bucket).await
                }
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

    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        // check if already exists
        {
            let config = self.rate_limit_config.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            if config.get_named_rule_settings(&rule_name).is_some() {
                return Ok(());
            }
        }

        // Add the rule to our local configuration
        {
            let mut config = self.rate_limit_config.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            let rule = settings::NamedRateLimitRule {
                name: rule_name.clone(),
                settings: settings.clone(),
            };
            config.add_named_rule(&rule);
        }

        // Create a new rate limiter for this rule
        let rate_limiter = token_bucket::TokenBucketLimiter::new(self.node_id, settings.clone());
        {
            let mut limiters = self.named_rate_limiters.write().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            limiters.insert(rule_name.clone(), Arc::new(Mutex::new(rate_limiter)));
        }

        // Propagate rule to other nodes
        for (node_address, _client) in self.topology.values() {
            if !self.topology_address_is_self(node_address) {
                let path = paths::drop_leading_slash(paths::custom::RULE_CONFIG)
                    .replace("{rule_name}", &rule_name);
                let url = format!("{}{}", node_address, path);
                let client = reqwest::Client::new();
                let response = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .json(&settings)
                    .send()
                    .await;

                if let Err(e) = response {
                    warn!(
                        "Failed to sync rule {} to node {}: {}",
                        rule_name, node_address, e
                    );
                }
            }
        }
        Ok(())
    }

    async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        self.rate_limit_config
            .read()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e)))
            .map(|rlconf| {
                rlconf
                    .get_named_rule_settings(&rule_name)
                    .cloned()
                    .map(|rl_settings| settings::NamedRateLimitRule {
                        name: rule_name,
                        settings: rl_settings,
                    })
            })
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
        for (node_address, client) in self.topology.values() {
            if !self.topology_address_is_self(node_address) {
                // Delete rule from remote node
                let path = paths::drop_leading_slash(paths::custom::RULE_CONFIG)
                    .replace("{rule_name}", &rule_name);
                let url = format!("{}{}", node_address, path);
                let response = client.delete(&url).send().await;

                if let Err(e) = response {
                    warn!(
                        "Failed to delete rule {} from node {}: {}",
                        rule_name, node_address, e
                    );
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

    async fn rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::custom::LIMIT)
                    .replace("{rule_name}", &rule_name)
                    .replace("{key}", &key);
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
                    Err(ColibriError::Api(format!(
                        "Remote node returned status {}",
                        status
                    )))
                }
            }
            None => {
                // Fallback to local handling
                warn!("No node found for bucket {}, handling locally", bucket);
                self.local_rate_limit_custom(rule_name, key).await
            }
        }
    }

    async fn check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some((host, client)) => {
                let path = paths::drop_leading_slash(paths::custom::CHECK)
                    .replace("{rule_name}", &rule_name)
                    .replace("{key}", &key);
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

impl Drop for HashringNode {
    fn drop(&mut self) {
        // Clean up the replication task when the node is dropped
        if let Ok(mut task_handle) = self.replication_task_handle.lock() {
            if let Some(handle) = task_handle.take() {
                handle.abort();
            }
        }
    }
}

impl HashringNode {
    /// Get the bucket ID that this node owns
    pub fn get_owned_bucket(&self) -> u32 {
        self.bucket
    }

    /// Get the buckets that this node replicates
    pub fn get_replica_buckets(&self) -> Vec<u32> {
        self.replica_buckets.clone()
    }

    /// Calculate which buckets this node should replicate based on replication factor
    fn calculate_replica_buckets(
        bucket: u32,
        number_of_buckets: u32,
        replication_factor: &ReplicationFactor,
    ) -> Vec<u32> {
        let rf = match replication_factor {
            ReplicationFactor::Zero => return Vec::new(),
            ReplicationFactor::Two => 2,
            ReplicationFactor::Three => 3,
        };
        let mut replicas = Vec::with_capacity(rf);
        // For RF=2, replicate next bucket. For RF=3, replicate next 2 buckets
        for i in 1..rf {
            let replica_bucket = (bucket + i as u32) % number_of_buckets;
            replicas.push(replica_bucket);
        }

        replicas
    }

    /// Start background replication sync task
    pub fn start_replication_sync(self: Arc<Self>) {
        let node = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            let mut sync_interval = interval(Duration::from_secs(5)); // Base sync interval
            loop {
                sync_interval.tick().await;
                if let Err(e) = node.sync_replicas().await {
                    warn!("Replication sync failed: {}", e);
                }
            }
        });

        // Store the handle for proper cleanup
        if let Ok(mut task_handle) = self.replication_task_handle.lock() {
            *task_handle = Some(handle);
        } else {
            warn!("Failed to acquire lock on replication_task_handle");
        }
    }

    /// Stop the background replication sync task
    pub fn stop_replication_sync(&self) {
        if let Ok(mut task_handle) = self.replication_task_handle.lock() {
            if let Some(handle) = task_handle.take() {
                handle.abort();
                info!("Replication sync task stopped");
            }
        } else {
            warn!("Failed to acquire lock on replication_task_handle during shutdown");
        }
    }

    /// Sync data from primary owners for buckets we replicate
    async fn sync_replicas(&self) -> Result<()> {
        for &bucket in &self.replica_buckets {
            // Check if we need to sync this bucket based on activity
            let should_sync = {
                let sync_state = self.sync_state.read().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to read sync state: {}", e))
                })?;

                match sync_state.get(&bucket) {
                    Some(last_sync) => {
                        // Exponential backoff: sync more frequently for recently active buckets
                        let since_last_sync = last_sync.elapsed();
                        since_last_sync >= Duration::from_secs(10) // Min sync interval
                    }
                    None => true, // Never synced before
                }
            };

            if should_sync {
                if let Err(e) = self.sync_bucket_from_primary(bucket).await {
                    warn!("Failed to sync bucket {}: {}", bucket, e);
                } else {
                    // Update last sync time
                    match self.sync_state.write() {
                        Ok(mut sync_state) => {
                            sync_state.insert(bucket, Instant::now());
                        }
                        Err(e) => {
                            warn!(
                                "Failed to acquire write lock on sync_state for bucket {}: {}",
                                bucket, e
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Sync a specific bucket from its primary owner
    async fn sync_bucket_from_primary(&self, bucket: u32) -> Result<()> {
        // Find the primary owner of this bucket
        match self.topology.get(&bucket) {
            Some((host, client)) if !self.topology_address_is_self(host) => {
                // Use the cluster API to export bucket data from the primary node
                let export_url = format!("{}{}", host, paths::cluster::export_bucket_path(bucket));
                debug!("Syncing bucket {} from primary at {}", bucket, export_url);

                match client.get(&export_url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.json::<crate::api::cluster::BucketExport>().await {
                                Ok(export_data) => {
                                    // Store count before moving the data
                                    let client_count = export_data.client_data.len();

                                    // Import the data into our local rate limiter
                                    let rate_limiter = self.rate_limiter.lock().map_err(|_| {
                                        ColibriError::Api(
                                            "Failed to acquire rate limiter lock".to_string(),
                                        )
                                    })?;

                                    let token_buckets: std::collections::HashMap<
                                        String,
                                        crate::limiters::token_bucket::TokenBucket,
                                    > = export_data
                                        .client_data
                                        .into_iter()
                                        .map(|client_data| {
                                            (
                                                client_data.client_id,
                                                crate::limiters::token_bucket::TokenBucket {
                                                    tokens: client_data.tokens,
                                                    last_call: client_data.last_call,
                                                },
                                            )
                                        })
                                        .collect();

                                    rate_limiter.import_buckets(token_buckets);
                                    drop(rate_limiter);

                                    debug!(
                                        "Successfully synced {} clients from bucket {}",
                                        client_count, bucket
                                    );
                                    Ok(())
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to parse export data for bucket {}: {}",
                                        bucket, e
                                    );
                                    Err(ColibriError::Api(format!(
                                        "Failed to parse export data: {}",
                                        e
                                    )))
                                }
                            }
                        } else {
                            warn!(
                                "Export request failed for bucket {}: {}",
                                bucket,
                                response.status()
                            );
                            Err(ColibriError::Api(format!(
                                "Export request failed: {}",
                                response.status()
                            )))
                        }
                    }
                    Err(e) => {
                        warn!("Failed to sync bucket {} from {}: {}", bucket, host, e);
                        Err(ColibriError::Api(format!("Sync request failed: {}", e)))
                    }
                }
            }
            _ => {
                debug!(
                    "Bucket {} primary is self or not found, skipping sync",
                    bucket
                );
                Ok(())
            }
        }
    }

    /// Get replica nodes for a given bucket (nodes that replicate this bucket's data)
    fn get_replica_nodes_for_bucket(&self, target_bucket: u32) -> Vec<(&Url, &reqwest::Client)> {
        // Find nodes whose replica list includes the target bucket
        self.topology
            .iter()
            .filter_map(|(&node_bucket, (url, client))| {
                if node_bucket == target_bucket {
                    return None; // Skip the primary owner
                }
                if self.topology_address_is_self(url) {
                    return None; // Skip self
                }

                // Check if this node replicates the target bucket
                let replicas = Self::calculate_replica_buckets(
                    node_bucket,
                    self.number_of_buckets,
                    &self.replication_factor,
                );
                if replicas.contains(&target_bucket) {
                    Some((url, client))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Try replica nodes for check_limit operations when primary fails
    async fn try_replicas_for_check(
        &self,
        client_id: &str,
        bucket: u32,
    ) -> Result<CheckCallsResponse> {
        let replica_nodes = self.get_replica_nodes_for_bucket(bucket);

        for (host, client) in replica_nodes {
            let path = paths::drop_leading_slash(paths::default_rate_limits::CHECK)
                .replace("{client_id}", client_id);
            let url = format!("{}{}", host, path);

            info!("Trying replica at {} for bucket {}", host, bucket);
            match client.get(&url).send().await {
                Ok(response) => {
                    info!("Replica success at {} for bucket {}", host, bucket);
                    return response.json().await.map_err(Into::into);
                }
                Err(e) => {
                    warn!("Replica at {} failed for bucket {}: {}", host, bucket, e);
                    continue;
                }
            }
        }

        // All replicas failed, check if this node owns or replicates the bucket
        let my_bucket = self.get_owned_bucket();
        let is_owner = my_bucket == bucket;
        let is_replica = {
            let replicas = Self::calculate_replica_buckets(
                my_bucket,
                self.number_of_buckets,
                &self.replication_factor,
            );
            replicas.contains(&bucket)
        };
        if is_owner || is_replica {
            warn!(
                "All replicas failed for bucket {}, handling locally (this node is owner or replica)",
                bucket
            );
            local_check_limit(client_id.to_string(), self.rate_limiter.clone()).await
        } else {
            error!(
                "All replicas failed for bucket {}, and this node is neither owner nor replica. Refusing to serve potentially stale data.",
                bucket
            );
            Err(ColibriError::Transport(format!(
                "All replicas failed for bucket {}, and this node is neither owner nor replica. Cannot serve request.",
                bucket
            )))
        }
    }

    /// Try replica nodes for rate_limit operations when primary fails
    async fn try_replicas_for_rate_limit(
        &self,
        client_id: &str,
        bucket: u32,
    ) -> Result<Option<CheckCallsResponse>> {
        let replica_nodes = self.get_replica_nodes_for_bucket(bucket);

        for (host, client) in replica_nodes {
            let path = paths::drop_leading_slash(paths::default_rate_limits::LIMIT)
                .replace("{client_id}", client_id);
            let url = format!("{}{}", host, path);

            info!("Trying replica at {} for bucket {}", host, bucket);
            match client
                .post(&url)
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    info!(
                        "Replica success at {} for bucket {}, status: {}",
                        host, bucket, status
                    );
                    if status == 429 {
                        return Ok(None);
                    } else {
                        return resp.json().await.map_err(Into::into);
                    }
                }
                Err(e) => {
                    warn!("Replica at {} failed for bucket {}: {}", host, bucket, e);
                    continue;
                }
            }
        }

        // All replicas failed, check if this node owns or replicates the bucket
        let my_bucket = self.get_owned_bucket();
        let is_owner = my_bucket == bucket;
        let is_replica = {
            let replicas = Self::calculate_replica_buckets(
                my_bucket,
                self.number_of_buckets,
                &self.replication_factor,
            );
            replicas.contains(&bucket)
        };
        if is_owner || is_replica {
            warn!(
                "All replicas failed for bucket {}, handling locally (this node is owner or replica)",
                bucket
            );
            local_rate_limit(client_id.to_string(), self.rate_limiter.clone()).await
        } else {
            error!(
                "All replicas failed for bucket {}, and this node is neither owner nor replica. Refusing to serve potentially stale data.",
                bucket
            );
            Err(ColibriError::Transport(format!(
                "All replicas failed for bucket {}, and this node is neither owner nor replica. Cannot serve request.",
                bucket
            )))
        }
    }

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
            Some((node_address, client)) => {
                if !self.topology_address_is_self(node_address) {
                    Some((node_address, client))
                } else {
                    None
                }
            }
            None => None,
        }
    }

    async fn local_rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
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
                None => {
                    return Err(ColibriError::Api(format!(
                        "Limiter for rule '{}' not found",
                        rule_name
                    )))
                }
            }
        };

        // Use the custom limiter with custom settings
        crate::node::single_node::local_rate_limit_with_settings(key, rate_limiter, &settings).await
    }

    async fn local_check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        // Get the limiter for this rule
        let rate_limiter = {
            let limiters = self.named_rate_limiters.read().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            match limiters.get(&rule_name) {
                Some(limiter) => limiter.clone(),
                None => {
                    return Err(ColibriError::Api(format!(
                        "Limiter for rule '{}' not found",
                        rule_name
                    )))
                }
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
            hash_replication_factor: 1,
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
            hash_replication_factor: 2,
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
