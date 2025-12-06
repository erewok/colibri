use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket;
use crate::node::{
    hashring::{consistent_hashing, HashringCommand, HashringController},
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
#[derive(Clone)]
pub struct HashringNode {
    pub node_id: NodeId,
    /// Command sender to the controller
    pub hashring_command_tx: Arc<tokio::sync::mpsc::Sender<HashringCommand>>,
    /// Controller handle
    pub controller_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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

        // Create bucket-to-address mapping using consistent hashing
        let mut bucket_to_address = HashMap::new();
        let mut our_bucket = None;

        for addr_str in &settings.topology {
            let addr: SocketAddr = addr_str.parse().map_err(|e| {
                ColibriError::Config(format!("Invalid topology address '{}': {}", addr_str, e))
            })?;

            let bucket_num = consistent_hashing::jump_consistent_hash(addr_str, number_of_buckets);
            bucket_to_address.insert(bucket_num, addr);

            // Check if this is our address
            debug!("Checking address {} against listen_api {}", addr_str, listen_api);
            if addr_str.contains(&listen_api) || addr_str.contains(&settings.listen_address) {
                our_bucket = Some(bucket_num);
            }
        }

        let bucket = our_bucket
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
            node_id, listen_api, bucket, number_of_buckets, replica_buckets, bucket_to_address.values().collect::<Vec<_>>()
        );
        let rate_limit_config = settings::RateLimitConfig::new(settings.rate_limit_settings());

        // Create cluster member using factory - unified UDP transport for cluster operations
        let cluster_member =
            crate::cluster::ClusterFactory::create_from_settings(node_id, &settings).await?;

        // Set up command channel
        let (hashring_command_tx, hashring_command_rx): (
            tokio::sync::mpsc::Sender<HashringCommand>,
            tokio::sync::mpsc::Receiver<HashringCommand>,
        ) = tokio::sync::mpsc::channel(1000);

        let hashring_command_tx = Arc::new(hashring_command_tx);

        // Create controller
        let controller = HashringController::new(
            node_id,
            bucket,
            number_of_buckets,
            listen_api,
            bucket_to_address,
            Arc::new(Mutex::new(rate_limiter)),
            Arc::new(Mutex::new(rate_limit_config)),
            Arc::new(Mutex::new(HashMap::new())),
            cluster_member,
        ).await;

        // Start the controller in a background task
        let controller_handle = tokio::spawn(async move {
            controller.start(hashring_command_rx).await;
        });

        Ok(Self {
            node_id,
            hashring_command_tx,
            controller_handle: Arc::new(Mutex::new(Some(controller_handle))),
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CheckLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed checking rate limit {}", e)))?;

        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed checking rate limit {}",
                e
            )))
        })
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::RateLimit {
                client_id,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed rate limiting {}", e)))?;

        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed rate limiting {}",
                e
            )))
        })
    }

    async fn expire_keys(&self) -> Result<()> {
        self.hashring_command_tx
            .send(HashringCommand::ExpireKeys)
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed expiring keys {}", e)))?;
        Ok(())
    }

    async fn create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CreateNamedRule {
                rule_name,
                settings,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending create_named_rule command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving create_named_rule response {}",
                e
            )))
        })
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

        // TODO: Propagate rule to other nodes using UDP cluster member
        // This needs to be redesigned to use AdminCommand for rule synchronization
        for addr in self.bucket_to_address.values() {
            if !self.topology_address_is_self(addr) {
                warn!("Rule propagation to {} not yet implemented for UDP transport", addr);
            }
        }
        Ok(())
    }

    async fn get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::GetNamedRule {
                rule_name,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending get_named_rule command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving get_named_rule response {}",
                e
            )))
        })
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::DeleteNamedRule {
                rule_name,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending delete_named_rule command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving delete_named_rule response {}",
                e
            )))
        })
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

        // TODO: Propagate the deletion to all other nodes using UDP cluster member
        for addr in self.bucket_to_address.values() {
            if !self.topology_address_is_self(addr) {
                warn!("Rule deletion propagation to {} not yet implemented for UDP transport", addr);
            }
        }
        Ok(())
    }

    async fn list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::ListNamedRules {
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending list_named_rules command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving list_named_rules response {}",
                e
            )))
        })
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::RateLimitCustom {
                rule_name,
                key,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending rate_limit_custom command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving rate_limit_custom response {}",
                e
            )))
        })
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some(addr) => {
                // TODO: Replace HTTP with UDP cluster member communication
                warn!("Remote rate limit custom to {} not yet implemented for UDP transport", addr);
                // For now, fallback to local handling
                self.local_rate_limit_custom(rule_name, key).await
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hashring_command_tx
            .send(HashringCommand::CheckLimitCustom {
                rule_name,
                key,
                resp_chan: tx,
            })
            .await
            .map_err(|e| ColibriError::Transport(format!("Failed sending check_limit_custom command {}", e)))?;
        rx.await.unwrap_or_else(|e| {
            Err(ColibriError::Transport(format!(
                "Failed receiving check_limit_custom response {}",
                e
            )))
        })
        // Use consistent hashing to determine which node should handle this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);
        match self.get_topology_address_for_bucket(bucket) {
            Some(addr) => {
                // TODO: Replace HTTP with UDP cluster member communication
                warn!("Remote check limit custom to {} not yet implemented for UDP transport", addr);
                // For now, fallback to local handling
                self.local_check_limit_custom(rule_name, key).await
            }
            None => {
                // Handle locally
                self.local_check_limit_custom(rule_name, key).await
            }
        }
    }
}

impl std::fmt::Debug for HashringNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringNode")
            .field("node_id", &self.node_id)
            .field("bucket", &self.bucket)
            .field("number_of_buckets", &self.number_of_buckets)
            .field("listen_api", &self.listen_api)
            .field("topology_size", &self.bucket_to_address.len())
            .field("replication_factor", &self.replication_factor)
            .field("replica_buckets", &self.replica_buckets)
            .finish()
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
        match self.bucket_to_address.get(&bucket) {
            Some(addr) if !self.topology_address_is_self(addr) => {
                // TODO: Implement UDP-based bucket sync using AdminCommand::ExportBuckets
                // For now, just log that sync is needed - the complex sync logic needs to be rewritten for UDP
                warn!("Bucket sync needed for bucket {} from {}, but UDP sync not yet implemented", bucket, addr);
                Ok(())
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
    fn get_replica_nodes_for_bucket(&self, target_bucket: u32) -> Vec<&SocketAddr> {
        // Find nodes whose replica list includes the target bucket
        self.bucket_to_address
            .iter()
            .filter_map(|(&node_bucket, addr)| {
                if node_bucket == target_bucket {
                    return None; // Skip the primary owner
                }
                if self.topology_address_is_self(addr) {
                    return None; // Skip self
                }

                // Check if this node replicates the target bucket
                let replicas = Self::calculate_replica_buckets(
                    node_bucket,
                    self.number_of_buckets,
                    &self.replication_factor,
                );
                if replicas.contains(&target_bucket) {
                    Some(addr)
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

        // TODO: Implement UDP-based replica communication for rate limiting
        // For now, skip replica communication - this needs to be redesigned
        // to use a proper rate limiting protocol over UDP
        for addr in replica_nodes {
            warn!("Replica communication not yet implemented for UDP transport at {}", addr);
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

        // TODO: Implement UDP-based replica communication for rate limiting
        // For now, skip replica communication - this needs to be redesigned
        // to use a proper rate limiting protocol over UDP
        for addr in replica_nodes {
            warn!("Replica communication not yet implemented for UDP transport at {}", addr);
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

    fn topology_address_is_self(&self, address: &SocketAddr) -> bool {
        // confirm that the address matches our own listen_api: we DO NOT WANT TO SEND REQUESTS TO SELF!
        address.to_string().contains(&self.listen_api)
    }

    fn get_topology_address_for_bucket(&self, bucket: u32) -> Option<&SocketAddr> {
        // Pull the address for the given bucket from the topology
        // If None -> local handling!
        if bucket == self.bucket {
            // Handle locally
            return None;
        }
        match self.bucket_to_address.get(&bucket) {
            Some(address) => {
                if !self.topology_address_is_self(address) {
                    Some(address)
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

    // Cluster-specific methods for API delegation

    pub async fn handle_export_buckets(&self) -> Result<crate::cluster::BucketExport> {
        use crate::cluster::{BucketExport, ClientBucketData};

        // Export all token buckets from the rate limiter
        let rate_limiter = self
            .rate_limiter
            .lock()
            .map_err(|_| ColibriError::Api("Failed to acquire rate limiter lock".to_string()))?;
        let all_buckets = rate_limiter.export_all_buckets();
        drop(rate_limiter); // Release lock early

        // Convert to our export format
        let client_data: Vec<ClientBucketData> = all_buckets
            .into_iter()
            .map(|(client_id, token_bucket)| ClientBucketData {
                client_id,
                remaining_tokens: token_bucket.tokens as i64,
                last_refill: token_bucket.last_call as u64,
                bucket_id: Some(self.bucket),
            })
            .collect();

        let export = BucketExport {
            client_data: client_data.clone(),
            metadata: crate::cluster::ExportMetadata {
                node_id: self.node_id.to_string(),
                export_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_type: "hashring".to_string(),
                bucket_count: client_data.len(),
            },
        };

        tracing::info!("Exported {} client records", export.client_data.len());
        Ok(export)
    }

    pub async fn handle_import_buckets(
        &self,
        import_data: crate::cluster::BucketExport,
    ) -> Result<()> {
        use crate::limiters::token_bucket::TokenBucket;

        // Store count before moving the data
        let client_count = import_data.client_data.len();

        // Convert import data to TokenBucket format, filtering by bucket ownership
        let token_buckets: std::collections::HashMap<String, TokenBucket> = import_data
            .client_data
            .into_iter()
            .filter_map(|client_data| {
                let bucket = consistent_hashing::jump_consistent_hash(
                    client_data.client_id.as_str(),
                    self.number_of_buckets,
                );
                if bucket != self.get_owned_bucket() {
                    // This client does not belong to this bucket
                    return None;
                }
                Some((
                    client_data.client_id,
                    TokenBucket {
                        tokens: client_data.remaining_tokens as f64,
                        last_call: client_data.last_refill as i64,
                    },
                ))
            })
            .collect();

        // Import into rate limiter
        let rate_limiter = self
            .rate_limiter
            .lock()
            .map_err(|_| ColibriError::Api("Failed to acquire rate limiter lock".to_string()))?;
        rate_limiter.import_buckets(token_buckets);
        drop(rate_limiter);

        tracing::info!("Successfully imported {} client records", client_count);
        Ok(())
    }

    pub async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        use crate::cluster::{ClusterStatus, StatusResponse};

        Ok(StatusResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            status: ClusterStatus::Healthy,
            active_clients: 0,          // TODO: implement client key counting
            last_topology_change: None, // TODO: track topology changes
        })
    }

    pub async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        use crate::cluster::TopologyResponse;

        let owned_bucket = self.get_owned_bucket();
        let replica_buckets = self.get_replica_buckets();
        let peer_nodes: Vec<String> = self
            .bucket_to_address
            .values()
            .map(|addr| addr.to_string())
            .collect();

        Ok(TopologyResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            owned_bucket: Some(owned_bucket),
            replica_buckets,
            cluster_nodes: self
                .bucket_to_address
                .values()
                .copied()
                .collect(),
            peer_nodes,
            errors: None,
        })
    }

    pub async fn handle_new_topology(
        &self,
        request: crate::cluster::TopologyChangeRequest,
    ) -> Result<crate::cluster::TopologyResponse> {
        use crate::cluster::TopologyResponse;

        tracing::info!(
            "Preparing for topology change with {} new nodes",
            request.new_topology.len()
        );

        // Validate the new topology format
        if request.new_topology.is_empty() {
            return Err(ColibriError::Api(
                "New topology cannot be empty for cluster participants".to_string(),
            ));
        }

        // Basic readiness check - node is ready if it's currently healthy
        // TODO: implement proper topology change preparation
        Ok(TopologyResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            owned_bucket: Some(self.get_owned_bucket()),
            replica_buckets: self.get_replica_buckets(),
            cluster_nodes: request.new_topology.clone(),
            peer_nodes: request
                .new_topology
                .iter()
                .map(|addr| addr.to_string())
                .collect(),
            errors: None,
        })
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
        assert!(node.bucket_to_address.contains_key(&0));
    }

    #[tokio::test]
    async fn test_hashring_node_creation_multi() {
        let node_id = NodeId::new(1);
        let settings = test_settings_multi_node();

        let node = HashringNode::new(node_id, settings).await.unwrap();

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.number_of_buckets, 3);

        // Due to consistent hashing collisions, topology size may be <= number_of_buckets
        assert!(node.bucket_to_address.len() <= 3);
        assert!(node.bucket_to_address.len() > 0); // Should have at least one entry

        // Should have assigned this node to one of the 3 buckets
        assert!(node.bucket < 3);
        assert!(node
            .bucket_to_address
            .values()
            .any(|host| node.topology_address_is_self(host)));
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
        let our_host = node.bucket_to_address.get(&node.bucket).unwrap();
        assert!(node.topology_address_is_self(our_host));
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
            assert!(node.bucket_to_address.contains_key(&bucket)); // Should have host for that bucket
        }
    }
}
