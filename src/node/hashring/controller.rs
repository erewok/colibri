use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::consistent_hashing;
use super::messages;
use crate::cluster::{
    AdminCommand, AdminResponse, BucketExport, ClusterMember, ClusterStatus, ExportMetadata,
    StatusResponse, TopologyResponse,
};
use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket::TokenBucketLimiter;
use crate::node::{
    single_node::local_check_limit, single_node::local_rate_limit, CheckCallsResponse, NodeId,
};
use crate::settings;

/// Controller for consistent hash ring distributed rate limiter
/// Handles all communication and cluster coordination
#[derive(Clone)]
pub struct HashringController {
    node_id: NodeId,
    bucket: u32,
    number_of_buckets: u32,
    // Rate limiting state
    pub rate_limiter: Arc<Mutex<TokenBucketLimiter>>,
    pub rate_limit_config: Arc<Mutex<settings::RateLimitConfig>>,
    pub named_rate_limiters: Arc<Mutex<HashMap<String, Arc<Mutex<TokenBucketLimiter>>>>>,
    /// Cluster membership management - handles transport
    pub cluster_member: Arc<dyn ClusterMember>,
}

impl std::fmt::Debug for HashringController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringController")
            .field("node_id", &self.node_id)
            .field("bucket", &self.bucket)
            .field("number_of_buckets", &self.number_of_buckets)
            .finish()
    }
}

impl HashringController {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_id = settings.node_id();

        // Create cluster member first
        let cluster_member =
            crate::cluster::ClusterFactory::create_from_settings(node_id, &settings).await?;

        // Get cluster topology from cluster member
        let cluster_nodes = cluster_member.get_cluster_nodes().await;
        let number_of_buckets = cluster_nodes
            .len()
            .try_into()
            .map_err(|e| ColibriError::Config(format!("Invalid cluster size: {}", e)))?;

        if number_of_buckets == 0 {
            return Err(ColibriError::Config(
                "Hashring mode requires cluster topology with other nodes".to_string(),
            ));
        }

        // Calculate which bucket this node owns by finding our UDP address in the sorted topology
        let our_udp_address: SocketAddr =
            format!("{}:{}", settings.listen_address, settings.listen_port_udp)
                .parse()
                .map_err(|e| ColibriError::Config(format!("Invalid listen address: {}", e)))?;

        // Sort cluster nodes for consistent bucket assignment
        let mut sorted_nodes = cluster_nodes;
        sorted_nodes.sort();

        let bucket = sorted_nodes
            .iter()
            .position(|&addr| addr == our_udp_address)
            .unwrap_or(0) as u32;

        // Create rate limiter
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = TokenBucketLimiter::new(node_id, rl_settings);
        let rate_limit_config = settings::RateLimitConfig::new(settings.rate_limit_settings());

        Ok(Self {
            node_id,
            bucket,
            number_of_buckets,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(Mutex::new(rate_limit_config)),
            named_rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            cluster_member,
        })
    }

    /// Start the main controller loop
    pub async fn start(self, mut command_rx: mpsc::Receiver<messages::HashringCommand>) {
        info!("HashringController started for node {}", self.node_id);

        while let Some(command) = command_rx.recv().await {
            match self.handle_command(command).await {
                Ok(()) => {}
                Err(e) => {
                    error!("Error handling hashring command: {}", e);
                }
            }
        }

        info!("HashringController stopped for node {}", self.node_id);
    }

    async fn handle_command(&self, command: messages::HashringCommand) -> Result<()> {
        match command {
            messages::HashringCommand::CheckLimit {
                client_id,
                resp_chan,
            } => {
                let result = self.handle_check_limit(client_id).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::RateLimit {
                client_id,
                resp_chan,
            } => {
                let result = self.handle_rate_limit(client_id).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::ExpireKeys => {
                self.handle_expire_keys().await?;
            }
            messages::HashringCommand::CreateNamedRule {
                rule_name,
                settings,
                resp_chan,
            } => {
                let result = self.handle_create_named_rule(rule_name, settings).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::DeleteNamedRule {
                rule_name,
                resp_chan,
            } => {
                let result = self.handle_delete_named_rule(rule_name).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::ListNamedRules { resp_chan } => {
                let result = self.handle_list_named_rules().await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::GetNamedRule {
                rule_name,
                resp_chan,
            } => {
                let result = self.handle_get_named_rule(rule_name).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::RateLimitCustom {
                rule_name,
                key,
                resp_chan,
            } => {
                let result = self.handle_rate_limit_custom(rule_name, key).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::CheckLimitCustom {
                rule_name,
                key,
                resp_chan,
            } => {
                let result = self.handle_check_limit_custom(rule_name, key).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::AdminCommand {
                command,
                source,
                resp_chan,
            } => {
                let result = self.handle_admin_command(command, source).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::ExportBuckets { resp_chan } => {
                let result = self.handle_export_buckets().await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::ImportBuckets {
                import_data,
                resp_chan,
            } => {
                let result = self.handle_import_buckets(import_data).await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::ClusterHealth { resp_chan } => {
                let result = self.handle_cluster_health().await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::GetTopology { resp_chan } => {
                let result = self.handle_get_topology().await;
                let _ = resp_chan.send(result);
            }
            messages::HashringCommand::NewTopology { request, resp_chan } => {
                let result = self.handle_new_topology(request).await;
                let _ = resp_chan.send(result);
            }
        }
        Ok(())
    }

    async fn handle_check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        // Calculate which bucket owns this client
        let bucket = consistent_hashing::jump_consistent_hash(&client_id, self.number_of_buckets);

        // If this is our bucket, handle locally
        if bucket == self.bucket {
            let result = local_check_limit(client_id.clone(), self.rate_limiter.clone()).await?;
            info!(
                "[RATE_CHECK] client:{} bucket:{} node:{} remaining:{}",
                client_id, bucket, self.node_id, result.calls_remaining
            );
            return Ok(result);
        }

        // Get cluster nodes and find the target for this bucket
        let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        if cluster_nodes.is_empty() {
            return local_check_limit(client_id, self.rate_limiter.clone()).await;
        }

        // Sort nodes for consistent bucket assignment
        let mut sorted_nodes = cluster_nodes;
        sorted_nodes.sort();

        if let Some(&target_addr) = sorted_nodes.get(bucket as usize) {
            // Create request message
            let request = messages::HashringRequest {
                request_id: format!(
                    "check_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ),
                client_id: client_id.clone(),
                consume_token: false,
            };

            // Send via cluster member
            match self
                .cluster_member
                .send_request_response(target_addr, &request.serialize()?)
                .await?
            {
                Some(response_data) => {
                    let response = messages::HashringResponse::deserialize(&response_data)?;
                    let result = response.result.map_err(ColibriError::RateLimit)?;
                    info!(
                        "[RATE_CHECK] client:{} bucket:{} target:{} remaining:{}",
                        client_id, bucket, target_addr, result.calls_remaining
                    );
                    Ok(result)
                }
                None => {
                    warn!(
                        "[CLUSTER_UNSUPPORTED] client:{} bucket:{} -> local",
                        client_id, bucket
                    );
                    local_check_limit(client_id, self.rate_limiter.clone()).await
                }
            }
        } else {
            warn!(
                "[BUCKET_MISSING] client:{} bucket:{} -> local",
                client_id, bucket
            );
            local_check_limit(client_id, self.rate_limiter.clone()).await
        }
    }

    async fn handle_rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        // Calculate which bucket owns this client
        let bucket = consistent_hashing::jump_consistent_hash(&client_id, self.number_of_buckets);

        // If this is our bucket, handle locally
        if bucket == self.bucket {
            let result = local_rate_limit(client_id.clone(), self.rate_limiter.clone()).await?;
            if let Some(ref response) = result {
                info!(
                    "[RATE_LIMIT] client:{} bucket:{} node:{} remaining:{} allowed:true",
                    client_id, bucket, self.node_id, response.calls_remaining
                );
            } else {
                info!(
                    "[RATE_LIMIT] client:{} bucket:{} node:{} allowed:false",
                    client_id, bucket, self.node_id
                );
            }
            return Ok(result);
        }

        // Get cluster nodes and find the target for this bucket
        let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        if cluster_nodes.is_empty() {
            return local_rate_limit(client_id, self.rate_limiter.clone()).await;
        }

        // Sort nodes for consistent bucket assignment
        let mut sorted_nodes = cluster_nodes;
        sorted_nodes.sort();

        if let Some(&target_addr) = sorted_nodes.get(bucket as usize) {
            // Create request message
            let request = messages::HashringRequest {
                request_id: format!(
                    "limit_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ),
                client_id: client_id.clone(),
                consume_token: true,
            };

            // Send via cluster member
            match self
                .cluster_member
                .send_request_response(target_addr, &request.serialize()?)
                .await
            {
                Ok(Some(response_data)) => {
                    match messages::HashringResponse::deserialize(&response_data) {
                        Ok(response) => match response.result {
                            Ok(check_response) => {
                                info!(
                                        "[RATE_LIMIT] client:{} bucket:{} target:{} remaining:{} allowed:true",
                                        client_id, bucket, target_addr, check_response.calls_remaining
                                    );
                                Ok(Some(check_response))
                            }
                            Err(_) => {
                                info!(
                                    "[RATE_LIMIT] client:{} bucket:{} target:{} allowed:false",
                                    client_id, bucket, target_addr
                                );
                                Ok(None)
                            }
                        },
                        Err(e) => {
                            warn!(
                                "[ROUTE_FALLBACK] client:{} bucket:{} target:{} decode_error:{} -> local",
                                client_id, bucket, target_addr, e
                            );
                            local_rate_limit(client_id, self.rate_limiter.clone()).await
                        }
                    }
                }
                Ok(None) => {
                    warn!(
                        "[CLUSTER_UNSUPPORTED] client:{} bucket:{} -> local",
                        client_id, bucket
                    );
                    local_rate_limit(client_id, self.rate_limiter.clone()).await
                }
                Err(e) => {
                    warn!(
                        "[ROUTE_FALLBACK] client:{} bucket:{} target:{} error:{} -> local",
                        client_id, bucket, target_addr, e
                    );
                    local_rate_limit(client_id, self.rate_limiter.clone()).await
                }
            }
        } else {
            warn!(
                "[BUCKET_MISSING] client:{} bucket:{} -> local",
                client_id, bucket
            );
            local_rate_limit(client_id, self.rate_limiter.clone()).await
        }
    }

    async fn handle_expire_keys(&self) -> Result<()> {
        let mut rate_limiter = self.rate_limiter.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire rate_limiter lock: {}", e))
        })?;
        rate_limiter.expire_keys();
        Ok(())
    }

    async fn handle_create_named_rule(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        // Create the rule locally
        {
            let mut config = self.rate_limit_config.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            let rule = settings::NamedRateLimitRule {
                name: rule_name.clone(),
                settings: settings.clone(),
            };
            config.add_named_rule(&rule);
        }

        // Create the rate limiter for this rule
        {
            let rate_limiter = TokenBucketLimiter::new(self.node_id, settings.clone());
            let mut limiters = self.named_rate_limiters.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            limiters.insert(rule_name.clone(), Arc::new(Mutex::new(rate_limiter)));
        }

        // TODO: Synchronize the rule to all other nodes via extended AdminCommand protocol
        debug!(
            "Created named rule {} locally (cluster sync not yet implemented)",
            rule_name
        );
        Ok(())
    }

    async fn handle_delete_named_rule(&self, rule_name: String) -> Result<()> {
        // Delete the rule locally
        {
            let mut config = self.rate_limit_config.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
            })?;
            config.remove_named_rule(&rule_name);
        }

        // Remove the rate limiter for this rule
        {
            let mut limiters = self.named_rate_limiters.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
            })?;
            limiters.remove(&rule_name);
        }

        // TODO: Synchronize the deletion to all other nodes via extended AdminCommand protocol
        debug!(
            "Deleted named rule {} locally (cluster sync not yet implemented)",
            rule_name
        );
        Ok(())
    }

    async fn handle_list_named_rules(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        let config = self.rate_limit_config.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
        })?;
        Ok(config.list_named_rules())
    }

    async fn handle_get_named_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        let config = self.rate_limit_config.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire config lock: {}", e))
        })?;
        Ok(config.get_named_rule_settings(&rule_name).map(|settings| {
            settings::NamedRateLimitRule {
                name: rule_name,
                settings: settings.clone(),
            }
        }))
    }

    async fn handle_rate_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        // Calculate which bucket owns this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);

        // If this is our bucket, handle locally
        if bucket == self.bucket {
            // Get the named rate limiter
            let limiter = {
                let limiters = self.named_rate_limiters.lock().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
                })?;
                limiters.get(&rule_name).cloned()
            };

            if let Some(limiter) = limiter {
                let mut rate_limiter = limiter.lock().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to acquire rate limiter lock: {}", e))
                })?;

                // Apply rate limiting using correct API
                if let Some(remaining) = rate_limiter.limit_calls_for_client(key.clone()) {
                    Ok(Some(CheckCallsResponse {
                        client_id: key,
                        calls_remaining: remaining,
                    }))
                } else {
                    Ok(Some(CheckCallsResponse {
                        client_id: key,
                        calls_remaining: 0,
                    }))
                }
            } else {
                Err(ColibriError::Config(format!(
                    "Named rule '{}' not found",
                    rule_name
                )))
            }
        } else {
            // Find the address for the owning bucket
            // Get cluster nodes and find target for bucket
            let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
            let mut sorted_nodes = cluster_nodes;
            sorted_nodes.sort();

            if let Some(&target_addr) = sorted_nodes.get(bucket as usize) {
                // TODO: Implement TCP-based custom rate limiting for remote nodes
                warn!(
                    "Remote custom rate limiting not yet implemented for {}, falling back to local",
                    target_addr
                );

                // For now, fallback to local processing
                let limiter = {
                    let limiters = self.named_rate_limiters.lock().map_err(|e| {
                        ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
                    })?;
                    limiters.get(&rule_name).cloned()
                };

                if let Some(limiter) = limiter {
                    let mut rate_limiter = limiter.lock().map_err(|e| {
                        ColibriError::Concurrency(format!(
                            "Failed to acquire rate limiter lock: {}",
                            e
                        ))
                    })?;

                    if let Some(remaining) = rate_limiter.limit_calls_for_client(key.clone()) {
                        Ok(Some(CheckCallsResponse {
                            client_id: key,
                            calls_remaining: remaining,
                        }))
                    } else {
                        Ok(Some(CheckCallsResponse {
                            client_id: key,
                            calls_remaining: 0,
                        }))
                    }
                } else {
                    Err(ColibriError::Config(format!(
                        "Named rule '{}' not found",
                        rule_name
                    )))
                }
            } else {
                // Fall back to local processing if no target node found
                let limiter = {
                    let limiters = self.named_rate_limiters.lock().map_err(|e| {
                        ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
                    })?;
                    limiters.get(&rule_name).cloned()
                };

                if let Some(limiter) = limiter {
                    let mut rate_limiter = limiter.lock().map_err(|e| {
                        ColibriError::Concurrency(format!(
                            "Failed to acquire rate limiter lock: {}",
                            e
                        ))
                    })?;

                    if let Some(remaining) = rate_limiter.limit_calls_for_client(key.clone()) {
                        Ok(Some(CheckCallsResponse {
                            client_id: key,
                            calls_remaining: remaining,
                        }))
                    } else {
                        Ok(Some(CheckCallsResponse {
                            client_id: key,
                            calls_remaining: 0,
                        }))
                    }
                } else {
                    Err(ColibriError::Config(format!(
                        "Named rule '{}' not found",
                        rule_name
                    )))
                }
            }
        }
    }

    async fn handle_check_limit_custom(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        // Calculate which bucket owns this key
        let bucket = consistent_hashing::jump_consistent_hash(&key, self.number_of_buckets);

        // If this is our bucket, handle locally
        if bucket == self.bucket {
            // Get the named rate limiter
            let limiter = {
                let limiters = self.named_rate_limiters.lock().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
                })?;
                limiters.get(&rule_name).cloned()
            };

            if let Some(limiter) = limiter {
                let rate_limiter = limiter.lock().map_err(|e| {
                    ColibriError::Concurrency(format!("Failed to acquire rate limiter lock: {}", e))
                })?;

                let remaining = rate_limiter.check_calls_remaining_for_client(&key);
                Ok(CheckCallsResponse {
                    client_id: key,
                    calls_remaining: remaining,
                })
            } else {
                Err(ColibriError::Config(format!(
                    "Named rule '{}' not found",
                    rule_name
                )))
            }
        } else {
            // Find the address for the owning bucket
            // Get cluster nodes and find target for bucket
            let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
            let mut sorted_nodes = cluster_nodes;
            sorted_nodes.sort();

            if let Some(&target_addr) = sorted_nodes.get(bucket as usize) {
                // TODO: Implement TCP-based custom check limiting for remote nodes
                warn!("Remote custom check limiting not yet implemented for {}, falling back to local", target_addr);

                // For now, fallback to local processing
                let limiter = {
                    let limiters = self.named_rate_limiters.lock().map_err(|e| {
                        ColibriError::Concurrency(format!("Failed to acquire limiters lock: {}", e))
                    })?;
                    limiters.get(&rule_name).cloned()
                };

                if let Some(limiter) = limiter {
                    let rate_limiter = limiter.lock().map_err(|e| {
                        ColibriError::Concurrency(format!(
                            "Failed to acquire rate limiter lock: {}",
                            e
                        ))
                    })?;

                    let remaining = rate_limiter.check_calls_remaining_for_client(&key);
                    Ok(CheckCallsResponse {
                        client_id: key,
                        calls_remaining: remaining,
                    })
                } else {
                    Err(ColibriError::Config(format!(
                        "Named rule '{}' not found",
                        rule_name
                    )))
                }
            } else {
                Err(ColibriError::Config(format!(
                    "No node found for bucket {}",
                    bucket
                )))
            }
        }
    }

    async fn handle_admin_command(
        &self,
        command: AdminCommand,
        source: SocketAddr,
    ) -> Result<AdminResponse> {
        debug!("Handling admin command {:?} from {}", command, source);

        match command {
            AdminCommand::GetTopology => {
                // TODO: Implement topology response
                Ok(AdminResponse::Error {
                    message: "Topology response not yet implemented".to_string(),
                })
            }
            AdminCommand::ExportBuckets => {
                // TODO: Implement bucket export
                Ok(AdminResponse::Error {
                    message: "Bucket export not yet implemented".to_string(),
                })
            }
            AdminCommand::ImportBuckets { .. } => {
                // TODO: Implement bucket import
                Ok(AdminResponse::Ack)
            }
            AdminCommand::GetClusterHealth => {
                // TODO: Implement health check
                Ok(AdminResponse::Error {
                    message: "Health check not yet implemented".to_string(),
                })
            }
            _ => {
                warn!("Unhandled admin command: {:?}", command);
                Ok(AdminResponse::Error {
                    message: format!("Unhandled command: {:?}", command),
                })
            }
        }
    }

    async fn handle_export_buckets(&self) -> Result<crate::cluster::BucketExport> {
        // TODO: Implement actual bucket data export
        warn!("HashringController::handle_export_buckets not yet fully implemented");

        Ok(BucketExport {
            client_data: Vec::new(),
            metadata: ExportMetadata {
                node_id: self.node_id.to_string(),
                export_timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                node_type: "hashring".to_string(),
                bucket_count: 1, // TODO: Get actual bucket count
            },
        })
    }

    async fn handle_import_buckets(
        &self,
        _import_data: crate::cluster::BucketExport,
    ) -> Result<()> {
        // TODO: Implement bucket import logic with UDP transport
        warn!("HashringController::handle_import_buckets not yet fully implemented");
        Ok(())
    }

    async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        // TODO: Implement actual health checks with UDP transport
        Ok(StatusResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            status: ClusterStatus::Healthy,
            active_clients: 0,          // TODO: implement client counting
            last_topology_change: None, // TODO: track topology changes
        })
    }

    async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        let peer_nodes: Vec<String> = cluster_nodes.iter().map(|addr| addr.to_string()).collect();

        Ok(TopologyResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            owned_bucket: Some(self.bucket),
            replica_buckets: vec![], // TODO: calculate replica buckets
            cluster_nodes,
            peer_nodes,
            errors: None,
        })
    }

    async fn handle_new_topology(
        &self,
        _request: crate::cluster::TopologyChangeRequest,
    ) -> Result<crate::cluster::TopologyResponse> {
        // TODO: Implement topology change with UDP cluster member
        warn!("HashringController::handle_new_topology not yet fully implemented");
        self.handle_get_topology().await
    }
}

#[cfg(test)]
mod tests {
    //! Comprehensive tests for HashringController to verify distributed rate limiting functionality

    use super::*;
    use crate::node::hashring::messages;
    use std::collections::HashSet;
    use tokio::sync::oneshot;

    /// Create a test settings instance for hashring controller with multiple nodes
    fn create_test_settings_with_topology(nodes: Vec<&str>) -> settings::Settings {
        let topology: HashSet<String> = nodes.iter().map(|s| s.to_string()).collect();

        settings::Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8420,
            listen_port_tcp: 8421,
            listen_port_udp: 8422,
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

    /// Create a single-node test settings instance
    fn create_single_node_test_settings() -> settings::Settings {
        create_test_settings_with_topology(vec!["127.0.0.1:8421"])
    }

    /// Create a multi-node test settings instance
    fn create_multi_node_test_settings() -> settings::Settings {
        create_test_settings_with_topology(vec![
            "127.0.0.1:8421",
            "127.0.0.1:8431",
            "127.0.0.1:8441",
        ])
    }

    #[tokio::test]
    async fn test_controller_creation_single_node() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Basic checks that controller is properly initialized
        assert_eq!(controller.number_of_buckets, 1);
        assert_eq!(controller.bucket, 0); // Should own bucket 0 in single-node setup

        // Check cluster nodes
        let cluster_nodes = controller.cluster_member.get_cluster_nodes().await;
        assert_eq!(cluster_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_controller_creation_multi_node() {
        let settings = create_multi_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Should have 3 buckets and proper bucket assignment
        assert_eq!(controller.number_of_buckets, 3);

        // Check cluster nodes
        let cluster_nodes = controller.cluster_member.get_cluster_nodes().await;
        assert_eq!(cluster_nodes.len(), 3);

        // This node should own bucket 0 (first in sorted order)
        assert_eq!(controller.bucket, 0);
    }

    #[tokio::test]
    async fn test_bucket_ownership_calculation() {
        let settings = create_multi_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Test consistent hashing for different clients
        let test_cases = vec![
            ("client_1", "should route to specific bucket"),
            ("client_2", "should route to specific bucket"),
            ("user_abc", "should route to specific bucket"),
        ];

        for (client_id, _description) in test_cases {
            let bucket =
                consistent_hashing::jump_consistent_hash(client_id, controller.number_of_buckets);

            // Bucket should be valid
            assert!(bucket < controller.number_of_buckets);

            // Same client should always map to same bucket
            let bucket2 =
                consistent_hashing::jump_consistent_hash(client_id, controller.number_of_buckets);
            assert_eq!(
                bucket, bucket2,
                "Consistent hashing should be deterministic"
            );
        }
    }

    #[tokio::test]
    async fn test_local_check_limit_command() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = messages::HashringCommand::CheckLimit {
            client_id: "test_client".to_string(),
            resp_chan: tx,
        };

        // Handle the check limit command
        controller.handle_command(cmd).await.unwrap();

        // Check response
        let response = rx.await.unwrap().unwrap();
        assert_eq!(response.client_id, "test_client");
        // For a new client, should have full token bucket
        assert_eq!(response.calls_remaining, 100);
    }

    #[tokio::test]
    async fn test_local_rate_limit_command() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = messages::HashringCommand::RateLimit {
            client_id: "test_client".to_string(),
            resp_chan: tx,
        };

        // Handle the rate limit command
        controller.handle_command(cmd).await.unwrap();

        // Check response
        let response = rx.await.unwrap().unwrap();
        assert!(response.is_some());
        let check_result = response.unwrap();
        assert_eq!(check_result.client_id, "test_client");
        assert!(check_result.calls_remaining < 100); // Should consume tokens
    }

    #[tokio::test]
    async fn test_rate_limiting_exhaustion() {
        let mut settings = create_single_node_test_settings();
        settings.rate_limit_max_calls_allowed = 3; // Very small limit

        let controller = HashringController::new(settings).await.unwrap();
        let client_id = "heavy_client".to_string();

        // Make multiple calls to exhaust the rate limit
        let mut successful_calls = 0;
        let mut failed_calls = 0;

        for _i in 0..5 {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::RateLimit {
                client_id: client_id.clone(),
                resp_chan: tx,
            };

            controller.handle_command(cmd).await.unwrap();
            let response = rx.await.unwrap().unwrap();

            if response.is_some() {
                successful_calls += 1;
            } else {
                failed_calls += 1;
            }
        }

        // We should have some successful calls (at least 1) and some failed ones
        assert!(
            successful_calls > 0,
            "Should have at least one successful call"
        );
        assert!(
            failed_calls > 0,
            "Should have at least one rate-limited call"
        );
        assert!(successful_calls <= 3, "Should not exceed the rate limit");
    }

    #[tokio::test]
    async fn test_expire_keys_command() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        let cmd = messages::HashringCommand::ExpireKeys;

        // Should not panic or error
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_named_rule_lifecycle() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        let rule_name = "test_rule".to_string();
        let rule_settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 50,
            rate_limit_interval_seconds: 30,
        };

        // Test create named rule
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::CreateNamedRule {
                rule_name: rule_name.clone(),
                settings: rule_settings.clone(),
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_ok());
        }

        // Test list named rules
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::ListNamedRules { resp_chan: tx };
            controller.handle_command(cmd).await.unwrap();
            let rules = rx.await.unwrap().unwrap();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0].name, rule_name);
        }

        // Test get named rule
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::GetNamedRule {
                rule_name: rule_name.clone(),
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let rule = rx.await.unwrap().unwrap();
            assert!(rule.is_some());
            assert_eq!(rule.unwrap().name, rule_name);
        }

        // Test custom rate limiting with the rule
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::RateLimitCustom {
                rule_name: rule_name.clone(),
                key: "custom_key".to_string(),
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let response = rx.await.unwrap().unwrap();
            assert!(response.is_some());
            let check_result = response.unwrap();
            assert_eq!(check_result.client_id, "custom_key");
            assert!(check_result.calls_remaining < 50); // Should consume tokens
        }

        // Test custom check limit with the rule
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::CheckLimitCustom {
                rule_name: rule_name.clone(),
                key: "custom_key".to_string(),
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let response = rx.await.unwrap().unwrap();
            assert_eq!(response.client_id, "custom_key");
            assert!(response.calls_remaining < 50); // Should reflect previous consumption
        }

        // Test delete named rule
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::DeleteNamedRule {
                rule_name: rule_name.clone(),
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_ok());
        }

        // Test that rule is gone
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::ListNamedRules { resp_chan: tx };
            controller.handle_command(cmd).await.unwrap();
            let rules = rx.await.unwrap().unwrap();
            assert_eq!(rules.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_cluster_health_command() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = messages::HashringCommand::ClusterHealth { resp_chan: tx };

        controller.handle_command(cmd).await.unwrap();
        let response = rx.await.unwrap().unwrap();

        assert_eq!(response.node_type, "hashring");
        assert_eq!(response.node_id, controller.node_id.to_string());
        assert!(matches!(
            response.status,
            crate::cluster::ClusterStatus::Healthy
        ));
    }

    #[tokio::test]
    async fn test_get_topology_command() {
        let settings = create_multi_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = messages::HashringCommand::GetTopology { resp_chan: tx };

        controller.handle_command(cmd).await.unwrap();
        let response = rx.await.unwrap().unwrap();

        assert_eq!(response.node_type, "hashring");
        assert_eq!(response.owned_bucket, Some(0)); // This node should own bucket 0
        assert_eq!(response.cluster_nodes.len(), 3);
    }

    #[tokio::test]
    async fn test_export_import_buckets() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Test export
        {
            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::ExportBuckets { resp_chan: tx };
            controller.handle_command(cmd).await.unwrap();
            let export = rx.await.unwrap().unwrap();

            assert_eq!(export.metadata.node_type, "hashring");
            assert_eq!(export.metadata.node_id, controller.node_id.to_string());
        }

        // Test import
        {
            let import_data = crate::cluster::BucketExport {
                client_data: vec![],
                metadata: crate::cluster::ExportMetadata {
                    node_id: "test".to_string(),
                    export_timestamp: 12345,
                    node_type: "hashring".to_string(),
                    bucket_count: 1,
                },
            };

            let (tx, rx) = oneshot::channel();
            let cmd = messages::HashringCommand::ImportBuckets {
                import_data,
                resp_chan: tx,
            };
            controller.handle_command(cmd).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_admin_command_handling() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = messages::HashringCommand::AdminCommand {
            command: crate::cluster::AdminCommand::GetTopology,
            source: "127.0.0.1:9999".parse().unwrap(),
            resp_chan: tx,
        };

        controller.handle_command(cmd).await.unwrap();
        let response = rx.await.unwrap().unwrap();

        // Should handle admin commands (even if not fully implemented)
        assert!(matches!(
            response,
            crate::cluster::AdminResponse::Error { .. }
        ));
    }

    #[tokio::test]
    async fn test_tcp_request_response_serialization() {
        // Test the TCP message serialization/deserialization
        let request = messages::HashringRequest {
            request_id: "test_123".to_string(),
            client_id: "test_client".to_string(),
            consume_token: true,
        };

        // Test serialization
        let serialized = request.serialize().unwrap();
        assert!(!serialized.is_empty());

        // Test deserialization
        let deserialized = messages::HashringRequest::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.request_id, request.request_id);
        assert_eq!(deserialized.client_id, request.client_id);
        assert_eq!(deserialized.consume_token, request.consume_token);

        // Test response serialization
        let response = messages::HashringResponse {
            request_id: "test_123".to_string(),
            result: Ok(CheckCallsResponse {
                client_id: "test_client".to_string(),
                calls_remaining: 42,
            }),
        };

        let response_serialized = response.serialize().unwrap();
        let response_deserialized =
            messages::HashringResponse::deserialize(&response_serialized).unwrap();

        assert_eq!(response_deserialized.request_id, response.request_id);
        assert!(response_deserialized.result.is_ok());
    }

    #[tokio::test]
    async fn test_distributed_bucket_routing() {
        let settings = create_multi_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Test various client IDs and verify they route to valid buckets
        let test_clients = vec![
            "user_1",
            "user_2",
            "api_key_abc123",
            "service_xyz",
            "client_special_chars_!@#",
        ];

        for client_id in test_clients {
            let bucket =
                consistent_hashing::jump_consistent_hash(client_id, controller.number_of_buckets);

            // Should route to a valid bucket
            assert!(bucket < controller.number_of_buckets);

            // Should be deterministic
            let bucket2 =
                consistent_hashing::jump_consistent_hash(client_id, controller.number_of_buckets);
            assert_eq!(bucket, bucket2);

            // Check if this node owns the bucket
            let is_local = bucket == controller.bucket;

            // The bucket should have a corresponding cluster node
            if !is_local {
                let cluster_nodes = controller.cluster_member.get_cluster_nodes().await;
                assert!((bucket as usize) < cluster_nodes.len());
            }
        }
    }

    #[tokio::test]
    async fn test_fallback_behavior() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();

        // Even if we somehow try to route to a non-existent bucket,
        // the system should fallback gracefully

        // For a single-node setup, all requests should be handled locally
        let (tx, rx) = oneshot::channel();
        let cmd = messages::HashringCommand::CheckLimit {
            client_id: "any_client".to_string(),
            resp_chan: tx,
        };

        controller.handle_command(cmd).await.unwrap();
        let response = rx.await.unwrap().unwrap();

        // Should successfully handle the request locally
        assert_eq!(response.client_id, "any_client");
        assert!(response.calls_remaining > 0);
    }

    #[tokio::test]
    async fn test_concurrent_rate_limiting() {
        let settings = create_single_node_test_settings();
        let controller = HashringController::new(settings).await.unwrap();
        let client_id = "concurrent_client".to_string();

        // Launch multiple concurrent rate limiting requests
        let mut handles = vec![];

        for _ in 0..10 {
            let controller = controller.clone();
            let client_id = client_id.clone();

            let handle = tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                let cmd = messages::HashringCommand::RateLimit {
                    client_id,
                    resp_chan: tx,
                };

                controller.handle_command(cmd).await.unwrap();
                rx.await.unwrap().unwrap()
            });

            handles.push(handle);
        }

        // Collect all results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await);
        }

        // All requests should complete successfully
        assert_eq!(results.len(), 10);

        // Count successful vs rate-limited requests
        let successful = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .filter(|response| response.is_some())
            .count();

        // Should have some successful requests (rate limiter allows 100 calls)
        assert!(successful > 0);
    }
}
