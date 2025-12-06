use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::cluster::{AdminCommand, AdminResponse, ClusterMember};
use crate::error::{ColibriError, Result};
use crate::limiters::token_bucket::TokenBucketLimiter;
use crate::node::{CheckCallsResponse, NodeId};
use crate::settings;

/// Commands that can be sent to the HashringController
#[derive(Debug)]
pub enum HashringCommand {
    /// Check remaining calls for a client
    CheckLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<CheckCallsResponse>>,
    },
    /// Apply rate limiting for a client
    RateLimit {
        client_id: String,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Expire old keys
    ExpireKeys,
    /// Create a named rate limiting rule
    CreateNamedRule {
        rule_name: String,
        settings: settings::RateLimitSettings,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Delete a named rate limiting rule
    DeleteNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// List all named rate limiting rules
    ListNamedRules {
        resp_chan: oneshot::Sender<Result<Vec<settings::NamedRateLimitRule>>>,
    },
    /// Get a specific named rule
    GetNamedRule {
        rule_name: String,
        resp_chan: oneshot::Sender<Result<Option<settings::NamedRateLimitRule>>>,
    },
    /// Apply custom rate limiting with a named rule
    RateLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<Result<Option<CheckCallsResponse>>>,
    },
    /// Check custom rate limiting with a named rule
    CheckLimitCustom {
        rule_name: String,
        key: String,
        resp_chan: oneshot::Sender<Result<CheckCallsResponse>>,
    },
    /// Handle admin commands from cluster
    AdminCommand {
        command: AdminCommand,
        source: SocketAddr,
        resp_chan: oneshot::Sender<Result<AdminResponse>>,
    },
    /// Export bucket data for cluster migration
    ExportBuckets {
        resp_chan: oneshot::Sender<Result<crate::cluster::BucketExport>>,
    },
    /// Import bucket data from cluster migration
    ImportBuckets {
        import_data: crate::cluster::BucketExport,
        resp_chan: oneshot::Sender<Result<()>>,
    },
    /// Get cluster health status
    ClusterHealth {
        resp_chan: oneshot::Sender<Result<crate::cluster::StatusResponse>>,
    },
    /// Get current topology
    GetTopology {
        resp_chan: oneshot::Sender<Result<crate::cluster::TopologyResponse>>,
    },
    /// Handle topology change
    NewTopology {
        request: crate::cluster::TopologyChangeRequest,
        resp_chan: oneshot::Sender<Result<crate::cluster::TopologyResponse>>,
    },
}

/// Controller for consistent hash ring distributed rate limiter
/// Handles all UDP communication and cluster coordination
#[derive(Clone)]
pub struct HashringController {
    node_id: NodeId,
    bucket: u32,
    number_of_buckets: u32,
    listen_api: String,
    // Bucket-to-address mapping for consistent hashing
    bucket_to_address: HashMap<u32, SocketAddr>,
    // Rate limiting state
    pub rate_limiter: Arc<Mutex<TokenBucketLimiter>>,
    pub rate_limit_config: Arc<Mutex<settings::RateLimitConfig>>,
    pub named_rate_limiters: Arc<Mutex<HashMap<String, Arc<Mutex<TokenBucketLimiter>>>>>,
    /// Cluster membership management - handles UDP transport
    pub cluster_member: Arc<dyn ClusterMember>,
}

impl std::fmt::Debug for HashringController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringController")
            .field("node_id", &self.node_id)
            .field("bucket", &self.bucket)
            .field("number_of_buckets", &self.number_of_buckets)
            .field("topology_size", &self.bucket_to_address.len())
            .finish()
    }
}

impl HashringController {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_id = NodeId::new(1); // TODO: extract from settings
        let number_of_buckets = settings.topology.len().try_into().map_err(|e| {
            ColibriError::Config(format!("Invalid topology size: {}", e))
        })?;

        let listen_api = format!("{}:{}", settings.listen_address, settings.listen_port_api);

        // Create bucket mapping (placeholder for now)
        let bucket_to_address = HashMap::new();
        let bucket = 0; // TODO: calculate actual bucket

        // Create rate limiter
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = TokenBucketLimiter::new(node_id, rl_settings);
        let rate_limit_config = settings::RateLimitConfig::new(settings.rate_limit_settings());

        // Create cluster member
        let cluster_member = crate::cluster::ClusterFactory::create_from_settings(node_id, &settings).await?;

        Ok(Self {
            node_id,
            bucket,
            number_of_buckets,
            listen_api,
            bucket_to_address,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(Mutex::new(rate_limit_config)),
            named_rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            cluster_member,
        })
    }

    /// Start the main controller loop
    pub async fn start(self, mut command_rx: mpsc::Receiver<HashringCommand>) {
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

    async fn handle_command(&self, command: HashringCommand) -> Result<()> {
        match command {
            HashringCommand::CheckLimit { client_id, resp_chan } => {
                let result = self.handle_check_limit(client_id).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::RateLimit { client_id, resp_chan } => {
                let result = self.handle_rate_limit(client_id).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::ExpireKeys => {
                self.handle_expire_keys().await?;
            }
            HashringCommand::CreateNamedRule { rule_name, settings, resp_chan } => {
                let result = self.handle_create_named_rule(rule_name, settings).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::DeleteNamedRule { rule_name, resp_chan } => {
                let result = self.handle_delete_named_rule(rule_name).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::ListNamedRules { resp_chan } => {
                let result = self.handle_list_named_rules().await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::GetNamedRule { rule_name, resp_chan } => {
                let result = self.handle_get_named_rule(rule_name).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::RateLimitCustom { rule_name, key, resp_chan } => {
                let result = self.handle_rate_limit_custom(rule_name, key).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::CheckLimitCustom { rule_name, key, resp_chan } => {
                let result = self.handle_check_limit_custom(rule_name, key).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::AdminCommand { command, source, resp_chan } => {
                let result = self.handle_admin_command(command, source).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::ExportBuckets { resp_chan } => {
                let result = self.handle_export_buckets().await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::ImportBuckets { import_data, resp_chan } => {
                let result = self.handle_import_buckets(import_data).await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::ClusterHealth { resp_chan } => {
                let result = self.handle_cluster_health().await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::GetTopology { resp_chan } => {
                let result = self.handle_get_topology().await;
                let _ = resp_chan.send(result);
            }
            HashringCommand::NewTopology { request, resp_chan } => {
                let result = self.handle_new_topology(request).await;
                let _ = resp_chan.send(result);
            }
        }
        Ok(())
    }

    // Command handlers implementation will follow...
    // For now, these are placeholder implementations

    async fn handle_check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        // TODO: Implement distributed check limit logic with UDP communication
        warn!("HashringController::handle_check_limit not yet fully implemented");

        // For now, handle locally
        use crate::node::single_node::local_check_limit;
        local_check_limit(client_id, self.rate_limiter.clone()).await
    }

    async fn handle_rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        // TODO: Implement distributed rate limit logic with UDP communication
        warn!("HashringController::handle_rate_limit not yet fully implemented");

        // For now, handle locally
        use crate::node::single_node::local_rate_limit;
        local_rate_limit(client_id, self.rate_limiter.clone()).await
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
        _rule_name: String,
        _settings: settings::RateLimitSettings,
    ) -> Result<()> {
        // TODO: Implement distributed rule creation with UDP sync
        warn!("HashringController::handle_create_named_rule not yet fully implemented");
        Ok(())
    }

    async fn handle_delete_named_rule(&self, _rule_name: String) -> Result<()> {
        // TODO: Implement distributed rule deletion with UDP sync
        warn!("HashringController::handle_delete_named_rule not yet fully implemented");
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
        _rule_name: String,
        _key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        // TODO: Implement distributed custom rate limit logic with UDP communication
        warn!("HashringController::handle_rate_limit_custom not yet fully implemented");
        Ok(None)
    }

    async fn handle_check_limit_custom(
        &self,
        _rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        // TODO: Implement distributed custom check limit logic with UDP communication
        warn!("HashringController::handle_check_limit_custom not yet fully implemented");

        // Placeholder response
        Ok(CheckCallsResponse {
            client_id: key,
            calls_remaining: 0,
        })
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
                    message: "Topology response not yet implemented".to_string()
                })
            }
            AdminCommand::ExportBuckets => {
                // TODO: Implement bucket export
                Ok(AdminResponse::Error {
                    message: "Bucket export not yet implemented".to_string()
                })
            }
            AdminCommand::ImportBuckets { .. } => {
                // TODO: Implement bucket import
                Ok(AdminResponse::Ack)
            }
            AdminCommand::GetClusterHealth => {
                // TODO: Implement health check
                Ok(AdminResponse::Error {
                    message: "Health check not yet implemented".to_string()
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
        use crate::cluster::{BucketExport, ExportMetadata};

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

    async fn handle_import_buckets(&self, _import_data: crate::cluster::BucketExport) -> Result<()> {
        // TODO: Implement bucket import logic with UDP transport
        warn!("HashringController::handle_import_buckets not yet fully implemented");
        Ok(())
    }

    async fn handle_cluster_health(&self) -> Result<crate::cluster::StatusResponse> {
        use crate::cluster::{ClusterStatus, StatusResponse};

        // TODO: Implement actual health checks with UDP transport
        Ok(StatusResponse {
            node_id: self.node_id.to_string(),
            node_type: "hashring".to_string(),
            status: ClusterStatus::Healthy,
            active_clients: 0, // TODO: implement client counting
            last_topology_change: None, // TODO: track topology changes
        })
    }

    async fn handle_get_topology(&self) -> Result<crate::cluster::TopologyResponse> {
        use crate::cluster::TopologyResponse;

        // TODO: Get cluster nodes via UDP cluster member
        let cluster_nodes: Vec<SocketAddr> = self.bucket_to_address.values().cloned().collect();
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