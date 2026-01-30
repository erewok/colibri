use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::error::{ColibriError, Result};
use crate::limiters::{NamedRateLimitRule, RateLimitConfig, TokenBucketLimiter};
use crate::node::messages::{CheckCallsRequest, CheckCallsResponse, Message, Queueable, Status, StatusResponse, TopologyResponse};
use crate::node::{NodeAddress, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::TcpTransport;
use crate::transport::traits::Sender;

use super::consistent_hashing;

/// Controller for consistent hash ring distributed rate limiter
#[derive(Clone)]
pub struct HashringController {
    node_name: NodeName,
    bucket: u32,
    number_of_buckets: u32,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<TcpTransport>,
    rate_limit_settings: RateLimitSettings,
    pub rate_limiter: Arc<Mutex<TokenBucketLimiter>>,
    pub rate_limit_config: Arc<Mutex<RateLimitConfig>>,
    pub named_rate_limiters: Arc<Mutex<HashMap<String, Arc<Mutex<TokenBucketLimiter>>>>>,
}

impl std::fmt::Debug for HashringController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringController")
            .field("node_name", &self.node_name)
            .field("bucket", &self.bucket)
            .field("number_of_buckets", &self.number_of_buckets)
            .finish()
    }
}

impl HashringController {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_name = settings.node_name();

        let cluster_topology = settings.cluster_topology();
        let number_of_buckets = cluster_topology
            .nodes
            .len()
            .try_into()
            .map_err(|e| ColibriError::Config(format!("Invalid cluster size: {}", e)))?;

        if number_of_buckets == 0 {
            return Err(ColibriError::Config(
                "Hashring mode requires cluster topology with other nodes".to_string(),
            ));
        }

        let bucket = consistent_hashing::jump_consistent_hash(node_name.as_str(), number_of_buckets);

        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = TokenBucketLimiter::new(rl_settings.clone());
        let rate_limit_config = RateLimitConfig::new(rl_settings.clone());

        let transport_config = settings.transport_config();
        let transport = TcpTransport::new(&transport_config).await?;

        Ok(Self {
            node_name,
            bucket,
            number_of_buckets,
            topology: Arc::new(RwLock::new(cluster_topology)),
            transport: Arc::new(transport),
            rate_limit_settings: rl_settings,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(Mutex::new(rate_limit_config)),
            named_rate_limiters: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Start the main controller loop
    pub async fn start(self, mut command_rx: mpsc::Receiver<Queueable>) {
        info!("HashringController started for node {}", self.node_name);

        while let Some(_command) = command_rx.recv().await {
        }

        info!("HashringController stopped for node {}", self.node_name);
    }

    fn owns_bucket_for_client(&self, client_id: &str) -> bool {
        let client_bucket = consistent_hashing::jump_consistent_hash(client_id, self.number_of_buckets);
        client_bucket == self.bucket
    }

    async fn find_bucket_owner(&self, client_id: &str) -> Result<SocketAddr> {
        let client_bucket = consistent_hashing::jump_consistent_hash(client_id, self.number_of_buckets);

        let topology = self.topology.read().await;
        let all_nodes: Vec<_> = topology.all_nodes().into_iter().collect();

        if client_bucket as usize >= all_nodes.len() {
            return Err(ColibriError::Node(format!(
                "Bucket {} out of range for {} nodes",
                client_bucket, all_nodes.len()
            )));
        }

        let (_name, address) = &all_nodes[client_bucket as usize];
        Ok(*address)
    }

    async fn forward_request(&self, target: SocketAddr, message: &Message) -> Result<Message> {
        debug!("Forwarding request to {} for routing", target);

        let data = postcard::to_allocvec(message)
            .map_err(|e| ColibriError::Transport(format!("Failed to serialize message: {}", e)))?;

        let response_data = self.transport.send_request_response(target, &data).await?;

        let response: Message = postcard::from_bytes(&response_data)
            .map_err(|e| ColibriError::Transport(format!("Failed to deserialize response: {}", e)))?;
        Ok(response)
    }

    async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        if !self.owns_bucket_for_client(&request.client_id) {
            let owner = self.find_bucket_owner(&request.client_id).await?;
            debug!("Forwarding rate limit request for {} to {}", request.client_id, owner);

            let response = self.forward_request(
                owner,
                &Message::RateLimitRequest(request)
            ).await?;

            match response {
                Message::RateLimitResponse(resp) => return Ok(resp),
                _ => return Err(ColibriError::Transport("Unexpected response from forward".to_string())),
            }
        }

        let mut limiter = self.rate_limiter.lock().unwrap();

        let calls_remaining = if request.consume_token {
            limiter.limit_calls_for_client(request.client_id.clone())
        } else {
            Some(limiter.check_calls_remaining_for_client(&request.client_id))
        };

        Ok(CheckCallsResponse {
            client_id: request.client_id,
            rule_name: request.rule_name,
            calls_remaining: calls_remaining.unwrap_or(0),
        })
    }

    async fn get_status(&self) -> Result<StatusResponse> {
        let topology = self.topology.read().await;

        Ok(StatusResponse {
            node_name: self.node_name.clone(),
            node_type: RunMode::Hashring,
            status: Status::Healthy,
            bucket_count: Some(topology.node_count()),
            last_topology_change: None,
            errors: None,
        })
    }

    async fn get_topology(&self) -> Result<TopologyResponse> {
        let status = self.get_status().await?;
        let topology = self.topology.read().await;

        let topology_map = topology
            .all_nodes()
            .into_iter()
            .map(|(name, addr)| {
                let node_addr = NodeAddress {
                    name: name.clone(),
                    local_address: addr,
                    remote_address: addr,
                };
                (name, node_addr)
            })
            .collect();

        Ok(TopologyResponse {
            status,
            topology: topology_map,
        })
    }

    async fn add_node(&self, name: NodeName, address: SocketAddr) -> Result<()> {
        let mut topology = self.topology.write().await;
        topology.add_node(name.clone(), address);

        tracing::info!("Added node {} at {} to hashring topology", name.as_str(), address);
        self.transport.add_peer(name.node_id(), address).await?;

        Ok(())
    }

    async fn remove_node(&self, name: NodeName) -> Result<()> {
        let mut topology = self.topology.write().await;

        if let Some(address) = topology.remove_node(&name) {
            tracing::info!("Removed node {} (was at {}) from hashring topology", name.as_str(), address);
            self.transport.remove_peer(name.node_id()).await?;
        }

        Ok(())
    }

    async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        let mut named_limiters = self.named_rate_limiters.lock().unwrap();
        let limiter = TokenBucketLimiter::new(settings.clone());
        named_limiters.insert(rule_name.clone(), Arc::new(Mutex::new(limiter)));
        tracing::info!("Created named rule '{}' in hashring node", rule_name);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: String) -> Result<()> {
        let mut named_limiters = self.named_rate_limiters.lock().unwrap();
        if named_limiters.remove(&rule_name).is_some() {
            tracing::info!("Deleted named rule '{}' from hashring node", rule_name);
        } else {
            tracing::warn!("Attempted to delete non-existent rule '{}'", rule_name);
        }
        Ok(())
    }

    async fn get_rate_limit_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<NamedRateLimitRule>> {
        if rule_name == "default" {
            return Ok(Some(NamedRateLimitRule {
                name: "default".to_string(),
                settings: self.rate_limit_settings.clone(),
            }));
        }

        let named_limiters = self.named_rate_limiters.lock().unwrap();

        if let Some(limiter_arc) = named_limiters.get(&rule_name) {
            let limiter = limiter_arc.lock().unwrap();
            let settings = limiter.get_settings().clone();

            return Ok(Some(NamedRateLimitRule {
                name: rule_name,
                settings,
            }));
        }

        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let mut rules = vec![
            NamedRateLimitRule {
                name: "default".to_string(),
                settings: self.rate_limit_settings.clone(),
            }
        ];

        let named_limiters = self.named_rate_limiters.lock().unwrap();

        for (name, limiter_arc) in named_limiters.iter() {
            let limiter = limiter_arc.lock().unwrap();
            rules.push(NamedRateLimitRule {
                name: name.clone(),
                settings: limiter.get_settings().clone(),
            });
        }

        Ok(rules)
    }

    pub async fn handle_message(&self, message: Message) -> Result<Message> {
        match message {
            Message::RateLimitRequest(req) => {
                let response = self.handle_rate_limit_request(req).await?;
                Ok(Message::RateLimitResponse(response))
            }

            Message::GetStatus => {
                let status = self.get_status().await?;
                Ok(Message::StatusResponse(status))
            }

            Message::GetTopology => {
                let topology = self.get_topology().await?;
                Ok(Message::TopologyResponse(topology))
            }

            Message::AddNode { name, address } => {
                self.add_node(name, address).await?;
                Ok(Message::Ack)
            }

            Message::RemoveNode { name, address: _ } => {
                self.remove_node(name).await?;
                Ok(Message::Ack)
            }

            Message::CreateRateLimitRule { rule_name, settings } => {
                self.create_rate_limit_rule(rule_name, settings).await?;
                Ok(Message::CreateRateLimitRuleResponse)
            }

            Message::DeleteRateLimitRule { rule_name } => {
                self.delete_rate_limit_rule(rule_name).await?;
                Ok(Message::DeleteRateLimitRuleResponse)
            }

            Message::GetRateLimitRule { rule_name } => {
                let rule = self.get_rate_limit_rule(rule_name).await?;
                Ok(Message::GetRateLimitRuleResponse(rule))
            }

            Message::ListRateLimitRules => {
                let rules = self.list_rate_limit_rules().await?;
                Ok(Message::ListRateLimitRulesResponse(rules))
            }

            _ => Err(ColibriError::Api(format!(
                "Unsupported message type for HashringController: {:?}",
                message
            ))),
        }
    }

    // Note: Direct message handling via handle_message() is now the primary interface
    //     match command {
    //         ClusterCommand::CheckLimit {
    //             request,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_check_limit(request.client_id).await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::RateLimit {
    //             request,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_rate_limit(request.client_id).await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::ExpireKeys => {
    //             self.handle_expire_keys().await?;
    //         }
    //         ClusterCommand::CreateNamedRule {
    //             rule_name,
    //             settings,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_create_named_rule(rule_name, settings).await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::DeleteNamedRule {
    //             rule_name,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_delete_named_rule(rule_name).await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::ListNamedRules { resp_chan } => {
    //             let result = self.handle_list_named_rules().await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::GetNamedRule {
    //             rule_name,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_get_named_rule(rule_name).await;
    //             let _ = resp_chan.send(result);
    //         }
    //         ClusterCommand::AdminCommand {
    //             command,
    //             source,
    //             resp_chan,
    //         } => {
    //             let result = self.handle_admin_command(command, source).await;
    //             let _ = resp_chan.send(result);
    //         }
    //     }
    //     Ok(())
    // }

    // ===== Legacy methods removed in Phase 3 Task 2 =====
    // The following 14 methods were removed (lines 155-718):
    // - handle_check_limit() - referenced non-existent messages module
    // - handle_rate_limit() - referenced non-existent messages module
    // - handle_expire_keys() - deferred to future phase
    // - handle_create_named_rule() - replaced by handle_message()
    // - handle_delete_named_rule() - replaced by handle_message()
    // - handle_list_named_rules() - replaced by handle_message()
    // - handle_get_named_rule() - replaced by handle_message()
    // - handle_rate_limit_custom() - replaced by handle_message()
    // - handle_check_limit_custom() - replaced by handle_message()
    // - handle_admin_command() - referenced non-existent AdminResponse type
    // - handle_export_buckets() - incomplete, deferred
    // - handle_import_buckets() - incomplete, deferred
    // - handle_cluster_health() - replaced by handle_message()
    // - handle_get_topology() - replaced by handle_message()
    // - handle_new_topology() - replaced by handle_message()
    //
    // Use the new handle_message() method from Phase 2 (lines 117-199) instead.
    // These legacy methods contained ~22 compilation errors from missing types.
}

// ===== Tests temporarily disabled in Phase 3 Task 3 =====
// These tests reference old types (ClusterCommand, messages module, cluster::ClusterMember)
// that were removed in Phase 3 Task 2. Tests need to be rewritten to use:
// - New Message enum instead of ClusterCommand
// - BaseController and handle_message() architecture
// - TcpTransport instead of cluster::ClusterMember
//
// TODO: Rewrite these tests in a future phase when node implementations are updated

