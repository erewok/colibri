use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::{NamedRateLimitRule, TokenBucketLimiter};
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, Queueable, Status, StatusResponse,
    TopologyResponse,
};
use crate::node::{NodeAddress, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::traits::Sender;
use crate::transport::{tcp_receiver::TcpRequest, TcpReceiver, TcpTransport};

use super::consistent_hashing;

/// Controller for consistent hash ring distributed rate limiter
#[derive(Clone)]
pub struct HashringController {
    node_name: NodeName,
    bucket: u32,
    number_of_buckets: u32,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<TcpTransport>,
    /// Lock-free concurrent HashMap for rate limiters
    pub named_rate_limiters: Arc<DashMap<String, Arc<Mutex<TokenBucketLimiter>>>>,
    /// Receiver address for logging (used in Phase 2)
    #[allow(dead_code)]
    receiver_addr: SocketAddr,
    /// Receiver for incoming TCP messages (used in Phase 2)
    #[allow(dead_code)]
    receiver: Arc<Mutex<TcpReceiver>>,
    /// Channel ownership transfer pattern (used in Phase 2)
    #[allow(dead_code)]
    receive_chan: Arc<Mutex<Option<mpsc::Receiver<TcpRequest>>>>,
    /// Spawned task handle for message processing
    pub receiver_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Shutdown signaling (used in Phase 2)
    #[allow(dead_code)]
    shutdown_tx: broadcast::Sender<()>,
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

        let bucket =
            consistent_hashing::jump_consistent_hash(node_name.as_str(), number_of_buckets);

        // Initialize DashMap with default limiter
        let named_rate_limiters = DashMap::new();
        let rate_limit_settings = settings.rate_limit_settings();
        let default_limiter = TokenBucketLimiter::new(rate_limit_settings.clone());
        named_rate_limiters.insert(
            "<_default>".to_string(),
            Arc::new(Mutex::new(default_limiter)),
        );

        let transport_config = settings.transport_config();
        let transport = TcpTransport::new(&transport_config).await?;

        // Create TCP receiver and channel
        let receiver_addr = settings.transport_config().peer_listen_url();
        let (message_tx, receive_chan) = tokio::sync::mpsc::channel(1000);
        let receiver = TcpReceiver::new(receiver_addr, Arc::new(message_tx)).await?;

        // Create shutdown channel
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            node_name,
            bucket,
            number_of_buckets,
            topology: Arc::new(RwLock::new(cluster_topology)),
            transport: Arc::new(transport),
            named_rate_limiters: Arc::new(named_rate_limiters),
            receiver_addr,
            receiver: Arc::new(Mutex::new(receiver)),
            receive_chan: Arc::new(Mutex::new(Some(receive_chan))),
            receiver_handle: Arc::new(Mutex::new(None)),
            shutdown_tx,
        })
    }

    /// Start the main controller loop
    pub async fn start(self, mut command_rx: mpsc::Receiver<Queueable>) {
        info!("HashringController started for node {}", self.node_name);

        while let Some(_command) = command_rx.recv().await {}

        info!("HashringController stopped for node {}", self.node_name);
    }

    /// Get rate limiter for a given rule name (or default if None)
    fn get_limiter(&self, rule_name: Option<String>) -> Result<Arc<Mutex<TokenBucketLimiter>>> {
        let rule_name = rule_name.unwrap_or_else(|| "<_default>".to_string());

        // DashMap provides lock-free concurrent access
        self.named_rate_limiters
            .get(&rule_name)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", rule_name)))
    }

    fn owns_bucket_for_client(&self, client_id: &str) -> bool {
        let client_bucket =
            consistent_hashing::jump_consistent_hash(client_id, self.number_of_buckets);
        client_bucket == self.bucket
    }

    async fn find_bucket_owner(&self, client_id: &str) -> Result<SocketAddr> {
        let client_bucket =
            consistent_hashing::jump_consistent_hash(client_id, self.number_of_buckets);

        let topology = self.topology.read().await;
        let all_nodes: Vec<_> = topology.all_nodes().into_iter().collect();

        if client_bucket as usize >= all_nodes.len() {
            return Err(ColibriError::Node(format!(
                "Bucket {} out of range for {} nodes",
                client_bucket,
                all_nodes.len()
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

        let response: Message = postcard::from_bytes(&response_data).map_err(|e| {
            ColibriError::Transport(format!("Failed to deserialize response: {}", e))
        })?;
        Ok(response)
    }

    async fn handle_rate_limit_request(
        &self,
        mut request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        // Prevent forwarding loops
        if request.forwarding_depth >= crate::node::messages::MAX_FORWARDING_DEPTH {
            warn!(
                "Max forwarding depth {} exceeded for client {}",
                crate::node::messages::MAX_FORWARDING_DEPTH,
                request.client_id
            );
            return Err(ColibriError::Transport(
                "Max forwarding depth exceeded - possible topology inconsistency".to_string(),
            ));
        }

        if !self.owns_bucket_for_client(&request.client_id) {
            let owner = self.find_bucket_owner(&request.client_id).await?;
            request.forwarding_depth = request.forwarding_depth.saturating_add(1);

            debug!(
                "Forwarding rate limit request for {} to {} (depth: {})",
                request.client_id, owner, request.forwarding_depth
            );

            let response = self
                .forward_request(owner, &Message::RateLimitRequest(request))
                .await?;

            match response {
                Message::RateLimitResponse(resp) => return Ok(resp),
                _ => {
                    return Err(ColibriError::Transport(
                        "Unexpected response from forward".to_string(),
                    ))
                }
            }
        }

        // Get the appropriate limiter (default or named rule)
        let limiter_arc = self.get_limiter(request.rule_name.clone())?;
        let mut limiter = limiter_arc
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;

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

        tracing::info!(
            "Added node {} at {} to hashring topology",
            name.as_str(),
            address
        );
        self.transport.add_peer(name.node_id(), address).await?;

        Ok(())
    }

    async fn remove_node(&self, name: NodeName) -> Result<()> {
        let mut topology = self.topology.write().await;

        if let Some(address) = topology.remove_node(&name) {
            tracing::info!(
                "Removed node {} (was at {}) from hashring topology",
                name.as_str(),
                address
            );
            self.transport.remove_peer(name.node_id()).await?;
        }

        Ok(())
    }

    async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        // Only create if it doesn't already exist
        if self.named_rate_limiters.contains_key(&rule_name) {
            return Ok(());
        }

        let limiter = TokenBucketLimiter::new(settings.clone());
        self.named_rate_limiters
            .insert(rule_name.clone(), Arc::new(Mutex::new(limiter)));
        tracing::info!("Created named rule '{}' in hashring node", rule_name);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: String) -> Result<()> {
        if self.named_rate_limiters.remove(&rule_name).is_some() {
            tracing::info!("Deleted named rule '{}' from hashring node", rule_name);
        } else {
            tracing::warn!("Attempted to delete non-existent rule '{}'", rule_name);
        }
        Ok(())
    }

    async fn get_rate_limit_rule(&self, rule_name: String) -> Result<Option<NamedRateLimitRule>> {
        // Translate "default" to internal key "<_default>"
        let internal_key = if rule_name == "default" {
            "<_default>".to_string()
        } else {
            rule_name.clone()
        };

        // Use DashMap - no lock needed
        if let Some(limiter_arc) = self.named_rate_limiters.get(&internal_key) {
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            let settings = limiter.get_settings().clone();

            return Ok(Some(NamedRateLimitRule {
                name: rule_name, // Return user-facing name "default", not internal "<_default>"
                settings,
            }));
        }

        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let mut rules = Vec::new();

        // DashMap iter doesn't require lock
        for entry in self.named_rate_limiters.iter() {
            let name = entry.key();
            let limiter_arc = entry.value();
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;

            // Translate internal "<_default>" key to user-facing "default"
            let user_facing_name = if name == "<_default>" {
                "default".to_string()
            } else {
                name.clone()
            };

            rules.push(NamedRateLimitRule {
                name: user_facing_name,
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

            Message::CreateRateLimitRule {
                rule_name,
                settings,
            } => {
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
}
