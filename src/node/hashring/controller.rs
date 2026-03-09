use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use papaya::HashMap;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::rules::{self, RuleName, SerializableRule};
use crate::limiters::TokenBucketLimiter;
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, Status, StatusResponse, TopologyResponse,
};
use crate::node::{NodeAddress, NodeName};
use crate::settings::{self, ClusterTopology, RunMode};
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
    /// Concurrent HashMap for rate limiters (papaya provides lock-free reads)
    pub named_rate_limiters: HashMap<String, Arc<Mutex<TokenBucketLimiter>>>,
    /// Receiver address for logging
    receiver_addr: SocketAddr,
    /// Receiver for incoming TCP messages
    receiver: Arc<TcpReceiver>,
    /// Channel ownership transfer pattern
    receive_chan: Arc<Mutex<Option<mpsc::Receiver<TcpRequest>>>>,
    /// Handle for the TCP accept loop (loops on socket.accept, pushes TcpRequests into the channel)
    msg_receive_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Handle for the message evaluation loop (reads from channel, calls handle_message)
    msg_eval_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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

        // Initialize HashMap with default limiter
        let named_rate_limiters = HashMap::new();
        let rate_limit_settings = settings.rate_limit_settings();
        let default_limiter = Arc::new(Mutex::new(TokenBucketLimiter::new(
            rate_limit_settings.clone(),
        )));
        named_rate_limiters
            .pin()
            .insert(rules::DEFAULT_RULE_NAME.to_string(), default_limiter);

        let transport_config = settings.transport_config();
        let transport = TcpTransport::new(&transport_config).await?;

        // Create TCP receiver and channel
        let receiver_addr = settings.transport_config().peer_listen_url();
        let (message_tx, receive_chan) = tokio::sync::mpsc::channel(1000);
        let receiver = TcpReceiver::new(receiver_addr, Arc::new(message_tx)).await?;

        Ok(Self {
            node_name,
            bucket,
            number_of_buckets,
            topology: Arc::new(RwLock::new(cluster_topology)),
            transport: Arc::new(transport),
            named_rate_limiters,
            receiver_addr,
            receiver: Arc::new(receiver),
            receive_chan: Arc::new(Mutex::new(Some(receive_chan))),
            msg_receive_handle: Arc::new(Mutex::new(None)),
            msg_eval_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Start the TCP receiver and message processing loop
    pub async fn start_receiver(&self) -> Result<()> {
        // Start the TCP accept loop and store its handle
        let receive_handle = self.receiver.start();
        {
            let mut h = self
                .msg_receive_handle
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            *h = Some(receive_handle);
        }

        // Take ownership of the receive channel
        let mut receive_chan = {
            let mut chan_opt = self
                .receive_chan
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            chan_opt
                .take()
                .ok_or_else(|| ColibriError::Transport("Receiver already started".to_string()))?
        };

        // Clone necessary data for the spawned task
        let receiver_addr = self.receiver_addr;
        let controller = self.clone();

        // Spawn message processing task
        let handle = tokio::spawn(async move {
            info!("Hashring TCP receiver started on {}", receiver_addr);

            while let Some(request) = receive_chan.recv().await {
                // Hashring mode only supports request-response protocol
                let response_tx = match request.response_tx {
                    Some(tx) => tx,
                    None => {
                        warn!(
                            "Received fire-and-forget message in hashring mode from {}, ignoring",
                            request.peer_addr
                        );
                        continue;
                    }
                };

                // Deserialize incoming message
                match Message::deserialize(&request.data) {
                    Ok(message) => {
                        // Handle message via controller
                        let response = match controller.handle_message(message).await {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Error handling hashring message: {}", e);
                                continue;
                            }
                        };

                        // Serialize and send response
                        match response.serialize() {
                            Ok(response_data) => {
                                let response_vec = response_data.to_vec();
                                if response_tx.send(response_vec).is_err() {
                                    warn!("Failed to send response back to peer (channel closed)");
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize response: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize message: {}", e);
                    }
                }
            }

            info!("Hashring TCP receiver stopped");
        });

        // Store the eval handle
        {
            let mut h = self
                .msg_eval_handle
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            *h = Some(handle);
        }

        Ok(())
    }

    /// Stop both the TCP accept loop and the message evaluation loop
    pub fn stop_receiver(&self) {
        if let Ok(mut h) = self.msg_receive_handle.lock() {
            if let Some(handle) = h.take() {
                handle.abort();
            }
        } else {
            error!("Failed to acquire lock on msg_receive_handle during shutdown");
        }
        if let Ok(mut h) = self.msg_eval_handle.lock() {
            if let Some(handle) = h.take() {
                handle.abort();
                info!("Hashring receiver stopped");
            }
        } else {
            error!("Failed to acquire lock on msg_eval_handle during shutdown");
        }
    }

    /// Get mutable reference to rate limiter for a given rule name (or default if None)
    fn with_limiter_mut<F, R>(&self, rule_name: Option<&RuleName>, f: F) -> Result<R>
    where
        F: FnOnce(&mut TokenBucketLimiter) -> R,
    {
        let key = rule_name
            .map(|r| r.as_str().to_string())
            .unwrap_or_else(|| "<_default>".to_string());

        let guard = self.named_rate_limiters.pin();
        let limiter_mutex = guard
            .get(&key)
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", key)))?;
        let mut limiter = limiter_mutex.lock().map_err(|e| {
            ColibriError::Concurrency(format!("Failed to acquire limiter lock: {}", e))
        })?;
        Ok(f(&mut limiter))
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
        let all_nodes: Vec<_> = topology.sorted_nodes().into_iter().collect();

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
        let calls_remaining = self.with_limiter_mut(request.rule_name.as_ref(), |limiter| {
            if request.consume_token {
                limiter.limit_calls_for_client(request.client_id.clone())
            } else {
                Some(limiter.check_calls_remaining_for_client(&request.client_id))
            }
        })?;

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

    async fn create_rate_limit_rule(&self, rule: SerializableRule) -> Result<()> {
        let rule_name_str = rule.name.as_str().to_string();
        // Validate rule name
        if rule_name_str.is_empty() {
            return Err(ColibriError::Api("Rule name cannot be empty".to_string()));
        }

        // Don't allow creating/overwriting the default rule
        if rule_name_str == "default" || rule_name_str == "<_default>" {
            return Err(ColibriError::Api(
                "Cannot create or modify the default rule".to_string(),
            ));
        }

        // Only create if it doesn't already exist
        if self.named_rate_limiters.pin().contains_key(&rule_name_str) {
            return Ok(());
        }

        let limiter = Arc::new(Mutex::new(TokenBucketLimiter::new(rule.settings)));
        self.named_rate_limiters
            .pin()
            .insert(rule_name_str.clone(), limiter);
        tracing::info!("Created named rule '{}' in hashring node", rule_name_str);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: RuleName) -> Result<()> {
        let key = rule_name.as_str().to_string();
        if self.named_rate_limiters.pin().remove(&key).is_some() {
            tracing::info!("Deleted named rule '{}' from hashring node", key);
        } else {
            tracing::warn!("Attempted to delete non-existent rule '{}'", key);
        }
        Ok(())
    }

    async fn get_rate_limit_rule(&self, rule_name: RuleName) -> Result<Option<SerializableRule>> {
        let key = rule_name.as_str().to_string();

        let guard = self.named_rate_limiters.pin();
        if let Some(limiter_mutex) = guard.get(&key) {
            let limiter = limiter_mutex.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiter lock: {}", e))
            })?;
            let settings = limiter.get_settings().clone();
            return Ok(Some(SerializableRule {
                name: rule_name,
                settings,
            }));
        }

        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<rules::RuleList> {
        let mut rule_list = Vec::new();

        let guard = self.named_rate_limiters.pin();
        for (name, limiter_mutex) in guard.iter() {
            let limiter = limiter_mutex.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to acquire limiter lock: {}", e))
            })?;
            // Normalize the internal sentinel key to the user-facing name
            let display_name = if name.as_str() == "<_default>" {
                "default"
            } else {
                name.as_str()
            };
            rule_list.push(SerializableRule {
                name: RuleName::from(display_name),
                settings: limiter.get_settings().clone(),
            });
        }

        Ok(rules::RuleList(rule_list))
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

            Message::CreateRateLimitRule(rule) => {
                self.create_rate_limit_rule(rule).await?;
                Ok(Message::CreateRateLimitRuleResponse)
            }

            Message::DeleteRateLimitRule { rule_name } => {
                self.delete_rate_limit_rule(rule_name.clone()).await?;
                Ok(Message::DeleteRateLimitRuleResponse)
            }

            Message::GetRateLimitRule { rule_name } => {
                let rule = self.get_rate_limit_rule(rule_name.clone()).await?;
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
