use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use papaya::HashMap;
use tokio::sync::{mpsc, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::{
    distributed_bucket::{DistributedBucketExternal, DistributedBucketLimiter},
    rules::{RuleList, RuleName, SerializableRule, DEFAULT_RULE_NAME},
};
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, Status, StatusResponse, TopologyResponse,
};
use crate::node::{NodeAddress, NodeId, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::{UdpReceiver, UdpTransport};

use super::{GossipMessage, GossipPacket};

type GossipReceiveChannel = Arc<Mutex<Option<mpsc::Receiver<(Bytes, SocketAddr)>>>>;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_name: NodeName,
    node_id: NodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<UdpTransport>,
    /// Concurrent HashMap for rate limiters (papaya provides lock-free reads)
    pub named_rate_limiters: HashMap<String, Arc<Mutex<DistributedBucketLimiter>>>,
    pub response_addr: SocketAddr,
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
    /// Receiver address for logging
    receiver_addr: SocketAddr,
    /// Receiver for incoming UDP datagrams
    receiver: Arc<UdpReceiver>,
    /// Channel ownership transfer pattern
    receive_chan: GossipReceiveChannel,
    /// Handle for the UDP recv loop (loops on socket.recv_from, pushes datagrams into the channel)
    msg_receive_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Handle for the message evaluation loop (reads from channel, calls process_gossip_packet)
    pub msg_eval_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for GossipController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipController")
            .field("node_id", &self.node_id)
            .field("gossip_interval_ms", &self.gossip_interval_ms)
            .field("gossip_fanout", &self.gossip_fanout)
            .finish()
    }
}

impl GossipController {
    pub async fn new(settings: settings::Settings) -> Result<Self> {
        let node_name = settings.node_name();
        let node_id = node_name.node_id();
        let topology = settings.cluster_topology();

        info!(
            "Created GossipNode {} <{}> (Peer address: {}:{})",
            node_name.as_str(),
            node_id,
            settings.peer_listen_address,
            settings.peer_listen_port
        );

        let named_rate_limiters = HashMap::new();
        let rl_settings = settings.rate_limit_settings();
        let default_limiter = Arc::new(Mutex::new(DistributedBucketLimiter::new(
            node_id,
            rl_settings.clone(),
        )));
        named_rate_limiters
            .pin()
            .insert(DEFAULT_RULE_NAME.to_string(), default_limiter);

        let transport_config = settings.transport_config();
        let transport = UdpTransport::new(&transport_config).await?;
        let response_addr = settings.transport_config().peer_listen_url();

        // Create UDP receiver and channel
        let receiver_addr = settings.transport_config().peer_listen_url();
        let (message_tx, receive_chan) = tokio::sync::mpsc::channel(1000);
        let receiver = UdpReceiver::new(receiver_addr, Arc::new(message_tx)).await?;

        Ok(Self {
            node_name,
            node_id,
            topology: Arc::new(RwLock::new(topology)),
            transport: Arc::new(transport),
            named_rate_limiters,
            response_addr,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
            receiver_addr,
            receiver: Arc::new(receiver),
            receive_chan: Arc::new(Mutex::new(Some(receive_chan))),
            msg_receive_handle: Arc::new(Mutex::new(None)),
            msg_eval_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Get rate limiter Arc for a given rule name (or default if None)
    fn get_limiter(&self, rule_name: Option<&str>) -> Result<Arc<Mutex<DistributedBucketLimiter>>> {
        let key = rule_name.unwrap_or(DEFAULT_RULE_NAME);
        self.named_rate_limiters
            .pin()
            .get(key)
            .cloned()
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", key)))
    }

    async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        let limiter_arc = self.get_limiter(request.rule_name.as_ref().map(|r| r.as_str()))?;
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
            node_type: RunMode::Gossip,
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
            "Added node {} at {} to gossip topology",
            name.as_str(),
            address
        );
        self.transport.add_peer(address, 1).await?;

        Ok(())
    }

    async fn remove_node(&self, name: NodeName) -> Result<()> {
        let mut topology = self.topology.write().await;

        if let Some(address) = topology.remove_node(&name) {
            tracing::info!(
                "Removed node {} (was at {}) from gossip topology",
                name.as_str(),
                address
            );
            self.transport.remove_peer(address).await?;
        }

        Ok(())
    }

    async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        // Reject empty or whitespace-only rule names
        let rule_name = rule_name.trim();
        if rule_name.is_empty() {
            warn!(
                "[{}] Ignoring create_rate_limit_rule with empty rule name",
                self.node_id
            );
            return Ok(());
        }

        // Don't allow creating/overwriting the default rule
        if rule_name == DEFAULT_RULE_NAME {
            debug!(
                "[{}] Ignoring request to create/modify default rule",
                self.node_id
            );
            return Ok(());
        }

        let limiter = Arc::new(Mutex::new(DistributedBucketLimiter::new(
            self.node_id,
            settings.clone(),
        )));
        self.named_rate_limiters
            .pin()
            .insert(rule_name.to_string(), limiter);
        tracing::info!("Created named rule '{}' in gossip node", rule_name);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: RuleName) -> Result<()> {
        // Don't allow deleting the default rule
        if rule_name.as_str() == DEFAULT_RULE_NAME {
            return Err(ColibriError::Api(
                "Cannot delete the default rule".to_string(),
            ));
        }

        if self
            .named_rate_limiters
            .pin()
            .remove(rule_name.as_str())
            .is_some()
        {
            tracing::info!("Deleted named rule '{}' from gossip node", rule_name);
        } else {
            tracing::warn!("Attempted to delete non-existent rule '{}'", rule_name);
        }
        Ok(())
    }

    async fn get_rate_limit_rule(&self, rule_name: RuleName) -> Result<Option<SerializableRule>> {
        let key = rule_name.as_str();
        if let Some(limiter_arc) = self.named_rate_limiters.pin().get(key).cloned() {
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            let settings = limiter.get_settings().clone();
            return Ok(Some(SerializableRule {
                name: rule_name,
                settings,
            }));
        }
        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<SerializableRule>> {
        let guard = self.named_rate_limiters.pin();
        let mut rules = Vec::new();

        for (name, limiter_arc) in guard.iter() {
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            rules.push(SerializableRule {
                name: RuleName::from(name.as_str()),
                settings: limiter.get_settings().clone(),
            });
        }

        Ok(rules)
    }

    /// Start the UDP receiver and gossip packet processing loop
    pub async fn start_receiver(&self) -> Result<()> {
        // Start the UDP recv loop and store its handle
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
            info!("Gossip UDP receiver started on {}", receiver_addr);

            while let Some((data, peer_addr)) = receive_chan.recv().await {
                if let Err(e) = controller.process_gossip_packet(data, peer_addr).await {
                    debug!("Error processing gossip packet from {}: {}", peer_addr, e);
                }
            }

            info!("Gossip UDP receiver stopped");
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

    /// Stop both the UDP recv loop and the message evaluation loop
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
                info!("Gossip receiver stopped");
            }
        } else {
            error!("Failed to acquire lock on msg_eval_handle during shutdown");
        }
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
                debug!(
                    "[{}] Received CreateRateLimitRule message: rule_name='{}'",
                    self.node_id, rule.name,
                );
                self.create_rate_limit_rule(rule.name.as_str().to_string(), rule.settings)
                    .await?;
                Ok(Message::Ack)
            }

            Message::DeleteRateLimitRule { rule_name } => {
                self.delete_rate_limit_rule(rule_name).await?;
                Ok(Message::Ack)
            }

            Message::GetRateLimitRule { rule_name } => {
                let rule = self.get_rate_limit_rule(rule_name).await?;
                Ok(Message::GetRateLimitRuleResponse(rule))
            }

            Message::ListRateLimitRules => {
                let rules = self.list_rate_limit_rules().await?;
                Ok(Message::ListRateLimitRulesResponse(RuleList(rules)))
            }

            Message::GossipDeltaSync {
                updates,
                propagation_factor,
            } => {
                self.handle_delta_sync(updates, propagation_factor).await?;
                Ok(Message::Ack)
            }

            Message::GossipHeartbeat { timestamp, vclock } => {
                self.handle_heartbeat(timestamp, vclock).await?;
                Ok(Message::Ack)
            }

            Message::GossipStateRequest { missing_keys } => {
                let data = self.handle_state_request(missing_keys).await?;
                Ok(Message::GossipStateResponse { data })
            }

            _ => Err(ColibriError::Api(format!(
                "Unsupported message type for GossipController: {:?}",
                message
            ))),
        }
    }

    /// Gossip-specific: Handle delta synchronization
    async fn handle_delta_sync(
        &self,
        updates: Vec<DistributedBucketExternal>,
        propagation_factor: u8,
    ) -> Result<()> {
        {
            let limiter_arc = self.get_limiter(None)?;
            let mut limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            limiter.accept_delta_state(&updates);
            debug!(
                "[{}] Merged {} delta updates from gossip",
                self.node_id,
                updates.len()
            );
        }

        // Propagate to random peers if propagation_factor > 0
        if propagation_factor > 0 {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id,
                response_addr: self.response_addr,
                propagation_factor: propagation_factor - 1,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Gossip-specific: Handle heartbeat
    async fn handle_heartbeat(&self, timestamp: u64, _vclock: crdts::VClock<NodeId>) -> Result<()> {
        // Update peer liveness tracking
        // For now, just acknowledge receipt
        debug!(
            "[{}] Received heartbeat from peer at timestamp {}",
            self.node_id, timestamp
        );
        Ok(())
    }

    /// Gossip-specific: Handle state request
    async fn handle_state_request(
        &self,
        missing_keys: Option<Vec<String>>,
    ) -> Result<Vec<DistributedBucketExternal>> {
        let limiter_arc = self.get_limiter(None)?;
        let limiter = limiter_arc
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;

        if let Some(_keys) = missing_keys {
            debug!(
                "[{}] State request with specific keys - returning all state",
                self.node_id
            );
            Ok(limiter.gossip_delta_state())
        } else {
            debug!(
                "[{}] State request - returning all delta state",
                self.node_id
            );
            Ok(limiter.gossip_delta_state())
        }
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        // For gossip mode, assume true since GossipController is only used in gossip mode
        true
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = match self.get_limiter(None) {
            Ok(arc) => arc.lock().map_or(0, |l| l.len()),
            Err(_) => {
                error!("Failed to get default limiter");
                0
            }
        };
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_name.as_str(),
            bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            self.node_name.as_str(),
            self.gossip_interval_ms
        );

        let mut gossip_timer = time::interval(time::Duration::from_millis(self.gossip_interval_ms));
        let node_id = self.node_name.node_id();
        // We are taking a single reference to this mutex here
        loop {
            tokio::select! {
                // Gossip timer - send delta updates
                _ = gossip_timer.tick() => {
                    if let Err(e) = self.handle_gossip_tick().await {
                        debug!("[{}] Error during gossip tick: {}", node_id, e);
                    }
                }

                // Exit if inbound channel is closed
                else => {
                    info!("[{}] IO loop inbound channel closed, exiting", node_id);
                    break;
                }
            }
        }
    }

    pub async fn handle_gossip_tick(&self) -> Result<()> {
        // Send delta updates for buckets that have changed
        let updates = self.collect_gossip_updates().await?;
        if !updates.is_empty() {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id,
                response_addr: self.response_addr,
                propagation_factor: 1,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        } else {
            // Send heartbeat if no updates to share
            let vclock = {
                let limiter_arc = self.get_limiter(None).map_err(|_| {
                    tracing::error!("Failed to get default limiter");
                    crate::error::ColibriError::Concurrency(
                        "Failed to get default limiter".to_string(),
                    )
                })?;
                let guard = limiter_arc
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
                guard.get_latest_updated_vclock()
            };

            let message = GossipMessage::Heartbeat {
                node_id: self.node_id,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                vclock,
                response_addr: self.response_addr,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Collect buckets that have been updated and should be gossiped
    pub async fn collect_gossip_updates(&self) -> Result<Vec<DistributedBucketExternal>> {
        let limiter_arc = self.get_limiter(None)?;
        let guard = limiter_arc
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
        Ok(guard.gossip_delta_state())
    }

    /// Process an incoming gossip packet
    pub async fn process_gossip_packet(&self, data: Bytes, peer_addr: SocketAddr) -> Result<()> {
        match GossipPacket::deserialize(&data) {
            Ok(packet) => {
                match packet.message {
                    GossipMessage::StateRequest { .. } => {
                        debug!(
                            "[{}] Received StateRequest - not yet implemented",
                            self.node_id
                        );
                    }
                    GossipMessage::DeltaStateSync {
                        updates,
                        sender_node_id,
                        response_addr: _,
                        propagation_factor,
                    } => {
                        // Don't process our own messages
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        info!(
                            "[GOSSIP_SYNC] node:{} from:{} updates:{}",
                            self.node_id,
                            sender_node_id,
                            updates.len()
                        );
                        self.merge_gossip_state(&updates).await;
                        // Propagate further if propagation_factor > 0
                        if propagation_factor > 0 {
                            let message = GossipMessage::DeltaStateSync {
                                updates, // propagate the same updates to other nodes
                                sender_node_id,
                                response_addr: self.response_addr,
                                propagation_factor: propagation_factor - 1,
                            };
                            let packet = GossipPacket::new(message);
                            self.send_gossip_packet(packet).await?;
                        }
                    }
                    GossipMessage::StateResponse {
                        responding_node_id,
                        requested_data,
                        response_addr: _,
                    } => {
                        // Don't process our own messages
                        if responding_node_id == self.node_id {
                            return Ok(());
                        }

                        info!(
                            "[GOSSIP_RESP] node:{} from:{} client:{}",
                            self.node_id, responding_node_id, requested_data.client_id
                        );
                        let req = vec![requested_data];
                        // Merge each incoming update
                        self.merge_gossip_state(&req).await;
                    }
                    GossipMessage::Heartbeat {
                        node_id: heartbeat_node_id,
                        timestamp: _,
                        vclock: _,
                        response_addr: _,
                    } => {
                        // Don't process our own heartbeat
                        if heartbeat_node_id == self.node_id {
                            return Ok(());
                        }
                    }

                    // Handle rate limit configuration messages
                    GossipMessage::RateLimitConfigCreate {
                        sender_node_id,
                        rule,
                        timestamp: _,
                        response_addr: _,
                    } => {
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        let rule_name = rule.name.as_str().to_string();
                        if let Err(e) = self
                            .create_named_rule_local(rule_name.clone(), rule.settings)
                            .await
                        {
                            error!(
                                "[{}] Failed to apply gossiped rule creation for '{}': {}",
                                self.node_id, rule_name, e
                            );
                        } else {
                            info!(
                                "[{}] Applied gossiped rule creation: '{}'",
                                self.node_id, rule_name
                            );
                        }
                    }

                    GossipMessage::RateLimitConfigDelete {
                        sender_node_id,
                        rule_name,
                        timestamp: _,
                        response_addr: _,
                    } => {
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        if let Err(e) = self.delete_named_rule_local(rule_name.clone()).await {
                            debug!(
                                "[{}] Failed to apply gossiped rule deletion for '{}': {}",
                                self.node_id, rule_name, e
                            );
                        } else {
                            info!(
                                "[{}] Applied gossiped rule deletion: '{}'",
                                self.node_id, rule_name
                            );
                        }
                    }

                    GossipMessage::RateLimitConfigRequest {
                        requesting_node_id,
                        rule_name,
                        response_addr: _,
                    } => {
                        if requesting_node_id == self.node_id {
                            return Ok(());
                        }

                        let rules = if let Some(rn) = rule_name {
                            if let Ok(config_rules) = self.list_named_rules_local().await {
                                config_rules.into_iter().filter(|r| r.name == rn).collect()
                            } else {
                                vec![]
                            }
                        } else {
                            self.list_named_rules_local().await.unwrap_or_default()
                        };

                        let response_message = GossipMessage::RateLimitConfigResponse {
                            response_addr: self.response_addr,
                            responding_node_id: self.node_id,
                            rules: RuleList(rules),
                        };

                        let response_packet = GossipPacket::new(response_message);
                        if let Err(e) = self.send_gossip_packet(response_packet).await {
                            warn!("Failed to send gossip config response: {}", e);
                        }
                    }

                    GossipMessage::RateLimitConfigResponse {
                        responding_node_id,
                        rules,
                        response_addr: _,
                    } => {
                        if responding_node_id == self.node_id {
                            return Ok(());
                        }

                        for rule in rules {
                            let rule_name = rule.name.as_str().to_string();
                            if let Err(e) = self
                                .create_named_rule_local(rule_name.clone(), rule.settings)
                                .await
                            {
                                debug!(
                                    "[{}] Failed to apply received rule '{}': {}",
                                    self.node_id, rule_name, e
                                );
                            } else {
                                debug!("[{}] Applied received rule: '{}'", self.node_id, rule_name);
                            }
                        }
                    }
                };
                Ok(())
            }
            Err(e) => {
                debug!(
                    "[{}] Failed to deserialize packet: {} from {}",
                    self.node_id, e, peer_addr
                );
                Err(ColibriError::Transport(format!(
                    "Deserialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Send a gossip packet to random peers using transport layer
    pub async fn send_gossip_packet(&self, packet: GossipPacket) -> Result<()> {
        let topology = self.topology.read().await;
        let all_nodes = topology.all_nodes();

        let peers: Vec<SocketAddr> = all_nodes
            .iter()
            .filter(|(name, _)| name.as_str() != self.node_name.as_str())
            .map(|(_, addr)| *addr)
            .collect();

        if peers.is_empty() {
            debug!(
                "[{}] No peer nodes configured, skipping packet send",
                self.node_id
            );
            return Ok(());
        }

        // Serialize the packet
        let data = packet.serialize().map_err(|e| {
            debug!("[{}] Failed to serialize packet: {}", self.node_id, e);
            ColibriError::Transport(format!("Serialization failed: {}", e))
        })?;

        // Send to multiple random peers up to gossip_fanout
        let send_count = self.gossip_fanout.min(peers.len());

        // Select random peers - must complete before any await to avoid Send issues
        let selected_peers: Vec<_> = {
            use rand::prelude::IndexedRandom;
            let mut rng = rand::rng();
            peers
                .choose_multiple(&mut rng, send_count)
                .copied()
                .collect()
        }; // RNG is dropped here, before any await

        let mut sent_count = 0;
        for peer in selected_peers {
            match self.transport.send_to_peer(peer, &data).await {
                Ok(_) => sent_count += 1,
                Err(e) => debug!(
                    "[{}] Failed to send gossip to {}: {}",
                    self.node_id, peer, e
                ),
            }
        }

        if sent_count > 0 {
            debug!(
                "[{}] Sent gossip packet to {} peers",
                self.node_id, sent_count
            );
            Ok(())
        } else {
            Err(ColibriError::Transport(
                "Failed to send to any peers".to_string(),
            ))
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub async fn merge_gossip_state(&self, entries: &[DistributedBucketExternal]) {
        let node_id = self.node_id;
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );
        match self.get_limiter(None) {
            Ok(arc) => match arc.lock() {
                Ok(mut limiter) => {
                    limiter.accept_delta_state(entries);
                    debug!("[{}] merge_gossip_state_static completed", node_id);
                }
                Err(e) => {
                    error!("[{}] Failed to acquire limiter lock: {}", node_id, e);
                }
            },
            Err(e) => {
                error!("[{}] Failed to get default limiter: {}", node_id, e);
            }
        }
    }

    /// Create a named rate limit rule locally (without gossiping)
    pub async fn create_named_rule_local(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        if rule_name.is_empty() {
            warn!(
                "[{}] Received gossip request to create rule with empty name, ignoring",
                self.node_id
            );
            return Ok(());
        }

        // Check if already exists
        if self.get_named_rule_local(&rule_name).await?.is_some() {
            return Ok(());
        }

        let rate_limiter = Arc::new(Mutex::new(DistributedBucketLimiter::new(
            self.node_id,
            settings,
        )));
        self.named_rate_limiters
            .pin()
            .insert(rule_name.clone(), rate_limiter);

        Ok(())
    }

    /// Get a named rate limit rule locally (without gossiping)
    pub async fn get_named_rule_local(&self, rule_name: &str) -> Result<Option<SerializableRule>> {
        if let Some(limiter_arc) = self.named_rate_limiters.pin().get(rule_name).cloned() {
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            let settings = limiter.get_settings().clone();
            return Ok(Some(SerializableRule {
                name: RuleName::from(rule_name),
                settings,
            }));
        }

        Ok(None)
    }

    /// Delete a named rate limit rule locally (without gossiping)
    pub async fn delete_named_rule_local(&self, rule_name: RuleName) -> Result<()> {
        if rule_name.as_str() == DEFAULT_RULE_NAME {
            return Err(ColibriError::Api(
                "Cannot delete the default rule".to_string(),
            ));
        }

        if self
            .named_rate_limiters
            .pin()
            .remove(rule_name.as_str())
            .is_some()
        {
            Ok(())
        } else {
            Err(ColibriError::Api(format!("Rule '{}' not found", rule_name)))
        }
    }

    /// List all named rate limit rules locally
    pub async fn list_named_rules_local(&self) -> Result<Vec<SerializableRule>> {
        let guard = self.named_rate_limiters.pin();
        let mut rules = Vec::new();

        for (name, limiter_arc) in guard.iter() {
            let limiter = limiter_arc
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            rules.push(SerializableRule {
                name: RuleName::from(name.as_str()),
                settings: limiter.get_settings().clone(),
            });
        }

        Ok(rules)
    }

    /// Apply rate limiting using a custom named rule locally
    pub async fn rate_limit_custom_local(
        &self,
        rule_name: RuleName,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let limiter_arc = self.get_limiter(Some(rule_name.as_str()))?;
        let mut limiter = limiter_arc
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;

        let calls_left = limiter.limit_calls_for_client(key.clone());
        calls_left
            .map(|calls_remaining| {
                Ok(Some(CheckCallsResponse {
                    client_id: key,
                    calls_remaining,
                    rule_name: Some(rule_name),
                }))
            })
            .unwrap_or_else(|| Ok(None))
    }

    /// Check remaining calls using a custom named rule locally
    pub async fn check_limit_custom_local(
        &self,
        rule_name: RuleName,
        key: String,
    ) -> Result<CheckCallsResponse> {
        let limiter_arc = self.get_limiter(Some(rule_name.as_str()))?;
        let limiter = limiter_arc
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
        let calls_remaining = limiter.check_calls_remaining_for_client(&key);
        Ok(CheckCallsResponse {
            client_id: key,
            calls_remaining,
            rule_name: Some(rule_name),
        })
    }
}

// ===== Tests temporarily disabled =====
