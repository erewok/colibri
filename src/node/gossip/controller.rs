use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::{
    distributed_bucket::{DistributedBucketExternal, DistributedBucketLimiter},
    NamedRateLimitRule,
};
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, Status, StatusResponse, TopologyResponse,
};
use crate::node::{NodeAddress, NodeId, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::traits::Sender;
use crate::transport::{tcp_receiver::TcpRequest, TcpReceiver, TcpTransport};

use super::{GossipMessage, GossipPacket};

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_name: NodeName,
    node_id: NodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<TcpTransport>,
    /// Lock-free concurrent HashMap for rate limiters (DashMap provides interior mutability)
    pub named_rate_limiters: DashMap<String, DistributedBucketLimiter>,
    pub response_addr: SocketAddr,
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
    /// Receiver address for logging
    receiver_addr: SocketAddr,
    /// Receiver for incoming TCP messages
    receiver: Arc<TcpReceiver>,
    /// Channel ownership transfer pattern
    receive_chan: Arc<Mutex<Option<mpsc::Receiver<TcpRequest>>>>,
    /// Spawned task handle for message processing
    pub receiver_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Shutdown signaling
    #[allow(dead_code)]
    shutdown_tx: broadcast::Sender<()>,
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

        // Initialize DashMap with default limiter (DashMap provides interior mutability)
        let named_rate_limiters = DashMap::new();
        let rl_settings = settings.rate_limit_settings();
        let default_limiter = DistributedBucketLimiter::new(node_id, rl_settings.clone());
        named_rate_limiters.insert("<_default>".to_string(), default_limiter);

        let transport_config = settings.transport_config();
        let transport = TcpTransport::new(&transport_config).await?;
        let response_addr = settings.transport_config().peer_listen_url();

        // Create TCP receiver and channel
        let receiver_addr = settings.transport_config().peer_listen_url();
        let (message_tx, receive_chan) = tokio::sync::mpsc::channel(1000);
        let receiver = TcpReceiver::new(receiver_addr, Arc::new(message_tx)).await?;

        // Create shutdown channel
        let (shutdown_tx, _) = broadcast::channel(1);

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
            receiver_handle: Arc::new(Mutex::new(None)),
            shutdown_tx,
        })
    }

    /// Get mutable reference to rate limiter for a given rule name (or default if None)
    /// Uses DashMap's interior mutability via get_mut() for lock-free access
    fn get_limiter_mut(
        &self,
        rule_name: Option<String>,
    ) -> Result<dashmap::mapref::one::RefMut<'_, String, DistributedBucketLimiter>> {
        let rule_name = rule_name.unwrap_or_else(|| "<_default>".to_string());

        // DashMap provides lock-free mutable access via RefMut
        self.named_rate_limiters
            .get_mut(&rule_name)
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", rule_name)))
    }

    async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        // Use DashMap's interior mutability - no external lock needed
        let mut limiter = self.get_limiter_mut(request.rule_name.clone())?;

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
        self.transport.add_peer(name.node_id(), address).await?;

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
            self.transport.remove_peer(name.node_id()).await?;
        }

        Ok(())
    }

    async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        // Handle empty rule name gracefully - treat as default
        let rule_name = if rule_name.is_empty() {
            warn!(
                "[{}] Received create_rate_limit_rule with empty rule name, treating as default",
                self.node_id
            );
            "default".to_string()
        } else {
            rule_name
        };

        // Don't allow creating/overwriting the default rule
        if rule_name == "default" || rule_name == "<_default>" {
            debug!(
                "[{}] Ignoring request to create/modify default rule",
                self.node_id
            );
            return Ok(());
        }

        let limiter = DistributedBucketLimiter::new(self.node_id, settings.clone());
        self.named_rate_limiters.insert(rule_name.clone(), limiter);
        tracing::info!("Created named rule '{}' in gossip node", rule_name);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: String) -> Result<()> {
        // Don't allow deleting the default rule
        if rule_name == "default" || rule_name == "<_default>" {
            return Err(ColibriError::Api(
                "Cannot delete the default rule".to_string(),
            ));
        }

        if self.named_rate_limiters.remove(&rule_name).is_some() {
            tracing::info!("Deleted named rule '{}' from gossip node", rule_name);
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

        // Use DashMap's immutable Ref - no external lock needed
        if let Some(limiter_ref) = self.named_rate_limiters.get(&internal_key) {
            let settings = limiter_ref.get_settings().clone();

            return Ok(Some(NamedRateLimitRule {
                name: rule_name, // Return user-facing name "default", not internal "<_default>"
                settings,
            }));
        }

        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let mut rules = Vec::new();

        // DashMap iter returns immutable Refs - no external lock needed
        for entry in self.named_rate_limiters.iter() {
            let name = entry.key();
            let limiter = entry.value();

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

    /// Start the TCP receiver and message processing loop
    pub async fn start_receiver(&self) -> Result<()> {
        // Start the TCP receiver
        self.receiver.start().await;

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
            info!("Gossip TCP receiver started on {}", receiver_addr);

            while let Some(request) = receive_chan.recv().await {
                // Deserialize incoming message
                match Message::deserialize(&request.data) {
                    Ok(message) => {
                        // Handle message via controller
                        let response = match controller.handle_message(message).await {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Error handling gossip message: {}", e);
                                continue;
                            }
                        };

                        // Send response only if response_tx is present (request-response protocol)
                        if let Some(response_tx) = request.response_tx {
                            // Serialize and send response
                            match response.serialize() {
                                Ok(response_data) => {
                                    let response_vec = response_data.to_vec();
                                    if response_tx.send(response_vec).is_err() {
                                        warn!(
                                            "Failed to send response back to peer (channel closed)"
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize response: {}", e);
                                }
                            }
                        } else {
                            // Fire-and-forget message, no response needed
                            debug!(
                                "Processed fire-and-forget message from {}",
                                request.peer_addr
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize message: {}", e);
                    }
                }
            }

            info!("Gossip TCP receiver stopped");
        });

        // Store the handle
        {
            let mut receiver_handle = self
                .receiver_handle
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
            *receiver_handle = Some(handle);
        }

        Ok(())
    }

    /// Stop the TCP receiver task
    pub fn stop_receiver(&self) {
        if let Ok(mut receiver_handle) = self.receiver_handle.lock() {
            if let Some(handle) = receiver_handle.take() {
                handle.abort();
                info!("Gossip TCP receiver task stopped");
            }
        } else {
            error!("Failed to acquire lock on receiver_handle during shutdown");
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

            Message::CreateRateLimitRule {
                rule_name,
                settings,
            } => {
                debug!(
                    "[{}] Received CreateRateLimitRule message: rule_name='{}' (len={})",
                    self.node_id,
                    rule_name,
                    rule_name.len()
                );
                self.create_rate_limit_rule(rule_name, settings).await?;
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
                Ok(Message::ListRateLimitRulesResponse(rules))
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
        // Merge updates into local rate limiter using DashMap's interior mutability
        {
            let mut limiter = self.get_limiter_mut(None)?;
            limiter.accept_delta_state(&updates);
            debug!(
                "[{}] Merged {} delta updates from gossip",
                self.node_id,
                updates.len()
            );
        } // RefMut dropped here

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
        let limiter = self
            .named_rate_limiters
            .get("<_default>")
            .ok_or_else(|| ColibriError::Api("Default rate limiter not found".to_string()))?;

        if let Some(_keys) = missing_keys {
            // For now, return all delta state since we don't have per-key lookup
            // TODO: Implement selective key export if needed
            debug!(
                "[{}] State request with specific keys - returning all state",
                self.node_id
            );
            Ok(limiter.gossip_delta_state())
        } else {
            // Return all buckets that have been updated
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
        let bucket_count = match self.named_rate_limiters.get("<_default>") {
            Some(limiter) => limiter.len(),
            None => {
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
                let limiter = self.named_rate_limiters.get("<_default>").ok_or_else(|| {
                    tracing::error!("Failed to get default limiter");
                    crate::error::ColibriError::Concurrency(
                        "Failed to get default limiter".to_string(),
                    )
                })?;
                limiter.get_latest_updated_vclock()
            }; // Ref dropped here

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
        let rl = self
            .named_rate_limiters
            .get("<_default>")
            .ok_or_else(|| ColibriError::Api("Default rate limiter not found".to_string()))?;
        Ok(rl.gossip_delta_state())
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
                        // TODO: Implement state request handling
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
                        rule_name,
                        settings,
                        timestamp: _,
                        response_addr: _,
                    } => {
                        // Don't process our own messages
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        // Apply the rule creation locally
                        if let Err(e) = self
                            .create_named_rule_local(rule_name.clone(), settings)
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
                        // Don't process our own messages
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }

                        // Apply the rule deletion locally
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
                        // Don't process our own requests
                        if requesting_node_id == self.node_id {
                            return Ok(());
                        }

                        // Send back the requested rule(s)
                        let rules = if let Some(rule_name) = rule_name {
                            // Send specific rule
                            if let Ok(config_rules) = self.list_named_rules_local().await {
                                config_rules
                                    .into_iter()
                                    .filter(|r| r.name == rule_name)
                                    .collect()
                            } else {
                                vec![]
                            }
                        } else {
                            // Send all rules
                            self.list_named_rules_local().await.unwrap_or_default()
                        };

                        let response_message = GossipMessage::RateLimitConfigResponse {
                            response_addr: self.response_addr,
                            responding_node_id: self.node_id,
                            rules,
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
                        // Don't process our own responses
                        if responding_node_id == self.node_id {
                            return Ok(());
                        }

                        // Apply the received rules locally
                        for rule in rules {
                            if let Err(e) = self
                                .create_named_rule_local(rule.name.clone(), rule.settings)
                                .await
                            {
                                debug!(
                                    "[{}] Failed to apply received rule '{}': {}",
                                    self.node_id, rule.name, e
                                );
                            } else {
                                debug!("[{}] Applied received rule: '{}'", self.node_id, rule.name);
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
            match self.transport.send_fire_and_forget(peer, &data).await {
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
        match self.get_limiter_mut(None) {
            Ok(mut limiter) => {
                limiter.accept_delta_state(entries);
                debug!("[{}] merge_gossip_state_static completed", node_id);
            }
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
        // Validate rule name
        if rule_name.is_empty() {
            warn!(
                "[{}] Received gossip request to create rule with empty name, ignoring",
                self.node_id
            );
            return Ok(());
        }

        // Translate "default" to internal key "<_default>"
        let internal_key = if rule_name == "default" {
            "<_default>".to_string()
        } else {
            rule_name.clone()
        };

        // Check if already exists
        if self.get_named_rule_local(&rule_name).await?.is_some() {
            return Ok(());
        }

        // Create the rate limiter for this rule and insert into DashMap
        let rate_limiter = DistributedBucketLimiter::new(self.node_id, settings);
        self.named_rate_limiters.insert(internal_key, rate_limiter);

        Ok(())
    }
    /// Get a named rate limit rule locally (without gossiping)
    pub async fn get_named_rule_local(
        &self,
        rule_name: &str,
    ) -> Result<Option<NamedRateLimitRule>> {
        // Translate "default" to internal key "<_default>"
        let internal_key = if rule_name == "default" {
            "<_default>".to_string()
        } else {
            rule_name.to_string()
        };

        if let Some(limiter_ref) = self.named_rate_limiters.get(&internal_key) {
            let settings = limiter_ref.get_settings().clone();

            return Ok(Some(NamedRateLimitRule {
                name: rule_name.to_string(),
                settings,
            }));
        }

        Ok(None)
    }

    /// Delete a named rate limit rule locally (without gossiping)
    pub async fn delete_named_rule_local(&self, rule_name: String) -> Result<()> {
        // Don't allow deleting the default rule
        if rule_name == "default" || rule_name == "<_default>" {
            return Err(ColibriError::Api(
                "Cannot delete the default rule".to_string(),
            ));
        }

        if self.named_rate_limiters.remove(&rule_name).is_some() {
            Ok(())
        } else {
            Err(ColibriError::Api(format!("Rule '{}' not found", rule_name)))
        }
    }

    /// List all named rate limit rules locally
    pub async fn list_named_rules_local(&self) -> Result<Vec<NamedRateLimitRule>> {
        let mut rules = Vec::new();

        for entry in self.named_rate_limiters.iter() {
            let name = entry.key();
            let limiter = entry.value();

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

    /// Apply rate limiting using a custom named rule locally
    pub async fn rate_limit_custom_local(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let mut limiter = self.get_limiter_mut(Some(rule_name.clone()))?;

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
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        let rule_key = rule_name.clone();
        let limiter = self.named_rate_limiters.get(&rule_key).ok_or_else(|| {
            ColibriError::Api(format!("Rate limit rule '{}' not found", rule_key))
        })?;

        let calls_remaining = limiter.check_calls_remaining_for_client(&key);
        Ok(CheckCallsResponse {
            client_id: key,
            calls_remaining,
            rule_name: Some(rule_name),
        })
    }
}

// ===== Tests temporarily disabled =====
