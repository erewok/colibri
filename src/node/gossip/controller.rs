use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::{NamedRateLimitRule, RateLimitConfig, distributed_bucket::{DistributedBucketExternal, DistributedBucketLimiter}};
use crate::node::messages::{CheckCallsRequest, CheckCallsResponse, Message, Status, StatusResponse, TopologyResponse};
use crate::node::{NodeAddress, NodeId, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::TcpTransport;
use crate::transport::traits::Sender;
use tokio::sync::RwLock;

use super::{GossipMessage, GossipPacket};

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_name: NodeName,
    node_id: NodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<TcpTransport>,
    pub rate_limit_settings: settings::RateLimitSettings,
    pub rate_limiter: Arc<Mutex<DistributedBucketLimiter>>,
    pub rate_limit_config: Arc<Mutex<RateLimitConfig>>,
    pub named_rate_limiters:
        Arc<Mutex<std::collections::HashMap<String, DistributedBucketLimiter>>>,
    pub response_addr: SocketAddr,
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
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
            node_name.as_str(), node_id, settings.peer_listen_address, settings.peer_listen_port
        );

        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = DistributedBucketLimiter::new(node_name.node_id(), rl_settings.clone());

        let transport_config = settings.transport_config();
        let transport = TcpTransport::new(&transport_config).await?;
        let response_addr = settings.transport_config().peer_listen_url();

        Ok(Self {
            node_name,
            node_id,
            topology: Arc::new(RwLock::new(topology)),
            transport: Arc::new(transport),
            rate_limit_settings: rl_settings.clone(),
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(Mutex::new(RateLimitConfig::new(rl_settings))),
            named_rate_limiters: Arc::new(Mutex::new(std::collections::HashMap::new())),
            response_addr,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
        })
    }

    async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        let mut limiter = self.rate_limiter.lock().await;

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

        tracing::info!("Added node {} at {} to gossip topology", name.as_str(), address);
        self.transport.add_peer(name.node_id(), address).await?;

        Ok(())
    }

    async fn remove_node(&self, name: NodeName) -> Result<()> {
        let mut topology = self.topology.write().await;

        if let Some(address) = topology.remove_node(&name) {
            tracing::info!("Removed node {} (was at {}) from gossip topology", name.as_str(), address);
            self.transport.remove_peer(name.node_id()).await?;
        }

        Ok(())
    }

    async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        settings: RateLimitSettings,
    ) -> Result<()> {
        let mut named_limiters = self.named_rate_limiters.lock().await;
        let limiter = DistributedBucketLimiter::new(self.node_id, settings.clone());
        named_limiters.insert(rule_name.clone(), limiter);
        tracing::info!("Created named rule '{}' in gossip node", rule_name);
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: String) -> Result<()> {
        let mut named_limiters = self.named_rate_limiters.lock().await;
        if named_limiters.remove(&rule_name).is_some() {
            tracing::info!("Deleted named rule '{}' from gossip node", rule_name);
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

        let named_limiters = self.named_rate_limiters.lock().await;
        if named_limiters.contains_key(&rule_name) {
            tracing::warn!("Rule '{}' exists but settings not retrievable yet", rule_name);
        }

        Ok(None)
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        let rules = vec![
            NamedRateLimitRule {
                name: "default".to_string(),
                settings: self.rate_limit_settings.clone(),
            }
        ];

        let named_limiters = self.named_rate_limiters.lock().await;
        for name in named_limiters.keys() {
            tracing::debug!("Named rule exists but settings not retrievable: {}", name);
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

            Message::GossipDeltaSync { updates, propagation_factor } => {
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
        // Merge updates into local rate limiter
        let mut limiter = self.rate_limiter
            .lock()
            .await;

        limiter.accept_delta_state(&updates);
        debug!("[{}] Merged {} delta updates from gossip", self.node_id, updates.len());

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
    async fn handle_heartbeat(
        &self,
        timestamp: u64,
        _vclock: crdts::VClock<NodeId>,
    ) -> Result<()> {
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
        let limiter = self.rate_limiter
            .lock()
            .await;

        if let Some(_keys) = missing_keys {
            // For now, return all delta state since we don't have per-key lookup
            // TODO: Implement selective key export if needed
            debug!("[{}] State request with specific keys - returning all state", self.node_id);
            Ok(limiter.gossip_delta_state())
        } else {
            // Return all buckets that have been updated
            debug!("[{}] State request - returning all delta state", self.node_id);
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
        let bucket_count = self.rate_limiter.lock().await.len();
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_name.as_str(), bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            self.node_name.as_str(), self.gossip_interval_ms
        );

        let mut gossip_timer = time::interval(time::Duration::from_millis(self.gossip_interval_ms));
        let node_id = self.node_name.node_id();
        // We are taking a single reference to this mutex here
        loop {
            tokio::select! {
                // Handle incoming messages from network
                // TODO: Re-enable when network receiver is implemented
                // Some(cmd) = gossip_command_rx.recv() => {
                //     if let Err(e) = self.handle_command(cmd).await {
                //         debug!("[{}] Error processing incoming message: {}", node_id, e);
                //     }
                // }
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
            let message = GossipMessage::Heartbeat {
                node_id: self.node_id,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                vclock: self
                    .rate_limiter
                    .lock()
                    .await
                    .get_latest_updated_vclock(),
                response_addr: self.response_addr,
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Collect buckets that have been updated and should be gossiped
    pub async fn collect_gossip_updates(&self) -> Result<Vec<DistributedBucketExternal>> {
        let rl = self.rate_limiter
            .lock()
            .await;
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
        let rate_limiter = Arc::clone(&self.rate_limiter);
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );
        rate_limiter
            .lock()
            .await
            .accept_delta_state(entries);
        debug!("[{}] merge_gossip_state_static completed", node_id);
    }

    /// Create a named rate limit rule locally (without gossiping)
    pub async fn create_named_rule_local(
        &self,
        rule_name: String,
        settings: settings::RateLimitSettings,
    ) -> Result<()> {
        // first check if already exists
        if self.get_named_rule_local(&rule_name).await?.is_some() {
            return Ok(());
        }

        let mut config = self
            .rate_limit_config
            .lock()
            .await;
        let rule = NamedRateLimitRule {
            name: rule_name.clone(),
            settings: settings.clone(),
        };
        config.add_named_rule(&rule);

        // Create the rate limiter for this rule
        let mut limiters = self
            .named_rate_limiters
            .lock()
            .await;

        let rate_limiter = DistributedBucketLimiter::new(self.node_id, settings);
        limiters.insert(rule_name, rate_limiter);

        Ok(())
    }
    /// Get a named rate limit rule locally (without gossiping)
    pub async fn get_named_rule_local(
        &self,
        rule_name: &str,
    ) -> Result<Option<NamedRateLimitRule>> {
        let rlconf = self.rate_limit_config
            .lock()
            .await;
        Ok(rlconf
            .get_named_rule_settings(rule_name)
            .cloned()
            .map(|rl_settings| NamedRateLimitRule {
                name: rule_name.to_string(),
                settings: rl_settings,
            }))
    }

    /// Delete a named rate limit rule locally (without gossiping)
    pub async fn delete_named_rule_local(&self, rule_name: String) -> Result<()> {
        let mut config = self
            .rate_limit_config
            .lock()
            .await;

        if config.remove_named_rule(&rule_name).is_some() {
            // Remove the rate limiter for this rule
            let mut limiters = self.named_rate_limiters.lock().await;

            limiters.remove(&rule_name);
            Ok(())
        } else {
            Err(ColibriError::Api(format!("Rule '{}' not found", rule_name)))
        }
    }

    /// List all named rate limit rules locally
    pub async fn list_named_rules_local(&self) -> Result<Vec<NamedRateLimitRule>> {
        let config = self
            .rate_limit_config
            .lock()
            .await;

        Ok(config.list_named_rules())
    }

    /// Apply rate limiting using a custom named rule locally
    pub async fn rate_limit_custom_local(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let mut limiters = self
            .named_rate_limiters
            .lock()
            .await;

        if let Some(rate_limiter) = limiters.get_mut(&rule_name) {
            let calls_left = rate_limiter.limit_calls_for_client(key.clone());
            calls_left
                .map(|calls_remaining| {
                    Ok(Some(CheckCallsResponse {
                        client_id: key,
                        calls_remaining,
                        rule_name: Some(rule_name),
                    }))
                })
                .unwrap_or_else(|| Ok(None))
        } else {
            Err(ColibriError::Api(format!(
                "Rate limit rule '{}' not found",
                rule_name
            )))
        }
    }

    /// Check remaining calls using a custom named rule locally
    pub async fn check_limit_custom_local(
        &self,
        rule_name: String,
        key: String,
    ) -> Result<CheckCallsResponse> {
        let limiters = self
            .named_rate_limiters
            .lock()
            .await;

        if let Some(rate_limiter) = limiters.get(&rule_name) {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(&key);
            Ok(CheckCallsResponse {
                client_id: key,
                calls_remaining,
                rule_name: Some(rule_name),
            })
        } else {
            Err(ColibriError::Api(format!(
                "Rate limit rule '{}' not found",
                rule_name
            )))
        }
    }
}

// ===== Tests temporarily disabled =====



