use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use super::{GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::distributed_bucket::{DistributedBucketExternal, DistributedBucketLimiter};
use crate::node::{CheckCallsResponse, NodeId, NodeName, commands::ClusterCommand};
use crate::settings;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_name: NodeName,
    node_id: NodeId,
    // rate-limit settings
    pub rate_limit_settings: settings::RateLimitSettings,
    /// Local rate limiter - handles all bucket operations
    pub rate_limiter: Arc<Mutex<DistributedBucketLimiter>>,
    /// Named rate limit configurations
    pub rate_limit_config: Arc<Mutex<settings::RateLimitConfig>>,
    /// Named rate limiters for custom configurations
    pub named_rate_limiters:
        Arc<Mutex<std::collections::HashMap<String, DistributedBucketLimiter>>>,
    pub response_addr: SocketAddr,
    /// Gossip configuration
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
    /// Cluster membership management - handles UDP transport
    pub cluster_member: Arc<dyn crate::cluster::ClusterMember>,
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
        info!(
            "Created GossipNode {} <{}> (Peer address: {}:{})",
            node_name.as_str(), node_id, settings.peer_listen_address, settings.peer_listen_port
        );
        let rl_settings = settings.rate_limit_settings();
        let rate_limiter = DistributedBucketLimiter::new(node_name.node_id(), rl_settings.clone());
        let rl_settings_clone = rl_settings.clone();

        // Create cluster member using factory - unified UDP transport for cluster operations
        let cluster_member =
            crate::cluster::ClusterFactory::create_from_settings(&settings).await?;

        // Use UDP listen address for response_addr in gossip messages
        let response_addr = settings.transport_config().peer_listen_url();

        Ok(Self {
            node_name,
            node_id,
            rate_limit_settings: rl_settings,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            rate_limit_config: Arc::new(Mutex::new(settings::RateLimitConfig::new(
                rl_settings_clone,
            ))),
            named_rate_limiters: Arc::new(Mutex::new(std::collections::HashMap::new())),
            response_addr,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
            cluster_member,
        })
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        // For gossip mode, assume true since GossipController is only used in gossip mode
        true
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.rate_limiter.lock().map(|rl| rl.len()).unwrap_or(0);
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_name.as_str(), bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self, mut gossip_command_rx: mpsc::Receiver<ClusterMessage>) {
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
                Some(cmd) = gossip_command_rx.recv() => {
                    if let Err(e) = self.handle_command(cmd).await {
                        debug!("[{}] Error processing incoming message: {}", node_id, e);
                    }
                }
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
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?
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
        self.rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
            .map(|rl| rl.gossip_delta_state())
    }
    /// Handle incoming gossip cmd from our channel
    pub async fn handle_command(&self, cmd: ClusterMessage) -> Result<()> {
        match cmd {
            ClusterMessage::ExpireKeys => {
                self.rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
                    .map(|mut rl| rl.expire_keys())?;
                Ok(())
            }
            ClusterMessage:: { data, peer_addr } => {
                self.process_gossip_packet(data, peer_addr).await
            }
            GossipCommand::CheckLimit {
                client_id,
                resp_chan,
            } => {
                let rl = self
                    .rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;
                let calls_remaining = rl.check_calls_remaining_for_client(&client_id);
                info!(
                    "[GOSSIP_CHECK] node:{} client:{} remaining:{}",
                    self.node_id, client_id, calls_remaining
                );

                if resp_chan
                    .send(Ok(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                    }))
                    .is_err()
                {
                    error!(
                        "[{}] Failed sending oneshot check_limit response",
                        self.node_id
                    );
                }
                Ok(())
            }
            GossipCommand::RateLimit {
                client_id,
                resp_chan,
            } => {
                let dstate: Option<DistributedBucketExternal>;
                // let mutable result with MutexGuard drop after this block
                {
                    let mut result = self
                        .rate_limiter
                        .lock()
                        .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;

                    let calls_left = result.limit_calls_for_client(client_id.to_string());
                    let response: Option<CheckCallsResponse>;
                    if let Some(calls_remaining) = calls_left {
                        response = Some(CheckCallsResponse {
                            client_id: client_id.clone(),
                            calls_remaining,
                        });
                        info!(
                            "[GOSSIP_LIMIT] node: {} client:{} remaining:{} allowed:true",
                            self.node_id, client_id, calls_remaining
                        );
                    } else {
                        response = None;
                        info!(
                            "[GOSSIP_LIMIT] node:{} client:{} allowed:false",
                            self.node_id, client_id
                        );
                    }
                    if resp_chan.send(Ok(response)).is_err() {
                        error!(
                            "[{}] Failed sending oneshot rate_limit response",
                            self.node_id
                        );
                    }
                    dstate = result.client_delta_state_for_gossip(&client_id);
                }
                // Send immediate gossip update after rate limiting
                if let Some(dstate) = dstate {
                    let message = GossipMessage::DeltaStateSync {
                        updates: vec![dstate],
                        sender_node_id: self.node_id,
                        response_addr: self.response_addr,
                        propagation_factor: 3, // Limit spread
                    };
                    let packet = GossipPacket::new(message);
                    self.send_gossip_packet(packet).await?;
                }
                Ok(())
            }

            // Handle rate limit configuration commands
            GossipCommand::CreateNamedRule {
                rule_name,
                settings,
                resp_chan,
            } => {
                let result = self
                    .create_named_rule_local(rule_name.clone(), settings.clone())
                    .await;
                if result.is_ok() {
                    // Gossip the new rule to other nodes
                    let message = GossipMessage::RateLimitConfigCreate {
                        response_addr: self.response_addr,
                        sender_node_id: self.node_id,
                        rule_name,
                        settings,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    };
                    let packet = GossipPacket::new(message);
                    if let Err(e) = self.send_gossip_packet(packet).await {
                        error!(
                            "[{}] Failed to gossip new rule creation: {}",
                            self.node_id, e
                        );
                    }
                }
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending create_named_rule response",
                        self.node_id
                    );
                }
                Ok(())
            }
            GossipCommand::GetNamedRule {
                rule_name,
                resp_chan,
            } => {
                let result = self.get_named_rule_local(&rule_name).await;
                if resp_chan.send(result).is_err() {
                    error!("[{}] Failed sending get_named_rule response", self.node_id);
                }
                Ok(())
            }

            GossipCommand::DeleteNamedRule {
                rule_name,
                resp_chan,
            } => {
                let result = self.delete_named_rule_local(rule_name.clone()).await;
                if result.is_ok() {
                    // Gossip the rule deletion to other nodes
                    let message = GossipMessage::RateLimitConfigDelete {
                        response_addr: self.response_addr,
                        sender_node_id: self.node_id,
                        rule_name,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    };
                    let packet = GossipPacket::new(message);
                    if let Err(e) = self.send_gossip_packet(packet).await {
                        error!("[{}] Failed to gossip rule deletion: {}", self.node_id, e);
                    }
                }
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending delete_named_rule response",
                        self.node_id
                    );
                }
                Ok(())
            }

            GossipCommand::ListNamedRules { resp_chan } => {
                let result = self.list_named_rules_local().await;
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending list_named_rules response",
                        self.node_id
                    );
                }
                Ok(())
            }

            GossipCommand::RateLimitCustom {
                rule_name,
                key,
                resp_chan,
            } => {
                let result = self.rate_limit_custom_local(rule_name, key).await;
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending rate_limit_custom response",
                        self.node_id
                    );
                }
                Ok(())
            }

            GossipCommand::CheckLimitCustom {
                rule_name,
                key,
                resp_chan,
            } => {
                let result = self.check_limit_custom_local(rule_name, key).await;
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending check_limit_custom response",
                        self.node_id
                    );
                }
                Ok(())
            }

            // Cluster management commands - delegate to cluster_member
            GossipCommand::GetClusterNodes { resp_chan } => {
                let result = Ok(self.cluster_member.get_cluster_nodes().await);
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending get_cluster_nodes response",
                        self.node_id
                    );
                }
                Ok(())
            }

            GossipCommand::AddClusterNode { address, resp_chan } => {
                self.cluster_member.add_node(address).await;
                let result = Ok(());
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending add_cluster_node response",
                        self.node_id
                    );
                }
                Ok(())
            }

            GossipCommand::RemoveClusterNode { address, resp_chan } => {
                self.cluster_member.remove_node(address).await;
                let result = Ok(());
                if resp_chan.send(result).is_err() {
                    error!(
                        "[{}] Failed sending remove_cluster_node response",
                        self.node_id
                    );
                }
                Ok(())
            }
        }
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
                            error!("[{}] Failed to send config response: {}", self.node_id, e);
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

    /// Send a gossip packet to random peers using cluster_member
    pub async fn send_gossip_packet(&self, packet: GossipPacket) -> Result<()> {
        let cluster_nodes = self.cluster_member.get_cluster_nodes().await;
        if cluster_nodes.is_empty() {
            debug!(
                "[{}] No cluster nodes configured, skipping packet send",
                self.node_id
            );
            return Ok(());
        }

        match packet.serialize() {
            Ok(data) => {
                // Send to multiple random peers up to gossip_fanout
                let send_count = self.gossip_fanout.min(cluster_nodes.len());
                let mut sent_count = 0;

                for _ in 0..send_count {
                    match self.cluster_member.send_to_random_node(&data).await {
                        Ok(_) => sent_count += 1,
                        Err(e) => debug!(
                            "[{}] Failed to send gossip to random peer: {}",
                            self.node_id, e
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
            Err(e) => {
                debug!("[{}] Failed to serialize packet: {}", self.node_id, e);
                Err(ColibriError::Transport(format!(
                    "Serialization failed: {}",
                    e
                )))
            }
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
        let _ = rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
            .map(|mut rl| rl.accept_delta_state(entries));
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
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock config: {}", e)))?;
        let rule = settings::NamedRateLimitRule {
            name: rule_name.clone(),
            settings: settings.clone(),
        };
        config.add_named_rule(&rule);

        // Create the rate limiter for this rule
        let mut limiters = self
            .named_rate_limiters
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock limiters: {}", e)))?;

        let rate_limiter = DistributedBucketLimiter::new(self.node_id, settings);
        limiters.insert(rule_name, rate_limiter);

        Ok(())
    }
    /// Get a named rate limit rule locally (without gossiping)
    pub async fn get_named_rule_local(
        &self,
        rule_name: &str,
    ) -> Result<Option<settings::NamedRateLimitRule>> {
        self.rate_limit_config
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock config: {}", e)))
            .map(|rlconf| {
                rlconf
                    .get_named_rule_settings(rule_name)
                    .cloned()
                    .map(|rl_settings| settings::NamedRateLimitRule {
                        name: rule_name.to_string(),
                        settings: rl_settings,
                    })
            })
    }

    /// Delete a named rate limit rule locally (without gossiping)
    pub async fn delete_named_rule_local(&self, rule_name: String) -> Result<()> {
        let mut config = self
            .rate_limit_config
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock config: {}", e)))?;

        if config.remove_named_rule(&rule_name).is_some() {
            // Remove the rate limiter for this rule
            let mut limiters = self.named_rate_limiters.lock().map_err(|e| {
                ColibriError::Concurrency(format!("Failed to lock limiters: {}", e))
            })?;

            limiters.remove(&rule_name);
            Ok(())
        } else {
            Err(ColibriError::Api(format!("Rule '{}' not found", rule_name)))
        }
    }

    /// List all named rate limit rules locally
    pub async fn list_named_rules_local(&self) -> Result<Vec<settings::NamedRateLimitRule>> {
        let config = self
            .rate_limit_config
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock config: {}", e)))?;

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
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock limiters: {}", e)))?;

        if let Some(rate_limiter) = limiters.get_mut(&rule_name) {
            let calls_left = rate_limiter.limit_calls_for_client(key.clone());
            calls_left
                .map(|calls_remaining| {
                    Ok(Some(CheckCallsResponse {
                        client_id: key,
                        calls_remaining,
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
            .map_err(|e| ColibriError::Concurrency(format!("Failed to lock limiters: {}", e)))?;

        if let Some(rate_limiter) = limiters.get(&rule_name) {
            let calls_remaining = rate_limiter.check_calls_remaining_for_client(&key);
            Ok(CheckCallsResponse {
                client_id: key,
                calls_remaining,
            })
        } else {
            Err(ColibriError::Api(format!(
                "Rate limit rule '{}' not found",
                rule_name
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Simple tests for GossipController to verify core functionality

    use super::*;
    use tokio::sync::oneshot;

    use crate::node::NodeName;

    #[tokio::test]
    async fn test_controller_creation() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();

        // Basic checks that controller is properly initialized
        assert!(controller.has_gossip()); // Always true for gossip mode
        assert_eq!(controller.gossip_interval_ms, 1000);
        assert_eq!(controller.gossip_fanout, 3);
    }

    #[tokio::test]
    async fn test_rate_limit_command() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = GossipCommand::RateLimit {
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
    async fn test_check_limit_command() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();
        let (tx, rx) = oneshot::channel();

        let cmd = GossipCommand::CheckLimit {
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
    async fn test_expire_keys_command() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();

        let cmd = GossipCommand::ExpireKeys;

        // Should not panic or error
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_gossip_message_processing() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();

        // Create a heartbeat message
        let message = GossipMessage::Heartbeat {
            node_id: NodeName::from("z").node_id(), // Different node ID
            timestamp: 1234567890,
            vclock: crdts::VClock::new(),
            response_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        let packet = GossipPacket::new(message);
        let data = packet.serialize().unwrap();

        let cmd = GossipCommand::GossipMessageReceived {
            data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should process without error
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_self_message_filtering() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();
        let our_node_id = controller.node_id;

        // Create a heartbeat from our own node
        let message = GossipMessage::Heartbeat {
            node_id: our_node_id, // Same node ID as controller
            timestamp: 1234567890,
            vclock: crdts::VClock::new(),
            response_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        let packet = GossipPacket::new(message);
        let data = packet.serialize().unwrap();

        let cmd = GossipCommand::GossipMessageReceived {
            data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should ignore our own message
        controller.handle_command(cmd).await.unwrap();
    }

    #[tokio::test]
    async fn test_rate_limiting_exhaustion() {
        let mut settings = settings::tests::sample();
        settings.rate_limit_max_calls_allowed = 3; // Very small limit

        let controller = GossipController::new(settings).await.unwrap();
        let client_id = "heavy_client".to_string();

        // Make multiple calls to exhaust the rate limit
        let mut successful_calls = 0;
        let mut failed_calls = 0;

        for _i in 0..5 {
            let (tx, rx) = oneshot::channel();
            let cmd = GossipCommand::RateLimit {
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
    async fn test_malformed_gossip_message() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings).await.unwrap();

        // Send garbage data
        let bad_data = bytes::Bytes::from_static(b"this is not a valid gossip packet");

        let cmd = GossipCommand::GossipMessageReceived {
            data: bad_data,
            peer_addr: "127.0.0.1:8412".parse().unwrap(),
        };

        // Should handle gracefully and return an error
        let result = controller.handle_command(cmd).await;
        assert!(result.is_err());
    }
}
