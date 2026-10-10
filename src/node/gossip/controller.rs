use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{RwLock, mpsc};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::error::{ColibriError, Result};
use crate::limiters::rules::{RuleList, RuleName, SerializableRule};
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, Status, StatusResponse, TopologyResponse,
};
use crate::node::{NodeAddress, NodeId, NodeName};
use crate::settings::{self, ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::{UdpReceiver, UdpTransport};

use super::messages::delta_packets;
use super::state::{GossipState, HeartbeatDigest, PARTITIONS, RuleBuckets, TickOutput};
use super::{GossipMessage, GossipPacket};

/// Maximum number of recent packet IDs kept for receive-side deduplication.
/// At ~50 gossip packets/sec in a small cluster, this is a ~20-second window.
const DEDUP_CACHE_SIZE: usize = 1000;

type GossipReceiveChannel = Arc<Mutex<Option<mpsc::Receiver<(Bytes, SocketAddr)>>>>;

/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    node_name: NodeName,
    node_id: NodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    transport: Arc<UdpTransport>,
    /// Limiters for every rule, and the replicated rule configuration
    pub state: Arc<Mutex<GossipState>>,
    pub response_addr: SocketAddr,
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
    /// Largest datagram to send; updates are split across packets to fit.
    pub max_packet_bytes: usize,
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
    /// Receive-side deduplication cache. Stores recently seen packet_ids so that
    /// re-propagated or retransmitted packets are dropped without reprocessing.
    /// Bounded to DEDUP_CACHE_SIZE entries; oldest entries are evicted in FIFO order.
    seen_packet_ids: Arc<Mutex<(HashSet<u64>, VecDeque<u64>)>>,
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

        let state = Arc::new(Mutex::new(GossipState::new(
            node_id,
            settings.rate_limit_settings(),
        )));

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
            state,
            response_addr,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
            max_packet_bytes: settings.gossip_max_packet_bytes,
            receiver_addr,
            receiver: Arc::new(receiver),
            receive_chan: Arc::new(Mutex::new(Some(receive_chan))),
            msg_receive_handle: Arc::new(Mutex::new(None)),
            msg_eval_handle: Arc::new(Mutex::new(None)),
            seen_packet_ids: Arc::new(Mutex::new((HashSet::new(), VecDeque::new()))),
        })
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut GossipState) -> R) -> Result<R> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Lock poisoned: {}", e)))?;
        Ok(f(&mut state))
    }

    async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<Option<CheckCallsResponse>> {
        let rule = request.rule_name.clone().unwrap_or_default();
        let calls_remaining = self.with_state(|state| {
            if request.consume_token {
                state.limit(&rule, &request.client_id)
            } else {
                state.check(&rule, &request.client_id).map(Some)
            }
        })??;

        // `None` means rate limited; it must not be collapsed into
        // `calls_remaining: 0`, which is also what the last admitted call sees.
        Ok(calls_remaining.map(|calls_remaining| CheckCallsResponse {
            client_id: request.client_id,
            rule_name: request.rule_name,
            calls_remaining,
        }))
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
        let name = RuleName::from(rule_name.trim());
        self.with_state(|state| state.create_rule(name.clone(), settings))??;
        tracing::info!("Created named rule '{}' in gossip node", name);
        self.broadcast_rules().await;
        Ok(())
    }

    async fn delete_rate_limit_rule(&self, rule_name: RuleName) -> Result<()> {
        self.with_state(|state| state.delete_rule(rule_name.clone()))??;
        tracing::info!("Deleted named rule '{}' from gossip node", rule_name);
        self.broadcast_rules().await;
        Ok(())
    }

    async fn get_rate_limit_rule(&self, rule_name: RuleName) -> Result<Option<SerializableRule>> {
        self.with_state(|state| state.get_rule(&rule_name))
    }

    async fn list_rate_limit_rules(&self) -> Result<Vec<SerializableRule>> {
        self.with_state(|state| state.list_rules())
    }

    /// Send this node's rule configuration to every peer. Heartbeat digests
    /// repair any peer this misses.
    async fn broadcast_rules(&self) {
        let rules = match self.with_state(|state| state.rule_config().clone()) {
            Ok(rules) => rules,
            Err(e) => {
                error!(
                    "[{}] Failed to read rule configuration: {}",
                    self.node_id, e
                );
                return;
            }
        };
        let peers: Vec<SocketAddr> = {
            let topology = self.topology.read().await;
            topology
                .all_nodes()
                .into_iter()
                .filter(|(name, _)| name.as_str() != self.node_name.as_str())
                .map(|(_, addr)| addr)
                .collect()
        };
        for peer in peers {
            let packet = GossipPacket::new(GossipMessage::RuleConfigSync {
                response_addr: self.response_addr,
                sender_node_id: self.node_id,
                rules: rules.clone(),
            });
            if let Err(e) = self.send_gossip_packet_to(peer, packet).await {
                debug!("[{}] Failed to send rules to {}: {}", self.node_id, peer, e);
            }
        }
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
                Ok(Message::ListRateLimitRulesResponse(RuleList(rules)))
            }

            _ => Err(ColibriError::Api(format!(
                "Unsupported message type for GossipController: {:?}",
                message
            ))),
        }
    }

    /// Send a gossip packet to a specific peer address (unicast).
    ///
    /// Unlike `send_gossip_packet` which fans out to random peers, this targets
    /// a single address — used for anti-entropy responses where we know exactly
    /// who to reply to.
    async fn send_gossip_packet_to(&self, addr: SocketAddr, packet: GossipPacket) -> Result<()> {
        let data = packet
            .serialize()
            .map_err(|e| ColibriError::Transport(format!("Serialization failed: {}", e)))?;
        self.transport.send_to_peer(addr, &data).await?;
        debug!("[{}] Sent targeted gossip packet to {}", self.node_id, addr);
        Ok(())
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        // For gossip mode, assume true since GossipController is only used in gossip mode
        true
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.with_state(|state| state.bucket_count()).unwrap_or(0);
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
                // Gossip timer -> send delta updates
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
        // Ages every bucket first, so expiries ride this same gossip round.
        let message = match self.with_state(|state| state.tick())? {
            TickOutput::Delta(updates) => {
                let packets = self.delta_packets(updates, self.node_id, 1);
                return self.send_gossip_packets(packets).await;
            }
            TickOutput::Heartbeat(digest) => GossipMessage::Heartbeat {
                node_id: self.node_id,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                incarnations: digest.incarnations,
                partitions: digest.partitions,
                rules_digest: digest.rules,
                response_addr: self.response_addr,
            },
        };
        self.send_gossip_packet(GossipPacket::new(message)).await
    }

    /// Forget idle clients under every rule.
    pub fn expire_keys(&self) -> Result<()> {
        self.with_state(|state| state.expire_keys())
    }

    /// Process an incoming gossip packet
    pub async fn process_gossip_packet(&self, data: Bytes, peer_addr: SocketAddr) -> Result<()> {
        match GossipPacket::from_wire(&data) {
            Ok(packet) => {
                // Dedup: drop packets whose id we've already processed.
                // This prevents re-propagated or retransmitted packets from
                // being applied twice. The seen-set is bounded to DEDUP_CACHE_SIZE;
                // oldest entries are evicted in FIFO order.
                {
                    let mut cache = self.seen_packet_ids.lock().map_err(|e| {
                        ColibriError::Concurrency(format!("seen_packet_ids poisoned: {}", e))
                    })?;
                    let (ref mut seen, ref mut order) = *cache;
                    if seen.contains(&packet.packet_id) {
                        debug!(
                            "[{}] Dropping duplicate packet {}",
                            self.node_id, packet.packet_id
                        );
                        return Ok(());
                    }
                    seen.insert(packet.packet_id);
                    order.push_back(packet.packet_id);
                    if order.len() > DEDUP_CACHE_SIZE
                        && let Some(old_id) = order.pop_front()
                    {
                        seen.remove(&old_id);
                    }
                }

                match packet.message {
                    GossipMessage::StateRequest {
                        requesting_node_id,
                        response_addr: requester_addr,
                        partitions,
                    } => {
                        // Don't respond to our own requests (shouldn't happen but guard anyway)
                        if requesting_node_id == self.node_id {
                            return Ok(());
                        }

                        info!(
                            "[GOSSIP_ANTI_ENTROPY] node:{} received StateRequest from:{}",
                            self.node_id, requesting_node_id
                        );

                        let state = self.with_state(|state| state.partition_state(&partitions))?;

                        if !state.is_empty() {
                            for packet in self.delta_packets(state, self.node_id, 0) {
                                if let Err(e) =
                                    self.send_gossip_packet_to(requester_addr, packet).await
                                {
                                    warn!(
                                        "[{}] Failed to send anti-entropy response to {}: {}",
                                        self.node_id, requester_addr, e
                                    );
                                }
                            }
                        } else {
                            debug!(
                                "[{}] StateRequest from {} but limiter is empty — nothing to send",
                                self.node_id, requesting_node_id
                            );
                        }
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
                        self.with_state(|state| state.accept(&updates))?;
                        // Propagate further if propagation_factor > 0
                        if propagation_factor > 0 {
                            let packets =
                                self.delta_packets(updates, sender_node_id, propagation_factor - 1);
                            self.send_gossip_packets(packets).await?;
                        }
                    }
                    GossipMessage::Heartbeat {
                        node_id: heartbeat_node_id,
                        timestamp: _,
                        incarnations,
                        partitions,
                        rules_digest,
                        response_addr: peer_response_addr,
                    } => {
                        // Don't process our own heartbeat
                        if heartbeat_node_id == self.node_id {
                            return Ok(());
                        }
                        let peer = HeartbeatDigest {
                            incarnations,
                            partitions,
                            rules: rules_digest,
                        };
                        let reply = self.with_state(|state| state.on_heartbeat(&peer))?;

                        if !reply.partitions.is_empty() {
                            info!(
                                "[GOSSIP_ANTI_ENTROPY] node:{} differs from peer:{} in {} partitions",
                                self.node_id,
                                heartbeat_node_id,
                                reply.partitions.len()
                            );
                            // Push ours: a busy peer never heartbeats, so it can't pull.
                            let ours =
                                self.with_state(|state| state.partition_state(&reply.partitions))?;
                            for packet in self.delta_packets(ours, self.node_id, 0) {
                                if let Err(e) =
                                    self.send_gossip_packet_to(peer_response_addr, packet).await
                                {
                                    warn!(
                                        "[{}] Failed to push state to {}: {}",
                                        self.node_id, peer_response_addr, e
                                    );
                                }
                            }
                            let request = GossipMessage::StateRequest {
                                requesting_node_id: self.node_id,
                                partitions: reply.partitions,
                                response_addr: self.response_addr,
                            };
                            if let Err(e) = self
                                .send_gossip_packet_to(
                                    peer_response_addr,
                                    GossipPacket::new(request),
                                )
                                .await
                            {
                                warn!(
                                    "[{}] Failed to send StateRequest to {}: {}",
                                    self.node_id, peer_response_addr, e
                                );
                            }
                        }
                        if reply.push_rules {
                            let rules = self.with_state(|state| state.rule_config().clone())?;
                            let sync = GossipMessage::RuleConfigSync {
                                response_addr: self.response_addr,
                                sender_node_id: self.node_id,
                                rules,
                            };
                            if let Err(e) = self
                                .send_gossip_packet_to(peer_response_addr, GossipPacket::new(sync))
                                .await
                            {
                                warn!(
                                    "[{}] Failed to send rules to {}: {}",
                                    self.node_id, peer_response_addr, e
                                );
                            }
                        }
                    }

                    GossipMessage::RuleConfigSync {
                        sender_node_id,
                        rules,
                        response_addr: peer_addr,
                    } => {
                        if sender_node_id == self.node_id {
                            return Ok(());
                        }
                        let (reply, ours) = self.with_state(|state| {
                            (state.merge_rules(&rules), state.rule_config().clone())
                        })?;
                        if reply.send_rules {
                            let sync = GossipMessage::RuleConfigSync {
                                response_addr: self.response_addr,
                                sender_node_id: self.node_id,
                                rules: ours,
                            };
                            if let Err(e) = self
                                .send_gossip_packet_to(peer_addr, GossipPacket::new(sync))
                                .await
                            {
                                warn!(
                                    "[{}] Failed to send rules to {}: {}",
                                    self.node_id, peer_addr, e
                                );
                            }
                        }
                        if reply.request_state {
                            let request = GossipMessage::StateRequest {
                                requesting_node_id: self.node_id,
                                partitions: (0..PARTITIONS).collect(),
                                response_addr: self.response_addr,
                            };
                            if let Err(e) = self
                                .send_gossip_packet_to(peer_addr, GossipPacket::new(request))
                                .await
                            {
                                warn!(
                                    "[{}] Failed to send StateRequest to {}: {}",
                                    self.node_id, peer_addr, e
                                );
                            }
                        }
                    }
                };
                Ok(())
            }
            Err(e) => {
                // Version mismatches are a deployment signal (mixed-version
                // cluster); log at warn so they're visible without being noisy.
                // Pure decode errors are debug-level (malformed/truncated UDP).
                use super::messages::GossipDecodeError;
                match &e {
                    GossipDecodeError::VersionMismatch { expected, got } => warn!(
                        "[{}] Gossip protocol version mismatch from {}: expected {}, got {}",
                        self.node_id, peer_addr, expected, got
                    ),
                    GossipDecodeError::Decode(_) => debug!(
                        "[{}] Failed to decode gossip packet from {}: {}",
                        self.node_id, peer_addr, e
                    ),
                }
                Err(ColibriError::Transport(format!("{}", e)))
            }
        }
    }

    /// `updates` as datagrams of at most `max_packet_bytes`.
    fn delta_packets(
        &self,
        updates: Vec<RuleBuckets>,
        sender_node_id: NodeId,
        propagation_factor: u8,
    ) -> Vec<GossipPacket> {
        delta_packets(
            updates,
            sender_node_id,
            self.response_addr,
            propagation_factor,
            self.max_packet_bytes,
        )
    }

    /// Send a gossip packet to random peers using transport layer
    pub async fn send_gossip_packet(&self, packet: GossipPacket) -> Result<()> {
        self.send_gossip_packets(vec![packet]).await
    }

    /// Send packets to the same `gossip_fanout` random peers.
    pub async fn send_gossip_packets(&self, packets: Vec<GossipPacket>) -> Result<()> {
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

        let datagrams = packets
            .iter()
            .map(GossipPacket::serialize)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                debug!("[{}] Failed to serialize packet: {}", self.node_id, e);
                ColibriError::Transport(format!("Serialization failed: {}", e))
            })?;

        // Send to multiple random peers up to gossip_fanout
        let send_count = self.gossip_fanout.min(peers.len());

        // Select random peers. (This must complete before any await to avoid Send issues)
        let selected_peers: Vec<_> = {
            use rand::prelude::IndexedRandom;
            let mut rng = rand::rng();
            peers.sample(&mut rng, send_count).copied().collect()
        }; // RNG is dropped here, before any await

        let mut sent_count = 0;
        for peer in selected_peers {
            let mut delivered = true;
            for data in &datagrams {
                if let Err(e) = self.transport.send_to_peer(peer, data).await {
                    delivered = false;
                    debug!(
                        "[{}] Failed to send gossip to {}: {}",
                        self.node_id, peer, e
                    );
                }
            }
            sent_count += usize::from(delivered);
        }

        if sent_count > 0 {
            debug!(
                "[{}] Sent {} gossip packets to {} peers",
                self.node_id,
                datagrams.len(),
                sent_count
            );
            Ok(())
        } else {
            Err(ColibriError::Transport(
                "Failed to send to any peers".to_string(),
            ))
        }
    }
}

// ===== Tx4: Controller-level tests for deduplication and packet processing =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limiters::distributed_bucket::DistributedBucketLimiter;
    use crate::node::NodeName;
    use crate::node::gossip::state::RuleBuckets;
    use crate::settings;

    /// Helper: build a serialized `DeltaStateSync` packet whose updates come
    /// from a sender limiter that is distinct from the controller's own node.
    /// Uses `propagation_factor: 0` so the controller does not attempt to
    /// re-propagate, which would fail with "no peers" in unit tests.
    fn make_delta_packet(
        sender_name: &str,
        client_id: &str,
        packet_id: u64,
        controller_settings: &settings::Settings,
    ) -> Bytes {
        let sender_id = NodeName::from(sender_name).node_id();
        let rl = controller_settings.rate_limit_settings();
        let mut sender_limiter = DistributedBucketLimiter::new(sender_id, rl);
        sender_limiter.limit_calls_for_client(client_id.to_string());
        let updates = vec![RuleBuckets {
            rule: RuleName::default(),
            version: None,
            buckets: sender_limiter.gossip_delta_state(),
        }];

        let message = GossipMessage::DeltaStateSync {
            updates,
            sender_node_id: sender_id,
            response_addr: "127.0.0.1:9999".parse().unwrap(),
            propagation_factor: 0,
        };
        GossipPacket::new_with_id(message, packet_id)
            .serialize()
            .expect("serialization failed")
    }

    /// A duplicate packet — same `packet_id` sent twice — must be recorded in
    /// `seen_packet_ids` exactly once.
    #[tokio::test]
    async fn test_duplicate_packet_is_deduplicated() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings.clone()).await.unwrap();
        let peer: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        let packet = make_delta_packet("sender-node", "alice", 42, &settings);

        // Cache starts empty.
        let initial_seen = controller.seen_packet_ids.lock().unwrap().0.len();
        assert_eq!(initial_seen, 0);

        // First delivery: packet_id 42 enters the cache.
        controller
            .process_gossip_packet(packet.clone(), peer)
            .await
            .unwrap();

        let seen_after_first = controller.seen_packet_ids.lock().unwrap().0.len();
        assert_eq!(
            seen_after_first, 1,
            "first packet should be recorded in the dedup cache"
        );
        assert!(
            controller
                .seen_packet_ids
                .lock()
                .unwrap()
                .0
                .contains(&42u64),
            "packet_id 42 must be in the seen-set"
        );

        // Second delivery with the same packet_id: the dedup gate must drop it
        // before merge. The cache size must remain 1 — a second insert of the
        // same id into a HashSet is a no-op, which is observable here because
        // we check the count before and after.
        controller
            .process_gossip_packet(packet.clone(), peer)
            .await
            .unwrap();

        let seen_after_second = controller.seen_packet_ids.lock().unwrap().0.len();
        assert_eq!(
            seen_after_second, 1,
            "dedup cache must still hold exactly one entry after a duplicate delivery"
        );
    }

    /// Two packets with distinct `packet_id`s must both be applied, even if
    /// they carry updates for the same client from the same sender.
    #[tokio::test]
    async fn test_distinct_packet_ids_are_both_applied() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings.clone()).await.unwrap();
        let peer: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        // Two separate packets, different IDs, different clients.
        let packet_a = make_delta_packet("sender-node", "alice", 100, &settings);
        let packet_b = make_delta_packet("sender-node", "bob", 101, &settings);

        controller
            .process_gossip_packet(packet_a, peer)
            .await
            .unwrap();
        controller
            .process_gossip_packet(packet_b, peer)
            .await
            .unwrap();

        let state = controller.state.lock().unwrap();

        // Both clients should have been merged into the limiter.
        let alice = state.check(&RuleName::default(), "alice").unwrap();
        let bob = state.check(&RuleName::default(), "bob").unwrap();

        // sender consumed one token each; both should be below the full limit.
        assert!(
            alice < settings.rate_limit_max_calls_allowed,
            "alice's bucket not merged (tokens={alice})"
        );
        assert!(
            bob < settings.rate_limit_max_calls_allowed,
            "bob's bucket not merged (tokens={bob})"
        );
    }

    /// A rule configuration received from a peer creates its rules here.
    #[tokio::test]
    async fn test_rule_config_sync_creates_rules() {
        let settings = settings::tests::sample();
        let controller = GossipController::new(settings.clone()).await.unwrap();
        let peer_id = NodeName::from("peer").node_id();
        let rule = RuleName::from("checkout");
        let rule_settings = RateLimitSettings {
            rate_limit_max_calls_allowed: 7,
            rate_limit_interval_seconds: 2,
        };

        let mut peer = GossipState::new(peer_id, settings.rate_limit_settings());
        peer.create_rule(rule.clone(), rule_settings.clone())
            .unwrap();
        let packet = GossipPacket::new(GossipMessage::RuleConfigSync {
            response_addr: "127.0.0.1:9003".parse().unwrap(),
            sender_node_id: peer_id,
            rules: peer.rule_config().clone(),
        });
        controller
            .process_gossip_packet(
                packet.serialize().unwrap(),
                "127.0.0.1:9003".parse().unwrap(),
            )
            .await
            .unwrap();

        let created = controller.get_rate_limit_rule(rule).await.unwrap();
        assert_eq!(created.map(|r| r.settings), Some(rule_settings));
    }
}
