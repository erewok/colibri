use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info};

use super::{GossipCommand, GossipMessage, GossipPacket};
use crate::error::{ColibriError, Result};
use crate::limiters::{rate_limit::RateLimiter, versioned_bucket::VersionedTokenBucket};
use crate::node::CheckCallsResponse;
use crate::transport::UdpTransport;
use crate::settings;



/// A gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol.
#[derive(Clone)]
pub struct GossipController {
    // rate-limit settings
    pub rate_limit_settings: settings::RateLimitSettings,
    /// Local rate limiter - handles all bucket operations
    pub rate_limiter: Arc<Mutex<RateLimiter<VersionedTokenBucket>>>,
    /// transport_config
    pub transport_config: settings::TransportConfig,
    /// Transport layer for sending UDP unicast gossip messages
    pub transport: Option<Arc<UdpTransport>>,
    /// Gossip configuration
    pub gossip_interval_ms: u64,
    pub gossip_fanout: usize,
}

impl std::fmt::Debug for GossipController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipController")
            .field("node_id", &self.rate_limit_settings.node_id)
            .field("has_transport", &self.transport.is_some())
            .field("gossip_interval_ms", &self.gossip_interval_ms)
            .field("gossip_fanout", &self.gossip_fanout)
            .finish()
    }
}

impl GossipController {
    pub async fn new(
        settings: settings::Settings,
        rate_limiter: RateLimiter<VersionedTokenBucket>,
    ) -> Result<Self> {
        let node_id = settings.node_id();
        info!(
            "Created GossipNode with ID: {} (port: {})",
            node_id, settings.listen_port_udp
        );
        let rl_settings = settings.rate_limit_settings();

        // Build transport
        let transport_config = settings.transport_config();
        let transport = if !settings.topology.is_empty() {
            info!(
                "Initializing gossip transport at {} with {} peers: {:?}",
                transport_config.listen_udp,
                settings.topology.len(),
                settings.topology
            );
            Some(Arc::new(
                UdpTransport::new(node_id, &transport_config)
                    .await
                    .map_err(|e| {
                        ColibriError::Transport(format!("Failed to create transport: {}", e))
                    })?,
            ))
        } else {
            None
        };

        Ok(Self {
            rate_limit_settings: rl_settings,
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            transport: transport.clone(),
            transport_config: transport_config,
            gossip_interval_ms: settings.gossip_interval_ms,
            gossip_fanout: settings.gossip_fanout,
        })
    }

    pub fn node_id(&self) -> u32 {
        self.rate_limit_settings.node_id
    }

    /// Check if this node has gossip enabled
    pub fn has_gossip(&self) -> bool {
        self.transport.is_some()
    }

    /// Log current statistics about the gossip node state for debugging
    pub async fn log_stats(&self) {
        let bucket_count = self.rate_limiter.lock().map(|rl| rl.len()).unwrap_or(0);
        debug!(
            "[{}] Gossip node stats: {} active client buckets",
            self.node_id(),
            bucket_count
        );
    }

    /// Handle messages and periodic gossip in an async loop
    pub async fn start(&self, mut gossip_command_rx: mpsc::Receiver<GossipCommand>) {
        info!(
            "[{}] Starting central IO loop with {}ms gossip interval",
            self.node_id(),
            self.gossip_interval_ms
        );

        let mut gossip_timer = time::interval(time::Duration::from_millis(self.gossip_interval_ms));
        let mut gossip_round = 0u64;

        let node_id = self.node_id();
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
                    gossip_round += 1;
                    if let Err(e) = self.handle_gossip_tick(gossip_round).await {
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

    async fn handle_gossip_tick(&self, gossip_round: u64) -> Result<()> {
        // Send delta updates for buckets that have changed
        let updates = self.collect_gossip_updates().await?;
        if !updates.is_empty() {
            let message = GossipMessage::DeltaStateSync {
                updates,
                sender_node_id: self.node_id(),
                gossip_round,
                last_seen_versions: HashMap::new(),
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        } else {
            // Send heartbeat if no updates to share
            let message = GossipMessage::Heartbeat {
                node_id: self.node_id(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                version_vector: HashMap::new(),
            };

            let packet = GossipPacket::new(message);
            self.send_gossip_packet(packet).await?;
        }

        Ok(())
    }

    /// Collect buckets that have been updated and should be gossiped
    async fn collect_gossip_updates(&self) -> Result<HashMap<String, VersionedTokenBucket>> {
        // For now, collect all buckets. In the future, this could be optimized
        // to only send buckets that have changed since the last gossip round.
        self.rate_limiter
            .lock()
            .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
            .map(|rl| rl.get_all_buckets())
    }
    /// Handle incoming gossip cmd from our channel
    async fn handle_command(&self, cmd: GossipCommand) -> Result<()> {
        match cmd {
            GossipCommand::ExpireKeys => {
                self.rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
                    .map(|mut rl| rl.expire_keys())?;
                Ok(())
            }
            GossipCommand::GossipMessageReceived { data, peer_addr } => {
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
                let calls_remaining = rl.check_calls_remaining_for_client(client_id.as_str());
                debug!(
                    "Check limit for client '{}': {} tokens remaining",
                    client_id, calls_remaining
                );

                if let Err(_) = resp_chan.send(Ok(CheckCallsResponse {
                    client_id,
                    calls_remaining,
                })) {
                    error!(
                        "[{}] Failed sending oneshot check_limit response",
                        self.node_id()
                    );
                }
                Ok(())
            }
            GossipCommand::RateLimit {
                client_id,
                resp_chan,
            } => {
                let mut result = self
                    .rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))?;
                let calls_left = result.limit_calls_for_client(client_id.to_string());
                let response: Option<CheckCallsResponse>;
                if let Some(calls_remaining) = calls_left {
                    response = Some(CheckCallsResponse {
                        client_id,
                        calls_remaining,
                    });
                } else {
                    response = None;
                }
                if let Err(_) = resp_chan.send(Ok(response)) {
                    error!(
                        "[{}] Failed sending oneshot rate_limit response",
                        self.node_id()
                    );
                }
                Ok(())
            }
        }
    }
    /// Process an incoming gossip packet
    async fn process_gossip_packet(&self, data: Bytes, peer_addr: SocketAddr) -> Result<()> {
        match GossipPacket::deserialize(&data) {
            Ok(packet) => {
                match packet.message {
                    GossipMessage::StateRequest { .. } => {
                        debug!(
                            "[{}] Received StateRequest - not yet implemented",
                            self.node_id()
                        );
                        // TODO: Implement state request handling
                    }
                    GossipMessage::DeltaStateSync {
                        updates,
                        sender_node_id,
                        gossip_round: _,
                        last_seen_versions: _,
                    } => {
                        // Don't process our own messages
                        if sender_node_id == self.node_id() {
                            debug!("[{}] Ignoring our own delta gossip message", self.node_id());
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing DeltaStateSync from node {} with {} updates",
                            self.node_id(),
                            sender_node_id,
                            updates.len()
                        );

                        self.merge_gossip_state(updates).await;
                    }
                    GossipMessage::StateResponse {
                        responding_node_id,
                        requested_data,
                        current_versions: _,
                    } => {
                        // Don't process our own messages
                        if responding_node_id == self.node_id() {
                            return Ok(());
                        }

                        debug!(
                            "[{}] Processing StateResponse from node {} with {} updates",
                            self.node_id(),
                            responding_node_id,
                            requested_data.len()
                        );

                        // Merge each incoming update
                        self.merge_gossip_state(requested_data).await;
                    }
                    GossipMessage::Heartbeat {
                        node_id: heartbeat_node_id,
                        timestamp: _,
                        version_vector: _,
                    } => {
                        // Don't process our own heartbeat
                        if heartbeat_node_id == self.node_id() {
                            return Ok(());
                        }
                    }
                };
                Ok(())
            }
            Err(e) => {
                debug!(
                    "[{}] Failed to deserialize packet: {} from {}",
                    self.node_id(),
                    e,
                    peer_addr
                );
                Err(ColibriError::Transport(format!(
                    "Deserialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Send a gossip packet to a random peer
    async fn send_gossip_packet(&self, packet: GossipPacket) -> Result<()> {
        if self.transport.is_none() {
            debug!(
                "[{}] Gossip transport not configured, skipping packet send",
                self.node_id()
            );
            return Ok(());
        }
        match packet.serialize() {
            Ok(data) => {
                let _peer = self
                    .transport
                    .as_ref()
                    .unwrap()
                    .send_to_random_peers(&data, self.gossip_fanout)
                    .await
                    .map_err(|e| ColibriError::Transport(format!("Send failed: {}", e)))?;
                Ok(())
            }
            Err(e) => {
                debug!("[{}] Failed to serialize packet: {}", self.node_id(), e);
                Err(ColibriError::Transport(format!(
                    "Serialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Merge incoming gossip state from other nodes (public interface)
    pub async fn merge_gossip_state(&self, entries: HashMap<String, VersionedTokenBucket>) {
        let node_id = self.node_id();
        let rate_limiter = Arc::clone(&self.rate_limiter);
        debug!(
            "[{}] Processing {} gossip entries for merge",
            node_id,
            entries.len()
        );

        for (client_id, incoming_bucket) in entries.into_iter() {
            debug!("[{}] Processing client: {}", node_id, client_id);
            let maybe_bucket = rate_limiter
                .lock()
                .map_err(|e| ColibriError::Concurrency(format!("Mutex lock fail {}", e)))
                .map(|rl| rl.get_bucket(&client_id));
            if let Ok(Some(mut current_entry)) = maybe_bucket {
                debug!(
                    "[{}] Found existing bucket for client: {}",
                    node_id, client_id
                );
                let current_version = current_entry
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);
                let incoming_version = incoming_bucket
                    .vector_clock
                    .get_timestamp(incoming_bucket.last_updated_by);

                // Try to merge with existing bucket
                if current_entry.merge(incoming_bucket.clone(), node_id) {
                    debug!(
                        "[{}] Merged gossip update for client '{}': {} -> {} tokens, version {} -> {}",
                        node_id,
                        client_id,
                        current_entry.bucket.tokens,
                        incoming_bucket.bucket.tokens,
                        current_version,
                        incoming_version
                    );
                    let _ = rate_limiter
                        .lock()
                        .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
                        .map(|mut rl| rl.set_bucket(&client_id, current_entry));
                } else {
                    debug!(
                        "[{}] Rejected gossip update for client '{}': incoming version {} <= current version {}",
                        node_id, client_id, incoming_version, current_version
                    );
                }
            } else {
                // No existing entry, accept incoming state
                debug!(
                    "[{}] Accepted new gossip state for client '{}': tokens={}, version={}",
                    node_id,
                    client_id,
                    incoming_bucket.bucket.tokens,
                    incoming_bucket
                        .vector_clock
                        .get_timestamp(incoming_bucket.last_updated_by)
                );
                let _ = rate_limiter
                    .lock()
                    .map_err(|e| ColibriError::Concurrency(format!("Failed to lock {}", e)))
                    .map(|mut rl| rl.set_bucket(&client_id, incoming_bucket));
                debug!(
                    "[{}] Finished setting new bucket for client: {}",
                    node_id, client_id
                );
            }
        }
        debug!("[{}] merge_gossip_state_static completed", node_id);
    }
}