//! TCP Transport Implementation
//!
//! Provides TCP-based transport implementing the RequestSender trait.
//! Used for request-response communication patterns (consistent hashing).
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::traits::{Sender, RequestSender};
use super::stats::FrozenSocketPoolStats;
use super::socket_pool_tcp::TcpSocketPool;
use crate::error::{Result, ColibriError};
use crate::node::{NodeId, messages::Message};
use crate::settings::TransportConfig;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

#[derive(Clone, Debug)]
pub struct TcpTransport {
    socket_pool: Arc<RwLock<TcpSocketPool>>,
}

impl TcpTransport {
    /// Create a new TCP transport instance
    pub async fn new(transport_config: &TransportConfig) -> Result<Self> {
        let socket_pool = TcpSocketPool::new(transport_config).await?;

        Ok(Self {
            socket_pool: Arc::new(RwLock::new(socket_pool)),
        })
    }

    // ============================================================================
    // MESSAGE ENUM SUPPORT (Phase 1)
    // ============================================================================

    /// Send a Message to a peer by address (fire-and-forget)
    /// This spawns a task to send asynchronously without waiting for a response
    pub async fn send_message_fire_and_forget(
        &self,
        peer: SocketAddr,
        message: &Message,
    ) -> Result<()> {
        let data = message.serialize()?;
        self.send_fire_and_forget(peer, &data).await
    }

    /// Send a Message to a peer by address and wait for response
    pub async fn send_message_request_response(
        &self,
        peer: SocketAddr,
        message: &Message,
    ) -> Result<Message> {
        let data = message.serialize()?;
        let response_data = self.send_request_response(peer, &data).await?;
        Message::deserialize(&response_data)
    }

    /// Broadcast a Message to multiple peers (fire-and-forget)
    /// Returns number of successful sends
    pub async fn broadcast_message(
        &self,
        peers: Vec<SocketAddr>,
        message: &Message,
    ) -> Result<usize> {
        let data = message.serialize()?;
        self.broadcast(peers, &data).await
    }

    // ============================================================================
    // FIRE-AND-FORGET OPERATIONS
    // ============================================================================

    /// Send raw data without waiting for response (fire-and-forget)
    /// This spawns a task to send the message asynchronously
    pub async fn send_fire_and_forget(
        &self,
        peer: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let data = data.to_vec();
        let socket_pool = self.socket_pool.clone();

        // Spawn task to send without waiting
        tokio::spawn(async move {
            if let Err(e) = Self::send_with_socket_pool(&socket_pool, peer, &data).await {
                tracing::warn!("Fire-and-forget send failed to {}: {}", peer, e);
            }
        });

        Ok(())
    }

    /// Internal helper to send data using the socket pool
    async fn send_with_socket_pool(
        socket_pool: &Arc<RwLock<TcpSocketPool>>,
        peer: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let mut stream = tokio::net::TcpStream::connect(peer).await?;

        // Write length prefix
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;

        // Write payload
        stream.write_all(data).await?;
        stream.flush().await?;

        Ok(())
    }

    /// Send raw data to a peer and wait for response
    pub async fn send_request_response(
        &self,
        peer: SocketAddr,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        let mut stream = tokio::net::TcpStream::connect(peer).await?;

        // Write request with length prefix
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(data).await?;
        stream.flush().await?;

        // Read response length
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let response_len = u32::from_be_bytes(len_bytes) as usize;

        if response_len > 10 * 1024 * 1024 {
            // 10MB limit
            return Err(ColibriError::Transport("Response too large".to_string()));
        }

        // Read response data
        let mut response_data = vec![0u8; response_len];
        stream.read_exact(&mut response_data).await?;

        Ok(response_data)
    }

    // ============================================================================
    // BROADCAST OPERATIONS
    // ============================================================================

    /// Broadcast raw data to multiple peers (fire-and-forget)
    /// Returns number of successful sends
    pub async fn broadcast(
        &self,
        peers: Vec<SocketAddr>,
        data: &[u8],
    ) -> Result<usize> {
        if peers.is_empty() {
            return Err(ColibriError::Transport("No peers to broadcast to".to_string()));
        }

        let mut success_count = 0;

        for peer in peers {
            match self.send_fire_and_forget(peer, data).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    tracing::debug!("Failed to broadcast to {}: {}", peer, e);
                }
            }
        }

        if success_count == 0 {
            return Err(ColibriError::Transport(
                "Failed to broadcast to any peers".to_string()
            ));
        }

        Ok(success_count)
    }

    /// Broadcast to a subset of random peers (gossip fanout)
    pub async fn broadcast_random(
        &self,
        fanout: usize,
        data: &[u8],
    ) -> Result<usize> {
        let peer_ids = self.get_peers().await;

        if peer_ids.is_empty() {
            return Err(ColibriError::Transport("No peers available".to_string()));
        }

        // Get peer addresses from the socket pool
        let peers: Vec<SocketAddr> = {
            let pool = self.socket_pool.read().await;
            peer_ids.iter()
                .filter_map(|id| pool.get_peer_address(*id))
                .collect()
        };

        // Select random peers up to fanout count
        let selected_peers: Vec<SocketAddr> = if fanout >= peers.len() {
            peers
        } else {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            let mut selected = peers;
            selected.shuffle(&mut rng);
            selected.into_iter().take(fanout).collect()
        };

        self.broadcast(selected_peers, data).await
    }
}

#[async_trait]
impl Sender for TcpTransport {
    /// Send data to a specific peer by NodeId (fire-and-forget)
    async fn send_to_peer(&self, target: NodeId, data: &[u8]) -> Result<()> {
        // For TCP, this could be a simplified send without waiting for response
        // or we could adapt it to send and ignore the response
        let _response = self.socket_pool.read().await
            .send_request_response(target, data).await?;
        Ok(())
    }

    /// Send data to a random peer (fire-and-forget)
    async fn send_to_random_peer(&self, data: &[u8]) -> Result<NodeId> {
        let (node_id, _response) = self.socket_pool.read().await
            .send_request_response_random(data).await?;
        Ok(node_id)
    }

    /// Send data to multiple random peers (fire-and-forget)
    async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<NodeId>> {
        let mut sent_to = Vec::new();
        for _ in 0..count {
            match self.send_to_random_peer(data).await {
                Ok(node_id) => sent_to.push(node_id),
                Err(_) => break, // Stop on first error
            }
        }
        if sent_to.is_empty() {
            Err(crate::error::ColibriError::Transport("All sends failed".to_string()))
        } else {
            Ok(sent_to)
        }
    }

    /// Add a new peer to the socket pool
    async fn add_peer(&mut self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        self.socket_pool.write().await.add_peer(node_id, addr).await
    }

    /// Remove a peer from the socket pool
    async fn remove_peer(&self, node_id: NodeId) -> Result<()> {
        self.socket_pool.write().await.remove_peer(node_id).await
    }

    /// Get list of current peer NodeIds
    async fn get_peers(&self) -> Vec<NodeId> {
        self.socket_pool.read().await.get_peers().await
    }

    /// Get transport statistics
    async fn get_stats(&self) -> FrozenSocketPoolStats {
        self.socket_pool.read().await.get_stats().freeze()
    }
}

#[async_trait]
impl RequestSender for TcpTransport {
    /// Send request to specific peer and wait for response
    async fn send_request_response(
        &self,
        target: NodeId,
        request_data: &[u8],
    ) -> Result<Vec<u8>> {
        self.socket_pool
            .read()
            .await
            .send_request_response(target, request_data)
            .await
    }

    /// Send request to random peer and wait for response
    async fn send_request_response_random(
        &self,
        request_data: &[u8],
    ) -> Result<(NodeId, Vec<u8>)> {
        self.socket_pool
            .read()
            .await
            .send_request_response_random(request_data)
            .await
    }

    /// Cleanup expired connections
    async fn cleanup_expired_connections(&self) {
        self.socket_pool
            .read()
            .await
            .cleanup_expired_connections()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::messages::{CheckCallsRequest, CheckCallsResponse};
    use crate::settings::TransportConfig;
    use indexmap::IndexMap;
    use crate::node::NodeName;

    fn create_test_transport_config() -> TransportConfig {
        let mut topology = IndexMap::new();
        topology.insert(
            NodeName::from("node-a").node_id(),
            "127.0.0.1:9001".parse().unwrap(),
        );
        topology.insert(
            NodeName::from("node-b").node_id(),
            "127.0.0.1:9002".parse().unwrap(),
        );

        TransportConfig {
            node_name: NodeName::from("test-node"),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 9000,
            topology,
        }
    }

    #[tokio::test]
    async fn test_tcp_transport_creation() {
        let config = create_test_transport_config();
        let transport = TcpTransport::new(&config).await;
        assert!(transport.is_ok());

        let transport = transport.unwrap();
        let peers = transport.get_peers().await;
        assert_eq!(peers.len(), 2);
    }

    #[tokio::test]
    async fn test_message_serialization() {
        let message = Message::RateLimitRequest(CheckCallsRequest {
            key: "test-key".to_string(),
            calls_requested: 1,
        });

        let serialized = message.serialize();
        assert!(serialized.is_ok());

        let data = serialized.unwrap();
        let deserialized = Message::deserialize(&data);
        assert!(deserialized.is_ok());

        match deserialized.unwrap() {
            Message::RateLimitRequest(req) => {
                assert_eq!(req.key, "test-key");
                assert_eq!(req.calls_requested, 1);
            }
            _ => panic!("Wrong message variant"),
        }
    }

    #[tokio::test]
    async fn test_fire_and_forget_returns_immediately() {
        let config = create_test_transport_config();
        let transport = TcpTransport::new(&config).await.unwrap();

        let message = Message::RateLimitRequest(CheckCallsRequest {
            key: "test".to_string(),
            calls_requested: 1,
        });

        // This should return immediately even if peer is not available
        let result = transport
            .send_message_fire_and_forget(
                "127.0.0.1:9999".parse().unwrap(),
                &message,
            )
            .await;

        // Fire-and-forget always returns Ok (errors are logged)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_broadcast_to_empty_peers() {
        let config = create_test_transport_config();
        let transport = TcpTransport::new(&config).await.unwrap();

        let message = Message::RateLimitResponse(CheckCallsResponse {
            allowed: true,
            remaining: 10,
        });

        // Broadcasting to empty list should fail
        let result = transport.broadcast_message(vec![], &message).await;
        assert!(result.is_err());
    }
}

