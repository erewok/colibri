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
use crate::error::Result;
use crate::node::NodeId;
use crate::settings::TransportConfig;

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
mod tests {}
