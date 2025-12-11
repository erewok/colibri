//! Generic TCP Transport Module
//!
//! Provides a pool of TCP unicast sockets for distributed communication.
//! Implements requests with responses for hashring nodes to communicate with each other.
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;

pub use super::common::SocketPoolStats;
pub use super::socket_pool_tcp::TcpSocketPool;
use crate::error::Result;
use crate::node::NodeId;
use crate::transport::common::FrozenSocketPoolStats;

#[derive(Clone, Debug)]
pub struct TcpTransport {
    socket_pool: Arc<RwLock<TcpSocketPool>>,
}

impl TcpTransport {
    /// Create a new TCP transport instance
    pub async fn new(
        node_id: NodeId,
        cluster_nodes: Vec<SocketAddr>,
        max_connections_per_peer: usize,
    ) -> Result<Self> {
        let socket_pool =
            TcpSocketPool::new(node_id, cluster_nodes, max_connections_per_peer).await?;

        Ok(Self {
            socket_pool: Arc::new(RwLock::new(socket_pool)),
        })
    }

    /// Send request to specific peer and wait for response
    pub async fn send_request_response(
        &self,
        target: SocketAddr,
        request_data: &[u8],
    ) -> Result<Vec<u8>> {
        self.socket_pool
            .read()
            .await
            .send_request_response(target, request_data)
            .await
    }

    /// Send request to random peer and wait for response
    pub async fn send_request_response_random(
        &self,
        request_data: &[u8],
    ) -> Result<(SocketAddr, Vec<u8>)> {
        self.socket_pool
            .read()
            .await
            .send_request_response_random(request_data)
            .await
    }

    /// Add a new peer to the socket pool
    pub async fn add_peer(&mut self, peer_addr: SocketAddr) -> Result<()> {
        self.socket_pool.write().await.add_peer(peer_addr).await
    }

    /// Remove a peer from the socket pool
    pub async fn remove_peer(&self, peer_addr: SocketAddr) -> Result<()> {
        self.socket_pool.write().await.remove_peer(peer_addr).await
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
        self.socket_pool.read().await.get_peers().await
    }

    /// Cleanup expired connections
    pub async fn cleanup_expired_connections(&self) {
        self.socket_pool
            .read()
            .await
            .cleanup_expired_connections()
            .await;
    }
    /// Get transport statistics
    pub async fn get_stats(&self) -> FrozenSocketPoolStats {
        self.socket_pool.read().await.get_stats().freeze()
    }
}

#[cfg(test)]
mod tests {}
