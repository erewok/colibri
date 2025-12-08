//! Generic UDP Transport Module
//!
//! Provides a pool of UDP unicast sockets for distributed communication.
//! Supports both request-response patterns (for consistent hashing) and
//! fire-and-forget patterns (for gossip protocols).
pub mod common;
pub mod receiver;
pub mod socket_pool_tcp;
pub mod socket_pool_udp;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::Result;
use crate::node::NodeId;
use crate::settings;
pub use common::SocketPoolStats;
pub use receiver::{ReceiverStats, UdpReceiver};
pub use socket_pool_tcp::{TcpSocketPool, TcpSocketPoolStats};
pub use socket_pool_udp::UdpSocketPool;

#[derive(Clone, Debug)]
pub struct UdpTransport {
    socket_pool: Arc<RwLock<UdpSocketPool>>,
}

#[derive(Clone, Debug)]
pub struct TcpTransport {
    socket_pool: Arc<RwLock<TcpSocketPool>>,
}

impl UdpTransport {
    /// Create a new UDP transport instance
    ///
    /// # Arguments
    /// * `listen_address` - Address to bind the receiver socket to
    /// * `cluster_socket_addresses` - URLs of other cluster members
    /// * `pool_size_per_peer` - Number of sockets to create per peer
    pub async fn new(
        node_id: NodeId,
        transport_config: &settings::TransportConfig,
    ) -> Result<Self> {
        // Create socket pool
        let socket_pool = UdpSocketPool::new(node_id, &transport_config.topology).await?;

        Ok(Self {
            socket_pool: Arc::new(RwLock::new(socket_pool)),
        })
    }
    /// Send data to a specific peer (for consistent hashing)
    pub async fn send_to_peer(&self, target: SocketAddr, data: &[u8]) -> Result<SocketAddr> {
        self.socket_pool.read().await.send_to(target, data).await
    }

    /// Send data to a random peer (for gossip)
    pub async fn send_to_random_peer(&self, data: &[u8]) -> Result<SocketAddr> {
        self.socket_pool.read().await.send_to_random(data).await
    }

    /// Send data to multiple random peers (for gossip)
    pub async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<SocketAddr>> {
        self.socket_pool
            .read()
            .await
            .send_to_random_peers(data, count)
            .await
    }

    /// Add a new peer to the socket pool
    pub async fn add_peer(&mut self, peer_addr: SocketAddr, pool_size: usize) -> Result<()> {
        self.socket_pool
            .write()
            .await
            .add_peer(peer_addr, pool_size)
            .await
    }

    /// Remove a peer from the socket pool
    pub async fn remove_peer(&self, peer_addr: SocketAddr) -> Result<()> {
        self.socket_pool.write().await.remove_peer(peer_addr).await
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
        self.socket_pool.read().await.get_peers().await
    }

    /// Get transport statistics
    pub async fn get_stats(&self) -> TransportStats {
        let send_pool_stats = self.socket_pool.read().await.get_stats();
        TransportStats { send_pool_stats }
    }
}

impl TcpTransport {
    /// Create a new TCP transport instance for request-response patterns
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

    /// Get TCP transport statistics
    pub async fn get_stats(&self) -> TcpTransportStats {
        let socket_pool = self.socket_pool.read().await;
        let tcp_pool_stats = socket_pool.get_stats();
        TcpTransportStats {
            peer_count: tcp_pool_stats
                .peer_count
                .load(std::sync::atomic::Ordering::Relaxed),
            total_connections: tcp_pool_stats
                .total_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            active_connections: tcp_pool_stats
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            requests_sent: tcp_pool_stats
                .requests_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            responses_received: tcp_pool_stats
                .responses_received
                .load(std::sync::atomic::Ordering::Relaxed),
            connection_errors: tcp_pool_stats
                .connection_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            timeout_errors: tcp_pool_stats
                .timeout_errors
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Cleanup expired connections
    pub async fn cleanup_expired_connections(&self) {
        self.socket_pool
            .read()
            .await
            .cleanup_expired_connections()
            .await;
    }
}

/// Transport statistics
#[derive(Debug)]
pub struct TransportStats {
    pub send_pool_stats: SocketPoolStats,
}

/// TCP Transport statistics
#[derive(Debug)]
pub struct TcpTransportStats {
    pub peer_count: usize,
    pub total_connections: usize,
    pub active_connections: usize,
    pub requests_sent: u64,
    pub responses_received: u64,
    pub connection_errors: u64,
    pub timeout_errors: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn get_transport_config() -> settings::TransportConfig {
        let mut cluster_urls = HashSet::new();
        cluster_urls.insert("127.0.0.1:8001".parse().unwrap());
        cluster_urls.insert("127.0.0.1:8002".parse().unwrap());

        settings::TransportConfig {
            listen_udp: "127.0.0.1:0".parse().unwrap(),
            topology: cluster_urls,
        }
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport_config = get_transport_config();
        let transport = UdpTransport::new(NodeId::new(0), &transport_config)
            .await
            .unwrap();

        assert_eq!(transport.get_peers().await.len(), 2);
    }
}
