//! Generic UDP Transport Module
//!
//! Provides a pool of UDP unicast sockets for distributed communication.
//! Supports both request-response patterns (for consistent hashing) and
//! fire-and-forget patterns (for gossip protocols).

pub mod receiver;
pub mod socket_pool;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::Result;
use crate::settings;
pub use receiver::{ReceiverStats, UdpReceiver};
pub use socket_pool::{SocketPoolStats, UdpSocketPool};

#[derive(Clone, Debug)]
pub struct UdpTransport {
    socket_pool: Arc<RwLock<UdpSocketPool>>,
}

impl UdpTransport {
    /// Create a new UDP transport instance
    ///
    /// # Arguments
    /// * `listen_address` - Address to bind the receiver socket to
    /// * `cluster_socket_addresses` - URLs of other cluster members
    /// * `pool_size_per_peer` - Number of sockets to create per peer
    pub async fn new(
        node_id: u32,
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
        TransportStats {
            send_pool_stats,
        }
    }
}

/// Transport statistics
#[derive(Debug)]
pub struct TransportStats {
    pub send_pool_stats: SocketPoolStats,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::error::ColibriError;

    fn get_transport_config() -> settings::TransportConfig {
        let mut cluster_urls = HashSet::new();
        cluster_urls.insert("127.0.0.1:8001".parse().unwrap());
        cluster_urls.insert("127.0.0.1:8002".parse().unwrap());

        settings::TransportConfig {
            listen_tcp: "127.0.0.1:0".parse().unwrap(),
            listen_udp: "127.0.0.1:0".parse().unwrap(),
            topology: cluster_urls,
        }
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport_config = get_transport_config();
        let transport = UdpTransport::new(0, &transport_config)
            .await
            .unwrap();

        assert_eq!(transport.get_peers().await.len(), 2);
    }

    #[tokio::test]
    async fn test_send_to_random_peer() {
        let transport_config = get_transport_config();

        let transport = UdpTransport::new(0, &transport_config)
            .await
            .unwrap();

        // This will fail to send since no one is listening, but tests the mechanism
        let result = transport.send_to_random_peer(b"test").await;
        // Should succeed in selecting and attempting to send to a peer
        assert!(result.is_ok() || matches!(result, Err(ColibriError::Transport(_))));
    }

    #[tokio::test]
    async fn test_peer_management() {
        let transport_config = get_transport_config();
        let mut transport = UdpTransport::new(0, &transport_config)
            .await
            .unwrap();

        assert_eq!(transport.get_peers().await.len(), 0);

        let peer_addr = "127.0.0.1:8001".parse().unwrap();
        transport.add_peer(peer_addr, 2).await.unwrap();
        let peers = transport.get_peers().await;
        assert_eq!(peers.len(), 1);
        assert!(peers.contains(&peer_addr));

        transport.remove_peer(peer_addr).await.unwrap();
        assert_eq!(transport.get_peers().await.len(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let transport_config = get_transport_config();
        let transport = UdpTransport::new(0, &transport_config)
            .await
            .unwrap();
        let stats = transport.get_stats().await;

        assert_eq!(stats.send_pool_stats.peer_count.into_inner(), 0);
        assert_eq!(stats.send_pool_stats.total_sockets.into_inner(), 0);
    }
}
