//! Generic UDP Transport Module
//!
//! Provides a pool of UDP unicast sockets for distributed communication.
//! Implements fire-and-forget requests for gossip nodes to send gossip messages.
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;

pub use super::common::{FrozenReceiverStats, FrozenSocketPoolStats};
pub use super::socket_pool_udp::UdpSocketPool;
pub use super::udp_receiver::UdpReceiver;
use crate::error::Result;
use crate::node::NodeId;
use crate::settings;

#[derive(Clone, Debug)]
pub struct UdpTransport {
    socket_pool: Arc<RwLock<UdpSocketPool>>,
}

impl UdpTransport {
    /// Create a new UDP transport instance
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
    pub async fn get_stats(&self) -> FrozenSocketPoolStats {
        self.socket_pool.read().await.get_stats().freeze()
    }
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
            listen_tcp: "127.0.0.1:0".parse().unwrap(),
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
