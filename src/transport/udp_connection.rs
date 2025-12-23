//! UDP Transport Implementation
//!
//! Provides UDP-based transport implementing the Sender trait.
//! Used primarily for fire-and-forget messaging in gossip protocols.
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::traits::Sender;
use super::stats::FrozenSocketPoolStats;
use super::socket_pool_udp::UdpSocketPool;
use crate::error::Result;
use crate::node::NodeId;
use crate::settings::TransportConfig;

#[derive(Clone, Debug)]
pub struct UdpTransport {
    socket_pool: Arc<RwLock<UdpSocketPool>>,
}

impl UdpTransport {
    /// Create a new UDP transport instance
    pub async fn new(transport_config: &TransportConfig) -> Result<Self> {
        let socket_pool = UdpSocketPool::new(transport_config).await?;

        Ok(Self {
            socket_pool: Arc::new(RwLock::new(socket_pool)),
        })
    }
}

#[async_trait]
impl Sender for UdpTransport {
    /// Send data to a specific peer by NodeId
    async fn send_to_peer(&self, target: NodeId, data: &[u8]) -> Result<()> {
        self.socket_pool.read().await.send_to(target, data).await?;
        Ok(())
    }

    /// Send data to a random peer (for gossip)
    async fn send_to_random_peer(&self, data: &[u8]) -> Result<NodeId> {
        self.socket_pool.read().await.send_to_random(data).await
    }

    /// Send data to multiple random peers (for gossip)
    async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<NodeId>> {
        self.socket_pool
            .read()
            .await
            .send_to_random_peers(data, count)
            .await
    }

    /// Add a new peer to the socket pool
    async fn add_peer(&mut self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        self.socket_pool
            .write()
            .await
            .add_peer(node_id, addr)
            .await
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

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use super::*;
    use crate::node::NodeName;
    use std::net::SocketAddr;

    fn get_transport_config() -> TransportConfig {
        let mut topology = IndexMap::new();
        topology.insert(
            NodeName::from("node_a").node_id(),
            "127.0.0.1:8001".parse().unwrap(),
        );
        topology.insert(
            NodeName::from("node_b").node_id(),
            "127.0.0.1:8002".parse().unwrap(),
        );

        TransportConfig {
            node_name: NodeName::from("test_node"),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8000,
            topology,
        }
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport_config = get_transport_config();
        let transport = UdpTransport::new(&transport_config)
            .await
            .unwrap();

        assert_eq!(transport.get_peers().await.len(), 2);
    }
}
