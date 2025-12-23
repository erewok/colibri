//! UDP Socket Pool
//!
//! Manages a pool of UDP sockets for each peer in the cluster.
//! Uses NodeId as the primary identifier for peers.
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::error;

use crate::error::{ColibriError, Result};
use crate::node::{NodeId, NodeName};
use crate::settings::TransportConfig;
use super::stats::SocketPoolStats;

/// Pool of UDP sockets for efficient peer communication
#[derive(Clone, Debug)]
pub struct UdpSocketPool {
    // IndexMap for easily getting a random peer
    peers: IndexMap<NodeId, PeerInfo>,
    // for debugging, identifying the node
    node_name: NodeName,
    // for statistics and monitoring
    stats: Arc<SocketPoolStats>,
}

#[derive(Clone, Debug)]
struct PeerInfo {
    socket_addr: SocketAddr,
    socket: Arc<Mutex<UdpSocket>>,
}

impl UdpSocketPool {
    /// Create a new socket pool
    pub async fn new(settings: &TransportConfig) -> Result<Self> {
        let mut peers = IndexMap::new();

        for (node_id, peer_addr) in &settings.topology {
            let local_addr: SocketAddr = if peer_addr.is_ipv4() {
                "0.0.0.0:0".parse().unwrap()
            } else {
                "[::]:0".parse().unwrap()
            };

            // need to bind to a local address!
            let socket = UdpSocket::bind(local_addr)
                .await
                .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

            let peer_info = PeerInfo {
                socket_addr: *peer_addr,
                socket: Arc::new(Mutex::new(socket)),
            };

            peers.insert(*node_id, peer_info);
        }

        let stats = Arc::new(SocketPoolStats::new(peers.len()));

        Ok(Self {
            node_name: settings.node_name.clone(),
            peers,
            stats,
        })
    }

    /// Send data to a specific peer
    pub async fn send_to(&self, target: NodeId, data: &[u8]) -> Result<()> {
        let peer_info = self
            .peers
            .get(&target)
            .ok_or_else(|| ColibriError::Transport(format!("Peer not found: {:?}", target)))?;

        match peer_info.socket.lock().await.send_to(data, peer_info.socket_addr).await {
            Ok(_write_size) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats
                    .errors
                    .send_errors
                    .fetch_add(1, Ordering::Relaxed);
                error!("[{:?}] Failed to send UDP data: {}", target, e);
                Err(ColibriError::Io(e))
            }
        }
    }

    /// Send data to a random peer
    pub async fn send_to_random(&self, data: &[u8]) -> Result<NodeId> {
        if self.peers.is_empty() {
            return Err(ColibriError::Transport("No peers available".to_string()));
        }

        let random_idx: usize = rand::rng().random_range(0..self.peers.len());
        match self.peers.get_index(random_idx) {
            Some((node_id, _)) => {
                self.send_to(*node_id, data).await?;
                Ok(*node_id)
            }
            None => Err(ColibriError::Transport("No peers available".to_string())),
        }
    }

    /// Send data to multiple random peers
    pub async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<NodeId>> {
        if self.peers.is_empty() {
            return Err(ColibriError::Transport(
                "No peers available for sending".to_string(),
            ));
        }

        let max_count = count.min(self.peers.len());
        let mut successful_sends = Vec::new();

        // Send to multiple random peers
        for _ in 0..max_count {
            self.send_to_random(data).await.map(|node_id| {
                successful_sends.push(node_id);
            });
        }

        if successful_sends.is_empty() {
            Err(ColibriError::Transport("All sends failed".to_string()))
        } else {
            Ok(successful_sends)
        }
    }

    /// Add a new peer to the pool
    pub async fn add_peer(&mut self, node_id: NodeId, peer_addr: SocketAddr) -> Result<()> {
        let local_addr: SocketAddr = if peer_addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let socket = UdpSocket::bind(local_addr)
            .await
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

        let peer_info = PeerInfo {
            socket_addr: peer_addr,
            socket: Arc::new(Mutex::new(socket)),
        };

        self.peers.insert(node_id, peer_info);

        self.stats
            .peer_count
            .store(self.peers.len(), Ordering::Relaxed);
        self.stats
            .total_connections
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a peer from the pool
    pub async fn remove_peer(&mut self, node_id: NodeId) -> Result<()> {
        self.peers.swap_remove(&node_id);
        self.stats
            .peer_count
            .store(self.peers.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<NodeId> {
        self.peers.keys().cloned().collect()
    }

    /// Get socket pool statistics
    pub fn get_stats(&self) -> &SocketPoolStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use indexmap::IndexMap;

    fn create_test_transport_config() -> TransportConfig {
        let mut topology = IndexMap::new();
        topology.insert(
            NodeName::from("test_peer").node_id(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001)
        );

        TransportConfig {
            node_name: NodeName::from("test_node"),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8000,
            topology,
        }
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_peer() {
        let config = create_test_transport_config();
        let pool = UdpSocketPool::new(&config)
            .await
            .unwrap();

        let result = pool.send_to(NodeName::new("nonexistent".to_string()).node_id(), b"test data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pool_creation() {
        let config = create_test_transport_config();
        let pool = UdpSocketPool::new(&config)
            .await
            .unwrap();

        assert_eq!(pool.get_peers().await.len(), 1);
    }
}
