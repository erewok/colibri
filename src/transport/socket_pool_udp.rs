//! UDP Socket Pool
//!
//! Manages a pool of UDP sockets for each peer in the cluster.
use rand::Rng;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::error;

use super::common::SocketPoolStats;
use crate::error::{ColibriError, Result};
use crate::node::NodeId;

/// Pool of UDP sockets for efficient peer communication
#[derive(Clone, Debug)]
pub struct UdpSocketPool {
    // IndexMap for easily getting a random peer
    peers: IndexMap<SocketAddr, Arc<Mutex<UdpSocket>>>,
    // for debugging, identifying the node
    node_id: NodeId,
    // for statistics and monitoring
    stats: Arc<SocketPoolStats>,
}

impl UdpSocketPool {
    /// Create a new socket pool
    pub async fn new(node_id: NodeId, peer_addrs: &HashSet<std::net::SocketAddr>) -> Result<Self> {
        let mut peers = IndexMap::new();

        for peer_addr in peer_addrs {
            let local_addr: SocketAddr = if peer_addr.is_ipv4() {
                "0.0.0.0:0".parse().unwrap()
            } else {
                "[::]:0".parse().unwrap()
            };

            // need to bind to a local address!
            let socket = UdpSocket::bind(local_addr)
                .await
                .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;
            peers.insert(*peer_addr, Arc::new(Mutex::new(socket)));
        }

        let stats = Arc::new(SocketPoolStats::new(peers.len()));

        Ok(Self {
            node_id,
            peers,
            stats,
        })
    }

    /// Send data to a specific peer
    pub async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<SocketAddr> {
        let peer_socket = self
            .peers
            .get(&target)
            .ok_or_else(|| ColibriError::Transport(format!("Peer not found: {}", target)))?;

        match peer_socket.lock().await.send_to(data, target).await {
            Ok(_write_size) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                Ok(target)
            }
            Err(e) => {
                self.stats
                    .errors
                    .send_errors
                    .fetch_add(1, Ordering::Relaxed);
                error!("[{}] Failed to send UDP data: {}", target, e);
                Err(ColibriError::Io(e))
            }
        }
    }

    /// Send data to a random peer
    pub async fn send_to_random(&self, data: &[u8]) -> Result<SocketAddr> {
        let random_usize: usize = rand::rng().random_range(0..self.peers.len());
        match self.peers.get_index(random_usize) {
            Some((target, _)) => {
                self.send_to(*target, data).await?;
                Ok(*target)
            }
            None => Err(ColibriError::Transport("No peers available".to_string())),
        }
    }

    /// Send data to multiple random peers
    pub async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<SocketAddr>> {
        if self.peers.is_empty() {
            return Err(ColibriError::Transport(
                "No peers available for sending".to_string(),
            ));
        }
        // Now send to each peer without holding the main lock
        let mut successful_sends = Vec::new();
        // Do not allow the random thread_range to be used across await points: generate all random indexes now.
        let rand_indexs = {
            let mut rng = rand::rng();
            [..count].map(|_| rng.random_range(0..self.peers.len()))
        };
        for random_usize in rand_indexs {
            if let Some((target, _)) = self.peers.get_index(random_usize) {
                match self.send_to(*target, data).await {
                    Ok(_) => successful_sends.push(*target),
                    Err(_) => {
                        // Log error but continue with other peers
                        error!("[{}] Failed to send to peer: {}", self.node_id, target);
                    }
                }
            }
        }

        if successful_sends.is_empty() {
            Err(ColibriError::Transport("All sends failed".to_string()))
        } else {
            Ok(successful_sends)
        }
    }

    /// Add a new peer to the pool
    pub async fn add_peer(&mut self, peer_addr: SocketAddr, pool_size: usize) -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;
        self.peers.insert(peer_addr, Arc::new(Mutex::new(socket)));

        self.stats
            .peer_count
            .store(self.peers.len(), Ordering::Relaxed);
        self.stats
            .total_connections
            .fetch_add(pool_size, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a peer from the pool
    pub async fn remove_peer(&mut self, peer_addr: SocketAddr) -> Result<()> {
        self.peers.swap_remove(&peer_addr);
        Ok(())
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
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

    #[tokio::test]
    async fn test_send_to_nonexistent_peer() {
        let pool = UdpSocketPool::new(NodeId::new(1), &HashSet::new())
            .await
            .unwrap();

        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let result = pool.send_to(peer, b"test").await;
        assert!(matches!(result, Err(ColibriError::Transport(_))));
    }

    #[tokio::test]
    async fn test_send_to_random_multiple() {
        let peers: HashSet<SocketAddr> = HashSet::from([
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8003),
        ]);

        let pool = UdpSocketPool::new(NodeId::new(2), &peers).await.unwrap();

        // Should attempt to send to 2 random peers
        // This will likely fail since nothing is listening, but tests the selection logic
        let result = pool.send_to_random_peers(b"test", 2).await;

        // Either succeeds with 0-2 peers (if sends fail) or fails completely
        match result {
            Ok(sent_to) => assert!(sent_to.len() <= 2),
            Err(ColibriError::Transport(_)) => {
                // Expected when no one is listening
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
}
