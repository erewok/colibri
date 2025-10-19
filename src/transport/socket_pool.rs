//! UDP Socket Pool
//!
//! Manages a pool of UDP sockets for each peer in the cluster.
//! Provides load balancing and fault tolerance through socket rotation.
use rand::prelude::IndexedRandom;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::UdpSocket;

use crate::error::{ColibriError, GossipError, Result};

/// Pool of UDP sockets for efficient peer communication
pub struct UdpSocketPool {
    peers: Arc<RwLock<HashMap<SocketAddr, PeerSocketPool>>>,
    stats: Arc<SocketPoolStats>,
}

/// Statistics for the socket pool
#[derive(Debug, Default)]
pub struct SocketPoolStats {
    pub peer_count: AtomicUsize,
    pub total_sockets: AtomicUsize,
    pub messages_sent: AtomicU64,
    pub send_errors: AtomicU64,
}

/// Socket pool for a single peer
struct PeerSocketPool {
    sockets: Vec<Arc<UdpSocket>>,
    next_socket: AtomicUsize,
}

impl UdpSocketPool {
    /// Create a new socket pool
    pub async fn new(
        peer_addrs: HashSet<std::net::SocketAddr>,
        pool_size_per_peer: usize,
    ) -> Result<Self> {
        let mut peers = HashMap::new();
        let mut total_sockets = 0;

        for peer_addr in peer_addrs {
            let peer_pool = PeerSocketPool::new(pool_size_per_peer).await?;
            total_sockets += pool_size_per_peer;
            peers.insert(peer_addr, peer_pool);
        }

        let stats = Arc::new(SocketPoolStats {
            peer_count: AtomicUsize::new(peers.len()),
            total_sockets: AtomicUsize::new(total_sockets),
            messages_sent: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
        });

        Ok(Self {
            peers: Arc::new(RwLock::new(peers)),
            stats,
        })
    }

    /// Send data to a specific peer
    pub async fn send_to(&self, target: SocketAddr, data: &[u8]) -> Result<()> {
        let peers = self.peers.read();
        let peer_pool = peers.get(&target).ok_or_else(|| {
            ColibriError::Gossip(GossipError::Transport(format!(
                "Peer not found: {}",
                target
            )))
        })?;

        match peer_pool.send(target, data).await {
            Ok(()) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Send data to a random peer
    pub async fn send_to_random(&self, data: &[u8]) -> Result<SocketAddr> {
        let peers = self.peers.read();
        let peer_addrs: Vec<SocketAddr> = peers.keys().cloned().collect();

        if peer_addrs.is_empty() {
            return Err(ColibriError::Gossip(GossipError::Transport(
                "No peers available".to_string(),
            )));
        }
        let mut rng = rand::rng();
        let target = *peer_addrs.choose(&mut rng).unwrap();

        self.send_to(target, data).await?;
        Ok(target)
    }

    /// Send data to multiple random peers
    pub async fn send_to_random_multiple(
        &self,
        data: &[u8],
        count: usize,
    ) -> Result<Vec<SocketAddr>> {
        let peers = self.peers.read();
        let peer_addrs: Vec<SocketAddr> = peers.keys().cloned().collect();

        if peer_addrs.is_empty() {
            return Err(ColibriError::Gossip(GossipError::Transport(
                "No peers available".to_string(),
            )));
        }

        let mut rng = rand::rng();
        let selected_count = count.min(peer_addrs.len());
        let selected_peers: Vec<SocketAddr> = peer_addrs
            .choose_multiple(&mut rng, selected_count)
            .cloned()
            .collect();

        let mut successful_sends = Vec::new();

        for target in selected_peers {
            match self.send_to(target, data).await {
                Ok(()) => successful_sends.push(target),
                Err(_) => {
                    // Log error but continue with other peers
                    eprintln!("Failed to send to peer: {}", target);
                }
            }
        }

        if successful_sends.is_empty() {
            Err(ColibriError::Gossip(GossipError::Transport(
                "All sends failed".to_string(),
            )))
        } else {
            Ok(successful_sends)
        }
    }

    /// Add a new peer to the pool
    pub async fn add_peer(&self, peer_addr: SocketAddr, pool_size: usize) -> Result<()> {
        let mut peers = self.peers.write();

        if peers.contains_key(&peer_addr) {
            return Ok(()); // Peer already exists
        }

        let peer_pool = PeerSocketPool::new(pool_size).await?;
        peers.insert(peer_addr, peer_pool);

        self.stats.peer_count.store(peers.len(), Ordering::Relaxed);
        self.stats
            .total_sockets
            .fetch_add(pool_size, Ordering::Relaxed);

        Ok(())
    }

    /// Remove a peer from the pool
    pub async fn remove_peer(&self, peer_addr: SocketAddr) -> Result<()> {
        let mut peers = self.peers.write();

        if let Some(peer_pool) = peers.remove(&peer_addr) {
            let socket_count = peer_pool.sockets.len();
            self.stats.peer_count.store(peers.len(), Ordering::Relaxed);
            self.stats
                .total_sockets
                .fetch_sub(socket_count, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Get list of current peers
    pub fn get_peers(&self) -> Vec<SocketAddr> {
        self.peers.read().keys().cloned().collect()
    }

    /// Get socket pool statistics
    pub fn get_stats(&self) -> SocketPoolStats {
        SocketPoolStats {
            peer_count: AtomicUsize::new(self.stats.peer_count.load(Ordering::Relaxed)),
            total_sockets: AtomicUsize::new(self.stats.total_sockets.load(Ordering::Relaxed)),
            messages_sent: AtomicU64::new(self.stats.messages_sent.load(Ordering::Relaxed)),
            send_errors: AtomicU64::new(self.stats.send_errors.load(Ordering::Relaxed)),
        }
    }
}

impl PeerSocketPool {
    /// Create a new peer socket pool
    async fn new(pool_size: usize) -> Result<Self> {
        let mut sockets = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|e| {
                ColibriError::Gossip(GossipError::Transport(format!(
                    "Socket creation failed: {}",
                    e
                )))
            })?;
            sockets.push(Arc::new(socket));
        }

        Ok(Self {
            sockets,
            next_socket: AtomicUsize::new(0),
        })
    }

    /// Send data using the next available socket (round-robin)
    async fn send(&self, target: SocketAddr, data: &[u8]) -> Result<()> {
        if self.sockets.is_empty() {
            return Err(ColibriError::Gossip(GossipError::Transport(
                "No sockets available for peer".to_string(),
            )));
        }

        let socket_index = self.next_socket.fetch_add(1, Ordering::Relaxed) % self.sockets.len();
        let socket = &self.sockets[socket_index];

        socket.send_to(data, target).await.map_err(|e| {
            ColibriError::Gossip(GossipError::Transport(format!("Send failed: {}", e)))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_socket_pool_creation() {
        let one = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let two = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002);
        let peers: HashSet<SocketAddr> = HashSet::from([one.clone(), two.clone()]);

        let pool = UdpSocketPool::new(peers.clone(), 3).await.unwrap();

        let stats = pool.get_stats();
        assert_eq!(stats.peer_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.total_sockets.load(Ordering::Relaxed), 6);

        let pool_peers = pool.get_peers();
        assert_eq!(pool_peers.len(), 2);
        assert!(pool_peers.contains(&one));
        assert!(pool_peers.contains(&two));
    }

    #[tokio::test]
    async fn test_peer_management() {
        let pool = UdpSocketPool::new(HashSet::new(), 2).await.unwrap();

        let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002);

        // Add peers
        pool.add_peer(peer1, 3).await.unwrap();
        pool.add_peer(peer2, 2).await.unwrap();

        let stats = pool.get_stats();
        assert_eq!(stats.peer_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.total_sockets.load(Ordering::Relaxed), 5);

        // Remove a peer
        pool.remove_peer(peer1).await.unwrap();

        let stats = pool.get_stats();
        assert_eq!(stats.peer_count.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_sockets.load(Ordering::Relaxed), 2);

        let remaining_peers = pool.get_peers();
        assert_eq!(remaining_peers.len(), 1);
        assert!(remaining_peers.contains(&peer2));
    }

    #[tokio::test]
    async fn test_send_to_random_no_peers() {
        let pool = UdpSocketPool::new(HashSet::new(), 1).await.unwrap();

        let result = pool.send_to_random(b"test").await;
        assert!(matches!(
            result,
            Err(ColibriError::Gossip(GossipError::Transport(_)))
        ));
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_peer() {
        let pool = UdpSocketPool::new(HashSet::new(), 1).await.unwrap();

        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let result = pool.send_to(peer, b"test").await;
        assert!(matches!(
            result,
            Err(ColibriError::Gossip(GossipError::Transport(_)))
        ));
    }

    #[tokio::test]
    async fn test_peer_socket_pool_round_robin() {
        let pool = PeerSocketPool::new(3).await.unwrap();

        // The round-robin behavior is internal, but we can test that
        // multiple sends don't fail due to socket management
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);

        // These will fail to send since nothing is listening, but should
        // exercise the round-robin socket selection
        for _ in 0..10 {
            let _ = pool.send(target, b"test").await;
            // Each call should use a different socket in round-robin fashion
        }
    }

    #[tokio::test]
    async fn test_send_to_random_multiple() {
        let peers: HashSet<SocketAddr> = HashSet::from([
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8003),
        ]);

        let pool = UdpSocketPool::new(peers, 1).await.unwrap();

        // Should attempt to send to 2 random peers
        // This will likely fail since nothing is listening, but tests the selection logic
        let result = pool.send_to_random_multiple(b"test", 2).await;

        // Either succeeds with 0-2 peers (if sends fail) or fails completely
        match result {
            Ok(sent_to) => assert!(sent_to.len() <= 2),
            Err(ColibriError::Gossip(GossipError::Transport(_))) => {
                // Expected when no one is listening
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
}
