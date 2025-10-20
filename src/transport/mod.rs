//! Generic UDP Transport Module
//!
//! Provides a pool of UDP unicast sockets for distributed communication.
//! Supports both request-response patterns (for consistent hashing) and
//! fire-and-forget patterns (for gossip protocols).

pub mod receiver;
pub mod socket_pool;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::RwLock;

use crate::error::Result;
pub use receiver::{ReceiverStats, UdpReceiver};
pub use socket_pool::{SocketPoolStats, UdpSocketPool};

#[derive(Clone)]
pub struct UdpTransport {
    socket_pool: Arc<RwLock<UdpSocketPool>>,
    receiver: Arc<UdpReceiver>,
    local_addr: SocketAddr,
}

impl UdpTransport {
    /// Create a new UDP transport instance
    ///
    /// # Arguments
    /// * `bind_port` - Port to bind the receiver socket to
    /// * `cluster_socket_addresses` - URLs of other cluster members
    /// * `pool_size_per_peer` - Number of sockets to create per peer
    pub async fn new(
        node_id: u32,
        bind_port: u16,
        cluster_socket_addresses: HashSet<SocketAddr>,
    ) -> Result<Self> {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], bind_port));

        // Create receiver socket
        let receiver = UdpReceiver::new(bind_addr).await?;
        let local_addr = receiver.local_addr();

        // Create socket pool
        let socket_pool = UdpSocketPool::new(node_id, cluster_socket_addresses).await?;

        Ok(Self {
            socket_pool: Arc::new(RwLock::new(socket_pool)),
            receiver: Arc::new(receiver),
            local_addr,
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
            .send_to_random_multiple(data, count)
            .await
    }

    /// Send request and wait for response (for consistent hashing)
    pub async fn send_request_with_response(
        &self,
        target: SocketAddr,
        request: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        // Send the request
        self.send_to_peer(target, request).await?;

        // Wait for response from the specific peer
        self.receiver.wait_for_response_from(target, timeout).await
    }

    /// Start receiving messages with an async callback
    pub async fn start_receiving_async<F, Fut>(&self, callback: F) -> Result<()>
    where
        F: Fn(Vec<u8>, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.receiver.start_receiving_async(callback).await
    }

    /// Get a channel receiver for incoming messages
    pub async fn get_message_receiver(&self) -> mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)> {
        self.receiver.get_message_receiver().await
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

    /// Get local socket address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
        self.socket_pool.read().await.get_peers().await
    }

    /// Get transport statistics
    pub async fn get_stats(&self) -> TransportStats {
        let send_pool_stats = self.socket_pool.read().await.get_stats();
        let receiver_stats = self.receiver.get_stats();

        TransportStats {
            local_addr: self.local_addr,
            receiver_stats,
            send_pool_stats,
        }
    }

    /// Get direct access to the receiver for advanced operations
    pub fn receiver(&self) -> &Arc<UdpReceiver> {
        &self.receiver
    }
}

/// Transport statistics
#[derive(Debug)]
pub struct TransportStats {
    pub local_addr: SocketAddr,
    pub receiver_stats: ReceiverStats,
    pub send_pool_stats: SocketPoolStats,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::error::{ColibriError, GossipError};

    #[tokio::test]
    async fn test_transport_creation() {
        let mut cluster_urls = HashSet::new();
        cluster_urls.insert("127.0.0.1:8001".parse().unwrap());
        cluster_urls.insert("127.0.0.1:8002".parse().unwrap());

        let transport = UdpTransport::new(1, 0, cluster_urls).await.unwrap();

        assert!(transport.local_addr().port() > 0);
        assert_eq!(transport.get_peers().await.len(), 2);
    }

    #[tokio::test]
    async fn test_send_to_random_peer() {
        let mut cluster_urls = HashSet::new();
        cluster_urls.insert("udp://127.0.0.1:8001".parse().unwrap());

        let transport = UdpTransport::new(1, 0, cluster_urls).await.unwrap();

        // This will fail to send since no one is listening, but tests the mechanism
        let result = transport.send_to_random_peer(b"test").await;
        // Should succeed in selecting and attempting to send to a peer
        assert!(result.is_ok() || matches!(result, Err(ColibriError::Transport(_))));
    }

    #[tokio::test]
    async fn test_peer_management() {
        let mut transport = UdpTransport::new(1, 0, HashSet::new()).await.unwrap();

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
        let transport = UdpTransport::new(1, 0, HashSet::new()).await.unwrap();
        let stats = transport.get_stats().await;

        assert_eq!(stats.send_pool_stats.peer_count.into_inner(), 0);
        assert_eq!(stats.send_pool_stats.total_sockets.into_inner(), 0);
        assert!(stats.local_addr.port() > 0);
    }
}
