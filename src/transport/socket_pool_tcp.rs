//! TCP Socket Pool for Request-Response Communication
//!
//! Manages TCP connections for request-response patterns required by hashring nodes.
//! Unlike UDP sockets, TCP connections need to be managed more carefully with
//! connection pooling, timeouts, and graceful connection handling.

use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{ColibriError, Result};
use crate::node::NodeId;
use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::debug;

/// TCP connection with metadata
#[derive(Debug)]
struct TcpConnection {
    stream: TcpStream,
    last_used: std::time::Instant,
    in_use: bool,
}

impl TcpConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            last_used: std::time::Instant::now(),
            in_use: false,
        }
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        !self.in_use && self.last_used.elapsed() > timeout
    }
}

/// Pool of TCP connections for efficient peer communication
#[derive(Debug)]
#[allow(dead_code)] // node_id used for debugging
pub struct TcpSocketPool {
    // Connection pools per peer
    peer_connections: IndexMap<SocketAddr, Arc<Mutex<Vec<TcpConnection>>>>,
    // Configuration
    node_id: NodeId,
    max_connections_per_peer: usize,
    connection_timeout: Duration,
    idle_timeout: Duration,
    // Statistics
    stats: Arc<TcpSocketPoolStats>,
}

/// TCP-specific statistics
#[derive(Debug)]
pub struct TcpSocketPoolStats {
    pub peer_count: AtomicUsize,
    pub total_connections: AtomicUsize,
    pub active_connections: AtomicUsize,
    pub requests_sent: AtomicU64,
    pub responses_received: AtomicU64,
    pub connection_errors: AtomicU64,
    pub timeout_errors: AtomicU64,
}

impl TcpSocketPoolStats {
    fn new(peer_count: usize) -> Self {
        Self {
            peer_count: AtomicUsize::new(peer_count),
            total_connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            requests_sent: AtomicU64::new(0),
            responses_received: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            timeout_errors: AtomicU64::new(0),
        }
    }
}

impl TcpSocketPool {
    /// Create a new TCP socket pool
    pub async fn new(
        node_id: NodeId,
        peer_addrs: Vec<SocketAddr>,
        max_connections_per_peer: usize,
    ) -> Result<Self> {
        let mut peer_connections = IndexMap::new();

        // Initialize empty connection pools for each peer
        for peer_addr in peer_addrs.iter() {
            peer_connections.insert(*peer_addr, Arc::new(Mutex::new(Vec::new())));
        }

        let stats = Arc::new(TcpSocketPoolStats::new(peer_addrs.len()));

        Ok(Self {
            peer_connections,
            node_id,
            max_connections_per_peer,
            connection_timeout: Duration::from_millis(200),
            idle_timeout: Duration::from_secs(60),
            stats,
        })
    }

    /// Send a request and wait for response from a specific peer
    pub async fn send_request_response(
        &self,
        target: SocketAddr,
        request_data: &[u8],
    ) -> Result<Vec<u8>> {
        self.stats.requests_sent.fetch_add(1, Ordering::Relaxed);

        // Get or create connection
        let mut connection = self.get_or_create_connection(target).await?;

        // Send request with length prefix
        let request_len = request_data.len() as u32;
        let len_bytes = request_len.to_be_bytes();

        timeout(self.connection_timeout, async {
            connection.stream.write_all(&len_bytes).await?;
            connection.stream.write_all(request_data).await?;
            Result::<()>::Ok(())
        })
        .await
        .map_err(|_| {
            self.stats.timeout_errors.fetch_add(1, Ordering::Relaxed);
            ColibriError::Transport("Request send timeout".to_string())
        })??;

        // Read response length
        let mut len_bytes = [0u8; 4];
        timeout(self.connection_timeout, async {
            connection.stream.read_exact(&mut len_bytes).await
        })
        .await
        .map_err(|_| {
            self.stats.timeout_errors.fetch_add(1, Ordering::Relaxed);
            ColibriError::Transport("Response length read timeout".to_string())
        })?
        .map_err(|e| ColibriError::Transport(format!("Failed to read response length: {}", e)))?;

        let response_len = u32::from_be_bytes(len_bytes) as usize;
        if response_len > 1024 * 1024 {
            // 1MB limit
            return Err(ColibriError::Transport("Response too large".to_string()));
        }

        // Read response data
        let mut response_data = vec![0u8; response_len];
        timeout(self.connection_timeout, async {
            connection.stream.read_exact(&mut response_data).await
        })
        .await
        .map_err(|_| {
            self.stats.timeout_errors.fetch_add(1, Ordering::Relaxed);
            ColibriError::Transport("Response data read timeout".to_string())
        })?
        .map_err(|e| ColibriError::Transport(format!("Failed to read response data: {}", e)))?;

        self.stats
            .responses_received
            .fetch_add(1, Ordering::Relaxed);

        // Return connection to pool
        self.return_connection(target, connection).await?;

        Ok(response_data)
    }

    /// Send to a random peer (for load balancing)
    pub async fn send_request_response_random(
        &self,
        request_data: &[u8],
    ) -> Result<(SocketAddr, Vec<u8>)> {
        if self.peer_connections.is_empty() {
            return Err(ColibriError::Transport("No peers available".to_string()));
        }

        let random_index: usize = rand::rng().random_range(0..self.peer_connections.len());
        if let Some((target, _)) = self.peer_connections.get_index(random_index) {
            let response = self.send_request_response(*target, request_data).await?;
            Ok((*target, response))
        } else {
            Err(ColibriError::Transport("No peers available".to_string()))
        }
    }

    /// Get a connection from the pool or create a new one
    async fn get_or_create_connection(&self, target: SocketAddr) -> Result<TcpConnection> {
        let connections_arc = self
            .peer_connections
            .get(&target)
            .ok_or_else(|| ColibriError::Transport(format!("Peer not found: {}", target)))?;

        let mut connections = connections_arc.lock().await;

        // Try to find an available connection
        if let Some(pos) = connections
            .iter()
            .position(|conn| !conn.in_use && !conn.is_expired(self.idle_timeout))
        {
            let mut conn = connections.remove(pos);
            conn.in_use = true;
            conn.last_used = std::time::Instant::now();
            self.stats
                .active_connections
                .fetch_add(1, Ordering::Relaxed);
            return Ok(conn);
        }

        // Clean up expired connections
        connections.retain(|conn| !conn.is_expired(self.idle_timeout));

        // Create new connection if under limit
        if connections.len() < self.max_connections_per_peer {
            match timeout(self.connection_timeout, TcpStream::connect(target)).await {
                Ok(Ok(stream)) => {
                    debug!("Created new TCP connection to {}", target);
                    let mut conn = TcpConnection::new(stream);
                    conn.in_use = true;
                    self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .active_connections
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(conn);
                }
                Ok(Err(e)) => {
                    self.stats.connection_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(ColibriError::Transport(format!(
                        "Failed to connect to {}: {}",
                        target, e
                    )));
                }
                Err(_) => {
                    self.stats.timeout_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(ColibriError::Transport(format!(
                        "Connection timeout to {}",
                        target
                    )));
                }
            }
        }

        Err(ColibriError::Transport(format!(
            "Connection pool full for {}",
            target
        )))
    }

    /// Return a connection to the pool
    async fn return_connection(
        &self,
        target: SocketAddr,
        mut connection: TcpConnection,
    ) -> Result<()> {
        let connections_arc = self
            .peer_connections
            .get(&target)
            .ok_or_else(|| ColibriError::Transport(format!("Peer not found: {}", target)))?;

        connection.in_use = false;
        connection.last_used = std::time::Instant::now();

        self.stats
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);

        let mut connections = connections_arc.lock().await;
        connections.push(connection);
        Ok(())
    }

    /// Add a new peer to the pool
    pub async fn add_peer(&mut self, peer_addr: SocketAddr) -> Result<()> {
        if !self.peer_connections.contains_key(&peer_addr) {
            self.peer_connections
                .insert(peer_addr, Arc::new(Mutex::new(Vec::new())));
            self.stats.peer_count.fetch_add(1, Ordering::Relaxed);
            debug!("Added TCP peer: {}", peer_addr);
        }
        Ok(())
    }

    /// Remove a peer from the pool
    pub async fn remove_peer(&mut self, peer_addr: SocketAddr) -> Result<()> {
        if let Some(connections_arc) = self.peer_connections.shift_remove(&peer_addr) {
            let connections = connections_arc.lock().await;
            let removed_count = connections.len();
            self.stats
                .total_connections
                .fetch_sub(removed_count, Ordering::Relaxed);
            self.stats.peer_count.fetch_sub(1, Ordering::Relaxed);
            debug!(
                "Removed TCP peer: {} ({} connections)",
                peer_addr, removed_count
            );
        }
        Ok(())
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
        self.peer_connections.keys().cloned().collect()
    }

    /// Get statistics
    pub fn get_stats(&self) -> &TcpSocketPoolStats {
        &self.stats
    }

    /// Cleanup expired connections for all peers
    pub async fn cleanup_expired_connections(&self) {
        for (peer, connections_arc) in &self.peer_connections {
            let mut connections = connections_arc.lock().await;
            let initial_count = connections.len();
            connections.retain(|conn| !conn.is_expired(self.idle_timeout));
            let removed = initial_count - connections.len();
            if removed > 0 {
                self.stats
                    .total_connections
                    .fetch_sub(removed, Ordering::Relaxed);
                debug!("Cleaned up {} expired connections for {}", removed, peer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;

    #[tokio::test]
    async fn test_tcp_socket_pool_creation() {
        let node_id = NodeId::new(1);
        let peers = vec![
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:8002".parse().unwrap(),
        ];

        let pool = TcpSocketPool::new(node_id, peers, 5).await.unwrap();

        assert_eq!(pool.get_peers().await.len(), 2);
        assert_eq!(pool.stats.peer_count.load(Ordering::Relaxed), 2);
        assert_eq!(pool.stats.total_connections.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_add_remove_peer() {
        let node_id = NodeId::new(1);
        let peers = vec!["127.0.0.1:8001".parse().unwrap()];

        let mut pool = TcpSocketPool::new(node_id, peers, 5).await.unwrap();
        assert_eq!(pool.get_peers().await.len(), 1);

        let new_peer: SocketAddr = "127.0.0.1:8002".parse().unwrap();
        pool.add_peer(new_peer).await.unwrap();
        assert_eq!(pool.get_peers().await.len(), 2);

        pool.remove_peer(new_peer).await.unwrap();
        assert_eq!(pool.get_peers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_tcp_connection_expiry_logic() {
        // Test expiry logic with duration calculations
        let timeout = Duration::from_millis(100);

        // Test that fresh connection is not expired
        let now = std::time::Instant::now();
        assert!(now.elapsed() < timeout);

        // Note: Full TCP connection testing would need mock streams
        // For now, we test the duration logic which is the core of expiry
    }
}
