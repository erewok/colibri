//! TCP Socket Pool for Request-Response Communication
//!
//! Manages TCP connections for request-response patterns required by hashring nodes.
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, error};

use super::stats::SocketPoolStats;
use crate::error::{ColibriError, Result};
use crate::node::{NodeId, NodeName};
use crate::settings::TransportConfig;

/// TCP connection with metadata
#[derive(Debug)]
#[allow(dead_code)] // Kept for future connection pooling implementation
struct TcpConnection {
    stream: TcpStream,
    last_used: std::time::Instant,
    in_use: bool,
}

#[allow(dead_code)] // Kept for future connection pooling implementation
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
#[allow(dead_code)] // node_name used for debugging
pub struct TcpSocketPool {
    // Connection pools per peer using NodeId
    peer_connections: IndexMap<NodeId, PeerConnectionInfo>,
    // Configuration
    node_name: NodeName,
    max_connections_per_peer: usize,
    connection_timeout: Duration,
    // for statistics and monitoring
    stats: Arc<SocketPoolStats>,
}

#[derive(Debug)]
#[allow(dead_code)] // Kept for future connection pooling implementation
struct PeerConnectionInfo {
    socket_addr: SocketAddr,
    connections: Arc<Mutex<Vec<TcpConnection>>>,
}

impl TcpSocketPool {
    /// Create a new TCP socket pool
    pub async fn new(transport_config: &TransportConfig) -> Result<Self> {
        let mut peer_connections = IndexMap::new();

        // Initialize empty connection pools for each peer
        for (node_id, socket_addr) in &transport_config.topology {
            let peer_info = PeerConnectionInfo {
                socket_addr: *socket_addr,
                connections: Arc::new(Mutex::new(Vec::new())),
            };
            peer_connections.insert(*node_id, peer_info);
        }

        let stats = Arc::new(SocketPoolStats::new(peer_connections.len()));

        Ok(Self {
            peer_connections,
            node_name: transport_config.node_name.clone(),
            max_connections_per_peer: 5, // Default value
            connection_timeout: Duration::from_millis(200),
            stats,
        })
    }

    /// Send a request and wait for response from a specific peer
    pub async fn send_request_response(
        &self,
        target: NodeId,
        request_data: &[u8],
    ) -> Result<Vec<u8>> {
        let peer_info = self
            .peer_connections
            .get(&target)
            .ok_or_else(|| ColibriError::Transport(format!("Peer not found: {:?}", target)))?;

        let mut connection = self
            .get_or_create_connection(target, peer_info.socket_addr)
            .await?;

        // Send request with length prefix
        let request_len = request_data.len() as u32;
        let len_bytes = request_len.to_be_bytes();

        match timeout(self.connection_timeout, async {
            use tokio::io::AsyncWriteExt;
            connection.write_all(&len_bytes).await?;
            connection.write_all(request_data).await?;
            Result::<()>::Ok(())
        })
        .await
        {
            Ok(Ok(_)) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                self.stats
                    .errors
                    .send_errors
                    .fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
            Err(_) => {
                self.stats
                    .errors
                    .timeout_errors
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ColibriError::Transport("Request send timeout".to_string()));
            }
        }

        // Read response length
        let mut len_bytes = [0u8; 4];
        match timeout(self.connection_timeout, async {
            use tokio::io::AsyncReadExt;
            connection.read_exact(&mut len_bytes).await
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(ColibriError::Transport(format!(
                    "Failed to read response length: {}",
                    e
                )));
            }
            Err(_) => {
                self.stats
                    .errors
                    .timeout_errors
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ColibriError::Transport(
                    "Response length read timeout".to_string(),
                ));
            }
        }

        let response_len = u32::from_be_bytes(len_bytes) as usize;
        if response_len > 1024 * 1024 {
            // 1MB limit
            return Err(ColibriError::Transport("Response too large".to_string()));
        }

        // Read response data
        let mut response_data = vec![0u8; response_len];
        match timeout(self.connection_timeout, async {
            use tokio::io::AsyncReadExt;
            connection.read_exact(&mut response_data).await
        })
        .await
        {
            Ok(Ok(_)) => {
                self.stats
                    .responses_received
                    .fetch_add(1, Ordering::Relaxed);
                Ok(response_data)
            }
            Ok(Err(e)) => Err(ColibriError::Transport(format!(
                "Failed to read response data: {}",
                e
            ))),
            Err(_) => {
                self.stats
                    .errors
                    .timeout_errors
                    .fetch_add(1, Ordering::Relaxed);
                Err(ColibriError::Transport(
                    "Response data read timeout".to_string(),
                ))
            }
        }
    }

    /// Send request to random peer and wait for response
    pub async fn send_request_response_random(
        &self,
        request_data: &[u8],
    ) -> Result<(NodeId, Vec<u8>)> {
        if self.peer_connections.is_empty() {
            return Err(ColibriError::Transport("No peers available".to_string()));
        }

        let random_idx = rand::rng().random_range(0..self.peer_connections.len());
        if let Some((node_id, _)) = self.peer_connections.get_index(random_idx) {
            let response = self.send_request_response(*node_id, request_data).await?;
            Ok((*node_id, response))
        } else {
            Err(ColibriError::Transport("No peers available".to_string()))
        }
    }
    /// Get a connection from the pool or create a new one
    async fn get_or_create_connection(
        &self,
        node_id: NodeId,
        socket_addr: SocketAddr,
    ) -> Result<TcpStream> {
        // For now, create a new connection each time (simplified implementation)
        // In a full implementation, you'd maintain a connection pool
        match timeout(self.connection_timeout, TcpStream::connect(socket_addr)).await {
            Ok(Ok(stream)) => {
                debug!(
                    "Created new TCP connection to {:?} at {}",
                    node_id, socket_addr
                );
                self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
                Ok(stream)
            }
            Ok(Err(e)) => {
                error!(
                    "Failed to connect to {:?} at {}: {}",
                    node_id, socket_addr, e
                );
                self.stats
                    .errors
                    .send_errors
                    .fetch_add(1, Ordering::Relaxed);
                Err(ColibriError::Transport(format!("Connection failed: {}", e)))
            }
            Err(_) => {
                self.stats
                    .errors
                    .timeout_errors
                    .fetch_add(1, Ordering::Relaxed);
                Err(ColibriError::Transport("Connection timeout".to_string()))
            }
        }
    }

    /// Add a new peer to the socket pool
    pub async fn add_peer(&mut self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        let peer_info = PeerConnectionInfo {
            socket_addr: addr,
            connections: Arc::new(Mutex::new(Vec::new())),
        };
        self.peer_connections.insert(node_id, peer_info);

        self.stats
            .peer_count
            .store(self.peer_connections.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Remove a peer from the socket pool
    pub async fn remove_peer(&mut self, node_id: NodeId) -> Result<()> {
        self.peer_connections.swap_remove(&node_id);
        self.stats
            .peer_count
            .store(self.peer_connections.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Get list of current peers
    pub async fn get_peers(&self) -> Vec<NodeId> {
        self.peer_connections.keys().cloned().collect()
    }

    /// Get the socket address for a specific peer
    pub fn get_peer_address(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.peer_connections
            .get(&node_id)
            .map(|info| info.socket_addr)
    }

    /// Cleanup expired connections (simplified for now)
    pub async fn cleanup_expired_connections(&self) {
        // In a full implementation, this would clean up expired connections
        // For now, it's a no-op since we're not pooling connections
    }

    /// Get socket pool statistics
    pub fn get_stats(&self) -> &SocketPoolStats {
        &self.stats
    }
}
