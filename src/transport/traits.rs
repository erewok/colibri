//! Transport traits for unified communication interface
//!
//! Provides common traits for different transport protocols (UDP, TCP)
//! to enable consistent peer-to-peer communication in the cluster.

use std::net::SocketAddr;
use async_trait::async_trait;

use crate::error::Result;
use crate::node::NodeId;
use super::stats::{FrozenSocketPoolStats, FrozenReceiverStats};

/// Trait for sending messages to peers in the cluster
#[async_trait]
pub trait Sender {
    /// Send data to a specific peer by NodeId
    async fn send_to_peer(&self, target: NodeId, data: &[u8]) -> Result<()>;

    /// Send data to a random peer (useful for gossip protocols)
    async fn send_to_random_peer(&self, data: &[u8]) -> Result<NodeId>;

    /// Send data to multiple random peers (for gossip broadcasting)
    async fn send_to_random_peers(&self, data: &[u8], count: usize) -> Result<Vec<NodeId>>;

    /// Add a new peer to the transport
    async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) -> Result<()>;

    /// Remove a peer from the transport
    async fn remove_peer(&self, node_id: NodeId) -> Result<()>;

    /// Get list of current peer NodeIds
    async fn get_peers(&self) -> Vec<NodeId>;

    /// Get transport statistics for monitoring
    async fn get_stats(&self) -> FrozenSocketPoolStats;
}

/// Trait for request-response communication (typically TCP)
#[async_trait]
pub trait RequestSender: Sender {
    /// Send request to specific peer and wait for response
    async fn send_request_response(
        &self,
        target: NodeId,
        request_data: &[u8],
    ) -> Result<Vec<u8>>;

    /// Send request to random peer and wait for response
    async fn send_request_response_random(
        &self,
        request_data: &[u8],
    ) -> Result<(NodeId, Vec<u8>)>;

    /// Cleanup expired connections (for connection-based transports)
    async fn cleanup_expired_connections(&self);
}

/// Trait for receiving messages from peers
#[async_trait]
pub trait Receiver {
    /// Start listening for incoming messages
    async fn start(&self) -> Result<()>;

    /// Stop the receiver
    async fn stop(&self) -> Result<()>;

    /// Check if the receiver is running
    fn is_running(&self) -> bool;

    /// Get receiver statistics
    async fn get_stats(&self) -> FrozenReceiverStats;
}

/// Trait for request-response receivers (typically TCP)
#[async_trait]
pub trait RequestReceiver: Receiver {
    /// Handle incoming request and return response
    async fn handle_request(&self, request: &[u8]) -> Result<Vec<u8>>;
}