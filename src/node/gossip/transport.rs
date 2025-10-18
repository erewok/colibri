//! Transport Layer for Gossip Communication
//!
//! UDP-based transport with dynamic peer management and multicast support.
//! All network communication uses bincode serialization for INTERNAL cluster communication.
//!
//! Example usage:
//! ```rust,no_run
//! use colibri::gossip::{DynamicMulticastTransport, GossipPacket, GossipMessage, ClusterMembership};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let transport = DynamicMulticastTransport::new(
//!     7946,
//!     "224.0.1.1:7946".parse()?,
//!     vec!["192.168.1.10:7946".parse()?],
//! ).await?;
//!
//! // Create a sample heartbeat message
//! let membership = ClusterMembership::new(1, "127.0.0.1:8001".parse()?);
//! let packet = GossipPacket::new(GossipMessage::MembershipUpdate {
//!     membership,
//!     sender_node_id: 1,
//! });
//! transport.send_to_all_peers(&packet).await?;
//! # Ok(())
//! # }
//! ```
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use super::messages::{ClusterMembership, GossipPacket, NodeStatus};
use crate::error::{GossipError, Result};

/// Dynamic UDP multicast transport for gossip communication
///
/// Manages peer connections and provides reliable UDP communication with timeouts.
pub struct DynamicMulticastTransport {
    socket: UdpSocket,
    multicast_addr: SocketAddr,
    current_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    local_address: SocketAddr,
    max_packet_size: usize,
}

impl DynamicMulticastTransport {
    /// Create a new transport instance
    ///
    /// # Arguments
    /// * `bind_port` - Port to bind the UDP socket to
    /// * `multicast_addr` - Multicast address for broadcast messages
    /// * `initial_peers` - Initial list of peer addresses
    pub async fn new(
        bind_port: u16,
        multicast_addr: SocketAddr,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<Self> {
        let bind_addr = format!("0.0.0.0:{}", bind_port);
        let socket = UdpSocket::bind(&bind_addr).await?;

        let local_address = socket.local_addr()?;

        // Configure multicast if using multicast address
        if multicast_addr.ip().is_multicast() {
            // Note: Full multicast implementation would include:
            // - socket.join_multicast_v4() for IPv4
            // - socket.join_multicast_v6() for IPv6
            // - TTL settings for multicast scope
            // This is simplified for initial implementation
            println!("Multicast address configured: {}", multicast_addr);
        }

        let current_peers = Arc::new(RwLock::new(initial_peers.into_iter().collect()));

        Ok(Self {
            socket,
            multicast_addr,
            current_peers,
            local_address,
            max_packet_size: 65536, // 64KB maximum UDP packet size
        })
    }

    /// Update peer list based on cluster membership
    ///
    /// This method synchronizes the transport's peer list with the current
    /// cluster membership, excluding the local node and inactive nodes.
    pub fn update_peers_from_membership(&self, membership: &ClusterMembership, local_node_id: u32) {
        let mut peers = self.current_peers.write();
        peers.clear();

        for (node_id, node_info) in &membership.nodes {
            if *node_id != local_node_id && matches!(node_info.status, NodeStatus::Active) {
                peers.insert(node_info.address);
            }
        }

        println!(
            "Updated peers from membership: {} active peers",
            peers.len()
        );
    }

    /// Add a peer to the current peer list
    pub fn add_peer(&self, address: SocketAddr) {
        let mut peers = self.current_peers.write();
        if peers.insert(address) {
            println!("Added peer: {}", address);
        }
    }

    /// Remove a peer from the current peer list
    pub fn remove_peer(&self, address: &SocketAddr) {
        let mut peers = self.current_peers.write();
        if peers.remove(address) {
            println!("Removed peer: {}", address);
        }
    }

    /// Get the local socket address
    pub fn get_local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Send a gossip packet to specific peers
    ///
    /// Continues sending to other peers even if some sends fail.
    pub async fn send_to_peers(
        &self,
        packet: &GossipPacket,
        target_peers: &[SocketAddr],
    ) -> Result<usize> {
        if target_peers.is_empty() {
            return Ok(0);
        }

        let data = packet.serialize()?;

        if data.len() > self.max_packet_size {
            return Err(GossipError::Transport(format!(
                "Packet too large: {} bytes (max: {} bytes)",
                data.len(),
                self.max_packet_size
            ))
            .into());
        }

        let mut successful_sends = 0;
        let mut errors = Vec::new();

        for peer in target_peers {
            match self.socket.send_to(&data, peer).await {
                Ok(bytes_sent) => {
                    if bytes_sent == data.len() {
                        successful_sends += 1;
                    } else {
                        errors.push(format!(
                            "Partial send to {}: {}/{} bytes",
                            peer,
                            bytes_sent,
                            data.len()
                        ));
                    }
                }
                Err(e) => {
                    errors.push(format!("Failed to send to {}: {}", peer, e));
                }
            }
        }

        if !errors.is_empty() {
            eprintln!("Send errors: {:?}", errors);
        }

        Ok(successful_sends)
    }

    /// Send a gossip packet to all current peers
    pub async fn send_to_all_peers(&self, packet: &GossipPacket) -> Result<usize> {
        let peers: Vec<SocketAddr> = { self.current_peers.read().iter().cloned().collect() };
        self.send_to_peers(packet, &peers).await
    }

    /// Send a gossip packet to a specific peer
    pub async fn send_to_specific_peer(
        &self,
        packet: &GossipPacket,
        target: SocketAddr,
    ) -> Result<()> {
        let result = self.send_to_peers(packet, &[target]).await?;
        if result == 1 {
            Ok(())
        } else {
            Err(GossipError::Transport(format!(
                "Failed to send to {}: No successful sends",
                target
            ))
            .into())
        }
    }

    /// Send a gossip packet via multicast
    ///
    /// Uses the configured multicast address for broadcast communication.
    /// Useful for discovery and cluster-wide announcements.
    pub async fn multicast(&self, packet: &GossipPacket) -> Result<()> {
        let data = packet.serialize()?;

        if data.len() > self.max_packet_size {
            return Err(GossipError::Transport(format!(
                "Packet too large: {} bytes (max: {} bytes)",
                data.len(),
                self.max_packet_size
            ))
            .into());
        }

        self.socket
            .send_to(&data, &self.multicast_addr)
            .await
            .map_err(|e| {
                GossipError::Transport(format!(
                    "Multicast failed to {}: {}",
                    self.multicast_addr, e
                ))
            })?;

        Ok(())
    }

    /// Receive a gossip packet with timeout
    ///
    /// Returns None if timeout expires or if packet deserialization fails.
    pub async fn receive_with_timeout(
        &self,
        timeout_duration: Duration,
    ) -> Result<Option<(GossipPacket, SocketAddr)>> {
        let mut buf = vec![0u8; self.max_packet_size];

        match timeout(timeout_duration, self.socket.recv_from(&mut buf)).await {
            Ok(Ok((len, addr))) => {
                match GossipPacket::deserialize(&buf[..len]) {
                    Ok(packet) => Ok(Some((packet, addr))),
                    Err(e) => {
                        // Log deserialization errors but don't fail the operation
                        eprintln!("Failed to deserialize packet from {}: {}", addr, e);
                        Ok(None) // Invalid packet, skip
                    }
                }
            }
            Ok(Err(e)) => Err(GossipError::Transport(format!("Receive failed: {}", e)).into()),
            Err(_) => Ok(None), // Timeout - not an error condition
        }
    }

    /// Receive packets continuously with a callback
    ///
    /// This method runs indefinitely, calling the provided callback for each
    /// successfully received and deserialized packet. Use this for the main
    /// message handling loop.
    pub async fn receive_loop<F, Fut>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(GossipPacket, SocketAddr) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let mut buf = vec![0u8; self.max_packet_size];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    match GossipPacket::deserialize(&buf[..len]) {
                        Ok(packet) => {
                            callback(packet, addr).await;
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize packet from {}: {}", addr, e);
                            // Continue processing other packets
                        }
                    }
                }
                Err(e) => {
                    return Err(GossipError::Transport(format!("Receive failed: {}", e)).into());
                }
            }
        }
    }

    /// Get current list of peers
    pub fn get_peers(&self) -> Vec<SocketAddr> {
        self.current_peers.read().iter().cloned().collect()
    }

    /// Get current number of peers
    pub fn peer_count(&self) -> usize {
        self.current_peers.read().len()
    }

    /// Get maximum packet size supported
    pub fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    /// Check if an address is in the current peer list
    pub fn has_peer(&self, address: &SocketAddr) -> bool {
        self.current_peers.read().contains(address)
    }

    /// Clear all peers (useful for testing or reset scenarios)
    pub fn clear_peers(&self) {
        let mut peers = self.current_peers.write();
        let count = peers.len();
        peers.clear();
        println!("Cleared {} peers", count);
    }

    /// Get transport statistics
    pub fn get_stats(&self) -> TransportStats {
        TransportStats {
            local_address: self.local_address,
            multicast_address: self.multicast_addr,
            peer_count: self.peer_count(),
            max_packet_size: self.max_packet_size,
        }
    }
}

/// Transport statistics and configuration info
#[derive(Debug, Clone)]
pub struct TransportStats {
    pub local_address: SocketAddr,
    pub multicast_address: SocketAddr,
    pub peer_count: usize,
    pub max_packet_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::gossip::messages::{
        ClusterMembership, GossipMessage, NodeCapabilities, NodeInfo, NodeStatus,
    };
    use std::net::{IpAddr, Ipv4Addr};

    async fn create_test_transport(port: u16) -> DynamicMulticastTransport {
        let multicast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)), 7946);
        DynamicMulticastTransport::new(port, multicast_addr, vec![])
            .await
            .expect("Failed to create test transport")
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = create_test_transport(0).await;
        let stats = transport.get_stats();

        assert_eq!(stats.peer_count, 0);
        assert_eq!(stats.max_packet_size, 65536);
        assert!(stats.local_address.port() > 0); // Should get assigned a port
    }

    #[tokio::test]
    async fn test_peer_management() {
        let transport = create_test_transport(0).await;

        let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 7946);
        let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 11)), 7946);

        // Initially no peers
        assert_eq!(transport.peer_count(), 0);
        assert!(!transport.has_peer(&peer1));

        // Add peers
        transport.add_peer(peer1);
        assert_eq!(transport.peer_count(), 1);
        assert!(transport.has_peer(&peer1));

        transport.add_peer(peer2);
        assert_eq!(transport.peer_count(), 2);
        assert!(transport.has_peer(&peer2));

        // Remove peer
        transport.remove_peer(&peer1);
        assert_eq!(transport.peer_count(), 1);
        assert!(!transport.has_peer(&peer1));
        assert!(transport.has_peer(&peer2));

        // Clear all peers
        transport.clear_peers();
        assert_eq!(transport.peer_count(), 0);
    }

    #[tokio::test]
    async fn test_membership_peer_update() {
        let transport = create_test_transport(0).await;
        let local_node_id = 1;

        // Create test membership with multiple nodes
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7947);
        let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7948);

        let mut membership = ClusterMembership::new(local_node_id, addr1);

        // Add active node
        let node2 = NodeInfo {
            node_id: 2,
            address: addr2,
            status: NodeStatus::Active,
            joined_at: 1234567890,
            last_seen: 1234567890,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 50_000,
                gossip_protocol_version: 1,
            },
        };
        membership.add_node(node2);

        // Add failed node (should be excluded)
        let node3 = NodeInfo {
            node_id: 3,
            address: addr3,
            status: NodeStatus::Failed,
            joined_at: 1234567890,
            last_seen: 1234567890,
            capabilities: NodeCapabilities {
                max_rate_limit_keys: 50_000,
                gossip_protocol_version: 1,
            },
        };
        membership.add_node(node3.clone());

        // Update peers from membership
        transport.update_peers_from_membership(&membership, local_node_id);

        // Should only include active nodes, excluding local node
        assert_eq!(transport.peer_count(), 1);
        assert!(transport.has_peer(&addr2));
        assert!(!transport.has_peer(&addr1)); // Local node excluded
        assert!(!transport.has_peer(&addr3)); // Failed node excluded

        // Update node3 to active
        membership.update_node_status(3, NodeStatus::Active);
        transport.update_peers_from_membership(&membership, local_node_id);

        // Should now include both active remote nodes
        assert_eq!(transport.peer_count(), 2);
        assert!(transport.has_peer(&addr2));
        assert!(transport.has_peer(&addr3));
    }

    #[tokio::test]
    async fn test_packet_serialization_size_limits() {
        let transport = create_test_transport(0).await;

        // Create a normal-sized packet
        let normal_message = GossipMessage::Heartbeat {
            node_id: 1,
            timestamp: 1234567890,
            membership_version: 1,
        };
        let normal_packet = GossipPacket::new(normal_message);

        // Should serialize fine
        let serialized = normal_packet
            .serialize()
            .expect("Normal packet should serialize");
        assert!(serialized.len() < transport.max_packet_size());

        // Test that we handle size limits properly
        assert!(
            serialized.len() < 65536,
            "Normal packets should be much smaller than max size"
        );
    }

    #[tokio::test]
    async fn test_send_to_empty_peer_list() {
        let transport = create_test_transport(0).await;

        let message = GossipMessage::Heartbeat {
            node_id: 1,
            timestamp: 1234567890,
            membership_version: 1,
        };
        let packet = GossipPacket::new(message);

        // Should succeed but send to 0 peers
        let result = transport
            .send_to_all_peers(&packet)
            .await
            .expect("Should not error");
        assert_eq!(result, 0);

        // Sending to empty list should also succeed
        let result = transport
            .send_to_peers(&packet, &[])
            .await
            .expect("Should not error");
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_receive_timeout() {
        let transport = create_test_transport(0).await;

        // Should timeout quickly since no one is sending
        let result = transport
            .receive_with_timeout(Duration::from_millis(10))
            .await
            .expect("Timeout should not be an error");

        assert!(result.is_none(), "Should timeout and return None");
    }

    #[tokio::test]
    async fn test_transport_stats() {
        let transport = create_test_transport(0).await;

        // Add some peers
        let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 7946);
        let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 11)), 7946);
        transport.add_peer(peer1);
        transport.add_peer(peer2);

        let stats = transport.get_stats();
        assert_eq!(stats.peer_count, 2);
        assert_eq!(stats.max_packet_size, 65536);
        assert!(stats.local_address.port() > 0);

        // Multicast address should match what we set
        let expected_multicast = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 1)), 7946);
        assert_eq!(stats.multicast_address, expected_multicast);
    }
}
