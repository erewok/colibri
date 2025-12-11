/// ClusterMember trait - separate concern from being a Node
/// Only gossip and hashring nodes implement this, single nodes do not
use async_trait::async_trait;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::transport::{
    FrozenReceiverStats, FrozenSocketPoolStats, SendReceiveStats, TcpTransport, UdpTransport,
};

#[async_trait]
pub trait ClusterMember: Send + Sync {
    /// Send data to a specific node address
    async fn send_to_node(&self, target: SocketAddr, data: &[u8]) -> Result<()>;

    /// Send data to a random node (for gossip)
    async fn send_to_random_node(&self, data: &[u8]) -> Result<()>;

    /// Get list of all cluster node addresses
    async fn get_cluster_nodes(&self) -> Vec<SocketAddr>;

    /// Get list of all cluster node addresses
    async fn get_cluster_stats(&self) -> SendReceiveStats;

    /// Add a node address to the cluster (human operator)
    async fn add_node(&self, address: SocketAddr);

    /// Remove a node address from the cluster (human operator)
    async fn remove_node(&self, address: SocketAddr);

    /// Send request and wait for response (for hashring nodes that need TCP)
    /// Returns None if this cluster member doesn't support request-response
    async fn send_request_response(
        &self,
        target: SocketAddr,
        request: &[u8],
    ) -> Result<Option<Vec<u8>>>;
}

/// Simple cluster membership tracking
#[derive(Debug, Clone)]
struct ClusterNodes {
    nodes: HashSet<SocketAddr>,
}

impl ClusterNodes {
    fn new(all_nodes: Vec<SocketAddr>) -> Self {
        Self {
            nodes: all_nodes.into_iter().collect(),
        }
    }

    fn add_node(&mut self, address: SocketAddr) {
        if !self.nodes.contains(&address) {
            self.nodes.insert(address);
        }
    }

    fn has_node(&self, address: SocketAddr) -> bool {
        self.nodes.contains(&address)
    }

    fn remove_node(&mut self, address: SocketAddr) {
        self.nodes.remove(&address);
    }

    fn all_nodes(&self) -> Vec<SocketAddr> {
        self.nodes.clone().into_iter().collect()
    }
}

/// Single nodes don't participate in clusters - all no-ops
pub struct NoOpClusterMember;

#[async_trait]
impl ClusterMember for NoOpClusterMember {
    async fn send_to_node(&self, _target: SocketAddr, _data: &[u8]) -> Result<()> {
        Err(crate::error::ColibriError::Api(
            "Single nodes don't participate in clusters".to_string(),
        ))
    }

    async fn send_to_random_node(&self, _data: &[u8]) -> Result<()> {
        Err(crate::error::ColibriError::Api(
            "Single nodes don't participate in clusters".to_string(),
        ))
    }

    async fn get_cluster_nodes(&self) -> Vec<SocketAddr> {
        vec![]
    }

    async fn get_cluster_stats(&self) -> SendReceiveStats {
        SendReceiveStats {
            sent: FrozenSocketPoolStats::default(),
            received: FrozenReceiverStats::default(),
        }
    }

    async fn add_node(&self, _address: SocketAddr) {
        // No-op
    }

    async fn remove_node(&self, _address: SocketAddr) {
        // No-op
    }

    async fn send_request_response(
        &self,
        _target: SocketAddr,
        _request: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        Err(crate::error::ColibriError::Api(
            "Single nodes don't support request-response patterns".to_string(),
        ))
    }
}

/// Implementation for nodes that actually use existing UDP transport
/// This is what gossip and hashring nodes will use
pub struct UdpClusterMember {
    transport: Arc<UdpTransport>,
    nodes: Arc<RwLock<ClusterNodes>>,
}

impl UdpClusterMember {
    pub fn new(transport: Arc<UdpTransport>, cluster_nodes: Vec<SocketAddr>) -> Self {
        Self {
            transport,
            nodes: Arc::new(RwLock::new(ClusterNodes::new(cluster_nodes))),
        }
    }
}

#[async_trait]
impl ClusterMember for UdpClusterMember {
    async fn send_to_node(&self, target: SocketAddr, data: &[u8]) -> Result<()> {
        let nodes = self.nodes.read().await;
        if nodes.has_node(target) {
            match self.transport.send_to_peer(target, data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    drop(nodes);
                    Err(e)
                }
            }
        } else {
            Err(crate::error::ColibriError::Transport(
                "Node is marked as unresponsive".to_string(),
            ))
        }
    }

    async fn send_to_random_node(&self, data: &[u8]) -> Result<()> {
        match self.transport.send_to_random_peer(data).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn get_cluster_nodes(&self) -> Vec<SocketAddr> {
        self.nodes.read().await.all_nodes()
    }

    async fn get_cluster_stats(&self) -> SendReceiveStats {
        let stats = self.transport.get_stats().await;
        SendReceiveStats {
            sent: stats,
            received: FrozenReceiverStats::default(), // UDP receiver stats can be added later
        }
    }

    async fn add_node(&self, address: SocketAddr) {
        self.nodes.write().await.add_node(address);
    }

    async fn remove_node(&self, address: SocketAddr) {
        self.nodes.write().await.remove_node(address);
    }

    async fn send_request_response(
        &self,
        _target: SocketAddr,
        _request: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // UDP cluster members don't support request-response (gossip is fire-and-forget)
        Ok(None)
    }
}

/// Implementation for nodes that need sync responses (hashring)
pub struct TcpClusterMember {
    transport: Arc<TcpTransport>,
    nodes: Arc<RwLock<ClusterNodes>>,
}

impl TcpClusterMember {
    pub fn new(transport: Arc<TcpTransport>, cluster_nodes: Vec<SocketAddr>) -> Self {
        Self {
            transport,
            nodes: Arc::new(RwLock::new(ClusterNodes::new(cluster_nodes))),
        }
    }
}

#[async_trait]
impl ClusterMember for TcpClusterMember {
    async fn send_to_node(&self, target: SocketAddr, data: &[u8]) -> Result<()> {
        self.send_request_response(target, data)
            .await?
            .ok_or_else(|| {
                crate::error::ColibriError::Transport(
                    "No response received from TCP cluster member".to_string(),
                )
            })
            .map(|_| ())
    }

    async fn send_to_random_node(&self, _data: &[u8]) -> Result<()> {
        // TCP cluster members don't typically need random sending
        Err(crate::error::ColibriError::Transport(
            "TCP cluster members use targeted request-response patterns".to_string(),
        ))
    }

    async fn get_cluster_nodes(&self) -> Vec<SocketAddr> {
        self.nodes.read().await.all_nodes()
    }
    async fn get_cluster_stats(&self) -> SendReceiveStats {
        let stats = self.transport.get_stats().await;
        SendReceiveStats {
            sent: stats,
            received: FrozenReceiverStats::default(), // TCP receiver stats can be added later
        }
    }

    async fn add_node(&self, address: SocketAddr) {
        // Note: TcpTransport doesn't support dynamic peer addition yet
        // This would require modifying the TcpSocketPool which is immutable
        tracing::debug!(
            "TCP transport doesn't support dynamic peer addition for {}",
            address
        );
        self.nodes.write().await.add_node(address);
    }

    async fn remove_node(&self, address: SocketAddr) {
        // Also need to remove from the transport
        if let Err(e) = self.transport.remove_peer(address).await {
            tracing::warn!(
                "Failed to remove peer {} from TCP transport: {}",
                address,
                e
            );
        }
        self.nodes.write().await.remove_node(address);
    }

    async fn send_request_response(
        &self,
        target: SocketAddr,
        request: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.transport
            .send_request_response(target, request)
            .await
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_cluster_member() {
        let noop = NoOpClusterMember;

        // Should start with empty nodes
        assert_eq!(noop.get_cluster_nodes().await.len(), 0);

        // Adding/removing should be no-ops
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        noop.add_node(addr).await;
        assert_eq!(noop.get_cluster_nodes().await.len(), 0);

        // Send should fail (single nodes don't participate in clusters)
        let result = noop.send_to_node(addr, b"test").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_nodes_internal() {
        // Test the internal ClusterNodes struct
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let mut nodes = ClusterNodes::new(vec![addr1, addr2]);
        assert_eq!(nodes.all_nodes().len(), 2);

        // Test removal
        nodes.remove_node(addr1);
        let remaining = nodes.all_nodes();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains(&addr2));
    }
}
