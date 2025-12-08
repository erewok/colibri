/// ClusterMember trait - separate concern from being a Node
/// Only gossip and hashring nodes implement this, single nodes do not
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::transport::{TcpTransport, UdpTransport};

#[async_trait]
pub trait ClusterMember: Send + Sync {
    /// Send data to a specific node address
    async fn send_to_node(&self, target: SocketAddr, data: &[u8]) -> Result<()>;

    /// Send data to a random node (for gossip)
    async fn send_to_random_node(&self, data: &[u8]) -> Result<()>;

    /// Get list of all cluster node addresses
    async fn get_cluster_nodes(&self) -> Vec<SocketAddr>;

    /// Mark a node as unresponsive (stop sending to it)
    async fn mark_unresponsive(&self, address: SocketAddr);

    /// Mark a node as responsive (resume sending to it)
    async fn mark_responsive(&self, address: SocketAddr);

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
    responsive_nodes: Vec<SocketAddr>,
    unresponsive_nodes: Vec<SocketAddr>,
}

impl ClusterNodes {
    fn new(all_nodes: Vec<SocketAddr>) -> Self {
        Self {
            responsive_nodes: all_nodes,
            unresponsive_nodes: Vec::new(),
        }
    }

    fn mark_unresponsive(&mut self, address: SocketAddr) {
        if let Some(pos) = self
            .responsive_nodes
            .iter()
            .position(|&addr| addr == address)
        {
            let node = self.responsive_nodes.remove(pos);
            if !self.unresponsive_nodes.contains(&node) {
                self.unresponsive_nodes.push(node);
            }
        }
    }

    fn mark_responsive(&mut self, address: SocketAddr) {
        if let Some(pos) = self
            .unresponsive_nodes
            .iter()
            .position(|&addr| addr == address)
        {
            let node = self.unresponsive_nodes.remove(pos);
            if !self.responsive_nodes.contains(&node) {
                self.responsive_nodes.push(node);
            }
        }
    }

    fn add_node(&mut self, address: SocketAddr) {
        if !self.responsive_nodes.contains(&address) && !self.unresponsive_nodes.contains(&address)
        {
            self.responsive_nodes.push(address);
        }
    }

    fn remove_node(&mut self, address: SocketAddr) {
        self.responsive_nodes.retain(|&addr| addr != address);
        self.unresponsive_nodes.retain(|&addr| addr != address);
    }

    fn all_nodes(&self) -> Vec<SocketAddr> {
        let mut all = self.responsive_nodes.clone();
        all.extend_from_slice(&self.unresponsive_nodes);
        all
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

    async fn mark_unresponsive(&self, _address: SocketAddr) {
        // No-op
    }

    async fn mark_responsive(&self, _address: SocketAddr) {
        // No-op
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
        if nodes.responsive_nodes.contains(&target) {
            match self.transport.send_to_peer(target, data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    drop(nodes);
                    // Mark as unresponsive if send fails
                    self.mark_unresponsive(target).await;
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
        let nodes = self.nodes.read().await;
        if nodes.responsive_nodes.is_empty() {
            return Err(crate::error::ColibriError::Transport(
                "No responsive nodes available".to_string(),
            ));
        }

        match self.transport.send_to_random_peer(data).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Could mark the random node as unresponsive, but we don't know which one failed
                Err(e)
            }
        }
    }

    async fn get_cluster_nodes(&self) -> Vec<SocketAddr> {
        self.nodes.read().await.all_nodes()
    }

    async fn mark_unresponsive(&self, address: SocketAddr) {
        self.nodes.write().await.mark_unresponsive(address);
    }

    async fn mark_responsive(&self, address: SocketAddr) {
        self.nodes.write().await.mark_responsive(address);
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

/// Implementation for nodes that need request-response patterns (hashring)
/// Uses TCP transport for reliable request-response communication
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

    async fn mark_unresponsive(&self, address: SocketAddr) {
        self.nodes.write().await.mark_unresponsive(address);
    }

    async fn mark_responsive(&self, address: SocketAddr) {
        self.nodes.write().await.mark_responsive(address);
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
        let nodes = self.nodes.read().await;
        if !nodes.responsive_nodes.contains(&target) {
            return Err(crate::error::ColibriError::Transport(
                "Node is marked as unresponsive".to_string(),
            ));
        }
        drop(nodes);

        match self.transport.send_request_response(target, request).await {
            Ok(response) => Ok(Some(response)),
            Err(e) => {
                // Mark as unresponsive if request fails
                self.mark_unresponsive(target).await;
                Err(e)
            }
        }
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

        // Test marking unresponsive/responsive
        nodes.mark_unresponsive(addr1);
        assert_eq!(nodes.all_nodes().len(), 2); // Still there

        nodes.mark_responsive(addr1);
        assert_eq!(nodes.all_nodes().len(), 2);

        // Test removal
        nodes.remove_node(addr1);
        let remaining = nodes.all_nodes();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains(&addr2));
    }
}
