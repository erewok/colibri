/// ClusterMember trait - separate concern from being a Node
/// Only gossip and hashring nodes implement this, single nodes do not
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::transport::UdpTransport;

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
        if let Some(pos) = self.responsive_nodes.iter().position(|&addr| addr == address) {
            let node = self.responsive_nodes.remove(pos);
            if !self.unresponsive_nodes.contains(&node) {
                self.unresponsive_nodes.push(node);
            }
        }
    }

    fn mark_responsive(&mut self, address: SocketAddr) {
        if let Some(pos) = self.unresponsive_nodes.iter().position(|&addr| addr == address) {
            let node = self.unresponsive_nodes.remove(pos);
            if !self.responsive_nodes.contains(&node) {
                self.responsive_nodes.push(node);
            }
        }
    }

    fn add_node(&mut self, address: SocketAddr) {
        if !self.responsive_nodes.contains(&address) && !self.unresponsive_nodes.contains(&address) {
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
        Err(crate::error::ColibriError::Api("Single nodes don't participate in clusters".to_string()))
    }

    async fn send_to_random_node(&self, _data: &[u8]) -> Result<()> {
        Err(crate::error::ColibriError::Api("Single nodes don't participate in clusters".to_string()))
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
            Err(crate::error::ColibriError::Transport("Node is marked as unresponsive".to_string()))
        }
    }

    async fn send_to_random_node(&self, data: &[u8]) -> Result<()> {
        let nodes = self.nodes.read().await;
        if nodes.responsive_nodes.is_empty() {
            return Err(crate::error::ColibriError::Transport("No responsive nodes available".to_string()));
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
}