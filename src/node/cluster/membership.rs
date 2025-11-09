use std::collections::HashMap;
use std::net::SocketAddr;

use bincode::{Decode, Encode};

/// Cluster membership state shared between nodes
#[derive(Clone, Debug, Decode, Encode)]
pub struct ClusterMembership {
    pub nodes: HashMap<u32, NodeInfo>,
    pub membership_version: u64,
}

/// Node information in the cluster
#[derive(Clone, Debug, Decode, Encode)]
pub struct NodeInfo {
    pub node_id: u32,
    pub address: SocketAddr,
    pub status: NodeStatus,
    pub joined_at: u64,
    pub last_seen: u64,
}

/// Node status in cluster lifecycle
#[derive(Clone, Debug, Decode, Encode)]
pub enum NodeStatus {
    Joining,
    Active,
    Leaving,
    Failed,
    Left,
}

impl ClusterMembership {
    /// Create initial cluster membership with local node
    pub fn new(local_node_id: u32, local_address: std::net::SocketAddr) -> Self {
        let mut nodes = HashMap::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        nodes.insert(
            local_node_id,
            NodeInfo {
                node_id: local_node_id,
                address: local_address,
                status: NodeStatus::Active,
                joined_at: now,
                last_seen: now,
            },
        );

        Self {
            nodes,
            membership_version: 1,
        }
    }

    /// Add a new node to the cluster
    pub fn add_node(&mut self, node_info: NodeInfo) -> bool {
        if self.nodes.contains_key(&node_info.node_id) {
            return false; // Node already exists
        }

        self.nodes.insert(node_info.node_id, node_info);
        self.membership_version += 1;
        true
    }

    /// Remove a node from the cluster
    pub fn remove_node(&mut self, node_id: u32) -> Option<NodeInfo> {
        if let Some(node) = self.nodes.remove(&node_id) {
            self.membership_version += 1;
            Some(node)
        } else {
            None
        }
    }

    /// Update the status of a node
    pub fn update_node_status(&mut self, node_id: u32, status: NodeStatus) -> bool {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.status = status;
            node.last_seen = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.membership_version += 1;
            true
        } else {
            false
        }
    }

    /// Get all active nodes in the cluster
    pub fn get_active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|node| matches!(node.status, NodeStatus::Active))
            .collect()
    }

    /// Detect nodes that have failed based on last seen timeout
    pub fn detect_failed_nodes(&mut self, failure_timeout: std::time::Duration) -> Vec<u32> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let timeout_secs = failure_timeout.as_secs();

        let mut failed_nodes = Vec::new();

        for (node_id, node) in &mut self.nodes {
            if matches!(node.status, NodeStatus::Active)
                && now.saturating_sub(node.last_seen) > timeout_secs
            {
                node.status = NodeStatus::Failed;
                failed_nodes.push(*node_id);
            }
        }

        if !failed_nodes.is_empty() {
            self.membership_version += 1;
        }

        failed_nodes
    }
}
