// This file is deprecated in favor of the unified cluster architecture.
// See src/cluster/manager.rs for the new ClusterManager trait.
//
// The new architecture provides:
// - Unified transport abstraction (TCP, UDP, gRPC)
// - Consolidated message protocols
// - Simplified membership management
// - Better separation of concerns
//
// TODO: Remove this file once migration is complete

use std::collections::HashMap;
use std::net::SocketAddr;

use bincode::{Decode, Encode};

/// Legacy cluster membership trait - DEPRECATED
/// Use crate::cluster::manager::ClusterManager instead
#[deprecated(note = "Use crate::cluster::manager::ClusterManager instead")]
pub trait ClusterMembership {
    // Define methods for managing cluster membership
    fn cluster_health_check(&self) -> bool;
    fn export_buckets(&self);
    fn import_buckets(&self);
    // cluster_health - missing return type, likely a typo
    fn get_topology(&self);
    fn new_topology(&self);
    fn get_cluster_status(&self);
}



// impl ClusterMembership {
//     /// Create initial cluster membership with local node
//     pub fn new(local_node_id: u32, local_address: std::net::SocketAddr) -> Self {
//         let mut nodes = HashMap::new();
//         let now = std::time::SystemTime::now()
//             .duration_since(std::time::UNIX_EPOCH)
//             .unwrap_or_default()
//             .as_secs();

//         nodes.insert(
//             local_node_id,
//             NodeInfo {
//                 node_id: local_node_id,
//                 address: local_address,
//                 status: NodeStatus::Active,
//                 joined_at: now,
//                 last_seen: now,
//             },
//         );

//         Self {
//             nodes,
//             membership_version: 1,
//         }
//     }

//     /// Add a new node to the cluster
//     pub fn add_node(&mut self, node_info: NodeInfo) -> bool {
//         if self.nodes.contains_key(&node_info.node_id) {
//             return false; // Node already exists
//         }

//         self.nodes.insert(node_info.node_id, node_info);
//         self.membership_version += 1;
//         true
//     }

//     /// Remove a node from the cluster
//     pub fn remove_node(&mut self, node_id: u32) -> Option<NodeInfo> {
//         if let Some(node) = self.nodes.remove(&node_id) {
//             self.membership_version += 1;
//             Some(node)
//         } else {
//             None
//         }
//     }

//     /// Update the status of a node
//     pub fn update_node_status(&mut self, node_id: u32, status: NodeStatus) -> bool {
//         if let Some(node) = self.nodes.get_mut(&node_id) {
//             node.status = status;
//             node.last_seen = std::time::SystemTime::now()
//                 .duration_since(std::time::UNIX_EPOCH)
//                 .unwrap_or_default()
//                 .as_secs();
//             self.membership_version += 1;
//             true
//         } else {
//             false
//         }
//     }

//     /// Get all active nodes in the cluster
//     pub fn get_active_nodes(&self) -> Vec<&NodeInfo> {
//         self.nodes
//             .values()
//             .filter(|node| matches!(node.status, NodeStatus::Active))
//             .collect()
//     }

//     /// Detect nodes that have failed based on last seen timeout
//     pub fn detect_failed_nodes(&mut self, failure_timeout: std::time::Duration) -> Vec<u32> {
//         let now = std::time::SystemTime::now()
//             .duration_since(std::time::UNIX_EPOCH)
//             .unwrap_or_default()
//             .as_secs();
//         let timeout_secs = failure_timeout.as_secs();

//         let mut failed_nodes = Vec::new();

//         for (node_id, node) in &mut self.nodes {
//             if matches!(node.status, NodeStatus::Active)
//                 && now.saturating_sub(node.last_seen) > timeout_secs
//             {
//                 node.status = NodeStatus::Failed;
//                 failed_nodes.push(*node_id);
//             }
//         }

//         if !failed_nodes.is_empty() {
//             self.membership_version += 1;
//         }

//         failed_nodes
//     }
// }
