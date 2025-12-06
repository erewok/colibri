/// Administrative command handling for cluster operations
/// This provides a unified way to send admin commands to cluster nodes
/// using internal UDP transport (not exposed in public API)
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::cluster::{AdminCommand, AdminResponse, ClusterMember};
use crate::error::{ColibriError, Result};

/// Administrative command dispatcher
/// Sends commands to cluster nodes using internal transport
pub struct AdminCommandDispatcher {
    cluster_member: Arc<dyn ClusterMember>,
}

impl AdminCommandDispatcher {
    pub fn new(cluster_member: Arc<dyn ClusterMember>) -> Self {
        Self { cluster_member }
    }

    /// Send an administrative command to a specific node
    pub async fn send_admin_command(
        &self,
        target: SocketAddr,
        command: AdminCommand,
    ) -> Result<AdminResponse> {
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&command, config).map_err(|e| {
            ColibriError::Serialization(crate::error::SerializationError::BinaryEncode(e))
        })?;

        // Send via cluster member (UDP transport)
        self.cluster_member
            .send_to_node(target, &serialized)
            .await?;

        // For now, return Ack - in a full implementation we'd wait for response
        // This would require implementing a request/response pattern over UDP
        Ok(AdminResponse::Ack)
    }

    /// Send command to all responsive cluster nodes
    pub async fn broadcast_admin_command(
        &self,
        command: AdminCommand,
    ) -> Result<Vec<AdminResponse>> {
        let nodes = self.cluster_member.get_cluster_nodes().await;
        let mut responses = Vec::new();

        for node in nodes {
            match self.send_admin_command(node, command.clone()).await {
                Ok(response) => responses.push(response),
                Err(e) => {
                    warn!("Failed to send admin command to {}: {}", node, e);
                    // Mark node as unresponsive if command fails
                    self.cluster_member.mark_unresponsive(node).await;
                }
            }
        }

        Ok(responses)
    }

    /// Add a new node to cluster membership (operator command)
    pub async fn add_cluster_node(&self, address: SocketAddr) -> Result<()> {
        debug!("Admin: Adding new cluster node {}", address);
        self.cluster_member.add_node(address).await;

        // Send AddNode command to existing nodes to notify them
        let command = AdminCommand::AddNode { address };
        let _responses = self.broadcast_admin_command(command).await?;

        Ok(())
    }

    /// Remove a node from cluster membership (operator command)
    pub async fn remove_cluster_node(&self, address: SocketAddr) -> Result<()> {
        debug!("Admin: Removing cluster node {}", address);

        // Send RemoveNode command to other nodes first
        let command = AdminCommand::RemoveNode { address };
        let _responses = self.broadcast_admin_command(command).await?;

        // Then remove from our own membership
        self.cluster_member.remove_node(address).await;

        Ok(())
    }

    /// Mark a node as unresponsive (automatic or operator command)
    pub async fn mark_node_unresponsive(&self, address: SocketAddr) -> Result<()> {
        debug!("Admin: Marking node {} as unresponsive", address);
        self.cluster_member.mark_unresponsive(address).await;

        // Notify other nodes
        let command = AdminCommand::MarkUnresponsive { address };
        let _responses = self.broadcast_admin_command(command).await?;

        Ok(())
    }

    /// Mark a node as responsive (operator command)
    pub async fn mark_node_responsive(&self, address: SocketAddr) -> Result<()> {
        debug!("Admin: Marking node {} as responsive", address);
        self.cluster_member.mark_responsive(address).await;

        // Notify other nodes
        let command = AdminCommand::MarkResponsive { address };
        let _responses = self.broadcast_admin_command(command).await?;

        Ok(())
    }

    /// Perform cluster topology change (replaces hashring export/restart/import cycle)
    pub async fn change_cluster_topology(&self, new_topology: Vec<SocketAddr>) -> Result<()> {
        debug!(
            "Admin: Changing cluster topology to {} nodes",
            new_topology.len()
        );

        // Send PrepareTopologyChange to all current nodes
        let command = AdminCommand::PrepareTopologyChange {
            new_topology: new_topology.clone(),
        };
        let _responses = self.broadcast_admin_command(command).await?;

        // Update our own cluster membership
        let current_nodes = self.cluster_member.get_cluster_nodes().await;
        let new_nodes_set: std::collections::HashSet<_> = new_topology.iter().cloned().collect();
        let current_nodes_set: std::collections::HashSet<_> =
            current_nodes.iter().cloned().collect();

        // Add new nodes
        for addr in new_nodes_set.difference(&current_nodes_set) {
            self.cluster_member.add_node(*addr).await;
        }

        // Remove nodes that are no longer in topology
        for addr in current_nodes_set.difference(&new_nodes_set) {
            self.cluster_member.remove_node(*addr).await;
        }

        Ok(())
    }

    /// Get current cluster topology
    pub async fn get_cluster_topology(&self) -> Vec<SocketAddr> {
        self.cluster_member.get_cluster_nodes().await
    }
}

/// Administrative command handler trait
/// Both gossip and hashring nodes implement this to handle incoming admin commands
#[async_trait]
pub trait AdminCommandHandler: Send + Sync {
    /// Handle an incoming administrative command
    async fn handle_admin_command(&self, command: AdminCommand) -> Result<AdminResponse>;
}

/// Parse an administrative command from UDP data
pub fn parse_admin_command(data: &[u8]) -> Result<AdminCommand> {
    let config = bincode::config::standard();
    bincode::decode_from_slice(data, config)
        .map(|(command, _)| command)
        .map_err(|e| ColibriError::Serialization(crate::error::SerializationError::BinaryDecode(e)))
}

/// Serialize an administrative response to UDP data
pub fn serialize_admin_response(response: &AdminResponse) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    bincode::encode_to_vec(response, config)
        .map_err(|e| ColibriError::Serialization(crate::error::SerializationError::BinaryEncode(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_command_parsing() {
        let command = AdminCommand::AddNode {
            address: "127.0.0.1:8080".parse().unwrap(),
        };

        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(&command, config).unwrap();

        let parsed = parse_admin_command(&serialized).unwrap();
        match parsed {
            AdminCommand::AddNode { address } => {
                assert_eq!(address.to_string(), "127.0.0.1:8080");
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_admin_response_serialization() {
        let response = AdminResponse::Ack;
        let serialized = serialize_admin_response(&response).unwrap();

        let config = bincode::config::standard();
        let (deserialized, _): (AdminResponse, _) =
            bincode::decode_from_slice(&serialized, config).unwrap();

        match deserialized {
            AdminResponse::Ack => {}
            _ => panic!("Wrong response type"),
        }
    }

    #[tokio::test]
    async fn test_admin_dispatcher_creation() {
        // Test creating dispatcher with NoOp member
        let no_op = Arc::new(crate::cluster::NoOpClusterMember);
        let dispatcher = AdminCommandDispatcher::new(no_op);

        // Should be able to get empty topology
        let topology = dispatcher.get_cluster_topology().await;
        assert_eq!(topology.len(), 0);
    }
}
