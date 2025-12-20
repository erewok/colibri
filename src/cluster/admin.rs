/// Administrative command handling for cluster operations
/// This provides a unified way to send admin commands to cluster nodes
/// using internal UDP transport (not exposed in public API)
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, warn};
use postcard::{from_bytes, to_allocvec};

use crate::cluster::ClusterMember;
use crate::error::{ColibriError, Result};
use crate::node::commands::{AdminCommand, AdminResponse};
use crate::transport::SendReceiveStats;

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
        let serialized = to_allocvec(&command).map_err(|e| {
            ColibriError::Serialization(crate::error::SerializationError::BinaryCodec(e))
        })?;

        // Send via cluster member
        self.cluster_member
            .send_to_node(target, &serialized)
            .await?;

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
                }
            }
        }

        Ok(responses)
    }

    /// Add a new node to cluster membership (operator command)
    pub async fn add_cluster_node(&self, name: String, address: SocketAddr) -> Result<()> {
        debug!("Admin: Adding new cluster node {}", address);
        self.cluster_member.add_node(address).await;

        // Send AddNode command to existing nodes to notify them
        let command = AdminCommand::AddNode { name, address };
        let _responses = self.broadcast_admin_command(command).await?;

        Ok(())
    }

    /// Remove a node from cluster membership (operator command)
    pub async fn remove_cluster_node(&self, name: String, address: SocketAddr) -> Result<()> {
        debug!("Admin: Removing cluster node {}", address);

        // Send RemoveNode command to other nodes first
        let command = AdminCommand::RemoveNode { name, address };
        let _responses = self.broadcast_admin_command(command).await?;

        // Then remove from our own membership
        self.cluster_member.remove_node(address).await;

        Ok(())
    }

    /// Get current cluster topology
    pub async fn get_cluster_topology(&self) -> Vec<SocketAddr> {
        self.cluster_member.get_cluster_nodes().await
    }

    /// Get current cluster topology
    pub async fn get_cluster_stats(&self) -> SendReceiveStats {
        self.cluster_member.get_cluster_stats().await
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
    from_bytes(data)
        .map_err(|e| ColibriError::Serialization(crate::error::SerializationError::BinaryCodec(e)))
}

/// Serialize an administrative response to UDP data
pub fn serialize_admin_response(response: &AdminResponse) -> Result<Vec<u8>> {
    to_allocvec(response)
        .map_err(|e| ColibriError::Serialization(crate::error::SerializationError::BinaryCodec(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_command_parsing() {
        let command = AdminCommand::AddNode {
            name: "test-node".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
        };

        let serialized = to_allocvec(&command).unwrap();

        let parsed = parse_admin_command(&serialized).unwrap();
        match parsed {
            AdminCommand::AddNode { name, address } => {
                assert_eq!(name, "test-node");
                assert_eq!(address.to_string(), "127.0.0.1:8080");
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_admin_response_serialization() {
        let response = AdminResponse::Ack;
        let serialized = serialize_admin_response(&response).unwrap();

        let deserialized: AdminResponse = from_bytes(&serialized).unwrap();

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
