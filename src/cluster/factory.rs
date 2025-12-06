/// The ClusterFactory provides a unified way to go from config/CLI params → ClusterMember
/// with no extra code or transport selection complexity
use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::{ClusterMember, NoOpClusterMember, UdpClusterMember};
use crate::error::Result;
use crate::node::NodeId;
use crate::settings::Settings;
use crate::transport::UdpTransport;

/// Factory for creating cluster members from configuration
pub struct ClusterFactory;

impl ClusterFactory {
    /// Create a ClusterMember from settings
    ///
    /// This is the path from configuration to cluster membership:
    /// 1. Empty topology → NoOpClusterMember (single nodes)
    /// 2. Non-empty topology → UdpClusterMember (gossip + hashring both use UDP)
    pub async fn create_from_settings(
        node_id: NodeId,
        settings: &Settings
    ) -> Result<Arc<dyn ClusterMember>> {
        if settings.topology.is_empty() {
            // Single node - no cluster operations
            Ok(Arc::new(NoOpClusterMember))
        } else {
            // Cluster node - use UDP transport for everything
            let transport_config = settings.transport_config();
            let udp_transport = Arc::new(UdpTransport::new(node_id, &transport_config).await?);

            // Convert topology strings to SocketAddr
            let cluster_nodes: Vec<SocketAddr> = settings.topology
                .iter()
                .filter_map(|addr_str| addr_str.parse().ok())
                .collect();

            if cluster_nodes.is_empty() {
                // Invalid topology addresses - treat as single node
                tracing::warn!("Invalid cluster topology addresses, falling back to single node mode");
                Ok(Arc::new(NoOpClusterMember))
            } else {
                Ok(Arc::new(UdpClusterMember::new(udp_transport, cluster_nodes)))
            }
        }
    }

    /// Create from CLI parameters directly (bypassing Settings)
    pub async fn create_from_cli_params(
        node_id: NodeId,
        listen_udp: SocketAddr,
        cluster_nodes: Vec<SocketAddr>,
    ) -> Result<Arc<dyn ClusterMember>> {
        if cluster_nodes.is_empty() {
            Ok(Arc::new(NoOpClusterMember))
        } else {
            // Create minimal transport config from CLI params
            let transport_config = crate::settings::TransportConfig {
                listen_udp,
                topology: cluster_nodes.iter().cloned().collect(),
            };

            let udp_transport = Arc::new(UdpTransport::new(node_id, &transport_config).await?);
            Ok(Arc::new(UdpClusterMember::new(udp_transport, cluster_nodes)))
        }
    }

    /// Create for testing with explicit nodes
    pub async fn create_for_testing(
        node_id: NodeId,
        cluster_nodes: Vec<SocketAddr>,
    ) -> Result<Arc<dyn ClusterMember>> {
        if cluster_nodes.is_empty() {
            Ok(Arc::new(NoOpClusterMember))
        } else {
            // Use any available port for testing
            let listen_addr = "127.0.0.1:0".parse().unwrap();
            Self::create_from_cli_params(node_id, listen_addr, cluster_nodes).await
        }
    }
}

/// Extension trait to make it easy to get cluster members from settings
pub trait SettingsExt {
    /// Get a cluster member directly from settings
    #[allow(async_fn_in_trait)]
    async fn cluster_member(&self, node_id: NodeId) -> Result<Arc<dyn ClusterMember>>;
}

impl SettingsExt for Settings {
    async fn cluster_member(&self, node_id: NodeId) -> Result<Arc<dyn ClusterMember>> {
        ClusterFactory::create_from_settings(node_id, self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_single_node_cluster_member() {
        let settings = Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8080,
            listen_port_tcp: 8081,
            listen_port_udp: 8082,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: crate::settings::RunMode::Single,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology: HashSet::new(), // Empty = single node
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        };

        let node_id = NodeId::new(1);
        let cluster_member = ClusterFactory::create_from_settings(node_id, &settings)
            .await
            .unwrap();

        // Should be empty cluster nodes for single node
        let nodes = cluster_member.get_cluster_nodes().await;
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_cluster_node_cluster_member() {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8001".to_string());
        topology.insert("127.0.0.1:8002".to_string());

        let settings = Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8080,
            listen_port_tcp: 8081,
            listen_port_udp: 8082,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: crate::settings::RunMode::Gossip,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology,
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        };

        let node_id = NodeId::new(1);
        let cluster_member = ClusterFactory::create_from_settings(node_id, &settings)
            .await
            .unwrap();

        // Should have the cluster nodes
        let nodes = cluster_member.get_cluster_nodes().await;
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&"127.0.0.1:8001".parse().unwrap()));
        assert!(nodes.contains(&"127.0.0.1:8002".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_cli_params_cluster_member() {
        let node_id = NodeId::new(1);
        let listen_addr = "127.0.0.1:9000".parse().unwrap();
        let cluster_nodes = vec![
            "127.0.0.1:9001".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
        ];

        let cluster_member = ClusterFactory::create_from_cli_params(
            node_id,
            listen_addr,
            cluster_nodes.clone()
        ).await.unwrap();

        let nodes = cluster_member.get_cluster_nodes().await;
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes, cluster_nodes);
    }

    #[tokio::test]
    async fn test_settings_extension() {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8001".to_string());

        let settings = Settings {
            listen_address: "127.0.0.1".to_string(),
            listen_port_api: 8080,
            listen_port_tcp: 8081,
            listen_port_udp: 8082,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: crate::settings::RunMode::Hashring,
            gossip_interval_ms: 1000,
            gossip_fanout: 3,
            topology,
            failure_timeout_secs: 30,
            hash_replication_factor: 1,
        };

        let node_id = NodeId::new(1);
        let cluster_member = settings.cluster_member(node_id).await.unwrap();

        let nodes = cluster_member.get_cluster_nodes().await;
        assert_eq!(nodes.len(), 1);
    }
}