/// The ClusterFactory provides a unified way to go from config/CLI params → ClusterMember
/// with no extra code or transport selection complexity
use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::{ClusterMember, NoOpClusterMember, TcpClusterMember, UdpClusterMember};
use crate::error::Result;
use crate::node::NodeId;
use crate::settings::Settings;
use crate::transport::{TcpTransport, UdpTransport};

/// Factory for creating cluster members from configuration
pub struct ClusterFactory;

impl ClusterFactory {
    /// Create a ClusterMember from settings
    ///
    /// This is the path from configuration to cluster membership:
    /// 1. Empty topology → NoOpClusterMember (single nodes)
    /// 2. Gossip mode → UdpClusterMember (fire-and-forget communication)
    /// 3. Hashring mode → TcpClusterMember (request-response communication)
    pub async fn create_from_settings(
        settings: &Settings,
    ) -> Result<Arc<dyn ClusterMember>> {
        if settings.topology.is_empty() {
            // Single node - no cluster operations
            Ok(Arc::new(NoOpClusterMember))
        } else {
            match settings.run_mode {
                crate::settings::RunMode::Hashring => {
                    // Hashring needs TCP for request-response patterns
                    Self::create_tcp_from_settings(settings).await
                }
                crate::settings::RunMode::Gossip => {
                    // Gossip uses UDP for fire-and-forget communication
                    let transport_config = settings.transport_config();
                    let udp_transport =
                        Arc::new(UdpTransport::new(&transport_config).await?);

                    // Convert topology strings to SocketAddr
                    let cluster_nodes: Vec<SocketAddr> = settings
                        .topology
                        .iter()
                        .filter_map(|addr_str| addr_str.parse().ok())
                        .collect();

                    if cluster_nodes.is_empty() {
                        tracing::warn!(
                            "Invalid cluster topology addresses, falling back to single node mode"
                        );
                        Ok(Arc::new(NoOpClusterMember))
                    } else {
                        Ok(Arc::new(UdpClusterMember::new(
                            udp_transport,
                            cluster_nodes,
                        )))
                    }
                }
                crate::settings::RunMode::Single => {
                    // Single mode doesn't use cluster
                    Ok(Arc::new(NoOpClusterMember))
                }
            }
        }
    }

    /// Create TCP cluster member for hashring nodes that need request-response
    pub async fn create_tcp_cluster_member(
        node_id: NodeId,
        cluster_nodes: Vec<SocketAddr>,
        max_connections_per_peer: usize,
    ) -> Result<Arc<dyn ClusterMember>> {
        if cluster_nodes.is_empty() {
            Ok(Arc::new(NoOpClusterMember))
        } else {
            let tcp_transport = Arc::new(
                TcpTransport::new(node_id, cluster_nodes.clone(), max_connections_per_peer).await?,
            );
            Ok(Arc::new(TcpClusterMember::new(
                tcp_transport,
                cluster_nodes,
            )))
        }
    }

    /// Create TCP cluster member from settings (for hashring mode)
    pub async fn create_tcp_from_settings(
        node_id: NodeId,
        settings: &Settings,
    ) -> Result<Arc<dyn ClusterMember>> {
        if settings.topology.is_empty() {
            Ok(Arc::new(NoOpClusterMember))
        } else {
            // For hashring, topology addresses should be TCP addresses
            let cluster_nodes: Vec<SocketAddr> = settings
                .topology
                .iter()
                .filter_map(|addr_str| addr_str.parse().ok())
                .collect();

            if cluster_nodes.is_empty() {
                tracing::warn!(
                    "Invalid cluster topology addresses for TCP, falling back to single node mode"
                );
                Ok(Arc::new(NoOpClusterMember))
            } else {
                let max_connections = 5; // Default max connections per peer
                Self::create_tcp_cluster_member(node_id, cluster_nodes, max_connections).await
            }
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
            config_file: None,
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
            config_file: None,
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
    async fn test_settings_extension() {
        let mut topology = HashSet::new();
        topology.insert("127.0.0.1:8001".to_string());

        let settings = Settings {
            config_file: None,
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
            hash_replication_factor: 1,
        };

        let node_id = NodeId::new(1);
        let cluster_member = settings.cluster_member(node_id).await.unwrap();

        let nodes = cluster_member.get_cluster_nodes().await;
        assert_eq!(nodes.len(), 1);
    }
}
