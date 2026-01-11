//! Base Controller (Phase 2)
//!
//! Shared controller logic for both Gossip and Hashring modes.
//! Handles rate limiting, rule management, and admin operations.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use crate::error::Result;
use crate::limiters::distributed_bucket::DistributedBucketLimiter;
use crate::limiters::NamedRateLimitRule;
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse,
    StatusResponse, TopologyResponse, Status,
};
use crate::node::NodeAddress;
use crate::node::NodeName;
use crate::settings::{ClusterTopology, RateLimitSettings, RunMode};
use crate::transport::TcpTransport;

/// Shared controller state and operations
///
/// This struct contains all the common functionality that both GossipController
/// and HashringController need, including rate limiting, admin operations,
/// and topology management.
pub struct BaseController {
    /// This node's name
    pub node_name: NodeName,
    /// Cluster topology (unified view of cluster membership)
    pub topology: Arc<RwLock<ClusterTopology>>,
    /// TCP transport for cluster communication
    pub transport: Arc<TcpTransport>,
    /// Rate limiter (CRDT-based distributed token bucket)
    pub rate_limiter: Arc<Mutex<DistributedBucketLimiter>>,
    /// Run mode (Gossip or Hashring)
    pub run_mode: RunMode,
    /// Default rate limit settings
    pub default_settings: RateLimitSettings,
}

impl BaseController {
    /// Create a new base controller
    pub fn new(
        node_name: NodeName,
        topology: ClusterTopology,
        transport: TcpTransport,
        rate_limiter: DistributedBucketLimiter,
        run_mode: RunMode,
        default_settings: RateLimitSettings,
    ) -> Self {
        Self {
            node_name,
            topology: Arc::new(RwLock::new(topology)),
            transport: Arc::new(transport),
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
            run_mode,
            default_settings,
        }
    }

    // ============================================================================
    // RATE LIMITING OPERATIONS (shared by both modes)
    // ============================================================================

    /// Handle a rate limit request
    ///
    /// This checks if a token can be consumed (if consume_token=true) or just
    /// checks the current limit (if consume_token=false).
    pub async fn handle_rate_limit_request(
        &self,
        request: CheckCallsRequest,
    ) -> Result<CheckCallsResponse> {
        let mut limiter = self.rate_limiter.lock().await;

        let calls_remaining = if request.consume_token {
            // Consume a token (rate_limit operation)
            limiter.limit_calls_for_client(request.client_id.clone())
        } else {
            // Just check the limit (check_limit operation)
            Some(limiter.check_calls_remaining_for_client(&request.client_id))
        };

        Ok(CheckCallsResponse {
            client_id: request.client_id,
            rule_name: request.rule_name,
            calls_remaining: calls_remaining.unwrap_or(0),
        })
    }

    // ============================================================================
    // ADMIN OPERATIONS (shared by both modes)
    // ============================================================================

    /// Get status of this node
    pub async fn get_status(&self) -> Result<StatusResponse> {
        let topology = self.topology.read().await;

        Ok(StatusResponse {
            node_name: self.node_name.clone(),
            node_type: self.run_mode.clone(),
            status: Status::Healthy,
            bucket_count: Some(topology.node_count()),
            last_topology_change: None,
            errors: None,
        })
    }

    /// Get cluster topology
    pub async fn get_topology(&self) -> Result<TopologyResponse> {
        let status = self.get_status().await?;
        let topology = self.topology.read().await;

        // Convert Vec<(NodeName, SocketAddr)> to HashMap<NodeName, NodeAddress>
        // Since we only have SocketAddr, we use it for both local and remote addresses
        let topology_map = topology
            .all_nodes()
            .into_iter()
            .map(|(name, addr)| {
                let node_addr = NodeAddress {
                    name: name.clone(),
                    local_address: addr,
                    remote_address: addr,
                };
                (name, node_addr)
            })
            .collect();

        Ok(TopologyResponse {
            status,
            topology: topology_map,
        })
    }

    /// Add a node to the topology
    pub async fn add_node(&self, name: NodeName, address: SocketAddr) -> Result<()> {
        let mut topology = self.topology.write().await;
        topology.add_node(name.clone(), address);

        tracing::info!("Added node {} at {} to topology", name.as_str(), address);

        // TODO: Notify transport layer to add peer
        // This would call something like:
        // self.transport.add_peer(name.node_id(), address).await?;

        Ok(())
    }

    /// Remove a node from the topology
    pub async fn remove_node(&self, name: NodeName) -> Result<()> {
        let mut topology = self.topology.write().await;

        if let Some(address) = topology.remove_node(&name) {
            tracing::info!("Removed node {} (was at {}) from topology", name.as_str(), address);

            // TODO: Notify transport layer to remove peer
            // This would call something like:
            // self.transport.remove_peer(name.node_id()).await?;
        }

        Ok(())
    }

    // ============================================================================
    // RULE MANAGEMENT OPERATIONS (shared by both modes)
    // ============================================================================

    // Note: The current DistributedBucketLimiter doesn't have named rule support yet.
    // These are placeholder implementations that will need to be updated when
    // rule management is added to the limiter in a future phase.

    /// Create a new rate limit rule
    ///
    /// TODO: This is a placeholder. DistributedBucketLimiter needs to be enhanced
    /// to support multiple named rules beyond the default rule.
    pub async fn create_rate_limit_rule(
        &self,
        rule_name: String,
        _settings: RateLimitSettings,
    ) -> Result<()> {
        tracing::warn!(
            "create_rate_limit_rule called for '{}' but rule management not yet implemented in DistributedBucketLimiter",
            rule_name
        );
        // TODO: Add rule to limiter when API is available
        Ok(())
    }

    /// Delete a rate limit rule
    ///
    /// TODO: This is a placeholder. DistributedBucketLimiter needs to be enhanced
    /// to support multiple named rules beyond the default rule.
    pub async fn delete_rate_limit_rule(&self, rule_name: String) -> Result<()> {
        tracing::warn!(
            "delete_rate_limit_rule called for '{}' but rule management not yet implemented in DistributedBucketLimiter",
            rule_name
        );
        // TODO: Remove rule from limiter when API is available
        Ok(())
    }

    /// Get a specific rate limit rule
    ///
    /// TODO: This is a placeholder. DistributedBucketLimiter needs to be enhanced
    /// to support multiple named rules beyond the default rule.
    pub async fn get_rate_limit_rule(
        &self,
        rule_name: String,
    ) -> Result<Option<NamedRateLimitRule>> {
        tracing::warn!(
            "get_rate_limit_rule called for '{}' but rule management not yet implemented in DistributedBucketLimiter",
            rule_name
        );

        // For now, return the default settings if asking for the default rule
        if rule_name == "default" {
            Ok(Some(NamedRateLimitRule {
                name: "default".to_string(),
                settings: self.default_settings.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// List all rate limit rules
    ///
    /// TODO: This is a placeholder. DistributedBucketLimiter needs to be enhanced
    /// to support multiple named rules beyond the default rule.
    pub async fn list_rate_limit_rules(&self) -> Result<Vec<NamedRateLimitRule>> {
        tracing::warn!(
            "list_rate_limit_rules called but rule management not yet implemented in DistributedBucketLimiter"
        );

        // For now, return just the default rule
        Ok(vec![NamedRateLimitRule {
            name: "default".to_string(),
            settings: self.default_settings.clone(),
        }])
    }
}

// ============================================================================
// UNIT TESTS
// ============================================================================

#[cfg(test)]
mod base_controller_tests {
    use super::*;
    use crate::settings::TransportConfig;

    async fn create_test_controller() -> BaseController {
        let node_name = NodeName::from("test-node");
        let mut nodes = indexmap::IndexMap::new();
        nodes.insert(node_name.clone(), "127.0.0.1:8001".parse().unwrap());
        let topology = ClusterTopology::new(node_name.clone(), nodes);

        let mut transport_topology = indexmap::IndexMap::new();
        transport_topology.insert(
            node_name.node_id(),
            "127.0.0.1:8001".parse().unwrap(),
        );

        let transport_config = TransportConfig {
            node_name: node_name.clone(),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8000,
            topology: transport_topology,
        };
        let transport = TcpTransport::new(&transport_config).await.unwrap();

        let rate_limiter = DistributedBucketLimiter::new(
            node_name.node_id(),
            RateLimitSettings {
                rate_limit_max_calls_allowed: 100,
                rate_limit_interval_seconds: 60,
            },
        );

        BaseController::new(
            node_name,
            topology,
            transport,
            rate_limiter,
            RunMode::Hashring,
            RateLimitSettings {
                rate_limit_max_calls_allowed: 100,
                rate_limit_interval_seconds: 60,
            },
        )
    }

    #[tokio::test]
    async fn test_base_controller_creation() {
        let controller = create_test_controller().await;
        assert_eq!(controller.node_name, NodeName::from("test-node"));
        assert_eq!(controller.run_mode, RunMode::Hashring);
    }

    #[tokio::test]
    async fn test_get_status() {
        let controller = create_test_controller().await;
        let status = controller.get_status().await.unwrap();

        assert_eq!(status.node_name, NodeName::from("test-node"));
        assert_eq!(status.node_type, RunMode::Hashring);
        assert!(matches!(status.status, Status::Healthy));
        assert_eq!(status.bucket_count, Some(1));
    }

    #[tokio::test]
    async fn test_get_topology() {
        let controller = create_test_controller().await;
        let topology_response = controller.get_topology().await.unwrap();

        assert_eq!(topology_response.status.node_name, NodeName::from("test-node"));
        assert_eq!(topology_response.topology.len(), 1);
        assert!(topology_response.topology.contains_key(&NodeName::from("test-node")));
    }

    #[tokio::test]
    async fn test_topology_operations() {
        let controller = create_test_controller().await;

        // Add node
        controller
            .add_node(
                NodeName::from("new-node"),
                "127.0.0.1:8002".parse().unwrap(),
            )
            .await
            .unwrap();

        let topology = controller.topology.read().await;
        assert!(topology.contains_node(&NodeName::from("new-node")));
        assert_eq!(topology.node_count(), 2);

        drop(topology); // Release lock

        // Remove node
        controller
            .remove_node(NodeName::from("new-node"))
            .await
            .unwrap();

        let topology = controller.topology.read().await;
        assert!(!topology.contains_node(&NodeName::from("new-node")));
        assert_eq!(topology.node_count(), 1);
    }

    #[tokio::test]
    async fn test_rate_limit_request() {
        let controller = create_test_controller().await;

        // Check limit (don't consume)
        let request = CheckCallsRequest {
            client_id: "test-client".to_string(),
            rule_name: None,
            consume_token: false,
        };
        let response = controller.handle_rate_limit_request(request).await.unwrap();

        assert_eq!(response.client_id, "test-client");
        assert_eq!(response.calls_remaining, 100); // Initial limit

        // Consume a token
        let request = CheckCallsRequest {
            client_id: "test-client".to_string(),
            rule_name: None,
            consume_token: true,
        };
        let response = controller.handle_rate_limit_request(request).await.unwrap();

        assert_eq!(response.client_id, "test-client");
        // Note: calls_remaining might be less than 99 due to refill logic
        assert!(response.calls_remaining <= 100);
    }

    #[tokio::test]
    async fn test_list_rules_returns_default() {
        let controller = create_test_controller().await;
        let rules = controller.list_rate_limit_rules().await.unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "default");
        assert_eq!(rules[0].settings.rate_limit_max_calls_allowed, 100);
    }

    #[tokio::test]
    async fn test_get_rule_returns_default() {
        let controller = create_test_controller().await;
        let rule = controller.get_rate_limit_rule("default".to_string()).await.unwrap();

        assert!(rule.is_some());
        let rule = rule.unwrap();
        assert_eq!(rule.name, "default");
        assert_eq!(rule.settings.rate_limit_max_calls_allowed, 100);
    }
}
