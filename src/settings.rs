//! Colibri application settings
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::node::{NodeName, NodeId};

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const STANDARD_PORT_HTTP: u16 = 8411;
pub const DEFAULT_PORT_HTTP: &str = "8411";
pub const STANDARD_PORT_PEERS: u16 = 8412;
pub const DEFAULT_PORT_PEERS: &str = "8412";

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct RateLimitSettings {
    pub rate_limit_max_calls_allowed: u32,
    pub rate_limit_interval_seconds: u32,
}


#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub node_name: NodeName,
    pub peer_listen_address: String,
    pub peer_listen_port: u16,
    // we use an IndexMap to preserve order so we can more easily pull a random peer
    pub topology: IndexMap<NodeId, SocketAddr>,
}

impl TransportConfig {
    pub fn peer_listen_url(&self) -> SocketAddr {
        format!("{}:{}", self.peer_listen_address, self.peer_listen_port)
            .parse()
            .expect("Invalid socket address")
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum RunMode {
    Gossip,
    Hashring,
    Single,
}

impl std::fmt::Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Gossip => write!(f, "gossip"),
            RunMode::Hashring => write!(f, "hashring"),
            RunMode::Single => write!(f, "single"),
        }
    }
}

impl std::str::FromStr for RunMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gossip" => Ok(RunMode::Gossip),
            "hashring" => Ok(RunMode::Hashring),
            "single" => Ok(RunMode::Single),
            _ => Err(format!("Invalid multi-mode: {}", s)),
        }
    }
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }
}

impl RateLimitSettings {
    pub fn token_rate_seconds(&self) -> f64 {
        let calls_allowed = f64::from(self.rate_limit_max_calls_allowed);
        let interval_seconds = f64::from(self.rate_limit_interval_seconds);
        calls_allowed / interval_seconds
    }

    pub fn token_rate_milliseconds(&self) -> f64 {
        self.token_rate_seconds() / 1000.0
    }
}

// ============================================================================
// CLUSTER TOPOLOGY
// ============================================================================

/// Single source of truth for cluster membership.
/// Maintains name-to-address mapping and provides bucket assignment operations.
///
/// This replaces the multiple topology representations that previously existed:
/// - Settings.topology (HashMap<String, String>)
/// - TransportConfig.topology (IndexMap<NodeId, SocketAddr>)
/// - ClusterNodes.nodes (HashSet<SocketAddr>)
///
/// Design: Uses IndexMap to preserve insertion order for deterministic iteration,
/// which is critical for consistent bucket assignment across all nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// This node's name
    pub this_node: NodeName,
    /// All cluster nodes (name -> address)
    /// IndexMap preserves order for consistent sorting
    pub nodes: IndexMap<NodeName, SocketAddr>,
}

impl ClusterTopology {
    /// Create a new cluster topology
    pub fn new(this_node: NodeName, nodes: IndexMap<NodeName, SocketAddr>) -> Self {
        Self { this_node, nodes }
    }

    /// Get the address for a node by name
    pub fn get_node_address(&self, name: &NodeName) -> Option<SocketAddr> {
        self.nodes.get(name).copied()
    }

    /// Get the node name for an address
    pub fn get_node_by_address(&self, addr: SocketAddr) -> Option<NodeName> {
        self.nodes
            .iter()
            .find(|(_, a)| **a == addr)
            .map(|(n, _)| n.clone())
    }

    /// Get all nodes as (name, address) pairs
    pub fn all_nodes(&self) -> Vec<(NodeName, SocketAddr)> {
        self.nodes
            .iter()
            .map(|(n, a)| (n.clone(), *a))
            .collect()
    }

    /// Get all node addresses (for transport initialization)
    pub fn all_addresses(&self) -> Vec<SocketAddr> {
        self.nodes.values().copied().collect()
    }

    /// Get nodes sorted by name (for consistent bucket assignment)
    /// This is critical for hashring mode - all nodes must agree on the same order
    pub fn sorted_nodes(&self) -> Vec<(NodeName, SocketAddr)> {
        let mut nodes = self.all_nodes();
        nodes.sort_by(|a, b| a.0.cmp(&b.0));
        nodes
    }

    /// Calculate bucket number for a given node name
    /// Returns None if topology is empty
    pub fn bucket_for_node(&self, node_name: &NodeName) -> Option<u32> {
        if self.nodes.is_empty() {
            return None;
        }
        let num_buckets = self.nodes.len() as u32;
        Some(crate::node::hashring::consistent_hashing::jump_consistent_hash(
            node_name.as_str(),
            num_buckets,
        ))
    }

    /// Get the node that owns a specific bucket
    /// Buckets are assigned to nodes in sorted order
    pub fn node_for_bucket(&self, bucket: u32) -> Option<NodeName> {
        let sorted = self.sorted_nodes();
        sorted.get(bucket as usize).map(|(n, _)| n.clone())
    }

    /// Get the address for the node that owns a specific bucket
    pub fn address_for_bucket(&self, bucket: u32) -> Option<SocketAddr> {
        let sorted = self.sorted_nodes();
        sorted.get(bucket as usize).map(|(_, a)| *a)
    }

    /// Calculate which bucket a key should be routed to
    /// Uses jump consistent hash for even distribution
    pub fn bucket_for_key(&self, key: &str) -> Option<u32> {
        if self.nodes.is_empty() {
            return None;
        }
        let num_buckets = self.nodes.len() as u32;
        Some(crate::node::hashring::consistent_hashing::jump_consistent_hash(
            key,
            num_buckets,
        ))
    }

    /// Get the node that should handle a specific key
    /// Combines bucket_for_key and node_for_bucket
    pub fn node_for_key(&self, key: &str) -> Option<NodeName> {
        self.bucket_for_key(key)
            .and_then(|bucket| self.node_for_bucket(bucket))
    }

    /// Get the address for the node that should handle a specific key
    /// This is the most common operation for routing requests
    pub fn address_for_key(&self, key: &str) -> Option<SocketAddr> {
        self.bucket_for_key(key)
            .and_then(|bucket| self.address_for_bucket(bucket))
    }

    /// Add a node to the topology
    /// Note: This changes bucket assignments, so use carefully
    pub fn add_node(&mut self, name: NodeName, address: SocketAddr) {
        self.nodes.insert(name, address);
    }

    /// Remove a node from the topology
    /// Note: This changes bucket assignments, so use carefully
    /// Returns the address if the node was present
    pub fn remove_node(&mut self, name: &NodeName) -> Option<SocketAddr> {
        self.nodes.shift_remove(name)
    }

    /// Check if this node is part of the topology
    pub fn contains_node(&self, name: &NodeName) -> bool {
        self.nodes.contains_key(name)
    }

    /// Get the number of nodes in the topology
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Check if this is a single-node topology
    pub fn is_single_node(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// Check if this is a valid cluster (2+ nodes)
    pub fn is_cluster(&self) -> bool {
        self.nodes.len() >= 2
    }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub server_name: String,

    // Config file path
    pub config_file: Option<PathBuf>,

    // Server listen address
    pub client_listen_address: String,

    // HTTP API listen port
    pub client_listen_port: u16,

    // Listen URL for Peer communcation
    pub peer_listen_address: String,

    // Listen Port for Peer communication
    pub peer_listen_port: u16,

    // Rate limit settings: max calls (over interval)
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    pub rate_limit_interval_seconds: u32,

    // Mode of multi-node operation
    pub run_mode: RunMode,

    // Cluster configuration information: topology
    pub topology: HashMap<String, String>,

    // Gossip Configuration
    pub gossip_interval_ms: u64, // Regular gossip interval (default: 25)
    pub gossip_fanout: usize,    // Number of peers per gossip round (default: 4)

    // Hashring replication factor
    pub hash_replication_factor: usize,
}

impl Settings {
    pub fn node_name(&self) -> NodeName {
        self.server_name.clone().into()
    }

    pub fn transport_config(&self) -> TransportConfig {
        TransportConfig {
            node_name: self.node_name(),
            peer_listen_address: self.peer_listen_address.clone(),
            peer_listen_port: self.peer_listen_port,
            topology: self
                .topology
                .iter()
                .map(|(k, v)| {
                    (
                        NodeName::new(k.into()).node_id(),
                        v.parse().expect("Invalid socket address in topology"),
                    )
                })
                .collect(),
        }
    }

    pub fn rate_limit_settings(&self) -> RateLimitSettings {
        RateLimitSettings {
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
        }
    }

    /// Create a ClusterTopology from the settings
    /// Converts HashMap<String, String> to IndexMap<NodeName, SocketAddr>
    pub fn cluster_topology(&self) -> ClusterTopology {
        let this_node = self.node_name();
        let mut nodes = IndexMap::new();

        for (name, addr_str) in &self.topology {
            let node_name = NodeName::from(name.clone());
            let addr = addr_str
                .parse()
                .expect("Invalid socket address in topology");
            nodes.insert(node_name, addr);
        }

        ClusterTopology::new(this_node, nodes)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn sample() -> Settings {
        Settings {
            config_file: None,
            server_name: "test-node".to_string(),
            client_listen_address: "127.0.0.1".to_string(),
            client_listen_port: 8411,
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: RunMode::Gossip,
            gossip_interval_ms: 1000, // Longer for testing
            gossip_fanout: 3,
            topology: HashMap::new(), // Empty topology for simple tests
            hash_replication_factor: 1,
        }
    }

    #[test]
    fn test_rate_limit_token_rate() {
        let settings = RateLimitSettings {
            rate_limit_max_calls_allowed: 120,
            rate_limit_interval_seconds: 60,
        };

        assert_eq!(settings.token_rate_seconds(), 2.0);
        assert_eq!(settings.token_rate_milliseconds(), 0.002);
    }

    // ============================================================================
    // CLUSTER TOPOLOGY TESTS
    // ============================================================================

    #[test]
    fn test_cluster_topology_creation() {
        let mut nodes = IndexMap::new();
        nodes.insert(NodeName::from("node-a"), "127.0.0.1:8001".parse().unwrap());
        nodes.insert(NodeName::from("node-b"), "127.0.0.1:8002".parse().unwrap());
        nodes.insert(NodeName::from("node-c"), "127.0.0.1:8003".parse().unwrap());

        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes.clone());

        assert_eq!(topology.this_node, NodeName::from("node-a"));
        assert_eq!(topology.node_count(), 3);
        assert!(topology.is_cluster());
        assert!(!topology.is_single_node());
    }

    #[test]
    fn test_cluster_topology_node_lookup() {
        let mut nodes = IndexMap::new();
        let addr_a = "127.0.0.1:8001".parse().unwrap();
        let addr_b = "127.0.0.1:8002".parse().unwrap();
        nodes.insert(NodeName::from("node-a"), addr_a);
        nodes.insert(NodeName::from("node-b"), addr_b);

        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        // Lookup by name
        assert_eq!(
            topology.get_node_address(&NodeName::from("node-a")),
            Some(addr_a)
        );
        assert_eq!(
            topology.get_node_address(&NodeName::from("node-b")),
            Some(addr_b)
        );
        assert_eq!(
            topology.get_node_address(&NodeName::from("node-x")),
            None
        );

        // Lookup by address
        assert_eq!(
            topology.get_node_by_address(addr_a),
            Some(NodeName::from("node-a"))
        );
        assert_eq!(
            topology.get_node_by_address(addr_b),
            Some(NodeName::from("node-b"))
        );
        assert_eq!(
            topology.get_node_by_address("127.0.0.1:9999".parse().unwrap()),
            None
        );
    }

    #[test]
    fn test_cluster_topology_sorted_nodes() {
        let mut nodes = IndexMap::new();
        // Insert in random order
        nodes.insert(NodeName::from("node-c"), "127.0.0.1:8003".parse().unwrap());
        nodes.insert(NodeName::from("node-a"), "127.0.0.1:8001".parse().unwrap());
        nodes.insert(NodeName::from("node-b"), "127.0.0.1:8002".parse().unwrap());

        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        let sorted = topology.sorted_nodes();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].0, NodeName::from("node-a"));
        assert_eq!(sorted[1].0, NodeName::from("node-b"));
        assert_eq!(sorted[2].0, NodeName::from("node-c"));
    }

    #[test]
    fn test_cluster_topology_bucket_assignment() {
        let mut nodes = IndexMap::new();
        nodes.insert(NodeName::from("node-a"), "127.0.0.1:8001".parse().unwrap());
        nodes.insert(NodeName::from("node-b"), "127.0.0.1:8002".parse().unwrap());
        nodes.insert(NodeName::from("node-c"), "127.0.0.1:8003".parse().unwrap());

        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        // Test bucket assignment for nodes
        let bucket_a = topology.bucket_for_node(&NodeName::from("node-a"));
        let bucket_b = topology.bucket_for_node(&NodeName::from("node-b"));
        let bucket_c = topology.bucket_for_node(&NodeName::from("node-c"));

        assert!(bucket_a.is_some());
        assert!(bucket_b.is_some());
        assert!(bucket_c.is_some());

        // Buckets should be in valid range [0, 2]
        assert!(bucket_a.unwrap() < 3);
        assert!(bucket_b.unwrap() < 3);
        assert!(bucket_c.unwrap() < 3);
    }

    #[test]
    fn test_cluster_topology_key_routing() {
        let mut nodes = IndexMap::new();
        nodes.insert(NodeName::from("node-a"), "127.0.0.1:8001".parse().unwrap());
        nodes.insert(NodeName::from("node-b"), "127.0.0.1:8002".parse().unwrap());

        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        // Test key routing
        let bucket = topology.bucket_for_key("user:123");
        assert!(bucket.is_some());
        assert!(bucket.unwrap() < 2);

        let node = topology.node_for_key("user:123");
        assert!(node.is_some());
        let node_name = node.as_ref().unwrap();
        assert!(*node_name == NodeName::from("node-a") || *node_name == NodeName::from("node-b"));

        let addr = topology.address_for_key("user:123");
        assert!(addr.is_some());
    }

    #[test]
    fn test_cluster_topology_add_remove() {
        let mut nodes = IndexMap::new();
        nodes.insert(NodeName::from("node-a"), "127.0.0.1:8001".parse().unwrap());

        let mut topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        assert_eq!(topology.node_count(), 1);
        assert!(topology.is_single_node());

        // Add node
        topology.add_node(NodeName::from("node-b"), "127.0.0.1:8002".parse().unwrap());
        assert_eq!(topology.node_count(), 2);
        assert!(topology.is_cluster());
        assert!(topology.contains_node(&NodeName::from("node-b")));

        // Remove node
        let removed_addr = topology.remove_node(&NodeName::from("node-b"));
        assert_eq!(removed_addr, Some("127.0.0.1:8002".parse().unwrap()));
        assert_eq!(topology.node_count(), 1);
        assert!(!topology.contains_node(&NodeName::from("node-b")));
    }

    #[test]
    fn test_cluster_topology_from_settings() {
        let mut topology = HashMap::new();
        topology.insert("node-a".to_string(), "127.0.0.1:8001".to_string());
        topology.insert("node-b".to_string(), "127.0.0.1:8002".to_string());

        let settings = Settings {
            config_file: None,
            server_name: "node-a".to_string(),
            client_listen_address: "127.0.0.1".to_string(),
            client_listen_port: 8411,
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: RunMode::Hashring,
            gossip_interval_ms: 100,
            gossip_fanout: 3,
            topology,
            hash_replication_factor: 1,
        };

        let cluster_topology = settings.cluster_topology();

        assert_eq!(cluster_topology.this_node, NodeName::from("node-a"));
        assert_eq!(cluster_topology.node_count(), 2);
        assert!(cluster_topology.contains_node(&NodeName::from("node-a")));
        assert!(cluster_topology.contains_node(&NodeName::from("node-b")));
        assert_eq!(
            cluster_topology.get_node_address(&NodeName::from("node-a")),
            Some("127.0.0.1:8001".parse().unwrap())
        );
    }

    #[test]
    fn test_cluster_topology_empty() {
        let nodes = IndexMap::new();
        let topology = ClusterTopology::new(NodeName::from("node-a"), nodes);

        assert_eq!(topology.node_count(), 0);
        assert!(!topology.is_cluster());
        assert!(topology.bucket_for_key("test").is_none());
        assert!(topology.node_for_key("test").is_none());
    }
}

