use rand::Rng;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::settings;

/// Unique identifier for cluster nodes
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash,
)]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn value(&self) -> u32 {
        self.0
    }

    pub fn generate_random() -> Self {
        let mut rng = rand::rng();
        NodeId(rng.random())
    }
}

impl From<u32> for NodeId {
    fn from(id: u32) -> Self {
        NodeId::new(id)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Generate node ID from hostname and port
pub fn generate_node_id(hostname: &str, port: u16) -> NodeId {
    let mut hasher = DefaultHasher::new();
    hostname.hash(&mut hasher);
    port.hash(&mut hasher);
    NodeId::new(hasher.finish() as u32)
}

/// Generate node ID from URL
pub fn generate_node_id_from_url(url: &Url) -> NodeId {
    let hostname = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(settings::STANDARD_PORT_HTTP);
    generate_node_id(hostname, port)
}

/// Generate node ID from socket address
pub fn generate_node_id_from_socket_addr(socket_addr: &SocketAddr) -> NodeId {
    let hostname = socket_addr.ip().to_string();
    let port = socket_addr.port();
    generate_node_id(&hostname, port)
}

pub fn validate_node_id(node_id: NodeId) -> Result<NodeId, String> {
    if node_id.value() == 0 {
        return Err("Node ID cannot be zero (reserved value)".to_string());
    }
    Ok(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_node_id_deterministic() {
        let id1 = generate_node_id("localhost", 8080);
        let id2 = generate_node_id("localhost", 8080);
        assert_eq!(id1, id2, "Same hostname+port should generate same node ID");
    }

    #[test]
    fn test_different_hostnames_different_ids() {
        let id1 = generate_node_id("node1", 8080);
        let id2 = generate_node_id("node2", 8080);
        assert_ne!(
            id1, id2,
            "Different hostnames should generate different node IDs"
        );
    }

    #[test]
    fn test_different_ports_different_ids() {
        let id1 = generate_node_id("localhost", 8080);
        let id2 = generate_node_id("localhost", 8081);
        assert_ne!(
            id1, id2,
            "Different ports should generate different node IDs"
        );
    }

    #[test]
    fn test_validate_node_id() {
        assert!(
            validate_node_id(NodeId::new(0)).is_err(),
            "Zero should be invalid"
        );
        assert!(
            validate_node_id(NodeId::new(1)).is_ok(),
            "Non-zero should be valid"
        );
        assert!(
            validate_node_id(NodeId::new(u32::MAX)).is_ok(),
            "Max u32 should be valid"
        );
    }

    #[test]
    fn test_node_id_range() {
        // Test that we generate IDs across the full u32 range
        let mut ids = std::collections::HashSet::new();

        for i in 0..1000 {
            let hostname = format!("node{}", i);
            let id = generate_node_id(&hostname, 8080);
            ids.insert(id);
        }

        // Should have good distribution (close to 1000 unique IDs)
        assert!(ids.len() > 990, "Should have good ID distribution");
    }

    #[test]
    fn test_collision_resistance() {
        let mut ids = std::collections::HashSet::new();
        let mut collisions = 0;

        // Test 10,000 different hostname+port combinations
        for i in 0..100 {
            for port in 8000..8100 {
                let hostname = format!("host{}", i);
                let id = generate_node_id(&hostname, port);

                if !ids.insert(id) {
                    collisions += 1;
                }
            }
        }

        // Should have very few collisions for reasonable cluster sizes
        assert!(
            collisions < 10,
            "Should have minimal collisions for typical cluster sizes"
        );
    }
}
