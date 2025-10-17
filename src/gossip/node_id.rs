use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Generate a deterministic u32 node ID from hostname and port
///
/// This creates a stable node ID that will be the same across restarts
/// as long as the hostname and port remain the same.
pub fn generate_node_id(hostname: &str, port: u16) -> u32 {
    let mut hasher = DefaultHasher::new();
    hostname.hash(&mut hasher);
    port.hash(&mut hasher);
    hasher.finish() as u32
}

/// Generate node ID from system hostname and given port
///
/// Uses the system hostname which provides better collision resistance
/// than IP addresses in containerized environments.
pub fn generate_node_id_from_system(port: u16) -> Result<u32, String> {
    let hostname = hostname::get()
        .map_err(|_| "Failed to get hostname".to_string())?
        .to_string_lossy()
        .to_string();

    validate_node_id(generate_node_id(&hostname, port))
}

/// Validate that a node ID is not zero (reserved for special cases)
pub fn validate_node_id(node_id: u32) -> Result<u32, String> {
    if node_id == 0 {
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
        assert!(validate_node_id(0).is_err(), "Zero should be invalid");
        assert!(validate_node_id(1).is_ok(), "Non-zero should be valid");
        assert!(
            validate_node_id(u32::MAX).is_ok(),
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
