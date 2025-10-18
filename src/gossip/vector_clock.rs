use bincode::{Decode, Encode};
use std::collections::HashMap;

/// VectorClock tracks causality between distributed events.
///
/// Each node maintains a logical timestamp that increments when it makes updates.
/// Vector clocks allow us to determine if one event happened before another,
/// preventing stale updates from overwriting newer ones in the gossip protocol.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq)]
pub struct VectorClock {
    clocks: HashMap<u32, u64>, // node_id -> logical_timestamp
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Increment the logical timestamp for a given node and return the new value
    pub fn increment(&mut self, node_id: u32) -> u64 {
        let counter = self.clocks.entry(node_id).or_insert(0);
        *counter += 1;
        *counter
    }

    /// Update this vector clock by taking the maximum of each timestamp
    /// This is used when receiving updates from other nodes
    pub fn update(&mut self, other: &VectorClock) {
        for (&node_id, &timestamp) in &other.clocks {
            let entry = self.clocks.entry(node_id).or_insert(0);
            *entry = (*entry).max(timestamp);
        }
    }

    /// Check if this vector clock represents a newer state than another
    /// Returns true if self dominates other (self happened after other)
    pub fn is_newer_than(&self, other: &VectorClock) -> bool {
        // Returns true if self dominates other (self is newer)
        let mut self_newer = false;
        let mut other_newer = false;

        // Check all entries in both clocks
        let all_nodes: std::collections::HashSet<u32> = self
            .clocks
            .keys()
            .chain(other.clocks.keys())
            .cloned()
            .collect();

        for node_id in all_nodes {
            let self_time = self.clocks.get(&node_id).unwrap_or(&0);
            let other_time = other.clocks.get(&node_id).unwrap_or(&0);

            if self_time > other_time {
                self_newer = true;
            } else if self_time < other_time {
                other_newer = true;
            }
        }

        // Self is newer if it has at least one higher timestamp and no lower timestamps
        self_newer && !other_newer
    }

    /// Check if two vector clocks are concurrent (neither dominates the other)
    pub fn is_concurrent_with(&self, other: &VectorClock) -> bool {
        !self.is_newer_than(other) && !other.is_newer_than(self) && self != other
    }

    /// Get the timestamp for a specific node
    pub fn get_timestamp(&self, node_id: u32) -> u64 {
        self.clocks.get(&node_id).copied().unwrap_or(0)
    }

    /// Get all node IDs that have timestamps in this vector clock
    pub fn node_ids(&self) -> impl Iterator<Item = &u32> {
        self.clocks.keys()
    }

    /// Check if the vector clock is empty (no timestamps recorded)
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    /// Get the total number of nodes with timestamps in this vector clock
    pub fn len(&self) -> usize {
        self.clocks.len()
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_vector_clock_is_empty() {
        let clock = VectorClock::new();
        assert!(clock.is_empty());
        assert_eq!(clock.len(), 0);
    }

    #[test]
    fn test_increment_creates_timestamp() {
        let mut clock = VectorClock::new();

        let timestamp = clock.increment(1);
        assert_eq!(timestamp, 1);
        assert_eq!(clock.get_timestamp(1), 1);
        assert_eq!(clock.len(), 1);
    }

    #[test]
    fn test_multiple_increments() {
        let mut clock = VectorClock::new();

        assert_eq!(clock.increment(1), 1);
        assert_eq!(clock.increment(1), 2);
        assert_eq!(clock.increment(2), 1);

        assert_eq!(clock.get_timestamp(1), 2);
        assert_eq!(clock.get_timestamp(2), 1);
        assert_eq!(clock.len(), 2);
    }

    #[test]
    fn test_update_takes_maximum() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();

        clock1.increment(1); // clock1[1] = 1
        clock1.increment(2); // clock1[2] = 1

        clock2.increment(1); // clock2[1] = 1
        clock2.increment(1); // clock2[1] = 2
        clock2.increment(3); // clock2[3] = 1

        clock1.update(&clock2);

        assert_eq!(clock1.get_timestamp(1), 2); // max(1, 2)
        assert_eq!(clock1.get_timestamp(2), 1); // max(1, 0)
        assert_eq!(clock1.get_timestamp(3), 1); // max(0, 1)
    }

    #[test]
    fn test_is_newer_than() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();

        // clock1 = [1:1], clock2 = [1:0] -> clock1 newer
        clock1.increment(1);
        assert!(clock1.is_newer_than(&clock2));
        assert!(!clock2.is_newer_than(&clock1));

        // clock1 = [1:1], clock2 = [1:1] -> equal
        clock2.increment(1);
        assert!(!clock1.is_newer_than(&clock2));
        assert!(!clock2.is_newer_than(&clock1));

        // clock1 = [1:2], clock2 = [1:1] -> clock1 newer
        clock1.increment(1);
        assert!(clock1.is_newer_than(&clock2));
        assert!(!clock2.is_newer_than(&clock1));
    }

    #[test]
    fn test_concurrent_clocks() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();

        // clock1 = [1:1, 2:0], clock2 = [1:0, 2:1] -> concurrent
        clock1.increment(1);
        clock2.increment(2);

        assert!(!clock1.is_newer_than(&clock2));
        assert!(!clock2.is_newer_than(&clock1));
        assert!(clock1.is_concurrent_with(&clock2));
        assert!(clock2.is_concurrent_with(&clock1));
    }

    #[test]
    fn test_complex_causality() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();
        let mut clock3 = VectorClock::new();

        // Simulate a sequence of events across three nodes
        clock1.increment(1); // Node 1 does something: [1:1]

        clock2.update(&clock1); // Node 2 learns about node 1's event
        clock2.increment(2); // Node 2 does something: [1:1, 2:1]

        clock3.update(&clock2); // Node 3 learns about both events
        clock3.increment(3); // Node 3 does something: [1:1, 2:1, 3:1]

        // clock3 should be newer than both clock1 and clock2
        assert!(clock3.is_newer_than(&clock1));
        assert!(clock3.is_newer_than(&clock2));

        // clock2 should be newer than clock1
        assert!(clock2.is_newer_than(&clock1));

        // None should be concurrent with each other
        assert!(!clock1.is_concurrent_with(&clock2));
        assert!(!clock2.is_concurrent_with(&clock3));
        assert!(!clock1.is_concurrent_with(&clock3));
    }

    #[test]
    fn test_get_timestamp() {
        let mut clock = VectorClock::new();
        clock.increment(1);
        clock.increment(2);
        clock.increment(1);

        // Test that get_timestamp returns correct values
        assert_eq!(clock.get_timestamp(1), 2);
        assert_eq!(clock.get_timestamp(2), 1);
        assert_eq!(clock.get_timestamp(3), 0); // Non-existent node
    }
}
