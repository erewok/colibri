//! Gossip Protocol for Distributed Rate Limiting
//! This module contains the implementation of a gossip protocol used for
//! distributed rate limiting across a cluster of nodes. It includes
//! versioned token buckets with vector clocks for conflict resolution,
//! transport mechanisms, and scheduling logic to efficiently propagate
//! rate limit state among nodes.
pub mod messages;
pub mod node_id;
pub mod scheduler;
pub mod transport;
pub mod vector_clock;
pub mod versioned_bucket;

pub use messages::{
    ClusterMembership, GossipMessage, GossipPacket, NodeCapabilities, NodeInfo, NodeStatus,
};
pub use node_id::{generate_node_id, generate_node_id_from_system, validate_node_id};
pub use scheduler::{GossipScheduler, GossipStats, QueueSizes, UrgentUpdate};
pub use transport::{DynamicMulticastTransport, TransportStats};
pub use vector_clock::VectorClock;
pub use versioned_bucket::VersionedTokenBucket;

// Additional modules will be added as we implement them:
// pub mod membership_manager;
