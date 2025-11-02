//! Gossip Protocol for Distributed Rate Limiting
//! This module contains the implementation of a gossip protocol used for
//! distributed rate limiting across a cluster of nodes. It includes
//! versioned token buckets with vector clocks for conflict resolution,
//! transport mechanisms, and scheduling logic to efficiently propagate
//! rate limit state among nodes.
pub mod controller;
pub mod gossip_node;
pub mod messages;

pub use controller::GossipController;
pub use gossip_node::GossipNode;
pub use messages::{GossipCommand, GossipMessage, GossipPacket};

// Additional modules will be added as we implement them:
// pub mod membership_manager;
