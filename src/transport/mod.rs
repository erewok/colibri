//! TCP Transport
//!
//! Provides transport abstractions for distributed communication.
//! The transport module provides:
//! - Common traits (`Sender`, `RequestSender`, `Receiver`, `RequestReceiver`)
//! - TCP implementation
//! - Socket pools for managing connections efficiently
//! - Statistics and monitoring for all transports

use serde::{Deserialize, Serialize};

pub mod stats;
pub mod traits;
pub mod socket_pool_tcp;
pub mod tcp_connection;
pub mod tcp_receiver;

// Re-export traits for easy access
pub use traits::{Sender, RequestSender, Receiver, RequestReceiver};

// Re-export statistics
pub use stats::{FrozenReceiverStats, FrozenSocketPoolStats, ReceiverStats, SocketPoolStats};

// Re-export transport implementations
pub use tcp_connection::TcpTransport;

// Re-export socket pools
pub use socket_pool_tcp::TcpSocketPool;

// Re-export receivers
pub use tcp_receiver::TcpReceiver;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SendReceiveStats {
    pub sent: FrozenSocketPoolStats,
    pub received: FrozenReceiverStats,
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use super::*;
    use crate::node::NodeName;
    use crate::settings::TransportConfig;

    fn get_transport_config() -> TransportConfig {
        let mut topology = IndexMap::new();
        topology.insert(
            NodeName::from("node_a").node_id(),
            "127.0.0.1:8001".parse().unwrap(),
        );
        topology.insert(
            NodeName::from("node_b").node_id(),
            "127.0.0.1:8002".parse().unwrap(),
        );

        TransportConfig {
            node_name: NodeName::from("test_node"),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8000,
            topology,
        }
    }

}
