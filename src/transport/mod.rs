//! Transports: UDP and TCP
//!
//! Provides transport abstractions for distributed communication.
//! TCP is used for request-response patterns (for consistent hashing) and
//! UDP is used for fire-and-forget patterns (for gossip protocols).
//!
//! The transport module provides:
//! - Common traits (`Sender`, `RequestSender`, `Receiver`, `RequestReceiver`)
//! - UDP implementation for fire-and-forget messaging
//! - TCP implementation for request-response patterns
//! - Socket pools for managing connections efficiently
//! - Statistics and monitoring for all transports

use serde::{Deserialize, Serialize};

pub mod stats;
pub mod traits;
pub mod socket_pool_tcp;
pub mod socket_pool_udp;
pub mod tcp_connection;
pub mod tcp_receiver;
pub mod udp_connection;
pub mod udp_receiver;

// Re-export traits for easy access
pub use traits::{Sender, RequestSender, Receiver, RequestReceiver};

// Re-export statistics
pub use stats::{FrozenReceiverStats, FrozenSocketPoolStats, ReceiverStats, SocketPoolStats};

// Re-export transport implementations
pub use tcp_connection::TcpTransport;
pub use udp_connection::UdpTransport;

// Re-export socket pools
pub use socket_pool_tcp::TcpSocketPool;
pub use socket_pool_udp::UdpSocketPool;

// Re-export receivers
pub use tcp_receiver::TcpReceiver;
pub use udp_receiver::UdpReceiver;

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

    #[tokio::test]
    async fn test_transport_creation() {
        let transport_config = get_transport_config();
        let transport = UdpTransport::new(&transport_config)
            .await
            .unwrap();

        assert_eq!(transport.get_peers().await.len(), 2);
    }
}
