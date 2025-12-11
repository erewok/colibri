//! Transports: udp and tcp
//!
//! Provides a pool of UDP unicast sockets or TCP connections for distributed communication.
//! TCP is used for request-response patterns (for consistent hashing) and
//! UDP is used for fire-and-forget patterns (for gossip protocols).
use serde::{Deserialize, Serialize};

pub mod common;
pub mod socket_pool_tcp;
pub mod socket_pool_udp;
pub mod tcp_connection;
pub mod tcp_receiver;
pub mod udp_connection;
pub mod udp_receiver;

pub use common::{FrozenReceiverStats, FrozenSocketPoolStats, ReceiverStats, SocketPoolStats};
pub use socket_pool_tcp::TcpSocketPool;
pub use socket_pool_udp::UdpSocketPool;
pub use tcp_connection::TcpTransport;
pub use tcp_receiver::TcpReceiver;
pub use udp_connection::UdpTransport;
pub use udp_receiver::UdpReceiver;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SendReceiveStats {
    pub sent: FrozenSocketPoolStats,
    pub received: FrozenReceiverStats,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::node::NodeName;
    use crate::settings;

    fn get_transport_config() -> settings::TransportConfig {
        let mut cluster_urls = HashMap::new();
        cluster_urls.insert("a".to_string(), "127.0.0.1:8001".to_string());
        cluster_urls.insert("b".to_string(), "127.0.0.1:8002".to_string());
        cluster_urls.insert("c".to_string(), "127.0.0.1:8003".to_string());

        settings::TransportConfig {
            node_name: NodeName::from("a"),
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8001,
            topology: cluster_urls,
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
