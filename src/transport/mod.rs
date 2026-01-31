//! TCP Transport
//!
//! Provides transport abstractions for distributed communication.
//! The transport module provides:
//! - Common traits (`Sender`, `RequestSender`, `Receiver`, `RequestReceiver`)
//! - TCP implementation
//! - Socket pools for managing connections efficiently
//! - Statistics and monitoring for all transports

use serde::{Deserialize, Serialize};

pub mod socket_pool_tcp;
pub mod stats;
pub mod tcp_connection;
pub mod tcp_receiver;
pub mod traits;

// Re-export traits for easy access
pub use traits::{Receiver, RequestReceiver, RequestSender, Sender};

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
