//! Provides load balancing and fault tolerance through socket rotation.
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Statistics for the socket pool
#[derive(Debug, Default)]
pub struct SocketPoolStats {
    pub peer_count: AtomicUsize,
    pub total_sockets: AtomicUsize,
    pub messages_sent: AtomicU64,
    pub send_errors: AtomicU64,
}
