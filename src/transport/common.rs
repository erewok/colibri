use std::sync::atomic::{AtomicU64, AtomicUsize};

use serde::{Deserialize, Serialize};

/// Statistics for the receiver
#[derive(Debug, Default)]
pub struct ReceiverStats {
    pub messages_received: AtomicU64,
    pub receive_errors: AtomicU64,
}
impl ReceiverStats {
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            receive_errors: AtomicU64::new(0),
        }
    }
    pub fn freeze(&self) -> FrozenReceiverStats {
        FrozenReceiverStats::from_stats(self)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FrozenReceiverStats {
    pub messages_received: u64,
    pub receive_errors: u64,
}
impl FrozenReceiverStats {
    pub fn from_stats(stats: &ReceiverStats) -> Self {
        Self {
            messages_received: stats
                .messages_received
                .load(std::sync::atomic::Ordering::Relaxed),
            receive_errors: stats
                .receive_errors
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Statistics for the socket pool
#[derive(Debug, Default)]
pub struct SocketPoolStats {
    pub peer_count: AtomicUsize,
    pub total_connections: AtomicUsize,
    pub active_connections: AtomicUsize,
    // TCP allows request-response, but UDP is fire-and-forget
    pub responses_received: AtomicU64,
    pub messages_sent: AtomicU64,
    pub errors: SocketPoolErrors,
}

impl SocketPoolStats {
    pub fn new(peer_count: usize) -> Self {
        Self {
            peer_count: AtomicUsize::new(peer_count),
            total_connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            responses_received: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            errors: SocketPoolErrors {
                send_errors: AtomicU64::new(0),
                connection_errors: AtomicU64::new(0),
                timeout_errors: AtomicU64::new(0),
            },
        }
    }
    pub fn freeze(&self) -> FrozenSocketPoolStats {
        FrozenSocketPoolStats::from_stats(self)
    }
}

/// Statistics for the socket pool
#[derive(Debug, Default)]
pub struct SocketPoolErrors {
    pub send_errors: AtomicU64,
    pub connection_errors: AtomicU64,
    pub timeout_errors: AtomicU64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FrozenSocketPoolStats {
    pub peer_count: usize,
    pub total_connections: usize,
    pub active_connections: usize,
    pub responses_received: u64,
    pub messages_sent: u64,
    pub send_errors: u64,
    pub connection_errors: u64,
    pub timeout_errors: u64,
}

impl FrozenSocketPoolStats {
    pub fn from_stats(stats: &SocketPoolStats) -> Self {
        Self {
            peer_count: stats.peer_count.load(std::sync::atomic::Ordering::Relaxed),
            total_connections: stats
                .total_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            active_connections: stats
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            responses_received: stats
                .responses_received
                .load(std::sync::atomic::Ordering::Relaxed),
            messages_sent: stats
                .messages_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            send_errors: stats
                .errors
                .send_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            connection_errors: stats
                .errors
                .connection_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            timeout_errors: stats
                .errors
                .timeout_errors
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}
