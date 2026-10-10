use std::net::SocketAddr;
use std::net::ToSocketAddrs;

use serde::{Deserialize, Serialize};

/// Unique identifier for cluster nodes
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct NodeName(String);

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash,
)]
pub struct NodeId(u32);

/// A node process: the node, plus an incarnation chosen when the process starts.
/// Each process writes its own CRDT slot, so a restarted node never reuses
/// its previous incarnation's slot.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash,
)]
pub struct ActorId {
    pub node: NodeId,
    pub incarnation: u64,
}

impl ActorId {
    /// A new incarnation of `node`: start time (seconds since 2020) in the high
    /// bits, so later processes compare greater, and 18 random bits.
    pub fn fresh(node: NodeId) -> Self {
        const EPOCH_2020_S: i64 = 1_577_836_800;
        let started = (crate::clock::wall_ms() / 1000 - EPOCH_2020_S).max(0) as u64;
        Self {
            node,
            incarnation: (started << 18) | (rand::random::<u64>() & 0x3_FFFF),
        }
    }
}

/// Incarnation 0 of a node; for tests and single-incarnation callers.
impl From<NodeId> for ActorId {
    fn from(node: NodeId) -> Self {
        Self {
            node,
            incarnation: 0,
        }
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.node, self.incarnation)
    }
}

impl NodeName {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn node_id(&self) -> NodeId {
        // FNV-1a hash — deterministic and seed-independent across all processes and restarts.
        // DefaultHasher is randomized per-process since Rust 1.36 and must not be used here.
        let mut hash: u64 = 14695981039346656037;
        for byte in self.0.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        NodeId(hash as u32)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NodeName {
    fn from(id: &str) -> Self {
        NodeName::new(id.to_string())
    }
}

impl From<String> for NodeName {
    fn from(id: String) -> Self {
        NodeName::new(id)
    }
}

impl std::fmt::Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Node addressing information
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeAddress {
    pub name: NodeName,
    pub local_address: SocketAddr,
    /// Network address for communication from other nodes
    pub remote_address: SocketAddr,
}

impl NodeAddress {
    pub fn new(
        name: impl Into<String>,
        local_address: impl ToSocketAddrs,
        remote_address: impl ToSocketAddrs,
    ) -> Self {
        Self {
            name: NodeName::new(name.into()),
            local_address: local_address
                .to_socket_addrs()
                .expect("Invalid local address")
                .next()
                .expect("No local address found"),
            remote_address: remote_address
                .to_socket_addrs()
                .expect("Invalid remote address")
                .next()
                .expect("No remote address found"),
        }
    }

    pub fn peer_address(&self) -> PeerAddress {
        PeerAddress {
            name: self.name.clone(),
            address: self.remote_address,
        }
    }
}

/// Peer addressing information
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerAddress {
    pub name: NodeName,
    pub address: SocketAddr,
}
