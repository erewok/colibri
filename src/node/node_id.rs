use std::hash::{DefaultHasher, Hash, Hasher};
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

impl NodeName {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn value(&self) -> &str {
        &self.0
    }

    pub fn node_id(&self) -> NodeId {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        NodeId(s.finish() as u32)
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
