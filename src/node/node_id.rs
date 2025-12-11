use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Unique identifier for cluster nodes
#[derive(
    Clone, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash,
)]
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