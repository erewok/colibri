//! Colibri application settings
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::node::NodeName;


pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const STANDARD_PORT_HTTP: u16 = 8411;
pub const DEFAULT_PORT_HTTP: &str = "8411";
pub const STANDARD_PORT_PEERS: u16 = 8412;
pub const DEFAULT_PORT_PEERS: &str = "8412";

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct RateLimitSettings {
    pub rate_limit_max_calls_allowed: u32,
    pub rate_limit_interval_seconds: u32,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct NamedRateLimitRule {
    pub name: String,
    pub settings: RateLimitSettings,
}

/// Alias for consistency with cluster protocol
pub type NamedRule = NamedRateLimitRule;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub default_settings: RateLimitSettings,
    pub named_rules: HashMap<String, RateLimitSettings>,
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub node_name: NodeName,
    pub peer_listen_address: String,
    pub peer_listen_port: u16,
    pub topology: HashMap<String, String>,
}

impl TransportConfig {
    pub fn peer_listen_url(&self) -> SocketAddr {
        format!("{}:{}", self.peer_listen_address, self.peer_listen_port).parse().expect("Invalid socket address")
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum RunMode {
    Gossip,
    Hashring,
    Single,
}

impl std::fmt::Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Gossip => write!(f, "gossip"),
            RunMode::Hashring => write!(f, "hashring"),
            RunMode::Single => write!(f, "single"),
        }
    }
}

impl std::str::FromStr for RunMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gossip" => Ok(RunMode::Gossip),
            "hashring" => Ok(RunMode::Hashring),
            "single" => Ok(RunMode::Single),
            _ => Err(format!("Invalid multi-mode: {}", s)),
        }
    }
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 60,
        }
    }
}

impl RateLimitSettings {
    pub fn token_rate_seconds(&self) -> f64 {
        let calls_allowed = f64::from(self.rate_limit_max_calls_allowed);
        let interval_seconds = f64::from(self.rate_limit_interval_seconds);
        calls_allowed / interval_seconds
    }

    pub fn token_rate_milliseconds(&self) -> f64 {
        self.token_rate_seconds() / 1000.0
    }
}

impl RateLimitConfig {
    pub fn new(default_settings: RateLimitSettings) -> Self {
        Self {
            default_settings,
            named_rules: HashMap::new(),
        }
    }

    pub fn get_default_settings(&self) -> &RateLimitSettings {
        &self.default_settings
    }

    pub fn get_named_rule_settings(&self, rule_name: &str) -> Option<&RateLimitSettings> {
        self.named_rules.get(rule_name)
    }

    pub fn add_named_rule(&mut self, rule: &NamedRateLimitRule) {
        self.named_rules
            .insert(rule.name.clone(), rule.settings.clone());
    }

    pub fn remove_named_rule(&mut self, rule_name: &str) -> Option<RateLimitSettings> {
        self.named_rules.remove(rule_name)
    }

    pub fn list_named_rules(&self) -> Vec<NamedRateLimitRule> {
        self.named_rules
            .iter()
            .map(|(name, settings)| NamedRateLimitRule {
                name: name.clone(),
                settings: settings.clone(),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub server_name: String,

    // Config file path
    pub config_file: Option<PathBuf>,

    // Server listen address
    pub client_listen_address: String,

    // HTTP API listen port
    pub client_listen_port: u16,

    // Listen URL for Peer communcation
    pub peer_listen_address: String,

    // Listen Port for Peer communication
    pub peer_listen_port: u16,

    // Rate limit settings: max calls (over interval)
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    pub rate_limit_interval_seconds: u32,

    // Mode of multi-node operation
    pub run_mode: RunMode,

    // Cluster configuration information: topology
    pub topology: HashMap<String, String>,

    // Gossip Configuration
    pub gossip_interval_ms: u64, // Regular gossip interval (default: 25)
    pub gossip_fanout: usize,    // Number of peers per gossip round (default: 4)

    // Hashring replication factor
    pub hash_replication_factor: usize,
}

impl Settings {
    pub fn node_name(&self) -> NodeName {
        self.server_name.clone().into()
    }

    pub fn transport_config(&self) -> TransportConfig {
        TransportConfig {
            node_name: self.node_name(),
            peer_listen_address: self.peer_listen_address.clone(),
            peer_listen_port: self.peer_listen_port,
            topology: self.topology.clone(),
        }
    }

    pub fn rate_limit_settings(&self) -> RateLimitSettings {
        RateLimitSettings {
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn sample() -> Settings {
        Settings {
            config_file: None,
            server_name: "test-node".to_string(),
            client_listen_address: "127.0.0.1".to_string(),
            client_listen_port: 8411,
            peer_listen_address: "127.0.0.1".to_string(),
            peer_listen_port: 8412,
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 60,
            run_mode: RunMode::Gossip,
            gossip_interval_ms: 1000, // Longer for testing
            gossip_fanout: 3,
            topology: HashMap::new(), // Empty topology for simple tests
            hash_replication_factor: 1,
        }
    }

    #[test]
    fn test_rate_limit_token_rate() {
        let settings = RateLimitSettings {
            rate_limit_max_calls_allowed: 120,
            rate_limit_interval_seconds: 60,
        };

        assert_eq!(settings.token_rate_seconds(), 2.0);
        assert_eq!(settings.token_rate_milliseconds(), 0.002);
    }
}
