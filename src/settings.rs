//! Colibri application settings
use bincode::{Decode, Encode};
use std::collections::HashSet;
use std::net::SocketAddr;

use crate::node::node_id::generate_node_id;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const STANDARD_PORT_HTTP: u16 = 8410;
pub const DEFAULT_PORT_HTTP: &str = "8410";
pub const STANDARD_PORT_TCP: u16 = 8411;
pub const DEFAULT_PORT_TCP: &str = "8411";
pub const STANDARD_PORT_UDP: u16 = 8412;
pub const DEFAULT_PORT_UDP: &str = "8412";

#[derive(Clone, Debug, Decode, Encode)]
pub struct RateLimitSettings {
    pub node_id: u32,
    pub rate_limit_max_calls_allowed: u32,
    pub rate_limit_interval_seconds: u32,
}

#[derive(Clone, Debug)]
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
            node_id: 0,
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

#[derive(Clone, Debug)]
pub struct Settings {
    // Server listen address
    pub listen_address: String,

    // HTTP API listen port
    pub listen_port: u16,

    // UDP listen port for Gossip
    pub listen_port_udp: u16,

    // Rate limit settings: max calls (over interval)
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    pub rate_limit_interval_seconds: u32,

    // Mode of multi-node operation
    pub run_mode: RunMode,

    // Gossip Configuration
    pub gossip_interval_ms: u64, // Regular gossip interval (default: 25)
    pub gossip_fanout: usize,    // Number of peers per gossip round (default: 4)

    // Cluster configuration information: topology
    pub topology: HashSet<SocketAddr>,
    // Cluster Configuration
    pub failure_timeout_secs: u64, // Node failure detection timeout (default: 30)
}

impl Settings {
    pub fn node_id(&self) -> u32 {
        generate_node_id(&self.listen_address, self.listen_port)
    }
    pub fn rate_limit_settings(&self) -> RateLimitSettings {
        RateLimitSettings {
            node_id: self.node_id(),
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
        }
    }
}
