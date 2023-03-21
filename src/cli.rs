/// CLI and configuration for this application
///
use serde::{Deserialize, Serialize};

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RateLimitSettings {
    pub rate_limit_max_calls_allowed: u32,
    pub rate_limit_interval_seconds: u32,
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
        let rate_seconds = self
            .rate_limit_max_calls_allowed
            .checked_div(self.rate_limit_interval_seconds)
            .unwrap_or(1);
        rate_seconds as f64
    }
    pub fn token_rate_milliseconds(&self) -> f64 {
        self.token_rate_seconds() / 1000f64
    }
}

#[derive(Clone, Debug, clap::Parser)]
pub struct Cli {
    // Server listen address
    #[clap(
        long,
        default_value = "0.0.0.0",
        env("LISTEN_ADDRESS"),
        help = "IP Address to listen on"
    )]
    pub listen_address: String,

    // Server listen port
    #[clap(
        long,
        default_value = "8000",
        env("LISTEN_PORT"),
        help = "Port to bind Colibri server to"
    )]
    pub listen_port: u16,

    // Rate limit settings: max calls (over interval)
    #[clap(
        long,
        default_value = "1000",
        env("RATE_LIMIT_MAX_CALLS_ALLOWED"),
        help = "Max calls allowed per interval"
    )]
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    #[clap(
        long,
        default_value = "60",
        env("RATE_LIMIT_INTERVAL_SECONDS"),
        help = "Interval in seconds to check limit"
    )]
    pub rate_limit_interval_seconds: u32,

    // Cluster configuration information: topology
    #[clap(
        long,
        default_value = "",
        env("TOPOLOGY"),
        help = "In multi-node mode, pass other node hostnames"
    )]
    pub topology: Vec<String>,

    // Cluster configuration information: this node-id
    #[clap(
        long,
        default_value = "",
        env("HOSTNAME"),
        help = "An identifier for this node"
    )]
    pub hostname: String,
}

impl Cli {
    pub fn rate_limit_settings(&self) -> RateLimitSettings {
        RateLimitSettings {
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
        }
    }
}
