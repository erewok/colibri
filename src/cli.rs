//! CLI for this application
//!
use crate::settings;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, clap::Parser)]
pub struct Cli {
    // Server listen address
    #[clap(
        long,
        default_value = "0.0.0.0",
        env("COLIBRI_LISTEN_ADDRESS"),
        help = "IP Address to listen on"
    )]
    pub listen_address: String,

    // HTTP API listen port
    #[clap(
        long,
        default_value = settings::DEFAULT_PORT_HTTP,
        env("COLIBRI_HTTP_LISTEN_PORT"),
        help = "Port to bind Colibri HTTP API server to"
    )]
    pub listen_port: u16,

    // TCP listen port for Gossip
    #[clap(
        long,
        default_value = settings::DEFAULT_PORT_TCP,
        env("COLIBRI_TCP_LISTEN_PORT"),
        help = "Port to bind Colibri TCP server to"
    )]
    pub listen_port_tcp: u16,

    // UDP listen port for Gossip
    #[clap(
        long,
        default_value = settings::DEFAULT_PORT_UDP,
        env("COLIBRI_UDP_LISTEN_PORT"),
        help = "Port to bind Colibri UDP server to"
    )]
    pub listen_port_udp: u16,

    // Rate limit settings: max calls (over interval)
    #[clap(
        long,
        default_value = "1000",
        env("COLIBRI_RATE_LIMIT_MAX_CALLS_ALLOWED"),
        help = "Max calls allowed per interval"
    )]
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    #[clap(
        long,
        default_value = "60",
        env("COLIBRI_RATE_LIMIT_INTERVAL_SECONDS"),
        help = "Interval in seconds to check limit"
    )]
    pub rate_limit_interval_seconds: u32,

    // Mode of multi-node operation
    #[clap(
        long,
        default_value = "single",
        env("COLIBRI_RUN_MODE"),
        help = "run-mode: 'gossip', 'hashring', or 'single'"
    )]
    pub run_mode: settings::RunMode,

    // Cluster configuration information: topology
    #[clap(
        long,
        env("COLIBRI_TOPOLOGY"),
        help = "Addresses for TCP (hashring) or UDP (gossip) (e.g., somedomain:8000,1.2.3.4:7001). If empty, runs in single-node mode."
    )]
    pub topology: Vec<String>,
    // failure_timeout_secs: u64, // Node failure detection timeout (default: 30)
    #[clap(
        long,
        default_value = "30",
        env("COLIBRI_FAILURE_TIMEOUT_SECS"),
        help = "Node failure detection timeout in seconds"
    )]
    pub failure_timeout_secs: u64,

    // Gossip settings
    #[clap(
        long,
        default_value = "25000",
        env("COLIBRI_GOSSIP_INTERVAL_MS"),
        help = "Gossip interval in milliseconds"
    )]
    pub gossip_interval_ms: u64,

    #[clap(
        long,
        default_value = "1",
        env("COLIBRI_GOSSIP_FANOUT"),
        help = "Number of peers to gossip to per round"
    )]
    pub gossip_fanout: usize,
}

impl Cli {
    pub fn into_settings(self) -> settings::Settings {
        settings::Settings {
            listen_address: self.listen_address,
            listen_port_api: self.listen_port,
            listen_port_tcp: self.listen_port_tcp,
            listen_port_udp: self.listen_port_udp,
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
            run_mode: self.run_mode,
            topology: self.topology.into_iter().collect(),
            failure_timeout_secs: self.failure_timeout_secs,
            gossip_interval_ms: self.gossip_interval_ms,
            gossip_fanout: self.gossip_fanout,
        }
    }
}
