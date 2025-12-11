//! CLI for this application
//!
use std::error::Error;
use std::path::PathBuf;

use clap::Parser;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::settings;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Purely for writing configuration files back to disk
#[derive(Serialize, Deserialize)]
pub struct ConfigFileToml {
    pub default: Cli,
}

#[derive(Clone, Debug, clap::Parser, Serialize, Deserialize)]
pub struct Cli {
    // Server name
    #[clap(
        short,
        long,
        env("COLIBRI_NAME"),
        help = "Name of this server (must be unique in cluster!)"
    )]
    pub name: String,

    // Configuration file (can be combined or overridden with other options)
    #[clap(
        short='f',
        long,
        default_value = "colibri.toml",
        env("COLIBRI_CONFIG_FILE"),
        help = "Path to toml configuration file"
    )]
    pub config_file: Option<PathBuf>,

    // Server listen address
    #[clap(
        long,
        default_value = "127.0.0.1",
        env("COLIBRI_CLIENT_LISTEN_ADDRESS"),
        help = "IP Address or URL to for client requests (HTTP Server) (default 127.0.0.1)"
    )]
    pub client_listen_address: String,

    // HTTP API listen port
    #[clap(
        long,
        default_value = settings::DEFAULT_PORT_HTTP,
        env("COLIBRI_CLIENT_LISTEN_PORT"),
        help = "Port to bind Colibri HTTP API server to (default 8411)"
    )]
    pub client_listen_port: u16,

    // Listen URL for Peer communcation
    #[clap(
        long,
        env("COLIBRI_PEER_LISTEN_ADDRESS"),
        help = "URL to bind Colibri Peer socket receiver server to (default to client_listen_address)"
    )]
    pub peer_listen_address: Option<String>,

    // Listen Port for Peer communication
    #[clap(
        long,
        default_value = settings::DEFAULT_PORT_PEERS,
        env("COLIBRI_PEER_LISTEN_PORT"),
        help = "Port to bind Colibri Peer socket receiver server to (default 8412)"
    )]
    pub peer_listen_port: u16,

    // Rate limit settings: max calls (over interval)
    #[clap(
        long,
        default_value = "1000",
        env("COLIBRI_RATE_LIMIT_MAX_CALLS_ALLOWED"),
        help = "Max calls allowed per interval (default 1000)"
    )]
    pub rate_limit_max_calls_allowed: u32,

    // Rate limit settings: interval in seconds to check if rate limit exceeded
    #[clap(
        long,
        default_value = "60",
        env("COLIBRI_RATE_LIMIT_INTERVAL_SECONDS"),
        help = "Interval in seconds to check limit (default 60 seconds)"
    )]
    pub rate_limit_interval_seconds: u32,

    // Mode of multi-node operation
    #[clap(
        long,
        default_value = "single",
        env("COLIBRI_RUN_MODE"),
        help = "run-mode: 'gossip', 'hashring', or 'single' (default 'single')"
    )]
    pub run_mode: settings::RunMode,

    /// Name -> address mapping for other nodes in the cluster
    #[clap(
        short = 't',
        long,
        env("COLIBRI_TOPOLOGY"),
        value_parser = parse_key_val::<String, String>)]
    topology: Vec<(String, String)>,

    // Gossip settings
    #[clap(
        long,
        default_value = "1000",
        env("COLIBRI_GOSSIP_INTERVAL_MS"),
        help = "Gossip interval in milliseconds (default 1000)"
    )]
    pub gossip_interval_ms: u64,

    #[clap(
        long,
        default_value = "1",
        env("COLIBRI_GOSSIP_FANOUT"),
        help = "Number of peers to gossip to per round (default 1"
    )]
    pub gossip_fanout: usize,

    #[clap(
        long,
        default_value = "1",
        env("COLIBRI_HASH_REPLICATION_FACTOR"),
        help = "Number of replicas for hashring mode (1, 2, or 3) (default 1)"
    )]
    pub hash_replication_factor: usize,
}

impl Cli {
    pub fn parse_with_file() -> Self {
        let parsed: Self = Cli::parse();
        match parsed.config_file {
            Some(ref path) => Figment::new()
                .merge(Toml::file(path))
                .merge(Serialized::defaults(Cli::parse()))
                .extract()
                .unwrap_or(parsed),
            None => parsed,
        }
    }

    pub fn into_settings(self) -> settings::Settings {
        let peer_listen_address = self
            .peer_listen_address
            .unwrap_or_else(|| self.client_listen_address.clone());
        settings::Settings {
            server_name: self.name,
            config_file: self.config_file,
            client_listen_address: self.client_listen_address,
            client_listen_port: self.client_listen_port,
            peer_listen_address,
            peer_listen_port: self.peer_listen_port,
            rate_limit_max_calls_allowed: self.rate_limit_max_calls_allowed,
            rate_limit_interval_seconds: self.rate_limit_interval_seconds,
            run_mode: self.run_mode,
            topology: self.topology.into_iter().collect(),
            gossip_interval_ms: self.gossip_interval_ms,
            gossip_fanout: self.gossip_fanout,
            hash_replication_factor: self.hash_replication_factor,
        }
    }

    pub fn to_config_file(&self) -> std::io::Result<()> {
        let config = ConfigFileToml {
            default: self.clone(),
        };
        let toml_string = toml::to_string_pretty(&config).unwrap();
        std::fs::write(
            self.config_file
                .as_ref()
                .unwrap_or(&PathBuf::from("colibri.toml")),
            toml_string,
        )
    }
}


/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
