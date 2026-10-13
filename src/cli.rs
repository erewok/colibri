//! CLI for this application
//!
use std::error::Error;
use std::path::PathBuf;

use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches};
use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
    value::{Dict, Value},
};
use serde::{Deserialize, Serialize};

use crate::error::{ColibriError, Result};
use crate::settings;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Purely for writing configuration files back to disk
#[derive(Serialize, Deserialize)]
pub struct ConfigFileToml {
    pub default: Cli,
}

#[derive(Clone, Debug, clap::Parser, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cli {
    // Server name
    #[clap(
        short,
        long,
        env("COLIBRI_NAME"),
        help = "Name of this server (must be unique in cluster!)"
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    // Configuration file: flags and environment variables override its values
    #[clap(
        short = 'f',
        long,
        env("COLIBRI_CONFIG_FILE"),
        help = "Path to a toml configuration file with a [default] table"
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(with = "topology_entries")]
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
        default_value = "1400",
        env("COLIBRI_GOSSIP_MAX_PACKET_BYTES"),
        help = "Largest gossip datagram to send, in bytes; state is split to fit (default 1400)"
    )]
    pub gossip_max_packet_bytes: usize,

    #[clap(
        long,
        value_enum,
        default_value = "text",
        env("COLIBRI_LOG_FORMAT"),
        help = "Log format: 'text' or 'json' (default 'text')"
    )]
    pub log_format: LogFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

impl Cli {
    /// Precedence, highest first: flags, environment variables, the config
    /// file, clap defaults. Exits on `--help` or a malformed flag.
    pub fn parse_with_file() -> Result<Self> {
        Self::from_matches(&Cli::command().get_matches())
    }

    pub fn from_matches(matches: &ArgMatches) -> Result<Self> {
        let parsed =
            Cli::from_arg_matches(matches).map_err(|e| ColibriError::Config(e.to_string()))?;
        let Some(path) = parsed.config_file.clone() else {
            return Ok(parsed);
        };
        if !path.is_file() {
            return Err(ColibriError::Config(format!(
                "config file not found: {}",
                path.display()
            )));
        }
        let explicit = explicit_values(&parsed, matches)?;
        Figment::from(Serialized::defaults(&parsed))
            .merge(Toml::file_exact(&path).nested())
            .merge(Serialized::defaults(explicit))
            .extract()
            .map_err(|e| ColibriError::Config(format!("{}: {e}", path.display())))
    }

    pub fn into_settings(self) -> Result<settings::Settings> {
        let server_name = self.name.ok_or_else(|| {
            ColibriError::Config(
                "a node name is required: set --name, COLIBRI_NAME, or `name` in the config file"
                    .to_string(),
            )
        })?;
        let peer_listen_address = self
            .peer_listen_address
            .unwrap_or_else(|| self.client_listen_address.clone());
        Ok(settings::Settings {
            server_name,
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
            gossip_max_packet_bytes: self.gossip_max_packet_bytes,
        })
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

/// The values set by a flag or an environment variable, keyed by field name.
fn explicit_values(parsed: &Cli, matches: &ArgMatches) -> Result<Dict> {
    let config_err = |e: figment::Error| ColibriError::Config(e.to_string());
    let Value::Dict(_, mut values) = Value::serialize(parsed).map_err(config_err)? else {
        return Err(ColibriError::Config(
            "CLI did not serialize to a table".to_string(),
        ));
    };
    values.retain(|key, _| {
        matches!(
            matches.value_source(key),
            Some(ValueSource::CommandLine | ValueSource::EnvVariable)
        )
    });
    Ok(values)
}

/// Topology entries as `name=address` strings, the same form `-t` takes.
mod topology_entries {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(
        entries: &[(String, String)],
        se: S,
    ) -> Result<S::Ok, S::Error> {
        se.collect_seq(entries.iter().map(|(name, addr)| format!("{name}={addr}")))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Vec<(String, String)>, D::Error> {
        Vec::<String>::deserialize(de)?
            .iter()
            .map(|entry| super::parse_key_val(entry).map_err(D::Error::custom))
            .collect()
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> std::result::Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn load(args: &[&str]) -> Result<Cli> {
        let argv = std::iter::once("colibri").chain(args.iter().copied());
        Cli::from_matches(&Cli::command().try_get_matches_from(argv).unwrap())
    }

    fn config_file(body: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(body.as_bytes()).unwrap();
        file
    }

    const FILE: &str = r#"
[default]
name = "from-file"
client_listen_port = 9000
run_mode = "gossip"
topology = ["node-2=127.0.0.1:9002"]
gossip_fanout = 3
log_format = "json"
"#;

    #[test]
    fn file_values_override_clap_defaults() {
        let file = config_file(FILE);
        let cli = load(&["-f", file.path().to_str().unwrap()]).unwrap();
        assert_eq!(cli.log_format, LogFormat::Json);
        let settings = cli.into_settings().unwrap();
        assert_eq!(settings.server_name, "from-file");
        assert_eq!(settings.client_listen_port, 9000);
        assert_eq!(settings.run_mode, settings::RunMode::Gossip);
        assert_eq!(settings.gossip_fanout, 3);
        assert_eq!(
            settings.topology.get("node-2").map(String::as_str),
            Some("127.0.0.1:9002")
        );
        // Unset in the file, so the clap default stands.
        assert_eq!(settings.rate_limit_max_calls_allowed, 1000);
    }

    #[test]
    fn flags_override_file_values() {
        let file = config_file(FILE);
        let settings = load(&[
            "-f",
            file.path().to_str().unwrap(),
            "--name",
            "from-flag",
            "--client-listen-port",
            "9100",
            "-t",
            "node-3=127.0.0.1:9003",
        ])
        .unwrap()
        .into_settings()
        .unwrap();
        assert_eq!(settings.server_name, "from-flag");
        assert_eq!(settings.client_listen_port, 9100);
        assert_eq!(settings.topology.len(), 1);
        assert!(settings.topology.contains_key("node-3"));
        assert_eq!(settings.gossip_fanout, 3);
    }

    #[test]
    fn name_is_required_somewhere() {
        let file = config_file("[default]\nclient_listen_port = 9000\n");
        let cli = load(&["-f", file.path().to_str().unwrap()]).unwrap();
        assert!(matches!(cli.into_settings(), Err(ColibriError::Config(_))));
        assert!(load(&[]).unwrap().into_settings().is_err());
    }

    #[test]
    fn missing_or_malformed_file_is_an_error() {
        assert!(load(&["-f", "/nonexistent/colibri.toml"]).is_err());
        let file = config_file("[default]\nclient_listen_port = \"not a port\"\n");
        assert!(load(&["-f", file.path().to_str().unwrap()]).is_err());
        let file = config_file("[default]\nclient_listen_prot = 9000\n");
        assert!(load(&["-f", file.path().to_str().unwrap()]).is_err());
    }

    #[test]
    fn written_config_file_loads_back() {
        let file = config_file("");
        let path = file.path().to_str().unwrap();
        let written = load(&["--name", "n1", "-f", path, "-t", "n2=127.0.0.1:1"]).unwrap();
        written.to_config_file().unwrap();

        let loaded = load(&["-f", path]).unwrap().into_settings().unwrap();
        assert_eq!(loaded.server_name, "n1");
        assert!(loaded.topology.contains_key("n2"));
    }
}
