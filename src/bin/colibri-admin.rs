//! Colibri cluster administration tool
//!
//! Sends internal TCP messages to cluster nodes for administrative operations.
//! Not exposed via public HTTP API - uses direct TCP transport.
//!
//! # Examples
//! ```
//! # Get status from a node
//! colibri-admin get-status --target 192.168.1.100:8421
//!
//! # Get topology from a node
//! colibri-admin get-topology --target 192.168.1.100:8421
//!
//! # Export data from a node (for migration)
//! colibri-admin export-buckets --target 192.168.1.100:8421 -f exported_data.bin
//!
//! # Import data to a node (for migration)
//! colibri-admin import-buckets --target 192.168.1.100:8421 -f exported_data.bin
//! ```

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{error, info};

use colibri::cli::Cli as ColibriCli;
use colibri::error::{ColibriError, Result};
use colibri::node::messages::Message;
use colibri::transport::TcpTransport;

#[derive(Parser)]
#[command(name = "colibri-admin")]
#[command(about = "Colibri cluster administration tool")]
struct Cli {
    /// Target node address (peer TCP port, not client API port)
    /// If not specified, uses first node from config topology
    #[arg(short, long, global = true)]
    target: Option<SocketAddr>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get cluster status from a node
    GetStatus,

    /// Get cluster topology from a node
    GetTopology,

    /// Export buckets from a node (for migration)
    ExportBuckets {
        /// File to write exported data to
        #[arg(short, long)]
        file: PathBuf,
    },

    /// Import buckets to a node (for migration)
    ImportBuckets {
        /// File to read imported data from
        #[arg(short, long)]
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Load config to get topology
    let colibri_conf: ColibriCli = ColibriCli::parse_with_file();
    let settings = colibri_conf.into_settings();

    // Determine target node
    let target = if let Some(addr) = cli.target {
        addr
    } else {
        // Use first node from topology
        let cluster_topology = settings.cluster_topology();
        cluster_topology
            .all_nodes()
            .first()
            .map(|(_, addr)| *addr)
            .ok_or_else(|| {
                ColibriError::Config(
                    "No target specified and no nodes in topology".to_string()
                )
            })?
    };

    info!("Sending admin command to node at {}", target);

    // Create TCP transport for sending internal messages
    let transport_config = settings.transport_config();
    let transport = TcpTransport::new(&transport_config).await?;

    // Execute command
    match cli.command {
        Commands::GetStatus => {
            let message = Message::GetStatus;
            match transport.send_message_request_response(target, &message).await? {
                Message::StatusResponse(response) => {
                    println!("Status Response:");
                    println!("  Status: {:?}", response.status);
                    println!("  Node: {}", response.node_name);
                    println!("  Mode: {:?}", response.node_type);
                    println!("  Bucket Count: {:?}", response.bucket_count);
                    println!("  Last Topology Change: {:?}", response.last_topology_change);
                }
                other => {
                    error!("Unexpected response: {:?}", other);
                    return Err(ColibriError::Api(
                        "Unexpected response type for GetStatus".to_string()
                    ));
                }
            }
        }

        Commands::GetTopology => {
            let message = Message::GetTopology;
            match transport.send_message_request_response(target, &message).await? {
                Message::TopologyResponse(response) => {
                    println!("Topology Response:");
                    println!("  Status: {:?}", response.status.status);
                    println!("  Topology:");
                    for (name, addr) in &response.topology {
                        println!("    {} -> {:?}", name, addr);
                    }
                }
                other => {
                    error!("Unexpected response: {:?}", other);
                    return Err(ColibriError::Api(
                        "Unexpected response type for GetTopology".to_string()
                    ));
                }
            }
        }

        Commands::ExportBuckets { file } => {
            error!("ExportBuckets not yet implemented");
            println!("Export buckets functionality will write to: {:?}", file);
            return Err(ColibriError::Api(
                "ExportBuckets not yet implemented".to_string()
            ));
        }

        Commands::ImportBuckets { file } => {
            error!("ImportBuckets not yet implemented");
            println!("Import buckets functionality will read from: {:?}", file);
            return Err(ColibriError::Api(
                "ImportBuckets not yet implemented".to_string()
            ));
        }
    }

    Ok(())
}
