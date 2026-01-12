//! Colibri cluster administration tool
//! Admin commands use  internal transport (not accessible to public clients)
//!
//! # Examples
//! ```
//! # Add a new node to the cluster
//! colibri-admin add-node 192.168.1.100:8080
//! # Remove a failed node
//! colibri-admin remove-node 192.168.1.50:8080
//! # Change entire cluster topology
//! colibri-admin change-topology -n 192.168.1.1:8080 -n 192.168.1.2:8080 -n 192.168.1.3:8080
//! # Get status from node mentioned in config file
//! colibri-admin get-status
//! # Export data from a node (for migration)
//! colibri-admin export-buckets -f exported_data.bin
//! # Import data to a node (for migration)
//! colibri-admin import-buckets -f exported_data.bin
//! ```

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{info, warn};

use colibri::cli::Cli as ColibriCli;
use colibri::error::Result;

#[derive(Parser)]
#[command(name = "colibri-admin")]
#[command(about = "Colibri cluster administration tool")]
struct Cli {
    /// Whether to write updated configuration back to file
    #[arg(short, long, default_value_t = false)]
    write_config: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new node to cluster membership
    AddNode {
        /// Address of the node to add
        address: SocketAddr,
    },
    /// Remove a node from cluster membership
    RemoveNode {
        /// Address of the node to remove
        address: SocketAddr,
    },
    /// Get cluster status from a node
    GetStatus,
    /// Get cluster topology from a node
    GetTopology,
    /// Import buckets to a node
    ImportBuckets {
        /// File to read imported data from
        file: PathBuf,
    },
    /// Export buckets from a node
    ExportBuckets {
        /// File to write exported data to
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let colibri_existing_conf: ColibriCli = ColibriCli::parse_with_file();
    let mut update_config = colibri_existing_conf.clone();

    let colibri_existing_settings = colibri_existing_conf.into_settings();

    let cluster_member = ClusterFactory::create_from_settings(
        colibri_existing_settings.node_id(),
        &colibri_existing_settings,
    )
    .await?;

    let dispatcher = AdminCommandDispatcher::new(cluster_member);

    // Admin uses internal transport
    match cli.command {
        // possibly writes back new config file
        Commands::AddNode { address } => {
            if !colibri_existing_settings
                .topology
                .contains(&address.to_string())
            {
                info!("Adding node {} to topology", address);
                // Dispatch command to add node
                dispatcher.add_cluster_node(address).await?;

                // Update local config
                update_config.topology.push(address.to_string());

                if cli.write_config {
                    if let Some(config_path) = &colibri_existing_settings.config_file {
                        update_config.to_config_file()?;
                        warn!(
                            "Wrote updated configuration with new topology to {:?}",
                            config_path
                        );
                    } else {
                        warn!("No configuration file path specified, not writing updated configuration to disk");
                    }
                }
            } else {
                println!("Node {} already in topology", address);
            }
        }
        Commands::RemoveNode { address } => {
            if colibri_existing_settings
                .topology
                .contains(&address.to_string())
            {
                info!("Removing node {} from topology", address);
                // Dispatch command to remove node
                dispatcher.remove_cluster_node(address).await?;

                // Update local config
                update_config.topology.retain(|x| x != &address.to_string());

                if cli.write_config {
                    if let Some(config_path) = &colibri_existing_settings.config_file {
                        update_config.to_config_file()?;
                        warn!(
                            "Wrote updated configuration with new topology to {:?}",
                            config_path
                        );
                    } else {
                        warn!("No configuration file path specified, not writing updated configuration to disk");
                    }
                }
            } else {
                println!("Node {} not found in topology", address);
            }
        }
        Commands::GetStatus => {
            let response = dispatcher.get_cluster_stats().await;
            info!("Cluster stats: {:?}", response);
        }
        Commands::GetTopology => {
            let response = dispatcher.get_cluster_topology().await;
            info!("Cluster topology: {:?}", response);
        }
        Commands::ImportBuckets { file } => todo!(),
        Commands::ExportBuckets { file } => todo!(),
    }
    Ok(())
}
