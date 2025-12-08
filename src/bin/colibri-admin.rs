//! Colibri cluster administration tool
//! Uses internal UDP transport (not accessible to public clients) for admin commands
//!
//! # Examples
//! ```
//! # Add a new node to the cluster
//! colibri-admin add-node 192.168.1.100:8080 -c 192.168.1.1:8080 -c 192.168.1.2:8080
//! # Remove a failed node
//! colibri-admin remove-node 192.168.1.50:8080 -c 192.168.1.1:8080 -c 192.168.1.2:8080
//! # Change entire cluster topology (replaces hashring export/restart/import cycle)
//! colibri-admin change-topology -n 192.168.1.1:8080 -n 192.168.1.2:8080 -n 192.168.1.3:8080 -c 192.168.1.1:8080
//!
//! # Mark a node as temporarily unresponsive
//! colibri-admin mark-unresponsive 192.168.1.2:8080 -c 192.168.1.1:8080 -c 192.168.1.3:8080
//!
//! # Get status from a specific node
//! colibri-admin get-status 192.168.1.1:8080
//!
//! # Export data from a node (for migration)
//! colibri-admin export-buckets 192.168.1.1:8080
//! ```

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing::info;

use colibri::cluster::{AdminCommand, AdminCommandDispatcher, ClusterFactory};
use colibri::error::Result;
use colibri::node::NodeId;

#[derive(Parser)]
#[command(name = "colibri-admin")]
#[command(about = "Colibri cluster administration tool")]
struct Cli {
    /// Admin node ID (for creating transport)
    #[arg(short, long, default_value = "0")]
    node_id: u32,

    /// UDP address to use for admin commands
    #[arg(short, long, default_value = "127.0.0.1:0")]
    listen_addr: SocketAddr,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new node to cluster membership
    AddNode {
        /// Address of the node to add
        address: SocketAddr,
        /// Cluster nodes to send the command to
        #[arg(short, long)]
        cluster: Vec<SocketAddr>,
    },
    /// Remove a node from cluster membership
    RemoveNode {
        /// Address of the node to remove
        address: SocketAddr,
        /// Cluster nodes to send the command to
        #[arg(short, long)]
        cluster: Vec<SocketAddr>,
    },
    /// Mark a node as unresponsive
    MarkUnresponsive {
        /// Address of the node to mark unresponsive
        address: SocketAddr,
        /// Cluster nodes to send the command to
        #[arg(short, long)]
        cluster: Vec<SocketAddr>,
    },
    /// Mark a node as responsive
    MarkResponsive {
        /// Address of the node to mark responsive
        address: SocketAddr,
        /// Cluster nodes to send the command to
        #[arg(short, long)]
        cluster: Vec<SocketAddr>,
    },
    /// Change cluster topology (replaces export/restart/import cycle)
    ChangeTopology {
        /// New topology nodes
        #[arg(short, long)]
        new_topology: Vec<SocketAddr>,
        /// Current cluster nodes to send the command to
        #[arg(short, long)]
        cluster: Vec<SocketAddr>,
    },
    /// Get cluster status from a node
    GetStatus {
        /// Target node address
        target: SocketAddr,
    },
    /// Get cluster topology from a node
    GetTopology {
        /// Target node address
        target: SocketAddr,
    },
    /// Export buckets from a node
    ExportBuckets {
        /// Target node address
        target: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let node_id = NodeId::new(cli.node_id);
    // This is the key separation: admin tool uses internal transport
    match cli.command {
        Commands::AddNode { address, cluster } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, cluster).await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            admin_dispatcher.add_cluster_node(address).await?;
            info!("Successfully added node {} to cluster", address);
        }
        Commands::RemoveNode { address, cluster } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, cluster).await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            admin_dispatcher.remove_cluster_node(address).await?;
            info!("Successfully removed node {} from cluster", address);
        }
        Commands::MarkUnresponsive { address, cluster } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, cluster).await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            admin_dispatcher.mark_node_unresponsive(address).await?;
            info!("Marked node {} as unresponsive", address);
        }
        Commands::MarkResponsive { address, cluster } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, cluster).await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            admin_dispatcher.mark_node_responsive(address).await?;
            info!("Marked node {} as responsive", address);
        }
        Commands::ChangeTopology {
            new_topology,
            cluster,
        } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, cluster).await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);
            admin_dispatcher
                .change_cluster_topology(new_topology.clone())
                .await?;
            info!(
                "Successfully changed cluster topology to {} nodes",
                new_topology.len()
            );
            for node in new_topology {
                info!("  - {}", node);
            }
        }
        Commands::GetStatus { target } => {
            // Send GetClusterHealth command to target node
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, vec![target])
                    .await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            let command = AdminCommand::GetClusterHealth;
            let response = admin_dispatcher.send_admin_command(target, command).await?;

            info!("Status response from {}: {:?}", target, response);
        }
        Commands::GetTopology { target } => {
            // Send GetTopology command to target node
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, vec![target])
                    .await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            let command = AdminCommand::GetTopology;
            let response = admin_dispatcher.send_admin_command(target, command).await?;

            info!("Topology response from {}: {:?}", target, response);
        }
        Commands::ExportBuckets { target } => {
            let cluster_member =
                ClusterFactory::create_from_cli_params(node_id, cli.listen_addr, vec![target])
                    .await?;
            let admin_dispatcher = AdminCommandDispatcher::new(cluster_member);

            let command = AdminCommand::ExportBuckets;
            let response = admin_dispatcher.send_admin_command(target, command).await?;

            info!("Export response from {}: {:?}", target, response);
        }
    }

    Ok(())
}
