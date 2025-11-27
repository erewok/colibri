#!/usr/bin/env cargo
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tracing::{error, info, warn};

use colibri::node::hashring::consistent_hashing;

/// Information about a topology change operation
#[derive(Debug)]
struct TopologyChangeInfo {
    current_nodes: HashSet<SocketAddr>,
    new_nodes: HashSet<SocketAddr>,
}

#[derive(Parser)]
#[command(name = "colibri-admin")]
#[command(about = "Colibri cluster administration tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a new cluster topology
    ValidateTopology {
        /// Current topology nodes (e.g., "127.0.0.1:8001,127.0.0.1:8002")
        #[arg(long)]
        current: String,
        /// New topology nodes (e.g., "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003")
        #[arg(long)]
        new: String,
    },
    /// Perform cluster resize operation
    Resize {
        /// Current topology nodes
        #[arg(long)]
        current: String,
        /// New topology nodes
        #[arg(long)]
        new: String,
        /// Dry run - validate only, don't execute
        #[arg(long)]
        dry_run: bool,
    },
    /// Check health of cluster nodes
    Health {
        /// Nodes to check (e.g., "127.0.0.1:8001,127.0.0.1:8002")
        #[arg(long)]
        nodes: String,
    },
    /// Export data from all nodes in a topology
    ExportData {
        /// Nodes to export from
        #[arg(long)]
        nodes: String,
        /// Output directory for exported data
        #[arg(long)]
        output_dir: String,
    },
    /// Import data to nodes in a topology
    ImportData {
        /// Nodes to import to
        #[arg(long)]
        nodes: String,
        /// Input directory containing exported data
        #[arg(long)]
        input_dir: String,
    },
    /// Commit a topology change
    CommitChange {
        /// Nodes in the new topology
        #[arg(long)]
        nodes: String,
        /// Change ID from the prepare phase
        #[arg(long)]
        change_id: String,
    },
    /// Abort a topology change
    AbortChange {
        /// Nodes in the current topology
        #[arg(long)]
        nodes: String,
        /// Change ID to abort
        #[arg(long)]
        change_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ValidateTopology { current, new } => {
            let _topology_info = validate_topology(&current, &new).await?;
        }
        Commands::Resize {
            current,
            new,
            dry_run,
        } => {
            resize_cluster(&current, &new, dry_run).await?;
        }
        Commands::Health { nodes } => {
            check_cluster_health(&nodes).await?;
        }
        Commands::ExportData { nodes, output_dir } => {
            export_cluster_data(&nodes, &output_dir).await?;
        }
        Commands::ImportData { nodes, input_dir } => {
            import_cluster_data(&nodes, &input_dir).await?;
        }
        Commands::CommitChange { nodes, change_id } => {
            commit_topology_change(&nodes, &change_id).await?;
        }
        Commands::AbortChange { nodes, change_id } => {
            abort_topology_change(&nodes, &change_id).await?;
        }
    }

    Ok(())
}

async fn validate_topology(
    current: &str,
    new: &str,
) -> Result<TopologyChangeInfo, Box<dyn std::error::Error>> {
    let current_nodes: HashSet<SocketAddr> = parse_topology(current)?;
    let new_nodes: HashSet<SocketAddr> = parse_topology(new)?;
    let current_count = current_nodes.len();
    let new_count = new_nodes.len();

    info!("Validating topology change...");
    info!("   Current: {} nodes", current_count);
    info!("   New: {} nodes", new_count);
    // Basic validation rules
    if new_nodes.is_empty() {
        return Err("New topology cannot be empty".into());
    }

    let added_nodes: Vec<_> = new_nodes.difference(&current_nodes).collect();
    let removed_nodes: Vec<_> = current_nodes.difference(&new_nodes).collect();

    if !added_nodes.is_empty() {
        info!(nodes_to_add = ?added_nodes, "Nodes to be added");
    }
    if !removed_nodes.is_empty() {
        info!(nodes_to_remove = ?removed_nodes, "Nodes to be removed");
    }

    // Validate we can reach existing nodes
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let mut unhealthy_nodes = Vec::new();

    for node in &current_nodes {
        let health_url = format!("http://{}/cluster/health", node);
        match client.get(&health_url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<Value>().await {
                    Ok(_) => {
                        info!(node = %node, "Node is healthy");
                    }
                    Err(_) => {
                        unhealthy_nodes.push(node);
                        warn!(node = %node, "Node health check failed (invalid response)");
                    }
                }
            }
            Ok(response) => {
                unhealthy_nodes.push(node);
                error!(node = %node, status = %response.status(), "Node returned error status");
            }
            Err(e) => {
                unhealthy_nodes.push(node);
                error!(node = %node, error = %e, "Node is unreachable");
            }
        }
    }

    if !unhealthy_nodes.is_empty() {
        return Err(format!(
            "Cannot proceed with topology change: {} nodes are unhealthy: {:?}",
            unhealthy_nodes.len(),
            unhealthy_nodes
        )
        .into());
    }

    info!("Topology validation complete");

    Ok(TopologyChangeInfo {
        current_nodes,
        new_nodes,
    })
}

async fn export_cluster_data(
    nodes: &str,
    output_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let node_count = node_addrs.len();

    info!(node_count = node_count, "Starting data export from nodes");

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    for node in &node_addrs {
        // For hashring nodes, we need to determine which bucket they own
        let bucketnum =
            consistent_hashing::jump_consistent_hash(&node.to_string(), node_count as u32);
        let export_url = format!("http://{}/cluster/bucket/{}/export", node, bucketnum);

        match client
            .get(&export_url)
            .header("Content-Type", "application/json")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let export_data = response.text().await?;
                let output_file = format!(
                    "{}/export_{}.json",
                    output_dir,
                    node.to_string().replace(':', "_")
                );
                std::fs::write(&output_file, export_data)?;
                info!(node = %node, output_file = %output_file, "Successfully exported data from node");
            }
            Ok(response) => {
                warn!(node = %node, status = %response.status(), "Export from node failed");
            }
            Err(e) => {
                error!(node = %node, error = %e, "Failed to export from node");
            }
        }
    }

    info!("Data export complete");
    Ok(())
}

async fn import_cluster_data(
    nodes: &str,
    input_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let node_count = node_addrs.len();
    info!(node_count = node_count, "Starting data import to nodes");

    for node in &node_addrs {
        let input_file = format!(
            "{}/export_{}.json",
            input_dir,
            node.to_string().replace(':', "_")
        );
        let bucketnum =
            consistent_hashing::jump_consistent_hash(&node.to_string(), node_count as u32);

        if let Ok(import_data) = std::fs::read_to_string(&input_file) {
            let import_url = format!("http://{}/cluster/bucket/{}/import", node, bucketnum);

            match client
                .post(&import_url)
                .header("Content-Type", "application/json")
                .body(import_data)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    info!(node = %node, input_file = %input_file, "Successfully imported data to node");
                }
                Ok(response) => {
                    warn!(node = %node, status = %response.status(), "Import to node failed");
                }
                Err(e) => {
                    error!(node = %node, error = %e, "Failed to import to node");
                }
            }
        } else {
            warn!(node = %node, input_file = %input_file, "No export file found for node");
        }
    }

    info!("Data import complete");
    Ok(())
}

async fn commit_topology_change(
    nodes: &str,
    change_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    info!(change_id = %change_id, node_count = node_addrs.len(), "Committing topology change on nodes");

    let mut all_committed = true;
    for node in &node_addrs {
        let commit_url = format!("http://{}/cluster/commit-change", node);

        match client.post(&commit_url).send().await {
            Ok(response) if response.status().is_success() => {
                info!(node = %node, "Node committed change successfully");
            }
            Ok(response) => {
                error!(node = %node, status = %response.status(), "Commit to node failed");
                all_committed = false;
            }
            Err(e) => {
                error!(node = %node, error = %e, "Failed to commit on node");
                all_committed = false;
            }
        }
    }

    if all_committed {
        info!(change_id = %change_id, "Topology change committed successfully!");
    } else {
        warn!("Some nodes failed to commit. Check cluster status manually.");
    }

    Ok(())
}

async fn abort_topology_change(
    nodes: &str,
    change_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    info!(change_id = %change_id, node_count = node_addrs.len(), "Aborting topology change on nodes");

    let mut all_aborted = true;
    for node in &node_addrs {
        let abort_url = format!("http://{}/cluster/abort-change", node);

        match client.post(&abort_url).send().await {
            Ok(response) if response.status().is_success() => {
                info!(node = %node, "Node aborted change successfully");
            }
            Ok(response) => {
                error!(node = %node, status = %response.status(), "Abort on node failed");
                all_aborted = false;
            }
            Err(e) => {
                error!(node = %node, error = %e, "Failed to abort on node");
                all_aborted = false;
            }
        }
    }

    if all_aborted {
        info!(change_id = %change_id, "Topology change aborted successfully!");
    } else {
        warn!("Some nodes failed to abort. Manual intervention may be required.");
    }

    Ok(())
}

async fn resize_cluster(
    current: &str,
    new: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting cluster resize operation...");

    if dry_run {
        info!("DRY RUN MODE - No changes will be made");
    }

    // Step 1: Validate topology
    let topology_info = validate_topology(current, new).await?;

    if dry_run {
        info!("Dry run complete - topology is valid for resize");
        return Ok(());
    }

    // Step 2: Prepare all nodes for topology change
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    let change_id = format!(
        "change_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    );

    info!(change_id = %change_id, "Preparing nodes for topology change");

    let prepare_request = serde_json::json!({
        "new_topology": new.split(',').map(|s| s.trim()).collect::<Vec<_>>(),
        "change_id": change_id,
        "timeout_seconds": 300
    });

    let mut all_ready = true;
    for node in &topology_info.current_nodes {
        let prepare_url = format!("http://{}/cluster/prepare-change", node);
        match client
            .post(&prepare_url)
            .header("Content-Type", "application/json")
            .body(prepare_request.to_string())
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                match response.json::<serde_json::Value>().await {
                    Ok(prep_response) => {
                        let ready = prep_response
                            .get("ready")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        if ready {
                            info!(node = %node, "Node ready for change");
                        } else {
                            error!(node = %node, error = ?prep_response.get("error"), "Node not ready for change");
                            all_ready = false;
                        }
                    }
                    Err(e) => {
                        error!(node = %node, error = %e, "Failed to parse prepare response from node");
                        all_ready = false;
                    }
                }
            }
            Ok(response) => {
                error!(node = %node, status = %response.status(), "Prepare request to node failed");
                all_ready = false;
            }
            Err(e) => {
                error!(node = %node, error = %e, "Failed to send prepare request to node");
                all_ready = false;
            }
        }
    }

    if !all_ready {
        return Err("Not all nodes are ready for topology change. Aborting.".into());
    }

    // Step 3: Export data from all current nodes
    info!("Exporting data from current nodes");
    export_cluster_data(current, "./cluster_exports").await?;

    // Step 4: Coordinated topology change instructions
    info!(change_id = %change_id, "Topology change ready to proceed!");
    info!("IMPORTANT: Execute these steps carefully:");
    info!("   1. Stop all current nodes gracefully");
    info!("   2. Update configuration files with new topology:");
    for node in &topology_info.new_nodes {
        info!(node = %node, "      - Configure node with full topology");
    }
    info!("   3. Start all nodes with new configuration");
    info!(import_cmd = %format!("colibri-admin import-data --nodes \"{}\" --input-dir ./cluster_exports", new), "   4. Import data");
    info!(commit_cmd = %format!("colibri-admin commit-change --nodes \"{}\" --change-id {}", new, change_id), "   5. Commit changes");
    info!(health_cmd = %format!("colibri-admin health --nodes \"{}\"", new), "   6. Verify cluster health");
    info!("   If anything goes wrong, you can abort with:");
    info!(abort_cmd = %format!("colibri-admin abort-change --nodes \"{}\" --change-id {}", current, change_id), "   Abort command");

    Ok(())
}

async fn check_cluster_health(nodes: &str) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::new();

    info!("Checking cluster health...");

    for node in &node_addrs {
        let health_url = format!("http://{}/cluster/health", node);
        match client.get(&health_url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<Value>().await {
                    Ok(health) => {
                        info!(node = %node, status = ?health.get("status"), "Node health status");
                    }
                    Err(_) => warn!(node = %node, "Node healthy but invalid response format"),
                }
            }
            Ok(response) => {
                error!(node = %node, status = %response.status(), "Node returned HTTP error");
            }
            Err(e) => {
                error!(node = %node, error = %e, "Node unreachable");
            }
        }
    }

    Ok(())
}

fn parse_topology(topology: &str) -> Result<HashSet<SocketAddr>, Box<dyn std::error::Error>> {
    topology
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<SocketAddr>()
                .map_err(|e| format!("Invalid address '{}': {}", s, e).into())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topology() {
        let topology = "127.0.0.1:8001, 127.0.0.1:8002, 127.0.0.1:8003";
        let parsed = parse_topology(topology).unwrap();
        assert_eq!(parsed.len(), 3);
        assert!(parsed.contains(&"127.0.0.1:8001".parse().unwrap()));
    }

    #[test]
    fn test_invalid_topology() {
        let topology = "invalid-address";
        assert!(parse_topology(topology).is_err());
    }
}
