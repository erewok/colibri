#!/usr/bin/env cargo
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use std::net::SocketAddr;

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ValidateTopology { current, new } => {
            validate_topology(&current, &new).await?;
        }
        Commands::Resize { current, new, dry_run } => {
            resize_cluster(&current, &new, dry_run).await?;
        }
        Commands::Health { nodes } => {
            check_cluster_health(&nodes).await?;
        }
    }

    Ok(())
}

async fn validate_topology(current: &str, new: &str) -> Result<(), Box<dyn std::error::Error>> {
    let current_nodes: HashSet<SocketAddr> = parse_topology(current)?;
    let new_nodes: HashSet<SocketAddr> = parse_topology(new)?;

    println!("🔍 Validating topology change...");
    println!("   Current: {} nodes", current_nodes.len());
    println!("   New: {} nodes", new_nodes.len());

    // Basic validation rules
    if new_nodes.is_empty() {
        return Err("New topology cannot be empty".into());
    }

    if new_nodes.len() % 2 == 0 {
        println!("⚠️  Warning: Even number of nodes may cause split-brain issues");
    }

    let added_nodes: HashSet<_> = new_nodes.difference(&current_nodes).collect();
    let removed_nodes: HashSet<_> = current_nodes.difference(&new_nodes).collect();

    if !added_nodes.is_empty() {
        println!("➕ Nodes to add: {:?}", added_nodes);
    }
    if !removed_nodes.is_empty() {
        println!("➖ Nodes to remove: {:?}", removed_nodes);
    }

    // Validate we can reach existing nodes
    let client = Client::new();
    for node in &current_nodes {
        let health_url = format!("http://{}/cluster/health", node);
        match client.get(&health_url).send().await {
            Ok(response) if response.status().is_success() => {
                println!("✅ Node {} is healthy", node);
            }
            Ok(response) => {
                println!("⚠️  Node {} returned status {}", node, response.status());
            }
            Err(e) => {
                println!("❌ Node {} is unreachable: {}", node, e);
            }
        }
    }

    println!("✅ Topology validation complete");
    Ok(())
}

async fn resize_cluster(current: &str, new: &str, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting cluster resize operation...");

    if dry_run {
        println!("🧪 DRY RUN MODE - No changes will be made");
    }

    // Step 1: Validate topology
    validate_topology(current, new).await?;

    if dry_run {
        println!("✅ Dry run complete - topology is valid for resize");
        return Ok(());
    }

    // Step 2: Export data from all current nodes
    let current_nodes = parse_topology(current)?;
    let client = Client::new();

    println!("📤 Exporting data from current nodes...");
    for node in &current_nodes {
        let export_url = format!("http://{}/cluster/bucket/0/export", node);
        match client.get(&export_url).send().await {
            Ok(_) => println!("✅ Exported data from {}", node),
            Err(e) => println!("❌ Failed to export from {}: {}", node, e),
        }
    }

    // Step 3: TODO - Coordinate restart with new topology
    println!("⚠️  Manual step required:");
    println!("   1. Update configuration files with new topology");
    println!("   2. Restart all nodes with new configuration");
    println!("   3. Verify cluster health with: colibri-admin health --nodes \"{}\"", new);

    Ok(())
}

async fn check_cluster_health(nodes: &str) -> Result<(), Box<dyn std::error::Error>> {
    let node_addrs = parse_topology(nodes)?;
    let client = Client::new();

    println!("🏥 Checking cluster health...");

    for node in &node_addrs {
        let health_url = format!("http://{}/cluster/health", node);
        match client.get(&health_url).send().await {
            Ok(response) if response.status().is_success() => {
                match response.json::<Value>().await {
                    Ok(health) => {
                        println!("✅ Node {}: {}", node, health.get("status").unwrap_or(&Value::String("unknown".to_string())));
                    }
                    Err(_) => println!("⚠️  Node {}: healthy but invalid response format", node),
                }
            }
            Ok(response) => {
                println!("❌ Node {}: HTTP {}", node, response.status());
            }
            Err(e) => {
                println!("❌ Node {}: unreachable ({})", node, e);
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
        .map(|s| s.parse::<SocketAddr>().map_err(|e| format!("Invalid address '{}': {}", s, e).into()))
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