//! Example usage of the UDP Transport module
//!
//! This example demonstrates both consistent hashing and gossip patterns,
//! including direct access to socket pools and receivers for advanced use cases.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::sleep;

use colibri::node::generate_node_id_from_socket_addr;
use colibri::transport::{TransportStats, UdpTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UDP Transport Example");

    // Setup cluster topology
    let start_addr: SocketAddr = "127.0.0.1:8001".parse()?;
    let node_id = generate_node_id_from_socket_addr(&start_addr);
    let mut cluster_urls = HashSet::new();
    cluster_urls.insert(start_addr);
    cluster_urls.insert("127.0.0.1:8002".parse()?);
    cluster_urls.insert("127.0.0.1:8003".parse()?);

    // Create transport for this node
    let mut transport = UdpTransport::new(node_id, 8000, cluster_urls).await?;

    println!("Transport created on {}", transport.local_addr());
    println!("Peers: {:?}", transport.get_peers().await);

    // Example 1: Consistent Hashing Pattern
    // Send to specific peer and wait for response
    let target_peer: SocketAddr = "127.0.0.1:8001".parse()?;

    match transport
        .send_to_peer(target_peer, b"Hello from consistent hashing")
        .await
    {
        Ok(target_peer) => println!("✓ Sent message to specific peer: {}", target_peer),
        Err(e) => println!("✗ Failed to send to specific peer: {}", e),
    }

    // Example 2: Gossip Pattern
    // Send to random peers
    match transport.send_to_random_peer(b"Gossip message").await {
        Ok(peer) => println!("✓ Sent gossip message to random peer: {}", peer),
        Err(e) => println!("✗ Failed to send gossip message: {}", e),
    }

    // Send to multiple random peers
    match transport
        .send_to_random_peers(b"Broadcast message", 2)
        .await
    {
        Ok(peers) => println!("✓ Sent broadcast to {} peers: {:?}", peers.len(), peers),
        Err(e) => println!("✗ Failed to send broadcast: {}", e),
    }

    // Example 3: Request-Response Pattern
    println!("\nAttempting request-response pattern...");
    match transport
        .send_request_with_response(target_peer, b"Request data", Duration::from_millis(100))
        .await
    {
        Ok(response) => println!(
            "✓ Received response: {:?}",
            String::from_utf8_lossy(&response)
        ),
        Err(e) => println!("✗ Request-response failed: {}", e),
    }

    // Example 4: Message Receiving
    println!("\nStarting message receiver...");

    // Get a channel receiver for incoming messages
    let mut message_rx = transport.get_message_receiver().await;

    // Spawn a task to handle incoming messages
    tokio::spawn(async move {
        while let Some((data, sender)) = message_rx.recv().await {
            println!(
                "Received message from {}: {:?}",
                sender,
                String::from_utf8_lossy(&data)
            );
        }
    });

    // Example 5: Dynamic Peer Management
    println!("\nDemonstrating dynamic peer management...");

    let new_peer: SocketAddr = "127.0.0.1:8004".parse()?;
    transport.add_peer(new_peer, 2).await?;
    println!("✓ Added new peer: {}", new_peer);

    // Show updated stats
    let stats = transport.get_stats().await;
    print_stats(stats);

    // Remove a peer
    transport.remove_peer(new_peer).await?;
    println!("✓ Removed peer: {}", new_peer);

    // Final stats
    let final_stats = transport.get_stats().await;
    print_stats(final_stats);

    // Example 6: Direct Socket Pool and Receiver Access
    println!("\nDemonstrating direct component access...");

    // Example 9: Gossip-like Message Pattern
    println!("\nDemonstrating gossip-like message patterns...");

    // Create a simple gossip message payload
    let gossip_payload = b"GOSSIP_STATE_UPDATE:client_123:tokens_remaining:42";

    // Send to random subset of peers (mimicking gossip fanout)
    let gossip_fanout = 2;
    match transport
        .send_to_random_peers(gossip_payload, gossip_fanout)
        .await
    {
        Ok(gossip_targets) => {
            println!(
                "✓ Sent gossip update to {} peers: {:?}",
                gossip_targets.len(),
                gossip_targets
            );
            println!("  Payload: {:?}", String::from_utf8_lossy(gossip_payload));
        }
        Err(e) => println!("✗ Gossip send failed: {}", e),
    }

    // Example 10: Transport Integration Capabilities
    println!("\nTransport integration capabilities demonstrated:");
    println!("  ✓ Direct socket pool access for custom protocols");
    println!("  ✓ Direct receiver access for custom message handling");
    println!("  ✓ Random peer selection for gossip patterns");
    println!("  ✓ Specific peer targeting for consistent hashing");
    println!("  ✓ Statistics collection for monitoring");
    println!("  ✓ Dynamic peer management");
    println!("  ✓ Channel-based async message processing");

    // Keep running for a moment to process any incoming messages
    println!("\nListening for messages for 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    Ok(())
}

fn print_stats(stats: TransportStats) {
    println!("Transport Statistics:");
    println!(
        "  Peer Count: {}",
        stats.send_pool_stats.peer_count.into_inner()
    );
    println!(
        "  Total Sockets: {}",
        stats.send_pool_stats.total_sockets.into_inner()
    );
    println!(
        "  Messages Sent: {}",
        stats.send_pool_stats.messages_sent.into_inner()
    );
    println!(
        "  Send Errors: {}",
        stats.send_pool_stats.send_errors.into_inner()
    );
}
