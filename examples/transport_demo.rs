//! Example usage of the UDP Transport module
//!
//! This example demonstrates both consistent hashing and gossip patterns.

use colibri::transport::{TransportStats, UdpTransport};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UDP Transport Example");

    // Setup cluster topology
    let mut cluster_urls = HashSet::new();
    cluster_urls.insert("udp://127.0.0.1:8001".parse()?);
    cluster_urls.insert("udp://127.0.0.1:8002".parse()?);
    cluster_urls.insert("udp://127.0.0.1:8003".parse()?);

    // Create transport for this node
    let transport = UdpTransport::new(8000, cluster_urls, 3).await?;

    println!("Transport created on {}", transport.local_addr());
    println!("Peers: {:?}", transport.get_peers());

    // Example 1: Consistent Hashing Pattern
    // Send to specific peer and wait for response
    let target_peer: SocketAddr = "127.0.0.1:8001".parse()?;

    match transport
        .send_to_peer(target_peer, b"Hello from consistent hashing")
        .await
    {
        Ok(()) => println!("✓ Sent message to specific peer: {}", target_peer),
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
    let mut message_rx = transport.get_message_receiver();

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
    let stats = transport.get_stats();
    print_stats(stats);

    // Remove a peer
    transport.remove_peer(new_peer).await?;
    println!("✓ Removed peer: {}", new_peer);

    // Final stats
    let final_stats = transport.get_stats();
    print_stats(final_stats);

    // Keep running for a moment to process any incoming messages
    println!("\nListening for messages for 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    Ok(())
}

fn print_stats(stats: TransportStats) {
    println!("Transport Statistics:");
    println!("  Local Address: {}", stats.local_addr);
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
        "  Messages Received: {}",
        stats.receiver_stats.messages_received.into_inner()
    );
    println!(
        "  Send Errors: {}",
        stats.send_pool_stats.send_errors.into_inner()
    );
    println!(
        "  Receive Errors: {}",
        stats.receiver_stats.receive_errors.into_inner()
    );
}
