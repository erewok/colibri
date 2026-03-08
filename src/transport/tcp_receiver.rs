//! TCP Receiver
//!
//! Handles incoming TCP messages with protocol-aware routing.
//! Supports both fire-and-forget (gossip) and request-response patterns.
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace, warn};

use super::stats::{FrozenReceiverStats, ReceiverStats};
use crate::error::{ColibriError, Result};

/// Protocol discriminator byte to identify message type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    /// Fire-and-forget gossip message (no response expected)
    Gossip = 0x01,
    /// Request-response message (response required)
    RequestResponse = 0x02,
}

impl ProtocolType {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(ProtocolType::Gossip),
            0x02 => Some(ProtocolType::RequestResponse),
            _ => None,
        }
    }

    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

/// A request with optional response channel based on protocol type
pub struct TcpRequest {
    pub data: bytes::Bytes,
    pub peer_addr: SocketAddr,
    pub protocol_type: ProtocolType,
    pub response_tx: Option<oneshot::Sender<Vec<u8>>>,
}

/// TCP message receiver
pub struct TcpReceiver {
    pub local_addr: SocketAddr,
    socket: Arc<TcpListener>,
    stats: Arc<ReceiverStats>,
    message_tx: Arc<mpsc::Sender<TcpRequest>>,
}

impl TcpReceiver {
    /// Create a new TCP receiver
    pub async fn new(
        bind_addr: SocketAddr,
        message_tx: Arc<mpsc::Sender<TcpRequest>>,
    ) -> Result<Self> {
        let socket = TcpListener::bind(bind_addr)
            .await
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

        let local_addr = socket
            .local_addr()
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

        Ok(Self {
            message_tx,
            local_addr,
            socket: Arc::new(socket),
            stats: Arc::new(ReceiverStats::default()),
        })
    }

    /// Start the receiving task
    pub async fn start(&self) -> () {
        let socket = self.socket.clone();
        let stats = self.stats.clone();
        let tx_clone = self.message_tx.clone();

        tokio::spawn(async move {
            debug!("TCP receiver task started");
            loop {
                let (mut stream, peer_addr) = match socket.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("TCP accept failed: {}", e);
                        continue;
                    }
                };

                trace!("Accepted connection from {}", peer_addr);

                let tx = tx_clone.clone();
                let stats_clone = stats.clone();

                // Spawn a task to handle this connection
                tokio::spawn(async move {
                    // Read protocol type (1 byte)
                    let mut protocol_byte = [0u8; 1];
                    if let Err(e) = stream.read_exact(&mut protocol_byte).await {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        debug!("Failed to read protocol byte from {}: {}", peer_addr, e);
                        return;
                    }

                    let protocol_type = match ProtocolType::from_byte(protocol_byte[0]) {
                        Some(pt) => pt,
                        None => {
                            stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "Invalid protocol byte 0x{:02x} from {}",
                                protocol_byte[0], peer_addr
                            );
                            return;
                        }
                    };

                    trace!("Protocol type: {:?} from {}", protocol_type, peer_addr);

                    // Read length prefix (4 bytes)
                    let mut len_bytes = [0u8; 4];
                    if let Err(e) = stream.read_exact(&mut len_bytes).await {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        debug!("Failed to read length prefix from {}: {}", peer_addr, e);
                        return;
                    }

                    let msg_len = u32::from_be_bytes(len_bytes) as usize;

                    if msg_len > 10 * 1024 * 1024 {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        warn!("Message too large from {}: {} bytes", peer_addr, msg_len);
                        return;
                    }

                    // Read message data
                    let mut buf = vec![0u8; msg_len];
                    if let Err(e) = stream.read_exact(&mut buf).await {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        debug!("Failed to read message data from {}: {}", peer_addr, e);
                        return;
                    }

                    stats_clone
                        .messages_received
                        .fetch_add(1, Ordering::Relaxed);

                    trace!(
                        "Received {} byte {:?} message from {}",
                        msg_len,
                        protocol_type,
                        peer_addr
                    );

                    // Create response channel only for request-response protocol
                    let (response_tx, response_rx_opt) = match protocol_type {
                        ProtocolType::RequestResponse => {
                            let (tx, rx) = oneshot::channel();
                            (Some(tx), Some(rx))
                        }
                        ProtocolType::Gossip => (None, None),
                    };

                    let request = TcpRequest {
                        data: bytes::Bytes::from(buf),
                        peer_addr,
                        protocol_type,
                        response_tx,
                    };

                    // Send the request to the handler
                    if tx.send(request).await.is_err() {
                        error!("Failed to send request to handler (channel closed)");
                        return;
                    }

                    // Only wait for response if it's a request-response protocol
                    if let Some(response_rx) = response_rx_opt {
                        match response_rx.await {
                            Ok(response_data) => {
                                // Write response length prefix
                                let len = response_data.len() as u32;
                                if let Err(e) = stream.write_all(&len.to_be_bytes()).await {
                                    // Connection closed by peer - expected in some cases
                                    debug!(
                                        "Failed to write response length to {}: {}",
                                        peer_addr, e
                                    );
                                    return;
                                }

                                // Write response data
                                if let Err(e) = stream.write_all(&response_data).await {
                                    debug!("Failed to write response data to {}: {}", peer_addr, e);
                                    return;
                                }

                                if let Err(e) = stream.flush().await {
                                    debug!("Failed to flush response to {}: {}", peer_addr, e);
                                    return;
                                }

                                trace!("Successfully sent {} byte response to {}", len, peer_addr);
                            }
                            Err(_) => {
                                warn!(
                                    "Handler dropped response channel for request from {}",
                                    peer_addr
                                );
                            }
                        }
                    } else {
                        // Fire-and-forget: no response needed, connection can close
                        trace!("Fire-and-forget message processed from {}", peer_addr);
                    }
                });
            }
        });
    }

    /// Get receiver statistics
    pub fn get_stats(&self) -> FrozenReceiverStats {
        self.stats.freeze()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_receiver_creation() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, _rx) = mpsc::channel(1000);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();

        assert_eq!(receiver.get_stats().messages_received, 0);
    }
}
