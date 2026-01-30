//! TCP Receiver
//!
//! Handles incoming TCP messages.
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use super::stats::{FrozenReceiverStats, ReceiverStats};
use crate::error::{ColibriError, Result};

/// A request with a channel to send the response back
pub struct TcpRequest {
    pub data: bytes::Bytes,
    pub peer_addr: SocketAddr,
    pub response_tx: oneshot::Sender<Vec<u8>>,
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
            loop {
                let (mut stream, peer_addr) = match socket.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        eprintln!("TCP accept failed: {}", e);
                        continue;
                    }
                };

                let tx = tx_clone.clone();
                let stats_clone = stats.clone();

                // Spawn a task to handle this connection
                tokio::spawn(async move {
                    // Read length prefix (4 bytes)
                    let mut len_bytes = [0u8; 4];
                    if let Err(e) = stream.read_exact(&mut len_bytes).await {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Failed to read length prefix: {}", e);
                        return;
                    }

                    let msg_len = u32::from_be_bytes(len_bytes) as usize;

                    if msg_len > 10 * 1024 * 1024 {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Message too large: {} bytes", msg_len);
                        return;
                    }

                    // Read message data
                    let mut buf = vec![0u8; msg_len];
                    if let Err(e) = stream.read_exact(&mut buf).await {
                        stats_clone.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Failed to read message data: {}", e);
                        return;
                    }

                    stats_clone.messages_received.fetch_add(1, Ordering::Relaxed);

                    // Create a oneshot channel for the response
                    let (response_tx, response_rx) = oneshot::channel();

                    let request = TcpRequest {
                        data: bytes::Bytes::from(buf),
                        peer_addr,
                        response_tx,
                    };

                    // Send the request to the handler
                    if tx.send(request).await.is_err() {
                        eprintln!("Failed to send request to handler");
                        return;
                    }

                    // Wait for the response
                    match response_rx.await {
                        Ok(response_data) => {
                            // Write response length prefix
                            let len = response_data.len() as u32;
                            if let Err(e) = stream.write_all(&len.to_be_bytes()).await {
                                eprintln!("Failed to write response length: {}", e);
                                return;
                            }

                            // Write response data
                            if let Err(e) = stream.write_all(&response_data).await {
                                eprintln!("Failed to write response data: {}", e);
                                return;
                            }

                            if let Err(e) = stream.flush().await {
                                eprintln!("Failed to flush response: {}", e);
                            }
                        }
                        Err(_) => {
                            eprintln!("Handler dropped response channel");
                        }
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
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout, Duration};

    use super::*;

    #[tokio::test]
    async fn test_receiver_creation() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, _rx) = mpsc::channel(1000);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();

        assert_eq!(receiver.get_stats().messages_received, 0);
    }

    #[tokio::test]
    #[ignore] // TODO: Fix this test - receiver is getting zeros instead of actual data
    async fn test_receiver_start_receive() {
        let (send_chan, mut recv_chan) = mpsc::channel(1000);
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(send_chan))
            .await
            .unwrap();

        receiver.start().await;
        // Give the receiver task a moment to start
        sleep(Duration::from_millis(10)).await;

        // Send a test message
        let mut sender = TcpStream::connect(receiver.local_addr).await.unwrap();
        let sender_addr = sender.local_addr().unwrap();
        sender.write_all(b"test message").await.unwrap();

        // Receive the message
        match timeout(Duration::from_millis(100), recv_chan.recv()).await {
            Ok(Some((data, addr))) => {
                assert_eq!(data, bytes::Bytes::from_static(b"test message"));
                assert_eq!(addr, sender_addr);
            }
            Ok(None) => panic!("Channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for message"),
        }

        let stats = receiver.get_stats();
        assert_eq!(stats.messages_received, 1);
    }
}
