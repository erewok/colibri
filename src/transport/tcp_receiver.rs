//! TCP Receiver
//!
//! Handles incoming TCP messages.
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::stats::{FrozenReceiverStats, ReceiverStats};
use crate::error::{ColibriError, Result};

/// TCP message receiver
pub struct TcpReceiver {
    pub local_addr: SocketAddr,
    socket: Arc<TcpListener>,
    stats: Arc<ReceiverStats>,
    message_tx: Arc<mpsc::Sender<(bytes::Bytes, SocketAddr)>>,
}

impl TcpReceiver {
    /// Create a new TCP receiver
    pub async fn new(
        bind_addr: SocketAddr,
        message_tx: Arc<mpsc::Sender<(bytes::Bytes, SocketAddr)>>,
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

    /// Get a channel receiver for incoming messages
    pub async fn start(&self) -> () {
        // Start the receiving task if not already started
        let socket = self.socket.clone();
        let stats = self.stats.clone();
        let tx_clone = self.message_tx.clone();

        tokio::spawn(async move {
            loop {
                let (_socket, send_addr) = socket.accept().await.unwrap_or_else(|e| {
                    panic!("TCP accept failed: {}", e);
                });
                // Wait for the socket to be readable
                _socket.readable().await;
                let mut buf = vec![0u8; 65536]; // 64KB buffer
                match _socket.try_read_buf(&mut buf) {
                    Ok(len) => {
                        let data = buf[..len].to_vec();
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);
                        let message = (bytes::Bytes::from(data), send_addr);
                        // Send to channel
                        if tx_clone.send(message).await.is_err() {
                            // Receiver dropped, exit the task
                            break;
                        }
                    }
                    Err(e) => {
                        stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("TCP receive error: {}", e);
                        // Continue receiving despite errors
                    }
                }
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
