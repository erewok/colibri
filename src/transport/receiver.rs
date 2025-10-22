//! UDP Receiver
//!
//! Handles incoming UDP messages with support for both callback-based
//! and channel-based message handling patterns.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::error::{ColibriError, Result};

/// UDP message receiver with flexible handling patterns
pub struct UdpReceiver {
    pub local_addr: SocketAddr,
    socket: Arc<UdpSocket>,
    stats: Arc<ReceiverStats>,
    message_tx: Arc<mpsc::Sender<(bytes::Bytes, SocketAddr)>>,
}

/// Statistics for the receiver
#[derive(Debug, Default)]
pub struct ReceiverStats {
    pub messages_received: AtomicU64,
    pub receive_errors: AtomicU64,
}

impl UdpReceiver {
    /// Create a new UDP receiver
    pub async fn new(bind_addr: SocketAddr, message_tx: Arc<mpsc::Sender<(bytes::Bytes, SocketAddr) >>) -> Result<Self> {
        let socket = UdpSocket::bind(bind_addr)
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
            let mut buf = vec![0u8; 65536]; // 64KB buffer

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let data = buf[..len].to_vec();
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);
                        let message = (bytes::Bytes::from(data), addr);
                        // Send to channel
                        if tx_clone.send(message).await.is_err() {
                            // Receiver dropped, exit the task
                            break;
                        }
                    }
                    Err(e) => {
                        stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("UDP receive error: {}", e);
                        // Continue receiving despite errors
                    }
                }
            }
        });
    }

    /// Get receiver statistics
    pub fn get_stats(&self) -> ReceiverStats {
        ReceiverStats {
            messages_received: AtomicU64::new(self.stats.messages_received.load(Ordering::Relaxed)),
            receive_errors: AtomicU64::new(self.stats.receive_errors.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use tokio::time::{sleep, timeout, Duration};

    use super::*;

    #[tokio::test]
    async fn test_receiver_creation() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, _rx) = mpsc::channel(1000);
        let receiver = UdpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();

        assert_eq!(
            receiver
                .get_stats()
                .messages_received
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_receiver_start_receive() {
        let (send_chan, mut recv_chan) = mpsc::channel(1000);
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr, Arc::new(send_chan))
            .await
            .unwrap();

        receiver.start().await;
        // Give the receiver task a moment to start
        sleep(Duration::from_millis(10)).await;

        // Send a test message
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender.local_addr().unwrap();
        sender
            .send_to(b"test message", receiver.local_addr)
            .await
            .unwrap();

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
        assert_eq!(stats.messages_received.load(Ordering::Relaxed), 1);
    }
}
