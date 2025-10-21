//! UDP Receiver
//!
//! Handles incoming UDP messages with support for both callback-based
//! and channel-based message handling patterns.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::timeout;

use crate::error::{ColibriError, Result};

/// UDP message receiver with flexible handling patterns
pub struct UdpReceiver {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    stats: Arc<ReceiverStats>,
    message_tx: Arc<Mutex<Option<mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>>>>,
    pending_responses: Arc<Mutex<HashMap<SocketAddr, Vec<oneshot::Sender<Vec<u8>>>>>>,
}

/// Statistics for the receiver
#[derive(Debug, Default)]
pub struct ReceiverStats {
    pub messages_received: AtomicU64,
    pub receive_errors: AtomicU64,
}

impl UdpReceiver {
    /// Create a new UDP receiver
    pub async fn new(bind_addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

        let local_addr = socket
            .local_addr()
            .map_err(|e| ColibriError::Transport(format!("Socket creation failed: {}", e)))?;

        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            stats: Arc::new(ReceiverStats::default()),
            message_tx: Arc::new(Mutex::new(None)),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Get the local socket address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Start receiving messages with a synchronous callback
    pub async fn start_receiving<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(Vec<u8>, SocketAddr) + Send + 'static,
    {
        let socket = Arc::clone(&self.socket);
        let stats = Arc::clone(&self.stats);
        let pending_responses = Arc::clone(&self.pending_responses);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536]; // 64KB buffer

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let data = buf[..len].to_vec();
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);

                        // Check if this is a response to a pending request
                        let mut pending = pending_responses.lock().await;
                        if let Some(waiters) = pending.get_mut(&addr) {
                            if let Some(waiter) = waiters.pop() {
                                if waiters.is_empty() {
                                    pending.remove(&addr);
                                }
                                drop(pending); // Release lock before sending

                                let _ = waiter.send(data.clone());
                                continue;
                            }
                        }
                        drop(pending);

                        // Normal message handling
                        callback(data, addr);
                    }
                    Err(e) => {
                        stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("UDP receive error: {}", e);
                        // Continue receiving despite errors
                    }
                }
            }
        });

        Ok(())
    }

    /// Start receiving messages with an async callback
    pub async fn start_receiving_async<F, Fut>(&self, callback: F) -> Result<()>
    where
        F: Fn(Vec<u8>, SocketAddr) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let socket = Arc::clone(&self.socket);
        let stats = Arc::clone(&self.stats);
        let pending_responses = Arc::clone(&self.pending_responses);
        let callback = Arc::new(callback);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536]; // 64KB buffer

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let data = buf[..len].to_vec();
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);

                        // Check if this is a response to a pending request
                        let mut pending = pending_responses.lock().await;
                        if let Some(waiters) = pending.get_mut(&addr) {
                            if let Some(waiter) = waiters.pop() {
                                if waiters.is_empty() {
                                    pending.remove(&addr);
                                }
                                drop(pending); // Release lock before sending

                                let _ = waiter.send(data.clone());
                                continue;
                            }
                        }
                        drop(pending);

                        // Normal message handling
                        let callback = Arc::clone(&callback);
                        tokio::spawn(async move {
                            callback(data, addr).await;
                        });
                    }
                    Err(e) => {
                        stats.receive_errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("UDP receive error: {}", e);
                        // Continue receiving despite errors
                    }
                }
            }
        });

        Ok(())
    }

    /// Get a channel receiver for incoming messages
    pub async fn get_message_receiver(&self) -> mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Start the receiving task if not already started
        let socket = Arc::clone(&self.socket);
        let stats = Arc::clone(&self.stats);
        let pending_responses = Arc::clone(&self.pending_responses);
        let tx_clone = tx.clone();

        // Store the sender for potential cleanup
        let mut chan = self.message_tx.lock().await;
        *chan = Some(tx);
        drop(chan);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536]; // 64KB buffer

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        let data = buf[..len].to_vec();
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);

                        // Check if this is a response to a pending request
                        let mut pending = pending_responses.lock().await;
                        if let Some(waiters) = pending.get_mut(&addr) {
                            if let Some(waiter) = waiters.pop() {
                                if waiters.is_empty() {
                                    pending.remove(&addr);
                                }
                                drop(pending); // Release lock before sending

                                let _ = waiter.send(data.clone());
                                continue;
                            }
                        }
                        drop(pending);

                        // Send to channel
                        if tx_clone.send((data, addr)).is_err() {
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

        rx
    }

    /// Wait for a response from a specific peer (for request-response pattern)
    pub async fn wait_for_response_from(
        &self,
        peer: SocketAddr,
        timeout_duration: Duration,
    ) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();

        // Register the pending response
        {
            let mut pending = self.pending_responses.lock().await;
            pending.entry(peer).or_insert_with(Vec::new).push(tx);
        }

        // Wait for response with timeout
        match timeout(timeout_duration, rx).await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(_)) => Err(ColibriError::Transport(
                "Response channel closed".to_string(),
            )),
            Err(_) => {
                // Remove the pending request on timeout
                let mut pending = self.pending_responses.lock().await;
                if let Some(waiters) = pending.get_mut(&peer) {
                    waiters.clear(); // Clear all waiters for this peer
                    if waiters.is_empty() {
                        pending.remove(&peer);
                    }
                }
                Err(ColibriError::Transport("Response timeout".to_string()))
            }
        }
    }

    /// Get receiver statistics
    pub fn get_stats(&self) -> ReceiverStats {
        ReceiverStats {
            messages_received: AtomicU64::new(self.stats.messages_received.load(Ordering::Relaxed)),
            receive_errors: AtomicU64::new(self.stats.receive_errors.load(Ordering::Relaxed)),
        }
    }

    /// Get count of pending response waiters (for debugging)
    pub async fn pending_response_count(&self) -> usize {
        let pending = self.pending_responses.lock().await;
        pending.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_receiver_creation() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();

        assert!(receiver.local_addr().port() > 0);
        assert_eq!(receiver.pending_response_count().await, 0);
    }

    #[tokio::test]
    async fn test_callback_receiving() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();
        let receiver_addr = receiver.local_addr();

        let message_count = Arc::new(AtomicUsize::new(0));
        let message_count_clone = Arc::clone(&message_count);

        // Start receiving with callback
        receiver
            .start_receiving(move |data, _addr| {
                if data == b"test" {
                    message_count_clone.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await
            .unwrap();

        // Give the receiver task a moment to start
        sleep(Duration::from_millis(10)).await;

        // Send a test message
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender.send_to(b"test", receiver_addr).await.unwrap();

        // Wait for message to be processed
        sleep(Duration::from_millis(50)).await;

        assert_eq!(message_count.load(Ordering::Relaxed), 1);

        let stats = receiver.get_stats();
        assert_eq!(stats.messages_received.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_channel_receiving() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();
        let receiver_addr = receiver.local_addr();

        let mut message_rx = receiver.get_message_receiver().await;

        // Give the receiver task a moment to start
        sleep(Duration::from_millis(10)).await;

        // Send a test message
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender.local_addr().unwrap();
        sender
            .send_to(b"test message", receiver_addr)
            .await
            .unwrap();

        // Receive the message
        match timeout(Duration::from_millis(100), message_rx.recv()).await {
            Ok(Some((data, addr))) => {
                assert_eq!(data, b"test message");
                assert_eq!(addr, sender_addr);
            }
            Ok(None) => panic!("Channel closed unexpectedly"),
            Err(_) => panic!("Timeout waiting for message"),
        }

        let stats = receiver.get_stats();
        assert_eq!(stats.messages_received.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_response_waiting() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();
        let receiver_addr = receiver.local_addr();

        // Start a background task to send a delayed response
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender.local_addr().unwrap();

        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            sender.send_to(b"response", receiver_addr).await.unwrap();
        });

        // Wait for response
        let response = receiver
            .wait_for_response_from(sender_addr, Duration::from_millis(200))
            .await;

        match response {
            Ok(data) => assert_eq!(data, b"response"),
            Err(e) => panic!("Failed to receive response: {:?}", e),
        }

        assert_eq!(receiver.pending_response_count().await, 0);
    }

    #[tokio::test]
    async fn test_response_timeout() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();

        let fake_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9999);

        // Wait for response that will never come
        let response = receiver
            .wait_for_response_from(fake_addr, Duration::from_millis(50))
            .await;

        assert!(matches!(response, Err(ColibriError::Transport(_))));
        assert_eq!(receiver.pending_response_count().await, 0);
    }

    #[tokio::test]
    async fn test_multiple_pending_responses() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let receiver = UdpReceiver::new(bind_addr).await.unwrap();
        let receiver_addr = receiver.local_addr();

        let sender1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender1_addr = sender1.local_addr().unwrap();
        let sender2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender2_addr = sender2.local_addr().unwrap();

        // Start waiting for responses from both senders
        let response1_task = {
            let receiver = &receiver;
            async move {
                receiver
                    .wait_for_response_from(sender1_addr, Duration::from_millis(200))
                    .await
            }
        };

        let response2_task = {
            let receiver = &receiver;
            async move {
                receiver
                    .wait_for_response_from(sender2_addr, Duration::from_millis(200))
                    .await
            }
        };

        // Should have 2 pending responses
        sleep(Duration::from_millis(10)).await;
        assert_eq!(receiver.pending_response_count().await, 2);

        // Send responses
        sender1.send_to(b"response1", receiver_addr).await.unwrap();
        sender2.send_to(b"response2", receiver_addr).await.unwrap();

        // Wait for both responses
        let (result1, result2) = tokio::join!(response1_task, response2_task);

        assert_eq!(result1.unwrap(), b"response1");
        assert_eq!(result2.unwrap(), b"response2");
        assert_eq!(receiver.pending_response_count().await, 0);
    }
}
