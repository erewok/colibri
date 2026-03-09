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

    /// Start the receiving task and return its join handle for cancellation
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
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
        })
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

    async fn connect_and_send(addr: SocketAddr, frame: &[u8]) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
        stream
    }

    fn framed(protocol: ProtocolType, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![protocol.to_byte()];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[tokio::test]
    async fn test_receiver_creation() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, _rx) = mpsc::channel(1000);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();

        assert_eq!(receiver.get_stats().messages_received, 0);
    }

    #[tokio::test]
    async fn test_gossip_protocol_no_response_channel() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, mut rx) = mpsc::channel(10);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();
        let addr = receiver.local_addr;
        let _handle = receiver.start();
        sleep(Duration::from_millis(10)).await;

        let payload = b"gossip message";
        connect_and_send(addr, &framed(ProtocolType::Gossip, payload)).await;

        let request = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(request.data.as_ref(), payload);
        assert_eq!(request.protocol_type, ProtocolType::Gossip);
        assert!(
            request.response_tx.is_none(),
            "Gossip must not have a response channel"
        );
    }

    #[tokio::test]
    async fn test_request_response_protocol_roundtrip() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, mut rx) = mpsc::channel(10);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();
        let addr = receiver.local_addr;
        let _handle = receiver.start();
        sleep(Duration::from_millis(10)).await;

        let payload = b"request payload";
        let frame = framed(ProtocolType::RequestResponse, payload);

        // Spawn the sender in a task so it can also wait to read the response
        let response_payload = b"response data";
        let response_clone: &'static [u8] = response_payload;

        let sender_task = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(&frame).await.unwrap();
            stream.flush().await.unwrap();

            // Read the length-prefixed response
            let mut len_buf = [0u8; 4];
            use tokio::io::AsyncReadExt;
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut resp = vec![0u8; len];
            stream.read_exact(&mut resp).await.unwrap();
            resp
        });

        // Receive the request and send back a response
        let request = timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        assert_eq!(request.data.as_ref(), payload);
        assert_eq!(request.protocol_type, ProtocolType::RequestResponse);
        let response_tx = request
            .response_tx
            .expect("RequestResponse must have a response channel");
        response_tx.send(response_clone.to_vec()).unwrap();

        let received_response = timeout(Duration::from_millis(200), sender_task)
            .await
            .expect("timeout")
            .expect("task panicked");
        assert_eq!(received_response, response_clone);
    }

    #[tokio::test]
    async fn test_unknown_protocol_byte_rejected() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, mut rx) = mpsc::channel(10);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();
        let addr = receiver.local_addr;
        let _handle = receiver.start();
        sleep(Duration::from_millis(10)).await;

        // Send an invalid protocol byte followed by a valid-looking frame
        let mut bad_frame = vec![0xFFu8]; // unknown protocol byte
        bad_frame.extend_from_slice(&(4u32).to_be_bytes());
        bad_frame.extend_from_slice(b"data");
        connect_and_send(addr, &bad_frame).await;

        // Nothing should arrive on the channel
        let result = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            result.is_err(),
            "Unknown protocol byte must be rejected silently"
        );
    }

    #[tokio::test]
    async fn test_oversized_message_rejected() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let (tx, mut rx) = mpsc::channel(10);
        let receiver = TcpReceiver::new(bind_addr, Arc::new(tx)).await.unwrap();
        let addr = receiver.local_addr;
        let _handle = receiver.start();
        sleep(Duration::from_millis(10)).await;

        // Declare a message larger than the 10 MB limit (without actually sending that data)
        let oversized_len: u32 = 11 * 1024 * 1024; // 11 MB
        let mut frame = vec![ProtocolType::Gossip.to_byte()];
        frame.extend_from_slice(&oversized_len.to_be_bytes());
        // Don't need to send the actual data — receiver rejects before reading it
        connect_and_send(addr, &frame).await;

        let result = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(result.is_err(), "Oversized message must be rejected");
    }
}
