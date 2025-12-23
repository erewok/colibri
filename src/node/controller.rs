use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::transport::{Receiver, Sender};

use super::{ClusterMessage, MessageEnvelope, NodeName};

/// A controller that manages communication channels
/// Receive -> internal cluster messages, HTTP messages.
/// Send -> cluster messages to other nodes.
pub struct Controller {
    topology: HashMap<NodeName, SocketAddr>,
    receiver: Arc<Receiver>,
    sender: Arc<Sender>,

    // Three communication channels
    cluster_outbox_tx: mpsc::Sender<ClusterMessage>,
    cluster_outbox_rx: Option<mpsc::Receiver<ClusterMessage>>,

    cluster_inbox_tx: mpsc::Sender<ClusterMessage>,
    cluster_inbox_rx: Option<mpsc::Receiver<ClusterMessage>>,
    http_inbox_tx: mpsc::Sender<MessageEnvelope>,
    http_inbox_rx: Option<mpsc::Receiver<MessageEnvelope>>,

    // Controller state
    is_running: Arc<RwLock<bool>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}