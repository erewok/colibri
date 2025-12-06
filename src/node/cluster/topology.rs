use std::net::{SocketAddr, ToSocketAddrs};

pub struct Topology {
    pub listen_addr: Box<dyn ToSocketAddrs<Iter = std::vec::IntoIter<String>>>,
    pub nodes: Vec<Box<dyn ToSocketAddrs<Iter = std::vec::IntoIter<String>>>>,
    pub topology_change_clock: Option<u64>,
}

impl Topology {
    pub fn new(
        listen_addr: Box<dyn ToSocketAddrs<Iter = std::vec::IntoIter<String>>>,
        nodes: Vec<Box<dyn ToSocketAddrs<Iter = std::vec::IntoIter<String>>>>,
        topology_change_clock: Option<u64>,
    ) -> Self {
        Self {
            listen_addr,
            nodes,
            topology_change_clock,
        }
    }
}

// pub topology: HashMap<u32, (Url, reqwest::Client)>,

// gossip uses a transport layer to send messages between nodes
// /// transport_config
// pub transport_config: settings::TransportConfig,
// /// Transport layer for sending UDP unicast gossip messages
// pub transport: Option<Arc<UdpTransport>>,
// pub response_addr: SocketAddr,

// #[derive(Clone, Debug)]
// pub struct TransportConfig {
//     pub listen_tcp: SocketAddr,
//     pub listen_udp: SocketAddr,
//     pub topology: HashSet<SocketAddr>,
// }


// /// Pool of UDP sockets for efficient peer communication
// #[derive(Clone, Debug)]
// pub struct UdpSocketPool {
//     // IndexMap for easily getting a random peer
//     peers: IndexMap<SocketAddr, Arc<Mutex<UdpSocket>>>,
//     // for debugging, identifying the node
//     node_id: NodeId,
//     // for statistics and monitoring
//     stats: Arc<SocketPoolStats>,
// }