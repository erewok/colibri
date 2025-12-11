use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use tokio::time::{self, Duration};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use colibri::api;
use colibri::cli;
use colibri::error::Result;
use colibri::node;

const KEY_EXPIRY_INTERVAL: u64 = 500;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,colibri=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse args and env vars
    let args = cli::Cli::parse_with_file();
    let settings = args.into_settings();

    // Socket server listen address setup
    let listen_address: IpAddr = settings
        .listen_address
        .parse::<IpAddr>()
        .expect("Invalid ip address");
    let socket_address = SocketAddr::from((listen_address, settings.listen_port_api));
    let listener = tokio::net::TcpListener::bind(socket_address)
        .await
        .expect("Failed to bind TCP listener");

    // Our rate-limit node will function as app-state

    let rl_node = node::NodeWrapper::new(settings).await?;
    // Build Axum Router and get shared state
    let api = api::api(rl_node.clone()).await?;

    tokio::spawn(async move {
        // Start Cache Expire Request Loop
        info!("Starting Cache Expiry background task");
        let mut interval = time::interval(Duration::from_millis(KEY_EXPIRY_INTERVAL));
        loop {
            interval.tick().await;
            let _ = rl_node
                .expire_keys()
                .await
                .map_err(|e| error!("Failed to expire keys {}", e));
        }
    });

    // Start server
    info!("Starting Colibri on {}", socket_address);
    axum::serve(listener, api).await?;

    Ok(())
}
