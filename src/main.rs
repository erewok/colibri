use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use tokio::time::{self, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use colibri::api;
use colibri::cli;

const KEY_EXPIRY_INTERVAL: u64 = 500;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,colibri=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse args and env vars
    let args = cli::Cli::parse();

    // Socket server listen address setup
    let listen_address: IpAddr = args
        .listen_address
        .parse::<IpAddr>()
        .expect("Invalid ip address");
    let socket_address = SocketAddr::from((listen_address, args.listen_port));
    let listener = tokio::net::TcpListener::bind(socket_address).await.unwrap();

    // Build Axum Router and get shared state
    let (api, app_state) = api::api(args).await?;
    let state_for_expiry = app_state.clone();

    tokio::spawn(async move {
        // Start Cache Expire Request Loop - direct method call instead of HTTP
        info!("Starting Cache Expiry background task");
        let mut interval = time::interval(Duration::from_millis(KEY_EXPIRY_INTERVAL));
        loop {
            interval.tick().await;
            state_for_expiry.expire_keys();
        }
    });

    // Start server
    info!("Starting Colibri on {}", socket_address);
    axum::serve(listener, api).await?;

    Ok(())
}
