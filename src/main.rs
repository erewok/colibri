use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use colibri::api;
use colibri::cli;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
    .with(
        tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "colibri=debug,tower_http=debug".into()),
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

    // Build Axum Router
    let api = api::api(args)
        .await?
        .layer(TraceLayer::new_for_http());

    // Start server
    info!("Starting Colibri on {}", socket_address);
    axum::Server::bind(&socket_address)
        .serve(api.into_make_service())
        .await?;

    Ok(())
}
