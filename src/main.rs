use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use tokio::time::{self, Duration};
use tower_http::trace::TraceLayer;
use tracing::{event, info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use colibri::api;
use colibri::cli;

const KEY_EXPIRY_INTERVAL: u64 = 500;

async fn fire_expire(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let _resp = client
        .post(format!("http://localhost:{}/expire-keys", port))
        .send()
        .await
        .map_err(|err| {
            event!(
                Level::ERROR,
                message = "Failed to hit expire-keys endpoint",
                err = format!("{:?}", err)
            );
            err
        })?;
    Ok(())
}

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

    // Pull app port for cleanup requests and serving app
    let app_port = args.listen_port;
    // Socket server listen address setup
    let listen_address: IpAddr = args
        .listen_address
        .parse::<IpAddr>()
        .expect("Invalid ip address");
    let socket_address = SocketAddr::from((listen_address, app_port));

    // Build Axum Router
    let api = api::api(args).await?.layer(TraceLayer::new_for_http());

    // Start server
    info!("Starting Cache Expiry background task");

    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(KEY_EXPIRY_INTERVAL));
        loop {
            interval.tick().await;
            fire_expire(app_port).await.unwrap();
        }
    });

    // Start server
    info!("Starting Colibri on {}", socket_address);
    axum::Server::bind(&socket_address)
        .serve(api.into_make_service())
        .await?;

    Ok(())
}
