use std::net::{IpAddr, SocketAddr};

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
    // Parse args, env vars and config file
    let args = cli::Cli::parse_with_file()?;
    init_tracing(args.log_format);
    let settings = args.into_settings()?;

    // Socket server listen address setup
    let listen_address: IpAddr = settings
        .client_listen_address
        .parse::<IpAddr>()
        .expect("Invalid ip address");
    let socket_address = SocketAddr::from((listen_address, settings.client_listen_port));
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
    axum::serve(listener, api)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("Colibri stopped");

    Ok(())
}

fn init_tracing(format: cli::LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,colibri=info,tower_http=info".into());
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        cli::LogFormat::Text => registry.with(tracing_subscriber::fmt::layer()).init(),
        cli::LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }
}

/// Resolves on SIGTERM or Ctrl-C; the server then finishes in-flight requests.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl-C: {}", e);
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                error!("Failed to listen for SIGTERM: {}", e);
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("Shutdown signal received; draining in-flight requests");
}
