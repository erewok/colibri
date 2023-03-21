mod base;
mod rate_limits;
mod state;

use std::{
    borrow::Cow,
    sync::{Arc, RwLock},
};

use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    response::IntoResponse,
    routing, Router,
};
use tokio::time::{self, Duration};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing::{event, instrument, Level};

use crate::cli;
use crate::rate_limit::RateLimiter;


pub async fn api(settings: cli::Cli) -> anyhow::Result<Router> {
    let state: state::SharedState = Arc::new(RwLock::new(state::AppState {
        topology: settings.topology.clone(),
        hostname: settings.hostname.clone(),
        rate_limiter: RateLimiter::new(settings),
    }));
    let api = Router::new()
        .route("/", routing::get(base::root))
        .route("/health", routing::get(base::health))
        .route("/about", routing::get(base::about))
        .route("/rate-limit-check/:client_id", routing::get(rate_limits::check_limit))
        .route("/rate-limit/:client_id", routing::post(rate_limits::rate_limit))
        .route("/expire-keys", routing::post(rate_limits::expire_keys))
        // .route("/topology", todo!())
        .layer(
            ServiceBuilder::new()
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .timeout(Duration::from_secs(10))
                .layer(TraceLayer::new_for_http()),
        )
        .with_state(state);
    Ok(api)
}

async fn handle_error(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        return (StatusCode::REQUEST_TIMEOUT, Cow::from("request timed out"));
    }

    if error.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::from("service is overloaded, try again later"),
        );
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("Unhandled internal error: {}", error)),
    )
}
