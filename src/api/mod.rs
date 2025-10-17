mod base;
mod rate_limits;

use std::borrow::Cow;

use axum::{
    error_handling::HandleErrorLayer, http::StatusCode, response::IntoResponse, routing, Router,
};
use tokio::time::Duration;
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;

use crate::cli;
use crate::node;

/// Build an API with a rate-limiter and a strategy
pub async fn api(settings: cli::Cli) -> anyhow::Result<(Router, node::NodeWrapper)> {
    // App state will automatically check limits or ask other nodes
    let app_state = node::NodeWrapper::new(settings);

    // Endpoints
    let api = Router::new()
        .route("/", routing::get(base::root))
        .route("/health", routing::get(base::health))
        .route("/about", routing::get(base::about))
        .route("/rl/{client_id}", routing::post(rate_limits::rate_limit))
        .route(
            "/rl-check/{client_id}",
            routing::get(rate_limits::check_limit),
        )
        .route("/expire-keys", routing::post(rate_limits::expire_keys))
        .layer(
            ServiceBuilder::new()
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .timeout(Duration::from_secs(10)),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(app_state.clone());

    Ok((api, app_state))
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
