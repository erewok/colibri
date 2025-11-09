mod base;
mod rate_limits;

use std::borrow::Cow;

use axum::{
    error_handling::HandleErrorLayer, http::StatusCode, response::IntoResponse, routing, Router,
};
use tokio::time::Duration;
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;

pub mod paths;

use crate::error::Result;
use crate::node;

/// Build an API with a rate-limiter and a strategy
pub async fn api(rl_node: node::NodeWrapper) -> Result<Router> {
    // App state will automatically check limits or ask other nodes

    // Endpoints
    let api = Router::new()
        .route(paths::base::ROOT, routing::get(base::root))
        .route(paths::base::HEALTH, routing::get(base::health))
        .route(paths::base::ABOUT, routing::get(base::about))
        // Default rate limiting (existing behavior)
        .route(paths::default_rate_limits::LIMIT, routing::post(rate_limits::rate_limit))
        .route(paths::default_rate_limits::CHECK, routing::get(rate_limits::check_limit))
        // Configuration management
        .route(paths::custom::RULE, routing::post(rate_limits::create_named_rate_limit_rule))
        .route(paths::custom::RULE, routing::delete(rate_limits::delete_named_rate_limit_rule))
        .route(paths::custom::RULE_CONFIG, routing::get(rate_limits::list_named_rate_limit_rules))
        // Custom rate limiting
        .route(paths::custom::LIMIT, routing::post(rate_limits::rate_limit_custom))
        .route(paths::custom::CHECK, routing::get(rate_limits::check_limit_custom))
        .route(paths::EXPIRE_KEYS, routing::post(rate_limits::expire_keys))
        .layer(
            ServiceBuilder::new()
                // Handle errors from middleware
                .layer(HandleErrorLayer::new(handle_error))
                .load_shed()
                .timeout(Duration::from_secs(10)),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(rl_node);

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
