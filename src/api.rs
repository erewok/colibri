use std::{
    borrow::Cow,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing, Router,
};
use serde::{Deserialize, Serialize};
use tower::{BoxError, ServiceBuilder};
use tower_http::trace::TraceLayer;
use tracing::{event, instrument, Level};

use crate::cli::{self, APP_NAME, APP_VERSION};
use crate::rate_limit::RateLimiter;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AppState {
    pub topology: Vec<String>,
    pub hostname: String,
    pub rate_limiter: RateLimiter,
}

type SharedState = Arc<RwLock<AppState>>;

pub async fn api(settings: cli::Cli) -> anyhow::Result<Router> {
    let state: SharedState = Arc::new(RwLock::new(AppState {
        topology: settings.topology.clone(),
        hostname: settings.hostname.clone(),
        rate_limiter: RateLimiter::new(settings),
    }));
    let api = Router::new()
        .route("/", routing::get(root))
        .route("/health", routing::get(health))
        .route("/about", routing::get(about))
        .route("/rate-limit-check/:client_id", routing::get(check_limit))
        .route("/rate-limit/:client_id", routing::get(rate_limit))
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

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Welcome to Colibri"
}

async fn health() -> &'static str {
    "OK"
}

#[derive(Serialize, Deserialize)]
struct AboutResponse {
    name: String,
    version: String,
}

impl Default for AboutResponse {
    fn default() -> Self {
        Self {
            name: APP_NAME.to_string(),
            version: APP_VERSION.to_string(),
        }
    }
}

#[instrument]
async fn about() -> axum::Json<AboutResponse> {
    axum::Json(AboutResponse::default())
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

#[derive(Serialize, Deserialize)]
struct CheckCallsResponse {
    client_id: String,
    calls_remaining: u32,
}

async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<Arc<RwLock<AppState>>>,
) -> Result<axum::Json<CheckCallsResponse>, StatusCode> {
    match state.read() {
        Ok(_state) => {
            let calls_remaining = _state
                .rate_limiter
                .check_calls_remaining_for_client(client_id.as_str());
            Ok(axum::Json(CheckCallsResponse {
                client_id,
                calls_remaining,
            }))
        }
        Err(err) => {
            event!(Level::ERROR, message = "Failed to read from RwLock", err=format!("{:?}", err));
            Err(StatusCode::SERVICE_UNAVAILABLE)
        },
    }
}

async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<Arc<RwLock<AppState>>>,
) -> Result<axum::Json<CheckCallsResponse>, StatusCode> {
    match state.write() {
        Ok(mut _state) => {
            if let Some(calls_remaining) = _state
                .rate_limiter
                .limit_calls_for_client(client_id.clone())
            {
                Ok(axum::Json(CheckCallsResponse {
                    client_id,
                    calls_remaining,
                }))
            } else {
                Err(StatusCode::TOO_MANY_REQUESTS)
            }
        }
        Err(err) => {
            event!(Level::ERROR, message = "Failed to writer RwLock", err=format!("{:?}", err));
            Err(StatusCode::SERVICE_UNAVAILABLE)
        },
    }
}
