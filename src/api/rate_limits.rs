use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};

use super::state;

#[derive(Serialize, Deserialize)]
pub struct CheckCallsResponse {
    client_id: String,
    calls_remaining: u32,
}

/// Multi Node implementation of checking limit
#[instrument(skip(state))]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<state::SharedState>,
) -> Result<axum::Json<CheckCallsResponse>, StatusCode> {
    match state.read() {
        Ok(_state) => {
            let calls_remaining = _state
                .get_rate_limiter()
                .check_calls_remaining_for_client(client_id.as_str());
            Ok(axum::Json(CheckCallsResponse {
                client_id,
                calls_remaining,
            }))
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed to read from RwLock",
                err = format!("{:?}", err)
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(state))]
pub async fn check_limit_cluster(
    client_id: String,
    state: State<state::SharedState>,
) -> Result<axum::Json<CheckCallsResponse>, StatusCode> {
    match state.read() {
        Ok(_state) => {
            let calls_remaining = _state
                .get_rate_limiter()
                .check_calls_remaining_for_client(client_id.as_str());
            Ok(axum::Json(CheckCallsResponse {
                client_id,
                calls_remaining,
            }))
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed to read from RwLock",
                err = format!("{:?}", err)
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(state))]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<state::SharedState>,
) -> Result<axum::Json<CheckCallsResponse>, StatusCode> {
    match state.write() {
        Ok(mut _state) => {
            if let Some(calls_remaining) = _state
                .get_rate_limiter_mut()
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
            event!(
                Level::ERROR,
                message = "Failed to writer RwLock",
                err = format!("{:?}", err)
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(state), level = "debug")]
pub async fn expire_keys(State(state): State<state::SharedState>) -> StatusCode {
    match state.write() {
        Ok(mut _state) => {
            _state.get_rate_limiter_mut().expire_keys();
            StatusCode::OK
        }
        Err(err) => {
            event!(
                Level::ERROR,
                message = "Failed to writer RwLock",
                err = format!("{:?}", err)
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
