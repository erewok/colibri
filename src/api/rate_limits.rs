use std::{
    sync::{Arc, RwLock},
};

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

#[instrument(skip(state))]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<Arc<RwLock<state::AppState>>>,
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
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        },
    }
}

#[instrument(skip(state))]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<Arc<RwLock<state::AppState>>>,
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
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        },
    }
}

#[instrument(skip(state))]
pub async fn expire_keys(
    State(state): State<Arc<RwLock<state::AppState>>>,
) -> StatusCode {
    match state.write() {
        Ok(mut _state) => {
            if let Ok(_) = _state
                .rate_limiter
                .expire_keys() {
                    StatusCode::OK
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
        }
        Err(err) => {
            event!(Level::ERROR, message = "Failed to writer RwLock", err=format!("{:?}", err));
            StatusCode::INTERNAL_SERVER_ERROR
        },
    }
}