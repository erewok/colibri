use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use tracing::{event, instrument, Level};

use super::state;
use crate::node;

#[axum::debug_handler]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<state::SharedState>,
) -> Result<axum::Json<node::CheckCallsResponse>, StatusCode> {
    match state.read() {
        Ok(_state) => {
            _state.check_limit(client_id)
                .await
                .map_err(|err| {
                    event!(
                        Level::ERROR,
                        message = "Failed checking limit",
                        err = format!("{:?}", err)
                    );
                    StatusCode::INTERNAL_SERVER_ERROR
                })
                .map(|resp| axum::Json(resp))
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

#[axum::debug_handler]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<state::SharedState>,
) -> Result<axum::Json<node::CheckCallsResponse>, StatusCode> {
    match state.write() {
        Ok(mut _state) => {
            let result = _state.rate_limit(client_id)
                .await
                .map_err(|err| {
                    event!(
                        Level::ERROR,
                        message = "Failed limiting client",
                        err = format!("{:?}", err)
                    );
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
                match result {
                    Some(resp) => Ok(axum::Json(resp)),
                    None => Err(StatusCode::TOO_MANY_REQUESTS),
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

// #[instrument(skip(state), level = "debug")]
#[axum::debug_handler]
pub async fn expire_keys(State(state): State<state::SharedState>) -> StatusCode {
    match state.write() {
        Ok(mut _state) => {
            _state.expire_keys();
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
