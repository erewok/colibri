use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use tracing::{event, instrument, Level};

use crate::node;

#[instrument(skip(state), level = "debug")]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<node::CheckCallsResponse>, StatusCode> {
    state
        .check_limit(client_id)
        .await
        .map_err(|err| {
            event!(
                Level::ERROR,
                message = "Failed checking limit",
                err = format!("{:?}", err)
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })
        .map(axum::Json)
}

#[instrument(skip(state), level = "debug")]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(mut state): State<node::NodeWrapper>,
) -> Result<axum::Json<node::CheckCallsResponse>, StatusCode> {
    let result = state.rate_limit(client_id).await.map_err(|err| {
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

#[instrument(skip(state), level = "debug")]
pub async fn expire_keys(State(mut state): State<node::NodeWrapper>) -> StatusCode {
    state.expire_keys();
    StatusCode::OK
}
