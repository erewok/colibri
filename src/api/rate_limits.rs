use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use tracing::instrument;

use crate::error::Result;
use crate::node;

#[instrument(skip(state), level = "info")]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<node::CheckCallsResponse>> {
    state.check_limit(client_id).await.map(axum::Json)
}

#[instrument(skip(state), level = "info")]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<node::CheckCallsResponse>> {
    let result = state.rate_limit(client_id).await?;

    match result {
        Some(resp) => Ok(axum::Json(resp)),
        None => Err(crate::rate_limit_error!("Rate limit exceeded")),
    }
}

#[instrument(skip(state), level = "debug")]
pub async fn expire_keys(State(state): State<node::NodeWrapper>) -> StatusCode {
    state.expire_keys();
    StatusCode::OK
}
