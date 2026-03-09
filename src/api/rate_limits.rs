use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use tracing::instrument;

use crate::error::Result;
use crate::limiters;
use crate::node;
use crate::node::messages;

#[instrument(skip(state), level = "info")]
pub async fn check_limit(
    Path(client_id): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<Option<messages::CheckCallsResponse>>> {
    //
    state.check_limit(client_id).await.map(axum::Json)
}

#[instrument(skip(state), level = "info")]
pub async fn rate_limit(
    Path(client_id): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<messages::CheckCallsResponse>> {
    let result = state.rate_limit(client_id).await?;

    match result {
        Some(resp) => Ok(axum::Json(resp)),
        None => Err(crate::rate_limit_error!("Rate limit exceeded")),
    }
}

#[instrument(skip(state), level = "debug")]
pub async fn expire_keys(State(state): State<node::NodeWrapper>) -> StatusCode {
    let _ = state.expire_keys().await;
    StatusCode::OK
}

// Custom rate-limit Configuration management endpoints
// List endpoints
#[instrument(skip(state), level = "info")]
pub async fn create_named_rate_limit_rule(
    State(state): State<node::NodeWrapper>,
    axum::Json(new_rule): axum::Json<limiters::SerializableRule>,
) -> Result<StatusCode> {
    state.create_named_rule(new_rule).await?;
    Ok(StatusCode::CREATED)
}

#[instrument(skip(state), level = "info")]
pub async fn list_named_rate_limit_rules(
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<limiters::RuleList>> {
    let rules = state.list_named_rules().await?;
    Ok(axum::Json(rules))
}

// Detail endpoints
#[instrument(skip(state), level = "info")]
pub async fn get_named_rate_limit_rule(
    Path(rule_name): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<(StatusCode, axum::Json<Option<limiters::SerializableRule>>)> {
    let rule = limiters::RuleName::from(rule_name);
    match state.get_named_rule(rule).await? {
        None => return Ok((StatusCode::NOT_FOUND, axum::Json(None))),
        Some(rule) => Ok((StatusCode::OK, axum::Json(Some(rule)))),
    }
}

#[instrument(skip(state), level = "info")]
pub async fn delete_named_rate_limit_rule(
    Path(rule_name): Path<String>,
    State(state): State<node::NodeWrapper>,
) -> Result<StatusCode> {
    let rule = limiters::RuleName::from(rule_name);
    state.delete_named_rule(rule).await?;
    Ok(StatusCode::NO_CONTENT)
}

// Custom rate limiting endpoints
#[instrument(skip(state), level = "info")]
pub async fn rate_limit_custom(
    Path((rule_name, key)): Path<(String, String)>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<messages::CheckCallsResponse>> {
    let rule = limiters::RuleName::from(rule_name);
    let result = state.rate_limit_custom(rule, key).await?;

    match result {
        Some(resp) => Ok(axum::Json(resp)),
        None => Err(crate::rate_limit_error!("Rate limit exceeded")),
    }
}

#[instrument(skip(state), level = "info")]
pub async fn check_limit_custom(
    Path((rule_name, key)): Path<(String, String)>,
    State(state): State<node::NodeWrapper>,
) -> Result<axum::Json<Option<messages::CheckCallsResponse>>> {
    let rule = limiters::RuleName::from(rule_name);
    state.check_limit_custom(rule, key).await.map(axum::Json)
}
