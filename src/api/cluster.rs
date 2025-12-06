use axum::{extract::State, http::StatusCode, Json};
use tracing::instrument;

use crate::cluster::{
    BucketExport,
    StatusResponse,
    TopologyChangeRequest,
    TopologyResponse,
};
use crate::node::NodeWrapper;
use crate::error::Result;


/// Export all rate limiting data for a specific bucket
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
/// See examples/colibri_admin_cli.rs for the new approach
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn export_buckets(State(state): State<NodeWrapper>) -> Result<Json<BucketExport>> {
    let export = state.handle_export_buckets().await?;
    Ok(Json(export))
}

/// Import rate limiting data into a specific bucket
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn import_buckets(
    State(state): State<NodeWrapper>,
    Json(import_data): Json<BucketExport>,
) -> Result<StatusCode> {
    state.handle_import_buckets(import_data).await?;
    Ok(StatusCode::OK)
}

/// Health check with cluster-specific information
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn cluster_health(State(state): State<NodeWrapper>) -> Result<Json<StatusResponse>> {
    let response = state.handle_cluster_health().await?;
    Ok(Json(response))
}

/// Get current node topology information
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn get_topology(State(state): State<NodeWrapper>) -> Result<Json<TopologyResponse>> {
    let response = state.handle_get_topology().await?;
    Ok(Json(response))
}

/// Prepare for topology change
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
/// The new approach provides atomic topology changes without service interruption
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn new_topology(
    State(state): State<NodeWrapper>,
    Json(request): Json<TopologyChangeRequest>,
) -> Result<Json<TopologyResponse>> {
    let response = state.handle_new_topology(request).await?;
    Ok(Json(response))
}

/// Get cluster operational status
///
/// DEPRECATED: This endpoint should be moved to admin-only access
/// Use the AdminCommandDispatcher with internal UDP transport instead
#[deprecated(since = "0.5.0", note = "Use AdminCommandDispatcher for cluster operations")]
#[instrument(skip(state), level = "info")]
pub async fn get_cluster_status(
    State(state): State<NodeWrapper>,
) -> Result<Json<StatusResponse>> {
    let response = state.handle_cluster_health().await?;
    Ok(Json(response))
}
