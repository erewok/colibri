use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::cli::{APP_NAME, APP_VERSION};

// basic handler that responds with a static string
pub async fn root() -> &'static str {
    "Welcome to Colibri"
}

pub async fn health() -> &'static str {
    "OK"
}

#[derive(Serialize, Deserialize)]
pub struct AboutResponse {
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
pub async fn about() -> axum::Json<AboutResponse> {
    axum::Json(AboutResponse::default())
}
