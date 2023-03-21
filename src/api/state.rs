use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::rate_limit::RateLimiter;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AppState {
    pub topology: Vec<String>,
    pub hostname: String,
    pub rate_limiter: RateLimiter,
}

pub type SharedState = Arc<RwLock<AppState>>;