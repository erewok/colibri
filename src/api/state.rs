use std::{
    borrow::BorrowMut,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::rate_limit::RateLimiter;

pub type SharedState = Arc<RwLock<WorkMode>>;

// Our state is either a rate-limiter or cluster state and rate-limiters
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum WorkMode {
    SingleNode(RateLimiter),
    MultiNode(MultiNodeState),
}

impl WorkMode {
    pub fn get_rate_limiter(&self) -> &RateLimiter {
        match &self {
            WorkMode::SingleNode(rl) => rl,
            WorkMode::MultiNode(mns) => &mns.rate_limiter,
        }
    }

    pub fn get_rate_limiter_mut(&mut self) -> &mut RateLimiter {
        match self {
            WorkMode::SingleNode(rl) => rl.borrow_mut(),
            WorkMode::MultiNode(mns) => mns.rate_limiter.borrow_mut(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MultiNodeState {
    pub topology: Vec<String>,
    pub hostname: String,
    pub rate_limiter: RateLimiter,
}
