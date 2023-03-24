use std::{
    sync::{Arc, RwLock},
};

use crate::cli;
use crate::node;

pub type SharedState = Arc<RwLock<node::NodeWrapper>>;

pub fn get_state(settings: cli::Cli) -> SharedState {
    Arc::new(RwLock::new(node::NodeWrapper::new(settings)))
}
