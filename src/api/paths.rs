//! All Paths are recorded here for use throughout this codebase
pub mod base {
    pub const ROOT: &str = "/";
    pub const HEALTH: &str = "/health";
    pub const ABOUT: &str = "/about";
}

pub const EXPIRE_KEYS: &str = "/expire-keys";

pub mod default_rate_limits {
    pub const LIMIT: &str = "/rl/{client_id}";
    pub const CHECK: &str = "/rl-check/{client_id}";
}

pub mod custom {
    pub const RULE_CONFIG: &str = "/rl-config";
    pub const RULE: &str = "/rl-config/{rule_name}";
    pub const LIMIT: &str = "/rl/{rule_name}/{key}";
    pub const CHECK: &str = "/rl-check/{rule_name}/{key}";
}

// NOTE: Cluster endpoints moved to admin-only access via UDP transport
// See examples/colibri_admin_cli.rs for cluster administration commands

pub fn drop_leading_slash(path: &str) -> &str {
    if let Some(stripped) = path.strip_prefix('/') {
        stripped
    } else {
        path
    }
}

pub fn default_limit_path(client_id: &str) -> String {
    default_rate_limits::LIMIT.replace("{client_id}", client_id)
}
