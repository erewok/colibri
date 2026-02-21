use std::collections::HashMap;

use serde::{Deserialize, Serialize};

crate::settings::RateLimitSettings

pub const DEFAULT_RULE_NAME: &str = "<_default>";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct RuleName(String);

impl From<&str> for RuleName {
    fn from(name: &str) -> Self {
        RuleName(name.to_string())
    }
}

impl From<String> for RuleName {
    fn from(name: String) -> Self {
        RuleName(name)
    }
}

impl Default for RuleName {
    fn default() -> Self {
        Self(DEFAULT_RULE_NAME.to_string())
    }
}


#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq)]
pub struct SerializableRule {
    pub name: RuleName,
    pub settings: crate::settings::RateLimitSettings,
}


// For serialization
#[derive(Clone, Debug, Deserialize, Serialize)]

pub struct RuleList(pub Vec<SerializableRule>);