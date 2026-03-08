use serde::{Deserialize, Serialize};

use crate::settings::RateLimitSettings;

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
        RuleName(DEFAULT_RULE_NAME.to_string())
    }
}

impl std::fmt::Display for RuleName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rule<{}>", self.0)
    }
}

impl PartialEq<str> for RuleName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl RuleName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, PartialOrd, Ord, Eq)]
pub struct SerializableRule {
    pub name: RuleName,
    pub settings: RateLimitSettings,
}

// For serialization
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RuleList(pub Vec<SerializableRule>);

impl IntoIterator for RuleList {
    type Item = SerializableRule;
    type IntoIter = std::vec::IntoIter<SerializableRule>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
