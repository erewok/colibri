use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::settings::RateLimitSettings;

pub const DEFAULT_RULE_NAME: &str = "<_default>";

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct NamedRateLimitRule {
    pub name: String,
    pub settings: RateLimitSettings,
}

impl Default for NamedRateLimitRule {
    fn default() -> Self {
        Self {
            name: DEFAULT_RULE_NAME.to_string(),
            settings: RateLimitSettings::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub named_rules: HashMap<String, RateLimitSettings>,
}

impl RateLimitConfig {
    pub fn new(default_settings: RateLimitSettings) -> Self {
        let mut named_rules = HashMap::new();
        named_rules.insert(DEFAULT_RULE_NAME.to_string(), default_settings);
        Self { named_rules }
    }

    pub fn get_default_settings(&self) -> &RateLimitSettings {
        self.named_rules
            .get(DEFAULT_RULE_NAME)
            .expect("Default rule must exist")
    }

    pub fn get_named_rule_settings(&self, rule_name: &str) -> Option<&RateLimitSettings> {
        self.named_rules.get(rule_name)
    }

    pub fn add_named_rule(&mut self, rule: &NamedRateLimitRule) {
        self.named_rules
            .insert(rule.name.clone(), rule.settings.clone());
    }

    pub fn remove_named_rule(&mut self, rule_name: &str) -> Option<RateLimitSettings> {
        self.named_rules.remove(rule_name)
    }

    pub fn list_named_rules(&self) -> Vec<NamedRateLimitRule> {
        self.named_rules
            .iter()
            .map(|(name, settings)| NamedRateLimitRule {
                name: name.clone(),
                settings: settings.clone(),
            })
            .collect()
    }
}
