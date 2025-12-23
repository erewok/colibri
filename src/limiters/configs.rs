use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::settings::RateLimitSettings;


#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct NamedRateLimitRule {
    pub name: String,
    pub settings: RateLimitSettings,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub default_settings: RateLimitSettings,
    pub named_rules: HashMap<String, RateLimitSettings>,
}


impl RateLimitConfig {
    pub fn new(default_settings: RateLimitSettings) -> Self {
        Self {
            default_settings,
            named_rules: HashMap::new(),
        }
    }

    pub fn get_default_settings(&self) -> &RateLimitSettings {
        &self.default_settings
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
