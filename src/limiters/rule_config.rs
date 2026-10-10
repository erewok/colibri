//! Named rate-limit rules, replicated on every node as a last-writer-wins map.
//! Deletes leave tombstones so a stale copy can't revive a rule. The default
//! rule isn't here: each node takes it from its startup flags.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::clock::wall_ms;
use crate::limiters::rules::{RuleName, SerializableRule};
use crate::node::ActorId;
use crate::settings::RateLimitSettings;

/// Orders changes to a rule: wall-clock ms (raised past the newest version seen), then
/// the writing process. Two processes never produce the same version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RuleVersion {
    pub ms: i64,
    pub actor: ActorId,
}

/// A rule's latest state: its settings, or `None` once deleted.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleEntry {
    pub settings: Option<RateLimitSettings>,
    pub version: RuleVersion,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleConfig {
    entries: BTreeMap<RuleName, RuleEntry>,
}

impl RuleConfig {
    /// Create or change a rule.
    pub fn set(&mut self, name: RuleName, settings: RateLimitSettings, actor: impl Into<ActorId>) {
        let version = self.next_version(&name, actor.into());
        self.entries.insert(
            name,
            RuleEntry {
                settings: Some(settings),
                version,
            },
        );
    }

    /// Delete a rule, leaving a tombstone.
    pub fn delete(&mut self, name: RuleName, actor: impl Into<ActorId>) {
        let version = self.next_version(&name, actor.into());
        self.entries.insert(
            name,
            RuleEntry {
                settings: None,
                version,
            },
        );
    }

    fn next_version(&self, name: &RuleName, actor: ActorId) -> RuleVersion {
        let after = self
            .entries
            .get(name)
            .map_or(i64::MIN, |e| e.version.ms + 1);
        RuleVersion {
            ms: wall_ms().max(after),
            actor,
        }
    }

    /// Fold another node's map into this one. Returns true if anything changed.
    pub fn merge(&mut self, other: &RuleConfig) -> bool {
        let mut changed = false;
        for (name, theirs) in &other.entries {
            let newer = self
                .entries
                .get(name)
                .is_none_or(|ours| theirs.version > ours.version);
            if newer {
                self.entries.insert(name.clone(), theirs.clone());
                changed = true;
            }
        }
        changed
    }

    /// The settings of a live rule.
    pub fn get(&self, name: &RuleName) -> Option<&RateLimitSettings> {
        self.entries.get(name).and_then(|e| e.settings.as_ref())
    }

    /// A rule's entry, live or deleted.
    pub fn entry(&self, name: &RuleName) -> Option<&RuleEntry> {
        self.entries.get(name)
    }

    /// The live rules, in name order.
    pub fn live(&self) -> impl Iterator<Item = (&RuleName, &RateLimitSettings)> {
        self.entries
            .iter()
            .filter_map(|(name, e)| e.settings.as_ref().map(|s| (name, s)))
    }

    /// The live rules with their versions, in name order.
    pub fn live_versions(
        &self,
    ) -> impl Iterator<Item = (&RuleName, &RateLimitSettings, RuleVersion)> {
        self.entries
            .iter()
            .filter_map(|(name, e)| e.settings.as_ref().map(|s| (name, s, e.version)))
    }

    /// The live rules as API values.
    pub fn live_rules(&self) -> Vec<SerializableRule> {
        self.live()
            .map(|(name, settings)| SerializableRule {
                name: name.clone(),
                settings: settings.clone(),
            })
            .collect()
    }

    /// Stable fingerprint of the map (FNV-1a of its encoding), for heartbeats.
    pub fn digest(&self) -> u64 {
        let bytes = postcard::to_allocvec(self).unwrap_or_default();
        // FNV-1a, 64-bit.
        bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeName;

    fn settings(max_calls: u32) -> RateLimitSettings {
        RateLimitSettings {
            rate_limit_max_calls_allowed: max_calls,
            rate_limit_interval_seconds: 1,
        }
    }

    #[test]
    fn a_later_change_beats_an_earlier_one_even_with_a_slower_clock() {
        let clock = crate::clock::virtual_clock::install();
        let a = NodeName::from("a").node_id();
        let b = NodeName::from("b").node_id();
        let name = RuleName::from("r");

        let mut on_a = RuleConfig::default();
        on_a.set(name.clone(), settings(1), a);
        let mut on_b = RuleConfig::default();
        on_b.merge(&on_a);
        // b's clock is behind a's, but b changes the rule after seeing a's.
        clock.step_wall(-1_000);
        on_b.delete(name.clone(), b);

        on_a.merge(&on_b);
        assert_eq!(
            on_a.get(&name),
            None,
            "the delete happened after the create"
        );
        assert_eq!(on_a, on_b);
    }

    #[test]
    fn a_deleted_rule_is_not_revived_by_a_stale_copy() {
        let _clock = crate::clock::virtual_clock::install();
        let a = NodeName::from("a").node_id();
        let name = RuleName::from("r");

        let mut config = RuleConfig::default();
        config.set(name.clone(), settings(1), a);
        let stale = config.clone();
        config.delete(name.clone(), a);

        assert!(!config.merge(&stale));
        assert_eq!(config.get(&name), None);
    }
}
