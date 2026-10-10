//! A gossip node's state and protocol decisions, without the network.
//! `GossipController` adds the UDP transport; tests drive this directly.
//! Named rules are gossiped exactly like the default rule.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{ColibriError, Result};
use crate::limiters::distributed_bucket::{
    DistributedBucketExternal, DistributedBucketLimiter, Incarnations,
};
use crate::limiters::rule_config::{RuleConfig, RuleVersion};
use crate::limiters::rules::{RuleName, SerializableRule};
use crate::node::{ActorId, NodeId};
use crate::settings::RateLimitSettings;

/// One rule's buckets, as carried in gossip messages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleBuckets {
    pub rule: RuleName,
    /// The rule's version (`None` for the default rule); buckets of any other
    /// version of the rule are ignored.
    pub version: Option<RuleVersion>,
    pub buckets: Vec<DistributedBucketExternal>,
}

/// What to do after merging a peer's rule configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RulesReply {
    /// The peer lacks changes this node has: send it this node's config.
    pub send_rules: bool,
    /// This node started a rule it may have missed buckets for: request state.
    pub request_state: bool,
}

/// Clients are split into this many partitions for anti-entropy.
pub const PARTITIONS: u16 = 64;

/// The anti-entropy partition of `client` (FNV-1a, stable across processes).
pub fn partition_of(client: &str) -> u16 {
    (fnv1a(client.as_bytes()) % u64::from(PARTITIONS)) as u16
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// splitmix64's finalizer. FNV-1a hashes of similar inputs differ by small
/// multiples, so unmixed sums of them can cancel.
fn mix(mut h: u64) -> u64 {
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// What a heartbeat tells peers about the sender's state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatDigest {
    /// The newest incarnation the sender knows of each node, so peers agree
    /// on which processes are dead.
    pub incarnations: Vec<ActorId>,
    /// A hash of each partition's live buckets.
    pub partitions: Vec<u64>,
    /// `RuleConfig::digest` of the sender's rule configuration.
    pub rules: u64,
}

/// What a node should do about a peer's heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatReply {
    /// Partitions whose state differs: pull the peer's and push ours.
    pub partitions: Vec<u16>,
    /// Rule configs differ: send ours to the peer (it does the same in turn).
    pub push_rules: bool,
}

/// Buckets sent to a peer, with the newest incarnations the sender knows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSync {
    pub incarnations: Vec<ActorId>,
    pub updates: Vec<RuleBuckets>,
}

/// A heartbeat goes out at least this often, even on ticks with changes.
pub const HEARTBEAT_EVERY_TICKS: u32 = 5;

/// What a node sends on a gossip tick: its unsent changes, a heartbeat, or both.
#[derive(Clone, Debug)]
pub struct TickOutput {
    pub delta: Option<StateSync>,
    pub heartbeat: Option<HeartbeatDigest>,
}

#[derive(Debug)]
pub struct GossipState {
    /// This process: the slot it writes, and the node its rule changes come from.
    actor: ActorId,
    rules: RuleConfig,
    /// One limiter per live rule, including the default rule.
    limiters: BTreeMap<RuleName, DistributedBucketLimiter>,
    /// The version of each named rule's limiter.
    versions: BTreeMap<RuleName, RuleVersion>,
    incarnations: Incarnations,
    /// Ticks since the last heartbeat.
    quiet_ticks: u32,
}

impl GossipState {
    /// A starting process for `node`, knowing only the default rule.
    pub fn new(node_id: NodeId, default_settings: RateLimitSettings) -> Self {
        Self::with_actor(ActorId::fresh(node_id), default_settings)
    }

    /// Like `new`, with a given incarnation; for deterministic tests.
    pub fn with_actor(actor: ActorId, default_settings: RateLimitSettings) -> Self {
        let incarnations = Incarnations::default();
        let mut limiters = BTreeMap::new();
        limiters.insert(
            RuleName::default(),
            DistributedBucketLimiter::with_incarnations(
                actor,
                default_settings.clone(),
                incarnations.clone(),
            ),
        );
        Self {
            actor,
            rules: RuleConfig::default(),
            limiters,
            versions: BTreeMap::new(),
            incarnations,
            quiet_ticks: 0,
        }
    }

    fn is_default(rule: &RuleName) -> bool {
        *rule == RuleName::default()
    }

    fn limiter_mut(&mut self, rule: &RuleName) -> Result<&mut DistributedBucketLimiter> {
        self.limiters
            .get_mut(rule)
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", rule)))
    }

    /// The limiter for `rule`, if this node knows the rule.
    pub fn limiter(&self, rule: &RuleName) -> Option<&DistributedBucketLimiter> {
        self.limiters.get(rule)
    }

    /// Consume a token for `client` under `rule`: `Some(remaining)` if
    /// admitted, `None` if rate limited, an error if the rule is unknown.
    pub fn limit(&mut self, rule: &RuleName, client: &str) -> Result<Option<u32>> {
        Ok(self
            .limiter_mut(rule)?
            .limit_calls_for_client(client.to_string()))
    }

    /// Calls remaining for `client` under `rule`, without consuming.
    pub fn check(&self, rule: &RuleName, client: &str) -> Result<u32> {
        self.limiters
            .get(rule)
            .map(|l| l.check_calls_remaining_for_client(&client.to_string()))
            .ok_or_else(|| ColibriError::Api(format!("Rate limit rule '{}' not found", rule)))
    }

    /// Create or change a named rule. Changing its settings resets its counts.
    pub fn create_rule(&mut self, name: RuleName, settings: RateLimitSettings) -> Result<()> {
        if name.as_str().trim().is_empty() {
            return Err(ColibriError::Api("Rule name cannot be empty".to_string()));
        }
        if Self::is_default(&name) {
            return Err(ColibriError::Api(
                "Cannot create or modify the default rule".to_string(),
            ));
        }
        self.rules.set(name, settings, self.actor);
        self.reconcile();
        Ok(())
    }

    /// Delete a named rule everywhere, along with its buckets.
    pub fn delete_rule(&mut self, name: RuleName) -> Result<()> {
        if Self::is_default(&name) {
            return Err(ColibriError::Api(
                "Cannot delete the default rule".to_string(),
            ));
        }
        self.rules.delete(name, self.actor);
        self.reconcile();
        Ok(())
    }

    pub fn get_rule(&self, name: &RuleName) -> Option<SerializableRule> {
        self.limiters.get(name).map(|l| SerializableRule {
            name: name.clone(),
            settings: l.get_settings().clone(),
        })
    }

    /// Every rule this node enforces, the default rule included.
    pub fn list_rules(&self) -> Vec<SerializableRule> {
        self.limiters
            .iter()
            .map(|(name, l)| SerializableRule {
                name: name.clone(),
                settings: l.get_settings().clone(),
            })
            .collect()
    }

    /// The version of a named rule this node enforces, if it knows the rule.
    pub fn rule_version(&self, rule: &RuleName) -> Option<RuleVersion> {
        self.versions.get(rule).copied()
    }

    pub fn actor(&self) -> ActorId {
        self.actor
    }

    pub fn rule_config(&self) -> &RuleConfig {
        &self.rules
    }

    /// Fold a peer's rule configuration into this node's.
    pub fn merge_rules(&mut self, other: &RuleConfig) -> RulesReply {
        let request_state = self.rules.merge(other) && self.reconcile();
        RulesReply {
            send_rules: self.rules != *other,
            request_state,
        }
    }

    /// Keep one limiter per live rule version. A new version (created, changed
    /// or recreated) starts from zero. Returns true if any limiter was started.
    fn reconcile(&mut self) -> bool {
        let default = RuleName::default();
        let live: BTreeMap<RuleName, (RateLimitSettings, RuleVersion)> = self
            .rules
            .live_versions()
            .map(|(name, settings, version)| (name.clone(), (settings.clone(), version)))
            .collect();
        self.limiters
            .retain(|name, _| *name == default || live.contains_key(name));
        self.versions.retain(|name, _| live.contains_key(name));
        let mut started = false;
        for (name, (settings, version)) in live {
            if self.versions.get(&name) != Some(&version) {
                self.limiters.insert(
                    name.clone(),
                    DistributedBucketLimiter::with_incarnations(
                        self.actor,
                        settings,
                        self.incarnations.clone(),
                    ),
                );
                self.versions.insert(name, version);
                started = true;
            }
        }
        started
    }

    /// Age every bucket, then return the changes to send, or a heartbeat.
    pub fn tick(&mut self) -> TickOutput {
        for limiter in self.limiters.values() {
            limiter.tick_expirations();
        }
        let updates: Vec<RuleBuckets> = self
            .limiters
            .iter()
            .map(|(rule, l)| RuleBuckets {
                rule: rule.clone(),
                version: self.versions.get(rule).copied(),
                buckets: l.gossip_delta_state(),
            })
            .filter(|r| !r.buckets.is_empty())
            .collect();
        self.quiet_ticks += 1;
        let heartbeat =
            (updates.is_empty() || self.quiet_ticks >= HEARTBEAT_EVERY_TICKS).then(|| {
                self.quiet_ticks = 0;
                self.digest()
            });
        TickOutput {
            delta: (!updates.is_empty()).then(|| self.sync(updates)),
            heartbeat,
        }
    }

    /// The newest incarnation this node knows of each node.
    pub fn incarnations(&self) -> Vec<ActorId> {
        self.incarnations.newest()
    }

    fn sync(&self, updates: Vec<RuleBuckets>) -> StateSync {
        StateSync {
            incarnations: self.incarnations.newest(),
            updates,
        }
    }

    /// Learn of newer processes. A newer one of our own node means a duplicate
    /// name or a clock that went backwards; this process keeps its slot.
    pub fn note_incarnations(&mut self, actors: &[ActorId]) {
        for &actor in actors {
            if self.incarnations.note(actor)
                && actor.node == self.actor.node
                && actor.incarnation > self.actor.incarnation
            {
                warn!(
                    "A newer process {actor} of this node exists (this is {}): duplicate node name or clock behind?",
                    self.actor
                );
            }
        }
    }

    /// Every bucket of every rule, for a peer catching up.
    pub fn full_state(&self) -> StateSync {
        self.sync(
            self.limiters
                .iter()
                .map(|(rule, l)| RuleBuckets {
                    rule: rule.clone(),
                    version: self.versions.get(rule).copied(),
                    buckets: l.full_state_dump(),
                })
                .filter(|r| !r.buckets.is_empty())
                .collect(),
        )
    }

    /// The buckets of every rule in `partitions`.
    pub fn partition_state(&self, partitions: &[u16]) -> StateSync {
        let mut state = self.full_state();
        for update in &mut state.updates {
            update
                .buckets
                .retain(|b| partitions.contains(&partition_of(&b.client_id)));
        }
        state.updates.retain(|update| !update.buckets.is_empty());
        state
    }

    /// Merge a peer's buckets, ignoring rules (or rule versions) this node doesn't have.
    pub fn accept(&mut self, state: &StateSync) {
        self.note_incarnations(&state.incarnations);
        for update in &state.updates {
            if self.versions.get(&update.rule).copied() != update.version {
                continue;
            }
            if let Some(limiter) = self.limiters.get_mut(&update.rule) {
                limiter.accept_delta_state(&update.buckets);
            }
        }
    }

    /// Partition hashes sum each bucket's hash, so they don't depend on order.
    pub fn digest(&self) -> HeartbeatDigest {
        let mut partitions = vec![0u64; usize::from(PARTITIONS)];
        for (rule, limiter) in &self.limiters {
            let version = self.versions.get(rule);
            for (client, slots) in limiter.digest_slots() {
                let bytes =
                    postcard::to_allocvec(&(rule, version, &client, slots)).unwrap_or_default();
                let partition = &mut partitions[usize::from(partition_of(&client))];
                *partition = partition.wrapping_add(mix(fnv1a(&bytes)));
            }
        }
        HeartbeatDigest {
            incarnations: self.incarnations.newest(),
            partitions,
            rules: self.rules.digest(),
        }
    }

    /// Decide what to do about a peer's heartbeat: which partitions to
    /// exchange, after learning which processes it knows are dead.
    pub fn on_heartbeat(&mut self, peer: &HeartbeatDigest) -> HeartbeatReply {
        self.note_incarnations(&peer.incarnations);
        let ours = self.digest();
        let partitions = (0..PARTITIONS)
            .filter(|&p| ours.partitions.get(usize::from(p)) != peer.partitions.get(usize::from(p)))
            .collect();
        HeartbeatReply {
            partitions,
            push_rules: peer.rules != ours.rules,
        }
    }

    /// Forget idle clients under every rule.
    pub fn expire_keys(&mut self) {
        for limiter in self.limiters.values_mut() {
            limiter.expire_keys();
        }
    }

    /// Buckets held, across every rule.
    pub fn bucket_count(&self) -> usize {
        self.limiters
            .values()
            .map(DistributedBucketLimiter::len)
            .sum()
    }
}
