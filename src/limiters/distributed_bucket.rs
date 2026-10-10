//! Distributed token bucket using CRDT for eventual consistency
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crdts::pncounter::{Dir, Op};
use crdts::{CmRDT, CvRDT, Dot, PNCounter, ResetRemove, VClock};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use papaya::HashMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::clock::monotonic_ms;
use crate::limiters::token_bucket::Bucket;
use crate::node::{ActorId, NodeId};
use crate::settings;

/// Distributed request counter using CRDT PN-counters, one slot per actor
/// (node process). Each actor writes only its own slot.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedRequestCounter {
    /// Origin metadata: the node that first created this counter. Not used for writes.
    pub node_id: NodeId,
    requests: PNCounter<ActorId>,
    pub vclock: VClock<ActorId>,
}

impl DistributedRequestCounter {
    /// Create a new DistributedRequestCounter for a given node
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            requests: PNCounter::new(),
            vclock: VClock::new(),
        }
    }

    pub fn inc_request(&mut self, actor: impl Into<ActorId>) {
        let actor = actor.into();
        let op = self.requests.inc(actor);
        self.requests.apply(op);
        let op = self.vclock.inc(actor);
        self.vclock.apply(op);
    }

    /// `actor`'s slot as (requests, expiries).
    pub fn slot(&self, actor: impl Into<ActorId>) -> (u64, u64) {
        let actor = actor.into();
        // `inc`/`dec` return the next dot, one past the current value.
        let requests = self.requests.inc(actor).dot.counter - 1;
        let expiries = self.requests.dec(actor).dot.counter - 1;
        (requests, expiries)
    }

    /// Requests `actor`'s slot still counts as live.
    pub fn outstanding(&self, actor: impl Into<ActorId>) -> u64 {
        let (requests, expiries) = self.slot(actor);
        requests.saturating_sub(expiries)
    }

    /// Highest value in `actor`'s slot (requests, expiries or vclock dot).
    fn own_high(&self, actor: ActorId) -> u64 {
        let (requests, expiries) = self.slot(actor);
        requests.max(expiries).max(self.vclock.get(&actor))
    }

    /// Actors with a slot in this counter.
    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.vclock.dots.keys().copied()
    }

    /// Set `writer`'s requests, expiries and dot to `base` (> `own_high`), leaving nothing live.
    fn rebase_own_slot(&mut self, writer: ActorId, base: u64) {
        let dot = Dot::new(writer, base);
        self.requests.apply(Op { dot, dir: Dir::Pos });
        self.requests.apply(Op { dot, dir: Dir::Neg });
        self.vclock.apply(dot);
    }

    /// Make `writer`'s slot count at most its `tracked` requests as live, by raising
    /// its expiries to requests - tracked. Absolute, so a peer having already retired
    /// some of them (see `retire`) is never counted twice.
    fn expire_untracked(&mut self, writer: ActorId, tracked: u64) {
        let (requests, expiries) = self.slot(writer);
        let target = requests.saturating_sub(tracked);
        if expiries < target {
            self.requests.apply(Op {
                dot: Dot::new(writer, target),
                dir: Dir::Neg,
            });
            let op = self.vclock.inc(writer);
            self.vclock.apply(op);
        }
    }

    /// Remove `actor`'s slot entirely.
    fn forget(&mut self, actor: ActorId) {
        let mut clock = VClock::new();
        clock.dots.insert(actor, u64::MAX);
        self.requests.reset_remove(&clock);
        self.vclock.reset_remove(&clock);
    }

    /// Expire everything `actor`'s slot counts as live (see `DistributedBucket::retire_stale`).
    fn retire(&mut self, actor: ActorId) {
        let (requests, _) = self.slot(actor);
        self.requests.apply(Op {
            dot: Dot::new(actor, requests),
            dir: Dir::Neg,
        });
    }

    /// No actor has live requests (expiries never exceed requests per actor).
    pub fn is_idle(&self) -> bool {
        self.requests.read() == BigInt::from(0)
    }

    /// Net change to the bucket's capacity: minus the requests not yet expired.
    pub fn tokens(&self) -> BigInt {
        let val = -self.requests.read();
        debug!("Calculating tokens {}", val);
        val
    }
}

// Operation-based CRDT implementation
impl CmRDT for DistributedRequestCounter {
    type Op = (crdts::pncounter::Op<ActorId>, VClock<ActorId>);
    type Validation = <PNCounter<ActorId> as CmRDT>::Validation;

    fn apply(&mut self, op: Self::Op) {
        self.requests.apply(op.0);
        self.vclock.merge(op.1);
    }

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        self.requests.validate_op(&op.0)?;
        self.vclock.validate_merge(&op.1)?;
        Ok(())
    }
}

// State-based CRDT merge implementation
impl CvRDT for DistributedRequestCounter {
    type Validation = <PNCounter<ActorId> as CvRDT>::Validation;

    fn merge(&mut self, other: Self) {
        self.requests.merge(other.requests);
        self.vclock.merge(other.vclock);
    }

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        self.requests.validate_merge(&other.requests)?;
        self.vclock.validate_merge(&other.vclock)?;
        Ok(())
    }
}

impl ResetRemove<ActorId> for DistributedRequestCounter {
    fn reset_remove(&mut self, clock: &VClock<ActorId>) {
        self.requests.reset_remove(clock);
        self.vclock.reset_remove(clock);
    }
}

impl Default for DistributedRequestCounter {
    fn default() -> Self {
        Self::new(NodeId::default())
    }
}

/// Internal operation entry for tracking requests with timestamps and vector clocks
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
enum InternalPnCounterOp {
    Requests(u64),
}

/// A request this node admitted and has not yet aged out. Local only.
#[derive(Clone, Debug, PartialEq, Eq)]
struct InternalRequestEntry {
    op: InternalPnCounterOp,
    timestamp_ms: i64,
}

/// External representation for gossip protocol
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedBucketExternal {
    pub client_id: String,
    pub node_id: NodeId,
    pub counter: DistributedRequestCounter,
    /// How long each actor's slot has been unchanged at the sender (ms), so a
    /// receiver doesn't restart the countdown to retirement.
    pub quiet_ms: BTreeMap<ActorId, u32>,
}

impl DistributedBucketExternal {
    /// Adopt a gossiped bucket as `local` (the receiving limiter's actor).
    /// `max_calls` is the local capacity; it is never written into the CRDT.
    fn bucket(&self, local: ActorId, max_calls: u32) -> DistributedBucket {
        let mut counter = self.counter.clone();
        counter.expire_untracked(local, 0);
        let now = monotonic_ms();
        DistributedBucket {
            node_id: self.node_id,
            writer: local,
            max_calls,
            last_changed: counter.actors().map(|a| (a, now - self.quiet(a))).collect(),
            counter,
            requests: Vec::new(),
            // Watermark at the incoming state so a release still gets gossiped.
            last_gossiped_vclock: self.counter.vclock.clone(),
        }
    }

    fn quiet(&self, actor: ActorId) -> i64 {
        let quiet = self.quiet_ms.get(&actor).copied().unwrap_or(0);
        i64::from(quiet).min(monotonic_ms())
    }
}

#[derive(Clone, Debug)]
struct DistributedBucket {
    /// Origin identity: the node that first created this bucket.
    pub node_id: NodeId,
    /// The local actor: the only slot this node writes.
    pub writer: ActorId,
    /// Capacity, a local constant; `tokens_to_u32` adds it to the CRDT's net count.
    pub max_calls: u32,
    pub counter: DistributedRequestCounter,
    // internal state only: timestamps
    requests: Vec<InternalRequestEntry>,
    // vclock at the time this bucket was last included in a gossip batch
    last_gossiped_vclock: VClock<ActorId>,
    /// When each other actor's slot last changed here (monotonic ms).
    last_changed: BTreeMap<ActorId, i64>,
}

impl DistributedBucket {
    fn created(max_calls: u32, writer: ActorId) -> Self {
        Self {
            node_id: writer.node,
            writer,
            max_calls,
            counter: DistributedRequestCounter::new(writer.node),
            requests: Vec::new(),
            last_gossiped_vclock: VClock::new(),
            last_changed: BTreeMap::new(),
        }
    }

    /// Requests this node made that have not aged out yet.
    fn tracked_requests(&self) -> u64 {
        self.requests
            .iter()
            .map(|entry| match entry.op {
                InternalPnCounterOp::Requests(steps) => steps,
            })
            .sum()
    }

    pub fn has_updates_since_last_gossip(&self) -> bool {
        self.counter.vclock > self.last_gossiped_vclock
    }

    pub fn mark_gossiped(&mut self) {
        self.last_gossiped_vclock = self.counter.vclock.clone();
    }

    /// Merge a peer's copy, noting which other actors' slots changed.
    fn merge_from(&mut self, incoming: &DistributedBucketExternal) {
        let before: Vec<(ActorId, (u64, u64))> = incoming
            .counter
            .actors()
            .map(|a| (a, self.counter.slot(a)))
            .collect();
        self.counter.merge(incoming.counter.clone());
        let now = monotonic_ms();
        for (actor, slot) in before {
            if actor != self.writer && self.counter.slot(actor) != slot {
                // The new values last changed at the sender `quiet` ms ago.
                let changed = now - incoming.quiet(actor);
                let entry = self.last_changed.entry(actor).or_insert(changed);
                *entry = (*entry).max(changed);
            }
        }
        let tracked = self.tracked_requests();
        self.counter.expire_untracked(self.writer, tracked);
    }

    /// Expire other actors' live requests once their slot has been unchanged here
    /// for longer than the window: every request it counts is then older than the
    /// window. Releases requests of crashed, restarted or removed nodes.
    fn retire_stale(&mut self, window_ms: i64) {
        let now = monotonic_ms();
        let stale: Vec<ActorId> = self
            .counter
            .actors()
            .filter(|&a| {
                a != self.writer
                    && self.counter.outstanding(a) > 0
                    && now - self.last_changed.get(&a).copied().unwrap_or(now) > window_ms
            })
            .collect();
        for actor in stale {
            self.counter.retire(actor);
        }
    }

    /// Forget the idle slots of processes known to be dead: a node's
    /// incarnations older than the newest seen. Merges may re-add them.
    fn compact(&mut self, newest: &BTreeMap<NodeId, u64>) {
        let dead: Vec<ActorId> = self
            .counter
            .actors()
            .filter(|a| {
                newest.get(&a.node).is_some_and(|&n| n > a.incarnation)
                    && self.counter.outstanding(*a) == 0
            })
            .collect();
        for actor in dead {
            self.counter.forget(actor);
            let mut clock = VClock::new();
            clock.dots.insert(actor, u64::MAX);
            self.last_gossiped_vclock.reset_remove(&clock);
            self.last_changed.remove(&actor);
        }
    }

    pub fn expire_entries(&mut self, expiration_threshold_ms: i64) {
        let now_ms = monotonic_ms();
        self.requests
            .retain(|entry| now_ms - entry.timestamp_ms <= expiration_threshold_ms);
        let tracked = self.tracked_requests();
        self.counter.expire_untracked(self.writer, tracked);
    }

    pub fn to_external(&self, client_id: &str) -> DistributedBucketExternal {
        let now = monotonic_ms();
        let quiet_ms = self
            .counter
            .actors()
            .map(|a| {
                let changed = if a == self.writer {
                    // Our own slot last changed with our newest tracked request.
                    self.requests.iter().map(|e| e.timestamp_ms).max()
                } else {
                    self.last_changed.get(&a).copied()
                };
                let quiet =
                    changed.map_or(if a == self.writer { i64::MAX } else { 0 }, |t| now - t);
                // Saturating only understates the age, which delays retirement.
                (a, quiet.clamp(0, i64::from(u32::MAX)) as u32)
            })
            .collect();
        DistributedBucketExternal {
            client_id: client_id.to_string(),
            node_id: self.node_id,
            counter: self.counter.clone(),
            quiet_ms,
        }
    }

    pub fn vclock(&self) -> VClock<ActorId> {
        self.counter.vclock.clone()
    }
}

impl Bucket for DistributedBucket {
    fn new(max_calls: u32, node_id: NodeId) -> Self {
        Self::created(max_calls, node_id.into())
    }

    /// Return tokens by ageing out this node's requests older than the
    /// rate-limit interval: the bucket is a sliding window, and expiry is the
    /// only way tokens come back.
    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self {
        let expiration_threshold_ms =
            (rate_limit_settings.rate_limit_interval_seconds * 1000 + 1) as i64;
        self.expire_entries(expiration_threshold_ms);
        self
    }

    fn decrement(&mut self) -> &mut Self {
        self.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: monotonic_ms(),
        });
        self.counter.inc_request(self.writer);
        self
    }

    fn check_if_allowed(&self) -> bool {
        self.tokens_to_u32() >= 1
    }

    fn tokens_to_u32(&self) -> u32 {
        (self.counter.tokens() + BigInt::from(self.max_calls))
            .clamp(BigInt::from(0), BigInt::from(u32::MAX))
            .to_u32()
            .unwrap_or(0)
    }
}

/// The newest incarnation seen of each node, shared by a process's limiters.
/// Older incarnations are dead processes.
#[derive(Clone, Debug, Default)]
pub struct Incarnations(Arc<Mutex<BTreeMap<NodeId, u64>>>);

impl Incarnations {
    pub fn note(&self, actor: ActorId) {
        let mut newest = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let entry = newest.entry(actor.node).or_insert(actor.incarnation);
        *entry = (*entry).max(actor.incarnation);
    }

    pub fn is_dead(&self, actor: ActorId) -> bool {
        self.snapshot()
            .get(&actor.node)
            .is_some_and(|&n| n > actor.incarnation)
    }

    fn snapshot(&self) -> BTreeMap<NodeId, u64> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Distributed rate limiter using CRDT buckets
#[derive(Clone, Debug)]
pub struct DistributedBucketLimiter {
    pub node_id: NodeId,
    /// This process's actor: the slot it writes in every bucket.
    pub actor: ActorId,
    node_counters: HashMap<String, DistributedBucket>,
    rate_limit_settings: settings::RateLimitSettings,
    /// Highest own-slot value in any bucket this node dropped; new slots start above it.
    dropped_high: Arc<AtomicU64>,
    incarnations: Incarnations,
}

impl DistributedBucketLimiter {
    pub fn new(
        actor: impl Into<ActorId>,
        rate_limit_settings: settings::RateLimitSettings,
    ) -> Self {
        Self::with_incarnations(actor, rate_limit_settings, Incarnations::default())
    }

    /// Like `new`, sharing what is known about incarnations with other limiters.
    pub fn with_incarnations(
        actor: impl Into<ActorId>,
        rate_limit_settings: settings::RateLimitSettings,
        incarnations: Incarnations,
    ) -> Self {
        let actor = actor.into();
        incarnations.note(actor);
        Self {
            node_id: actor.node,
            actor,
            node_counters: HashMap::new(),
            rate_limit_settings,
            dropped_high: Arc::new(AtomicU64::new(0)),
            incarnations,
        }
    }

    fn expiration_threshold_ms(&self) -> i64 {
        (self.rate_limit_settings.rate_limit_interval_seconds * 1000 + 1) as i64
    }

    /// Before writing with nothing live, restart the own slot above anything it
    /// held before, including in buckets this process dropped.
    fn rebase_if_unused(&self, bucket: &mut DistributedBucket) {
        if bucket.requests.is_empty() {
            let floor = self.dropped_high.load(Ordering::Relaxed) + 1;
            let base = floor.max(bucket.counter.own_high(self.actor) + 1);
            bucket.counter.rebase_own_slot(self.actor, base);
        }
    }

    pub fn get_settings(&self) -> &settings::RateLimitSettings {
        &self.rate_limit_settings
    }

    pub fn check_calls_remaining_for_client(&self, key: &String) -> u32 {
        self.node_counters
            .pin()
            .get(key)
            .map(|counter| counter.tokens_to_u32())
            .unwrap_or(self.rate_limit_settings.rate_limit_max_calls_allowed)
    }

    /// Drop idle buckets (nothing live anywhere) whose changes have already been gossiped.
    pub fn expire_keys(&mut self) {
        let guard = self.node_counters.pin();
        let idle: Vec<(String, u64)> = guard
            .iter()
            .filter(|(_, bucket)| {
                bucket.requests.is_empty()
                    && bucket.counter.is_idle()
                    && !bucket.has_updates_since_last_gossip()
            })
            .map(|(key, bucket)| (key.clone(), bucket.counter.own_high(self.actor)))
            .collect();
        for (client, own_high) in idle {
            self.dropped_high.fetch_max(own_high, Ordering::Relaxed);
            guard.remove(&client);
        }
    }

    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        let guard = self.node_counters.pin();
        let bucket = guard.update_or_insert_with(
            key.clone(),
            |existing: &DistributedBucket| {
                let mut bucket = existing.clone();
                bucket.add_tokens_to_bucket(&self.rate_limit_settings);
                bucket
            },
            || {
                DistributedBucket::created(
                    self.rate_limit_settings.rate_limit_max_calls_allowed,
                    self.actor,
                )
            },
        );
        // Only an admitted request writes, so only it rebases the slot.
        if bucket.check_if_allowed() {
            guard
                .update(key, |b| {
                    let mut b = b.clone();
                    self.rebase_if_unused(&mut b);
                    b.decrement();
                    b
                })
                .map(|b| b.tokens_to_u32())
        } else {
            None
        }
    }

    /// Buckets with changes not yet gossiped; advances each one's gossip watermark.
    pub fn gossip_delta_state(&self) -> Vec<DistributedBucketExternal> {
        let guard = self.node_counters.pin();
        let result: Vec<DistributedBucketExternal> = guard
            .iter()
            .filter(|(_, bucket)| bucket.has_updates_since_last_gossip())
            .map(|(client_id, bucket)| bucket.to_external(client_id))
            .collect();
        for ext in &result {
            guard.update(ext.client_id.clone(), |bucket| {
                let mut b = bucket.clone();
                b.mark_gossiped();
                b
            });
        }
        result
    }

    pub fn client_delta_state_for_gossip(
        &self,
        client_id: &String,
    ) -> Option<DistributedBucketExternal> {
        self.node_counters
            .pin()
            .get(client_id)
            .map(|counter| counter.to_external(client_id))
    }

    /// Every bucket, for anti-entropy. Does not advance gossip watermarks.
    pub fn full_state_dump(&self) -> Vec<DistributedBucketExternal> {
        self.node_counters
            .pin()
            .iter()
            .map(|(client_id, bucket)| bucket.to_external(client_id))
            .collect()
    }

    pub fn accept_delta_state(&mut self, delta: &[DistributedBucketExternal]) {
        let local = self.actor;
        for actor in delta.iter().flat_map(|b| b.counter.actors()) {
            self.incarnations.note(actor);
        }
        for incoming_bucket in delta.iter() {
            // Don't resurrect idle buckets.
            let known = self
                .node_counters
                .pin()
                .contains_key(&incoming_bucket.client_id);
            if !known && incoming_bucket.counter.is_idle() {
                continue;
            }
            self.node_counters.pin().update_or_insert_with(
                incoming_bucket.client_id.clone(),
                |existing| {
                    let mut bucket = existing.clone();
                    bucket.merge_from(incoming_bucket);
                    // Absorb the peer's dots so this node doesn't re-broadcast
                    // them; its own unsent writes stay above the watermark.
                    bucket
                        .last_gossiped_vclock
                        .merge(incoming_bucket.counter.vclock.clone());
                    bucket
                },
                || {
                    incoming_bucket
                        .bucket(local, self.rate_limit_settings.rate_limit_max_calls_allowed)
                },
            );
        }
    }

    /// Age out this node's expired requests, and retire other actors' stale
    /// ones, in every bucket. Runs on each gossip tick.
    pub fn tick_expirations(&self) {
        let threshold = self.expiration_threshold_ms();
        let newest = self.incarnations.snapshot();
        let guard = self.node_counters.pin();
        let keys: Vec<String> = guard.iter().map(|(k, _)| k.clone()).collect();
        for key in keys {
            guard.update(key, |b| {
                let mut b = b.clone();
                b.expire_entries(threshold);
                b.retire_stale(threshold);
                b.compact(&newest);
                b
            });
        }
    }

    /// Every bucket's vclock summed per actor, for heartbeats. (A max would let
    /// one client's count hide another client's missing write.)
    pub fn get_latest_updated_vclock(&self) -> VClock<ActorId> {
        self.node_counters
            .pin()
            .values()
            .fold(VClock::new(), |mut acc, bucket| {
                for (actor, count) in bucket.vclock().dots {
                    let total = acc.dots.entry(actor).or_insert(0);
                    *total = total.saturating_add(count);
                }
                acc
            })
    }

    /// For tests: (live requests in own slot, requests tracked). The first is at most
    /// the second; less only when peers retired requests this node has yet to age.
    #[doc(hidden)]
    pub fn own_slot_audit(&self, client: &str) -> Option<(u64, u64)> {
        self.node_counters.pin().get(client).map(|bucket| {
            (
                bucket.counter.outstanding(self.actor),
                bucket.tracked_requests(),
            )
        })
    }

    /// For tests: the actors with a slot in `client`'s bucket.
    #[doc(hidden)]
    pub fn bucket_actors(&self, client: &str) -> Option<Vec<ActorId>> {
        self.node_counters
            .pin()
            .get(client)
            .map(|bucket| bucket.counter.actors().collect())
    }

    pub fn is_empty(&self) -> bool {
        self.node_counters.pin().is_empty()
    }

    pub fn len(&self) -> usize {
        self.node_counters.pin().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crdts::CvRDT;
    use num_bigint::BigInt;
    use std::thread;
    use std::time::Duration;

    use crate::node::NodeName;

    fn test_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 1,
        }
    }

    fn node_id() -> NodeId {
        NodeName::from("a").node_id()
    }

    #[test]
    fn test_counter_basic_operations() {
        let node_id = node_id();
        let mut counter = DistributedRequestCounter::new(node_id);

        // Should start with 0 tokens
        assert_eq!(counter.tokens(), BigInt::from(0));

        // Each request consumes one token from the bucket's capacity
        counter.inc_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(-1));

        counter.inc_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(-2));
    }

    #[test]
    fn test_counter_crdt_merge() {
        let node1 = node_id();
        let node2 = NodeName::from("b").node_id();

        let mut counter1 = DistributedRequestCounter::new(node1);
        let mut counter2 = DistributedRequestCounter::new(node2);

        // Each counter operates independently
        counter1.inc_request(node1);

        counter2.inc_request(node2);
        counter2.inc_request(node2);

        assert_eq!(counter1.tokens(), BigInt::from(-1));
        assert_eq!(counter2.tokens(), BigInt::from(-2));

        // After merge: requests from distinct writers add up
        counter1.merge(counter2.clone());
        assert_eq!(counter1.tokens(), BigInt::from(-3));
    }

    // === Bucket-Level Tests ===

    #[test]
    fn test_bucket_rate_limiting() {
        let node_id = node_id();
        let mut bucket = DistributedBucket::new(1, node_id);

        // Start with 1 token should be allowed
        assert!(bucket.check_if_allowed());

        // Consume the token (seed no longer stored in CRDT; counter starts at 0)
        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(-1));

        // Should not be allowed anymore (tokens_to_u32 = counter.tokens + max_calls = -1 + 1 = 0)
        assert!(!bucket.check_if_allowed());

        // Decrementing goes below 0 (consistent with TokenBucket behavior)
        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(-2));
    }

    #[test]
    fn test_bucket_tokens_return_only_by_expiry() {
        let node_id = node_id();
        let mut bucket = DistributedBucket::new(2, node_id);
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 2,
            rate_limit_interval_seconds: 1,
        };

        bucket.decrement();
        thread::sleep(Duration::from_millis(100));
        bucket.add_tokens_to_bucket(&settings);
        // Elapsed time alone adds nothing: the request is still in the window.
        assert_eq!(bucket.tokens_to_u32(), 1);

        // Once the request ages out of the window its token comes back,
        // but never beyond capacity.
        bucket.requests[0].timestamp_ms = monotonic_ms() - 5000;
        bucket.add_tokens_to_bucket(&settings);
        assert_eq!(bucket.tokens_to_u32(), 2);
    }

    #[test]
    fn test_bucket_expiration() {
        let node_id = node_id();
        let mut bucket = DistributedBucket::new(1000, node_id);

        // Add entries with different ages
        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: monotonic_ms() - 2000, // Old
        });

        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: monotonic_ms() - 200, // Recent
        });

        assert_eq!(bucket.requests.len(), 2);

        // Expire entries older than 1 second
        bucket.expire_entries(1000);

        // Should have removed the old entry only
        assert_eq!(bucket.requests.len(), 1);
    }

    // === Limiter-Level Tests ===

    #[test]
    fn test_limiter_basic_rate_limiting() {
        let node_id = node_id();
        let settings = test_settings();
        let mut limiter = DistributedBucketLimiter::new(node_id, settings);

        let client_id = "test_client".to_string();

        // First request should succeed
        let result = limiter.limit_calls_for_client(client_id.clone());
        assert!(result.is_some());
        assert_eq!(limiter.len(), 1);

        // Check remaining calls
        let remaining = limiter.check_calls_remaining_for_client(&client_id);
        assert!(remaining > 0);

        // Unknown client should report full limit
        let unknown_remaining = limiter.check_calls_remaining_for_client(&"unknown".to_string());
        assert_eq!(unknown_remaining, 10); // From test_settings
    }

    #[test]
    fn test_limiter_gossip_protocol() {
        let node1 = node_id();
        let node2 = NodeName::from("b").node_id();
        let settings = test_settings();

        let mut limiter1 = DistributedBucketLimiter::new(node1, settings.clone());
        let mut limiter2 = DistributedBucketLimiter::new(node2, settings);

        let client_id = "shared_client".to_string();

        // Both nodes handle requests for same client
        limiter1.limit_calls_for_client(client_id.clone());
        limiter2.limit_calls_for_client(client_id.clone());

        // Get gossip state — node2 made a rate-limit call so it must have a delta
        let gossip_from_node2 = limiter2.gossip_delta_state();
        assert!(
            !gossip_from_node2.is_empty(),
            "node2 should have a delta after rate-limiting"
        );

        limiter1.accept_delta_state(&gossip_from_node2);

        // After merge, node1 sees node2's request: tokens consumed on both nodes
        let tokens = limiter1.check_calls_remaining_for_client(&client_id);
        assert!(tokens > 0);

        // A second call to gossip_delta_state should return empty (watermark is current)
        let gossip_from_node2_again = limiter2.gossip_delta_state();
        assert!(
            gossip_from_node2_again.is_empty(),
            "no new operations since last gossip"
        );
    }

    #[test]
    fn test_limiter_key_expiration() {
        let node_id = node_id();
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 100,
            rate_limit_interval_seconds: 1, // Short interval for testing
        };
        let mut limiter = DistributedBucketLimiter::new(node_id, settings.clone());

        // Test that empty limiter handles expiration correctly
        assert_eq!(limiter.len(), 0);
        limiter.expire_keys(); // Should not panic
        assert_eq!(limiter.len(), 0);

        // Create a client bucket and make multiple calls to ensure it has activity
        limiter.limit_calls_for_client("test_client".to_string());
        // Make another call to add more activity/timestamps
        thread::sleep(Duration::from_millis(10)); // Small delay to ensure timestamp difference
        limiter.limit_calls_for_client("test_client".to_string());
        assert_eq!(limiter.len(), 1);

        // Recent buckets should not expire (expire_keys uses 2x interval threshold)
        limiter.expire_keys();
        assert_eq!(limiter.len(), 1);

        // Add foreign node data via gossip
        let foreign_node = NodeName::from("z").node_id();
        let mut foreign_limiter = DistributedBucketLimiter::new(foreign_node, settings.clone());
        foreign_limiter.limit_calls_for_client("foreign_client".to_string());

        let gossip_state = foreign_limiter.gossip_delta_state();
        assert!(
            !gossip_state.is_empty(),
            "foreign_limiter should have a delta after rate-limiting"
        );
        limiter.accept_delta_state(&gossip_state);

        // limiter now holds both "test_client" (local) and "foreign_client" (gossiped in)
        assert_eq!(limiter.len(), 2);

        // Should not expire either bucket (both are recent)
        limiter.expire_keys();
        assert_eq!(limiter.len(), 2);
    }

    #[test]
    fn test_tick_expirations_ages_idle_buckets() {
        // Regression: an idle bucket's ops must age out via the
        // periodic tick alone — `add_tokens_to_bucket` is never called on a
        // bucket that has no new traffic, so without a background tick the
        // un-aged entries sit in the CRDT forever.
        let node_id = node_id();
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 1,
        };
        let limiter = DistributedBucketLimiter::new(node_id, settings);
        let client_id = "idle_client".to_string();

        // Stage a bucket that already has a stale op whose timestamp predates
        // the expiration threshold. This avoids a 1+ second sleep in the test.
        let threshold_ms: i64 = 1001;
        {
            let guard = limiter.node_counters.pin();
            let mut bucket = DistributedBucket::new(10, node_id);
            bucket.counter.inc_request(node_id);
            bucket.requests.push(InternalRequestEntry {
                op: InternalPnCounterOp::Requests(1),
                timestamp_ms: monotonic_ms() - threshold_ms - 500,
            });
            guard.insert(client_id.clone(), bucket);
        }

        // Sanity: the staged decrement is visible before the tick.
        {
            let guard = limiter.node_counters.pin();
            let bucket = guard.get(&client_id).unwrap();
            assert_eq!(bucket.counter.tokens(), BigInt::from(-1));
            assert_eq!(bucket.requests.len(), 1);
        }

        // No user traffic on this client — just a background tick.
        limiter.tick_expirations();

        // The stale op should be gone from the ageing log and the CRDT should
        // have been compensated back to zero via the local writer slot.
        let guard = limiter.node_counters.pin();
        let bucket = guard.get(&client_id).unwrap();
        assert_eq!(
            bucket.requests.len(),
            0,
            "tick_expirations should have removed the expired entry"
        );
        assert_eq!(
            bucket.counter.tokens(),
            BigInt::from(0),
            "tick_expirations should have compensated the CRDT counter"
        );
    }

    // === CRDT Properties Tests ===

    #[test]
    fn test_crdt_properties() {
        let node1 = node_id();
        let node2 = NodeName::from("b").node_id();

        let mut counter_a = DistributedRequestCounter::new(node1);
        let mut counter_b = DistributedRequestCounter::new(node2);

        counter_a.inc_request(node1);
        counter_a.inc_request(node1);
        counter_b.inc_request(node2);

        // Commutative: a + b = b + a
        let mut ab = counter_a.clone();
        ab.merge(counter_b.clone());

        let mut ba = counter_b.clone();
        ba.merge(counter_a.clone());

        assert_eq!(ab.tokens(), ba.tokens());

        // Idempotent: a + a = a
        let original_tokens = ab.tokens();
        ab.merge(ab.clone());
        assert_eq!(ab.tokens(), original_tokens);
    }

    #[test]
    fn test_multi_node_convergence() {
        // Test that multiple nodes converge to same state after gossip
        let settings = test_settings();

        let mut limiter1 = DistributedBucketLimiter::new(node_id(), settings.clone());
        let mut limiter2 =
            DistributedBucketLimiter::new(NodeName::from("b").node_id(), settings.clone());
        let mut limiter3 = DistributedBucketLimiter::new(NodeName::from("c").node_id(), settings);

        let client_id = "shared_client".to_string();

        // Each node processes requests for same client
        limiter1.limit_calls_for_client(client_id.clone());
        limiter2.limit_calls_for_client(client_id.clone());
        limiter3.limit_calls_for_client(client_id.clone());

        // Full mesh gossip where each node receives updates from all others
        let gossip1 = limiter1.gossip_delta_state();
        let gossip2 = limiter2.gossip_delta_state();
        let gossip3 = limiter3.gossip_delta_state();

        // Apply all gossip to all nodes
        limiter1.accept_delta_state(&gossip2);
        limiter1.accept_delta_state(&gossip3);

        limiter2.accept_delta_state(&gossip1);
        limiter2.accept_delta_state(&gossip3);

        limiter3.accept_delta_state(&gossip1);
        limiter3.accept_delta_state(&gossip2);

        // All nodes should converge to same state
        let tokens1 = limiter1.check_calls_remaining_for_client(&client_id);
        let tokens2 = limiter2.check_calls_remaining_for_client(&client_id);
        let tokens3 = limiter3.check_calls_remaining_for_client(&client_id);

        assert_eq!(tokens1, tokens2);
        assert_eq!(tokens2, tokens3);
        assert!(tokens1 > 0); // Should still have tokens after 3 requests
    }

    #[test]
    fn test_external_serialization() {
        let node_id = node_id();
        let bucket = DistributedBucket::new(10, node_id);

        // Convert to external format (for gossip)
        let external = bucket.to_external("test_client");
        assert_eq!(external.client_id, "test_client");
        assert_eq!(external.node_id, node_id);

        // Convert back to internal format (as if adopted by the same node).
        let restored_bucket = external.bucket(node_id.into(), 10);
        assert_eq!(restored_bucket.node_id, node_id);
        assert_eq!(restored_bucket.writer, node_id.into());
        assert_eq!(restored_bucket.counter.tokens(), bucket.counter.tokens());
    }

    /// Regression test for safety issue: writer identity.
    ///
    /// When a bucket is adopted from a gossip peer, subsequent local mutations
    /// must attribute their CRDT writes to the LOCAL node's actor slot, not the
    /// sender's. Under concurrent writes at both nodes, the GCounter max-merge
    /// would otherwise silently drop one side's updates.
    #[test]
    fn test_adopted_bucket_writes_do_not_clobber_origin_slot() {
        let node_a = node_id();
        let node_b = NodeName::from("b").node_id();

        // Node A creates a bucket for a client and consumes one token.
        // Use the bucket API directly (not the limiter) so wall-clock expiry
        // can't contaminate the exact-count assertion below.
        let mut bucket_a = DistributedBucket::new(100, node_a);
        bucket_a.decrement(); // A local: 1 request

        // Simulate B adopting A's bucket via gossip. Before the safety bug fix this
        // step would copy A's `counter.node_id` into B's bucket and silently
        // route all of B's future writes into slot A of the PNCounter.
        let external = bucket_a.to_external("test_client");
        let mut bucket_b = external.bucket(node_b.into(), 100);

        // Post-adoption invariants: origin preserved for GC, writer is local.
        assert_eq!(bucket_b.node_id, node_a, "origin must be preserved");
        assert_eq!(
            bucket_b.writer,
            ActorId::from(node_b),
            "writer must be the local (adopting) node"
        );

        // Concurrent writes: both A and B process additional requests against
        // their own replicas before any further gossip.
        bucket_a.decrement(); // A local: 2 total
        bucket_a.decrement(); // A local: 3 total
        bucket_b.decrement(); // B local: 1 new request (on top of the adopted state)

        // Gossip B's state back to A and merge.
        let external_from_b = bucket_b.to_external("test_client");
        bucket_a.counter.merge(external_from_b.counter);

        // Total requests counted across the cluster:
        //   1 (A's original, already in both replicas) + 2 (A's new) + 1 (B's new) = 4
        // With capacity 100, net tokens after merge must be 100 - 4 = 96.
        //
        // With safety bug present, B's decrement landed in slot A. Node A's own slot A
        // reached 3 requests locally, while B's slot A reached 2 (1 inherited + 1 new).
        // merge = max(3, 2) = 3 → buggy net tokens = 97.
        let tokens_after_merge = bucket_a.tokens_to_u32();
        assert_eq!(
            tokens_after_merge, 96,
            "expected 96 tokens after 3 writes from A and 1 from B; \
             got {}. An off-by-one here is the regression signature.",
            tokens_after_merge
        );

        // Symmetric check: merging A's post-mutation state into B should yield
        // the same total (idempotent, commutative).
        let external_from_a = bucket_a.to_external("test_client");
        bucket_b.counter.merge(external_from_a.counter);
        assert_eq!(
            bucket_b.tokens_to_u32(),
            96,
            "both replicas must converge to the same token count"
        );
    }

    /// Regression test for gossip merge cascades: accepting gossip from a peer must NOT cause
    /// the receiver to re-broadcast that same state in the next gossip tick.
    ///
    /// Before the fix, `accept_delta_state` never advanced `last_gossiped_vclock`,
    /// so every merge left `counter.vclock > last_gossiped_vclock`, which made
    /// `has_updates_since_last_gossip` return true for every merged bucket,
    /// triggering a re-broadcast cascade each tick.
    #[test]
    fn test_accepted_delta_does_not_cause_rebroadcast() {
        let node_a = node_id();
        let node_b = NodeName::from("b").node_id();
        let settings = test_settings();

        let mut limiter_a = DistributedBucketLimiter::new(node_a, settings.clone());
        let mut limiter_b = DistributedBucketLimiter::new(node_b, settings);

        // Node A processes a request so it has a delta to gossip.
        limiter_a.limit_calls_for_client("client_1".to_string());

        // Node B accepts A's delta.
        let delta_from_a = limiter_a.gossip_delta_state();
        assert!(!delta_from_a.is_empty(), "A should have a delta");
        limiter_b.accept_delta_state(&delta_from_a);

        // B has no local writes — it must produce an empty delta next tick.
        // Before the fix, merging A's state advanced counter.vclock past B's
        // (empty) last_gossiped_vclock, so B would re-broadcast A's data here.
        let delta_from_b = limiter_b.gossip_delta_state();
        assert!(
            delta_from_b.is_empty(),
            "receiver must not re-broadcast state it just accepted; \
             got {} bucket(s) — gossip merge cascades regression",
            delta_from_b.len()
        );

        // After a local write on B, B should produce a delta (its own write only).
        limiter_b.limit_calls_for_client("client_1".to_string());
        let delta_from_b_after_write = limiter_b.gossip_delta_state();
        assert!(
            !delta_from_b_after_write.is_empty(),
            "B must gossip after a local write"
        );

        // --- Merge path (bucket already exists in the receiver) ---
        //
        // The insert-when-absent case above is handled by `bucket()` setting
        // last_gossiped_vclock to the incoming vclock. The merge case (bucket
        // already present) is a separate code path in update_or_insert_with and
        // requires the accept_delta_state fix to advance the watermark after merge.

        // Drain B's watermark so it's current again.
        let _ = limiter_b.gossip_delta_state();

        // A makes another request; B already has client_1 so the next accept will
        // take the UPDATE (merge) path, not the INSERT path.
        limiter_a.limit_calls_for_client("client_1".to_string());
        let delta_from_a_2 = limiter_a.gossip_delta_state();
        assert!(!delta_from_a_2.is_empty(), "A should have a second delta");
        limiter_b.accept_delta_state(&delta_from_a_2);

        // B again has no local writes — delta must be empty even though the merge
        // updated an existing bucket (the merge path of update_or_insert_with).
        let delta_from_b_after_merge = limiter_b.gossip_delta_state();
        assert!(
            delta_from_b_after_merge.is_empty(),
            "receiver must not re-broadcast after merging into an existing bucket; \
             got {} bucket(s) — S5/S6 merge-path regression",
            delta_from_b_after_merge.len()
        );
    }

    /// `full_state_dump` must return all buckets regardless of whether they have
    /// been gossiped since their last update, unlike `gossip_delta_state` which
    /// skips watermark-current buckets.
    #[test]
    fn test_full_state_dump_returns_all_buckets() {
        let node_a = node_id();
        let settings = test_settings();
        let mut limiter = DistributedBucketLimiter::new(node_a, settings);

        limiter.limit_calls_for_client("client_1".to_string());
        limiter.limit_calls_for_client("client_2".to_string());

        // After gossip_delta_state advances the watermarks, those buckets are
        // excluded from the next delta call.
        let delta = limiter.gossip_delta_state();
        assert_eq!(delta.len(), 2, "both buckets should appear in first delta");

        let delta_again = limiter.gossip_delta_state();
        assert_eq!(
            delta_again.len(),
            0,
            "no new ops since last gossip — delta should be empty"
        );

        // But full_state_dump must still return everything for anti-entropy.
        let dump = limiter.full_state_dump();
        assert_eq!(
            dump.len(),
            2,
            "full_state_dump must return all buckets regardless of watermark"
        );
        let client_ids: Vec<&str> = dump.iter().map(|b| b.client_id.as_str()).collect();
        assert!(client_ids.contains(&"client_1"));
        assert!(client_ids.contains(&"client_2"));
    }

    /// Anti-entropy: a node that received state via gossip and then went idle should
    /// be able to recover that state after a `StateRequest`/`full_state_dump` exchange.
    #[test]
    fn test_full_state_dump_used_for_anti_entropy_recovery() {
        let node_a = node_id();
        let node_b = NodeName::from("b").node_id();
        let settings = test_settings();

        let mut limiter_a = DistributedBucketLimiter::new(node_a, settings.clone());
        let mut limiter_b = DistributedBucketLimiter::new(node_b, settings);

        // Node A processes requests for two clients.
        limiter_a.limit_calls_for_client("alice".to_string());
        limiter_a.limit_calls_for_client("alice".to_string());
        limiter_a.limit_calls_for_client("bob".to_string());

        // Simulate UDP packet loss: node B never receives A's delta gossip.
        // Node B has no knowledge of alice or bob.
        assert_eq!(limiter_b.full_state_dump().len(), 0);

        // Anti-entropy: B sends a StateRequest; A responds with full_state_dump.
        // (In production this round-trip happens over UDP; here we test the
        // limiter-layer semantics directly.)
        let full_state = limiter_a.full_state_dump();
        assert_eq!(full_state.len(), 2, "A should have 2 clients");

        limiter_b.accept_delta_state(&full_state);

        // After recovery B should see both clients with the same token counts as A.
        let alice_a = limiter_a.check_calls_remaining_for_client(&"alice".to_string());
        let alice_b = limiter_b.check_calls_remaining_for_client(&"alice".to_string());
        assert_eq!(
            alice_a, alice_b,
            "alice's quota must match after anti-entropy"
        );

        let bob_a = limiter_a.check_calls_remaining_for_client(&"bob".to_string());
        let bob_b = limiter_b.check_calls_remaining_for_client(&"bob".to_string());
        assert_eq!(bob_a, bob_b, "bob's quota must match after anti-entropy");
    }

    /// Regression test: seed tokens must NOT be stored in the CRDT.
    ///
    /// Before the fix, `DistributedBucket::new` called `inc_refills(node_id, max_calls)`
    /// which wrote the initial capacity into the PNCounter. Those writes propagated
    /// via gossip and were never tracked in the ageing vec, so they could never
    /// be expired — immortal "ghost" tokens that accumulated with every new peer
    /// that adopted the bucket and then re-wrote their own seed.
    ///
    /// After the fix, `max_calls` is stored as a local field and added as a
    /// constant offset in `tokens_to_u32()`. The CRDT state for a freshly
    /// created bucket must be zero.
    #[test]
    fn test_seed_tokens_not_in_crdt() {
        let node_id = node_id();

        // A new bucket must have an empty CRDT (tokens == 0 in the counter).
        let bucket = DistributedBucket::new(100, node_id);
        assert_eq!(
            bucket.counter.tokens(),
            BigInt::from(0),
            "CRDT counter must start empty; seed tokens must not be written into the CRDT"
        );

        // The visible quota must still equal max_calls via the constant offset.
        assert_eq!(
            bucket.tokens_to_u32(),
            100,
            "tokens_to_u32 must return max_calls for a fresh bucket"
        );

        // When this bucket is serialised and adopted by a peer, the peer's copy
        // must also show zero tokens in the CRDT — no seed doubles on adoption.
        let node_b = NodeName::from("b").node_id();
        let external = bucket.to_external("client_a");
        let adopted = external.bucket(node_b.into(), 100);
        assert_eq!(
            adopted.counter.tokens(),
            BigInt::from(0),
            "adopted bucket must not gain extra CRDT tokens; seed doubling regression"
        );

        // Verify two-node convergence: after one request each the quota seen by
        // both replicas must be max_calls - 2, regardless of which node adopted
        // whose bucket.
        let mut bucket_a = DistributedBucket::new(100, node_id);
        bucket_a.decrement(); // A: 1 request

        let external_a = bucket_a.to_external("client_a");
        let mut bucket_b = external_a.bucket(node_b.into(), 100);
        bucket_b.decrement(); // B: 1 additional request

        // Merge B → A
        let external_b = bucket_b.to_external("client_a");
        bucket_a.counter.merge(external_b.counter);

        assert_eq!(
            bucket_a.tokens_to_u32(),
            98,
            "after 2 total requests quota must be 98 (100 - 2), not 198"
        );
    }

    /// Regression test for `get_latest_updated_vclock` returning the join of all
    /// bucket vclocks rather than the max of any single bucket vclock.
    ///
    /// When multiple clients have buckets written by *different* nodes, their vclocks
    /// are concurrent (each has dots the other lacks). The old implementation used
    /// `acc > bucket.vclock()` to fold, and because `>` is false for concurrent
    /// vclocks it dropped earlier actors on every step, producing a result that varied
    /// with HashMap iteration order. This made heartbeat comparisons spuriously
    /// `None` (concurrent) and triggered permanent anti-entropy `StateRequest` loops.
    ///
    /// The fix uses `acc.merge(vclock)` to compute the element-wise maximum (join),
    /// which always produces a stable vclock that covers every known actor slot.
    #[test]
    fn test_get_latest_updated_vclock_is_join_of_all_buckets() {
        let node_a = node_id();
        let node_b = NodeName::from("b").node_id();
        let node_c = NodeName::from("c").node_id();
        let settings = test_settings();

        let mut limiter = DistributedBucketLimiter::new(node_a, settings.clone());

        // Each client is processed by a different writer node, so their bucket
        // vclocks are concurrent (disjoint actor sets).
        let mut limiter_b = DistributedBucketLimiter::new(node_b, settings.clone());
        let mut limiter_c = DistributedBucketLimiter::new(node_c, settings);

        limiter.limit_calls_for_client("client_a".to_string()); // vclock {node_a: n}
        limiter_b.limit_calls_for_client("client_b".to_string()); // vclock {node_b: n}
        limiter_c.limit_calls_for_client("client_c".to_string()); // vclock {node_c: n}

        // Converge all state into `limiter`.
        limiter.accept_delta_state(&limiter_b.gossip_delta_state());
        limiter.accept_delta_state(&limiter_c.gossip_delta_state());

        // The summary vclock must cover all three actor slots.
        let summary = limiter.get_latest_updated_vclock();

        // The summary must dominate each individual bucket vclock.
        for ext in limiter.full_state_dump() {
            let bucket_vc = ext.counter.vclock;
            assert!(
                matches!(
                    summary.partial_cmp(&bucket_vc),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                "summary vclock must dominate every bucket vclock; \
                 summary={:?} did not dominate bucket={:?}",
                summary,
                bucket_vc
            );
        }

        // Specifically: the summary must not be concurrent with any peer's
        // individual bucket vclock. If it were, heartbeat comparisons would
        // produce None and trigger spurious StateRequests.
        let b_vc = limiter_b.get_latest_updated_vclock();
        let c_vc = limiter_c.get_latest_updated_vclock();

        assert!(
            summary.partial_cmp(&b_vc).is_some(),
            "summary must not be concurrent with node_b's vclock after convergence"
        );
        assert!(
            summary.partial_cmp(&c_vc).is_some(),
            "summary must not be concurrent with node_c's vclock after convergence"
        );
    }

    /// Regression (found by tests/hegel_gossip.rs): receiving a peer's delta for
    /// a client must not swallow this node's own not-yet-gossiped write for that
    /// client. The next delta must still carry it.
    #[test]
    fn test_accept_delta_keeps_unsent_local_writes_above_watermark() {
        let settings = test_settings();
        let mut local = DistributedBucketLimiter::new(node_id(), settings.clone());
        let mut peer = DistributedBucketLimiter::new(NodeName::from("b").node_id(), settings);

        peer.limit_calls_for_client("alice".to_string());
        local.limit_calls_for_client("alice".to_string());
        local.accept_delta_state(&peer.gossip_delta_state());

        let delta = local.gossip_delta_state();
        assert_eq!(
            delta.len(),
            1,
            "local write to alice must still be gossiped"
        );
        peer.accept_delta_state(&delta);
        assert_eq!(
            peer.check_calls_remaining_for_client(&"alice".to_string()),
            local.check_calls_remaining_for_client(&"alice".to_string()),
        );
    }

    /// A denied request must not produce gossip.
    #[test]
    fn test_denied_request_produces_no_delta() {
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 1,
            rate_limit_interval_seconds: 60,
        };
        let mut writer = DistributedBucketLimiter::new(node_id(), settings.clone());
        let mut peer = DistributedBucketLimiter::new(NodeName::from("b").node_id(), settings);

        assert!(writer.limit_calls_for_client("alice".to_string()).is_some());
        peer.accept_delta_state(&writer.gossip_delta_state());
        assert!(peer.gossip_delta_state().is_empty());

        assert!(peer.limit_calls_for_client("alice".to_string()).is_none());
        assert!(
            peer.gossip_delta_state().is_empty(),
            "a denied request must not change gossiped state"
        );
    }

    /// Regression (found by tests/hegel_gossip.rs): when a delta is lost, the
    /// heartbeat digest must still reveal the gap. With an element-wise-max
    /// digest, the writer's later dot on another client masked the missing one.
    #[test]
    fn test_heartbeat_digest_reveals_lost_delta() {
        let settings = test_settings();
        let mut writer = DistributedBucketLimiter::new(node_id(), settings.clone());
        let mut peer = DistributedBucketLimiter::new(NodeName::from("b").node_id(), settings);

        writer.limit_calls_for_client("alice".to_string());
        let _lost = writer.gossip_delta_state();
        writer.limit_calls_for_client("bob".to_string());
        peer.accept_delta_state(&writer.gossip_delta_state());

        let peer_digest = peer.get_latest_updated_vclock();
        let writer_digest = writer.get_latest_updated_vclock();
        assert_eq!(
            peer_digest.partial_cmp(&writer_digest),
            Some(std::cmp::Ordering::Less),
            "peer missing alice must see itself behind the writer: \
             peer={peer_digest:?} writer={writer_digest:?}"
        );
    }

    /// ===== Property tests for CRDT convergence under arbitrary interleaving =====
    ///
    /// The unit tests above each exercise one or two concrete op sequences, but
    /// the `DistributedBucket` layer adds ageing, seed-token removal, and
    /// writer-vs-origin identity that the upstream `crdts` crate tests do not
    /// cover. Generating random op sequences and asserting the CRDT laws hold at
    /// the `DistributedBucketLimiter` level gives us broad, adversarial coverage.
    ///
    /// Three properties are tested:
    ///   1. Convergence — full-mesh gossip terminates in agreement.
    ///   2. Idempotency — applying the same delta twice == applying it once.
    ///   3. Commutativity — the merge order of two independent deltas does not
    ///      matter.
    mod prop_tests {
        use super::*;
        use proptest::prelude::*;

        const CLIENTS: &[&str] = &["alice", "bob", "carol"];
        const NUM_NODES: usize = 3;

        fn prop_node_ids() -> Vec<NodeId> {
            (0..NUM_NODES)
                .map(|i| NodeName::from(format!("node-{i}").as_str()).node_id())
                .collect()
        }

        /// Long interval so no entries expire during a fast property-test run.
        /// `expire_entries` threshold = interval_seconds * 1000 + 1 ms.
        /// At 60 s that is 60 001 ms, well above the process-local monotonic
        /// clock value for any test that runs in under a minute.
        fn long_interval_settings() -> settings::RateLimitSettings {
            settings::RateLimitSettings {
                rate_limit_max_calls_allowed: 100,
                rate_limit_interval_seconds: 60,
            }
        }

        // After any sequence of rate-limit ops distributed across N nodes,
        // a single full-mesh gossip round must bring every node to identical
        // `check_calls_remaining_for_client` values for every client that
        // received at least one op.
        //
        // This is the primary regression test for S1-style violations: if
        // writes land in the wrong actor slot they get silently discarded on
        // merge and the nodes diverge.
        proptest! {
            #[test]
            fn prop_full_mesh_gossip_converges(
                ops in proptest::collection::vec(
                    (0usize..NUM_NODES, 0usize..CLIENTS.len()),
                    1..30usize,
                )
            ) {
                let settings = long_interval_settings();
                let ids = prop_node_ids();

                let mut limiters: Vec<DistributedBucketLimiter> = ids
                    .iter()
                    .map(|&id| DistributedBucketLimiter::new(id, settings.clone()))
                    .collect();

                for &(node_idx, client_idx) in &ops {
                    limiters[node_idx].limit_calls_for_client(CLIENTS[client_idx].to_string());
                }

                // Full-mesh gossip: each node pushes its current delta to every
                // other node. Collect all deltas before applying any so that the
                // merge order within a round does not affect the outcome.
                let deltas: Vec<Vec<DistributedBucketExternal>> = limiters
                    .iter()
                    .map(|l| l.gossip_delta_state())
                    .collect();
                for (i, delta) in deltas.iter().enumerate() {
                    if !delta.is_empty() {
                        for (j, limiter) in limiters.iter_mut().enumerate() {
                            if i != j {
                                limiter.accept_delta_state(delta);
                            }
                        }
                    }
                }

                // Assert convergence for every client that saw at least one op.
                for &(_, ci) in &ops {
                    let client_str = CLIENTS[ci].to_string();
                    let counts: Vec<u32> = limiters
                        .iter()
                        .map(|l| l.check_calls_remaining_for_client(&client_str))
                        .collect();
                    let first = counts[0];
                    for &c in &counts[1..] {
                        prop_assert_eq!(
                            c, first,
                            "convergence violated for '{}': node counts = {:?}",
                            CLIENTS[ci], counts
                        );
                    }
                }
            }
        }

        // `accept_delta_state` must be idempotent: applying the same delta a
        // second time must not change the merged state.
        proptest! {
            #[test]
            fn prop_accept_delta_is_idempotent(
                ops in proptest::collection::vec(0usize..CLIENTS.len(), 1..15usize)
            ) {
                let settings = long_interval_settings();
                let ids = prop_node_ids();

                let mut sender = DistributedBucketLimiter::new(ids[0], settings.clone());
                let mut recv_once = DistributedBucketLimiter::new(ids[1], settings.clone());
                let mut recv_twice = DistributedBucketLimiter::new(ids[2], settings.clone());

                for &ci in &ops {
                    sender.limit_calls_for_client(CLIENTS[ci].to_string());
                }
                let delta = sender.gossip_delta_state();

                recv_once.accept_delta_state(&delta);

                recv_twice.accept_delta_state(&delta);
                recv_twice.accept_delta_state(&delta); // second application must be a no-op

                for client in CLIENTS {
                    let client_str = client.to_string();
                    let once = recv_once.check_calls_remaining_for_client(&client_str);
                    let twice = recv_twice.check_calls_remaining_for_client(&client_str);
                    prop_assert_eq!(
                        once, twice,
                        "idempotency violated for '{}': once={}, twice={}",
                        client_str, once, twice
                    );
                }
            }
        }

        // When some gossip deltas are dropped (packet loss), anti-entropy —
        // simulated here as a full-state-dump round — must bring all nodes to
        // the same state regardless of which deltas were lost.
        //
        // delivery_matrix[i * N + j] is true when node i's delta reaches
        // node j in the lossy round; false means the packet was dropped.
        // A subsequent full-state-dump round (what the Heartbeat →
        // StateRequest → StateResponse loop does) must repair every gap.
        proptest! {
            #[test]
            fn prop_anti_entropy_recovers_after_packet_loss(
                ops in proptest::collection::vec(
                    (0usize..NUM_NODES, 0usize..CLIENTS.len()),
                    1..20usize,
                ),
                delivery_matrix in proptest::collection::vec(
                    proptest::bool::ANY,
                    NUM_NODES * NUM_NODES,
                ),
            ) {
                let settings = long_interval_settings();
                let ids = prop_node_ids();

                let mut limiters: Vec<DistributedBucketLimiter> = ids
                    .iter()
                    .map(|&id| DistributedBucketLimiter::new(id, settings.clone()))
                    .collect();

                for &(node_idx, client_idx) in &ops {
                    limiters[node_idx].limit_calls_for_client(CLIENTS[client_idx].to_string());
                }

                // Lossy first round: collect all deltas before any delivery so
                // that drop decisions are independent of merge order.
                let deltas: Vec<Vec<DistributedBucketExternal>> = limiters
                    .iter()
                    .map(|l| l.gossip_delta_state())
                    .collect();
                for i in 0..NUM_NODES {
                    if !deltas[i].is_empty() {
                        for j in 0..NUM_NODES {
                            if i != j && delivery_matrix[i * NUM_NODES + j] {
                                limiters[j].accept_delta_state(&deltas[i]);
                            }
                        }
                    }
                }

                // Anti-entropy recovery round: full state dump from every node.
                let full_states: Vec<Vec<DistributedBucketExternal>> = limiters
                    .iter()
                    .map(|l| l.full_state_dump())
                    .collect();
                for (i, state) in full_states.iter().enumerate() {
                    if !state.is_empty() {
                        for (j, limiter) in limiters.iter_mut().enumerate() {
                            if i != j {
                                limiter.accept_delta_state(state);
                            }
                        }
                    }
                }

                // After anti-entropy all nodes must agree.
                for &(_, ci) in &ops {
                    let client_str = CLIENTS[ci].to_string();
                    let counts: Vec<u32> = limiters
                        .iter()
                        .map(|l| l.check_calls_remaining_for_client(&client_str))
                        .collect();
                    let first = counts[0];
                    for &c in &counts[1..] {
                        prop_assert_eq!(
                            c, first,
                            "anti-entropy failed to recover '{}': node counts = {:?}",
                            CLIENTS[ci], counts
                        );
                    }
                }
            }
        }

        // After a bidirectional gossip exchange (A→B and B→A in one round),
        // both nodes must agree on all token counts AND neither node should
        // produce further deltas. A spurious re-broadcast here is the S5/S6
        // watermark loop; this test is the Tx3 regression guard.
        proptest! {
            #[test]
            fn prop_bidirectional_gossip_converges_without_rebroadcast(
                ops_a in proptest::collection::vec(0usize..CLIENTS.len(), 1..15usize),
                ops_b in proptest::collection::vec(0usize..CLIENTS.len(), 1..15usize),
            ) {
                let settings = long_interval_settings();
                let ids = prop_node_ids();

                let mut limiter_a = DistributedBucketLimiter::new(ids[0], settings.clone());
                let mut limiter_b = DistributedBucketLimiter::new(ids[1], settings.clone());

                for &ci in &ops_a {
                    limiter_a.limit_calls_for_client(CLIENTS[ci].to_string());
                }
                for &ci in &ops_b {
                    limiter_b.limit_calls_for_client(CLIENTS[ci].to_string());
                }

                // Collect both deltas before applying either — simultaneous exchange.
                let delta_a = limiter_a.gossip_delta_state();
                let delta_b = limiter_b.gossip_delta_state();

                limiter_a.accept_delta_state(&delta_b);
                limiter_b.accept_delta_state(&delta_a);

                // Convergence: both sides must agree on every client's count.
                for client in CLIENTS {
                    let client_str = client.to_string();
                    let a_count = limiter_a.check_calls_remaining_for_client(&client_str);
                    let b_count = limiter_b.check_calls_remaining_for_client(&client_str);
                    prop_assert_eq!(
                        a_count, b_count,
                        "bidirectional convergence violated for '{}': a={}, b={}",
                        client, a_count, b_count
                    );
                }

                // No re-broadcast loop: accept_delta_state must advance
                // last_gossiped_vclock so gossip_delta_state finds nothing new.
                let rebroadcast_a = limiter_a.gossip_delta_state();
                let rebroadcast_b = limiter_b.gossip_delta_state();
                prop_assert!(
                    rebroadcast_a.is_empty(),
                    "node A has unexpected re-broadcast delta: {:?}", rebroadcast_a
                );
                prop_assert!(
                    rebroadcast_b.is_empty(),
                    "node B has unexpected re-broadcast delta: {:?}", rebroadcast_b
                );
            }
        }

        // Merging deltas from two independent nodes must be commutative: the
        // token count after (A then B) must equal the count after (B then A).
        proptest! {
            #[test]
            fn prop_accept_delta_is_commutative(
                ops_a in proptest::collection::vec(0usize..CLIENTS.len(), 1..15usize),
                ops_b in proptest::collection::vec(0usize..CLIENTS.len(), 1..15usize),
            ) {
                let settings = long_interval_settings();
                let ids = prop_node_ids();

                let mut limiter_a = DistributedBucketLimiter::new(ids[0], settings.clone());
                let mut limiter_b = DistributedBucketLimiter::new(ids[1], settings.clone());

                for &ci in &ops_a {
                    limiter_a.limit_calls_for_client(CLIENTS[ci].to_string());
                }
                for &ci in &ops_b {
                    limiter_b.limit_calls_for_client(CLIENTS[ci].to_string());
                }

                let delta_a = limiter_a.gossip_delta_state();
                let delta_b = limiter_b.gossip_delta_state();

                // r_ab: merge A first, then B
                let mut r_ab = DistributedBucketLimiter::new(ids[2], settings.clone());
                r_ab.accept_delta_state(&delta_a);
                r_ab.accept_delta_state(&delta_b);

                // r_ba: merge B first, then A
                let mut r_ba = DistributedBucketLimiter::new(ids[2], settings.clone());
                r_ba.accept_delta_state(&delta_b);
                r_ba.accept_delta_state(&delta_a);

                for client in CLIENTS {
                    let client_str = client.to_string();
                    let ab = r_ab.check_calls_remaining_for_client(&client_str);
                    let ba = r_ba.check_calls_remaining_for_client(&client_str);
                    prop_assert_eq!(
                        ab, ba,
                        "commutativity violated for '{}': A∘B={}, B∘A={}",
                        client_str, ab, ba
                    );
                }
            }
        }
    }
}
