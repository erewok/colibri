//! Distributed token bucket using CRDT for eventual consistency
use std::sync::OnceLock;
use std::time::Instant;

use crdts::{CmRDT, CvRDT, PNCounter, ResetRemove, VClock};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use papaya::HashMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::limiters::token_bucket::Bucket;
use crate::node::NodeId;
use crate::settings;

/// Monotonic millisecond clock for all elapsed-time decisions in `DistributedBucket`.
///
/// Backed by `std::time::Instant`, which is guaranteed not to go backwards
/// regardless of NTP corrections, leap seconds, or operator clock resets.
/// The epoch is anchored at the first call (effectively process start) and is
/// process-local — values are not meaningful across process restarts or node
/// boundaries. That is fine: `DistributedBucket` state is in-memory only, and
/// the fields that use this clock (`last_call`, `InternalRequestEntry::timestamp_ms`)
/// are never included in the gossip wire format.
///
/// `TokenBucket` intentionally retains wall-clock time because its `last_call`
/// field *is* serialized and imported by other nodes in cluster mode, where
/// cross-node timestamp comparability matters.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

fn monotonic_ms() -> i64 {
    PROCESS_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis() as i64
}

/// Distributed request counter using CRDT PN-counters
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedRequestCounter {
    /// Origin metadata: the node that first created this counter.
    ///
    /// IMPORTANT: this field is **not** consulted when performing local writes.
    /// The owning `DistributedBucket` holds `writer_node_id` separately, and all
    /// mutation methods below take the writer identity as an explicit parameter.
    /// Mixing these up would violate the GCounter single-writer-per-actor invariant
    /// under concurrent cross-node updates.
    pub node_id: NodeId,
    refills: PNCounter<NodeId>,
    requests: PNCounter<NodeId>,
    pub vclock: VClock<NodeId>,
}

impl DistributedRequestCounter {
    /// Create a new DistributedRequestCounter for a given node
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            refills: PNCounter::new(),
            requests: PNCounter::new(),
            vclock: VClock::new(),
        }
    }

    /// Apply expiration decrements as the given `writer`. The caller is responsible
    /// for passing the local node's identity — never the counter's origin `node_id`,
    /// which may belong to a remote node if this bucket was adopted via gossip.
    fn expire_op_steps(&mut self, writer: NodeId, op: &InternalPnCounterOp) {
        match op {
            InternalPnCounterOp::Fills(steps) => {
                let op = self.refills.dec_many(writer, *steps);
                self.refills.apply(op);
            }
            InternalPnCounterOp::Requests(steps) => {
                let op = self.requests.dec_many(writer, *steps);
                self.requests.apply(op);
            }
        }
        let op = self.vclock.inc(writer);
        self.vclock.apply(op);
    }

    pub fn inc_refills(&mut self, node_id: NodeId, amount: u64) {
        let op = self.refills.inc_many(node_id, amount);
        self.refills.apply(op);
        let op = self.vclock.inc(node_id);
        self.vclock.apply(op);
    }

    pub fn inc_request(&mut self, node_id: NodeId) {
        let op = self.requests.inc(node_id);
        self.requests.apply(op);
        let op = self.vclock.inc(node_id);
        self.vclock.apply(op);
    }

    pub fn tokens(&self) -> BigInt {
        let refills = self.refills.read();
        let requests = self.requests.read();
        let val = &refills - &requests;
        debug!(
            "Calculating tokens {}: refills={}, requests={}",
            val, refills, requests
        );
        val
    }
}

// Operation-based CRDT implementation
impl CmRDT for DistributedRequestCounter {
    type Op = (
        crdts::pncounter::Op<NodeId>,
        crdts::pncounter::Op<NodeId>,
        VClock<NodeId>,
    );
    type Validation = <PNCounter<NodeId> as CmRDT>::Validation;

    fn apply(&mut self, op: Self::Op) {
        self.refills.apply(op.0);
        self.requests.apply(op.1);
        self.vclock.merge(op.2);
    }

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        self.refills.validate_op(&op.0)?;
        self.requests.validate_op(&op.1)?;
        self.vclock.validate_merge(&op.2)?;
        Ok(())
    }
}

// State-based CRDT merge implementation
impl CvRDT for DistributedRequestCounter {
    type Validation = <PNCounter<NodeId> as CvRDT>::Validation;

    fn merge(&mut self, other: Self) {
        // Apply other's state
        self.refills.merge(other.refills);
        self.requests.merge(other.requests);
        self.vclock.merge(other.vclock);
    }

    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        self.refills.validate_merge(&other.refills)?;
        self.requests.validate_merge(&other.requests)?;
        self.vclock.validate_merge(&other.vclock)?;
        Ok(())
    }
}

impl ResetRemove<NodeId> for DistributedRequestCounter {
    fn reset_remove(&mut self, clock: &VClock<NodeId>) {
        self.refills.reset_remove(clock);
        self.requests.reset_remove(clock);
        self.vclock.reset_remove(clock);
    }
}

impl Default for DistributedRequestCounter {
    /// Null node check; null-value is not used for NodeId
    /// If this is a null node, it indicates a bucket that will never get updated.
    /// However, this is required for the CRDT Default implementation.
    fn default() -> Self {
        Self {
            node_id: NodeId::default(),
            refills: PNCounter::new(),
            requests: PNCounter::new(),
            vclock: VClock::new(),
        }
    }
}

/// Internal operation entry for tracking requests with timestamps and vector clocks
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
enum InternalPnCounterOp {
    Fills(u64),
    Requests(u64),
}

/// We use these entries to expire old operations from the request tracker
/// and then PN refills *and* requests for the expired entries.
/// These types are internal-only and not serialized/gossiped.
#[derive(Clone, Debug, PartialEq, Eq)]
struct InternalRequestEntry {
    op: InternalPnCounterOp,
    timestamp_ms: i64,
    vclock: VClock<NodeId>,
}

/// External representation for gossip protocol
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedBucketExternal {
    pub client_id: String,
    pub node_id: NodeId,
    pub counter: DistributedRequestCounter,
}

/// Convert external gossiped bucket into internal bucket
/// Used when receiving gossiped state from other nodes
/// when we've never seen this client before.
impl DistributedBucketExternal {
    /// Adopt a gossiped bucket into local state.
    ///
    /// `local_node_id` must be the receiving limiter's node id. It becomes the
    /// adopted bucket's `writer_node_id`, so any subsequent local mutations write
    /// into this node's own PNCounter actor slot rather than the sender's.
    /// The origin `node_id` is preserved from the gossip message so that
    /// `DistributedBucketLimiter::expire_keys` continues to refuse to GC buckets
    /// this node did not create.
    /// `max_calls` is the limiter's configured capacity. It is stored locally on
    /// the bucket and never written into the CRDT (no immortal seed tokens).
    fn bucket(&self, local_node_id: NodeId, max_calls: u32) -> DistributedBucket {
        DistributedBucket {
            node_id: self.node_id,
            writer_node_id: local_node_id,
            max_calls,
            counter: self.counter.clone(),
            requests: Vec::new(),
            last_call: monotonic_ms(),
            // Initialise the watermark to the incoming state rather than VClock::new().
            // A freshly-adopted bucket has nothing local to re-broadcast: the
            // original sender already gossiped it. Only local writes that arrive
            // *after* adoption should trigger a push.
            last_gossiped_vclock: self.counter.vclock.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct DistributedBucket {
    /// Origin identity: the node that first created this bucket. Used by
    /// `DistributedBucketLimiter::expire_keys` to refuse GC of buckets adopted
    /// from peers (we cannot trust foreign timestamps for expiration decisions).
    pub node_id: NodeId,
    /// Writer identity: the PNCounter actor slot this node writes into for this
    /// bucket. Always the local limiter's node id, regardless of whether the
    /// bucket was created locally or adopted from a gossip peer. Every call
    /// into `DistributedRequestCounter` mutation methods must pass this value
    /// as the writer — never `counter.node_id`, which may be a remote origin.
    pub writer_node_id: NodeId,
    /// The configured capacity for this bucket. Kept as a local constant and
    /// never written into the CRDT (no immortal seed tokens).
    ///
    /// `tokens_to_u32()` returns `max_calls + counter.tokens()` so the full
    /// capacity is visible at the API level without polluting gossip state.
    /// All nodes must agree on this value via their rate-limit configuration.
    pub max_calls: u32,
    pub counter: DistributedRequestCounter,
    // internal state only: timestamps
    requests: Vec<InternalRequestEntry>,
    last_call: i64,
    // vclock at the time this bucket was last included in a gossip batch
    last_gossiped_vclock: VClock<NodeId>,
}

impl DistributedBucket {
    pub fn can_expire(&self, expiration_threshold_ms: i64) -> bool {
        let now_ms = monotonic_ms();
        for entry in self.requests.iter() {
            if now_ms - (entry.timestamp_ms) <= expiration_threshold_ms {
                // Found an entry that is still valid; cannot expire
                return false;
            }
        }
        // All entries are expired
        true
    }
    pub fn has_updates_since_last_gossip(&self) -> bool {
        self.counter.vclock > self.last_gossiped_vclock
    }

    pub fn mark_gossiped(&mut self) {
        self.last_gossiped_vclock = self.counter.vclock.clone();
    }

    pub fn expire_entries(&mut self, expiration_threshold_ms: i64) {
        let now_ms = monotonic_ms();
        let mut entries_to_remove = Vec::new();
        for (idx, entry) in self.requests.iter().enumerate() {
            if now_ms - (entry.timestamp_ms) > expiration_threshold_ms {
                // Expire this entry. Decrements go to the local writer slot —
                // never to `counter.node_id`, which may point at a remote origin.
                debug!(
                    "Expiring entry {:?} from bucket for node {}: timestamp_ms={}, now_ms={}",
                    entry, self.node_id, entry.timestamp_ms, now_ms
                );
                self.counter.expire_op_steps(self.writer_node_id, &entry.op);
                entries_to_remove.push(idx);
            }
        }
        debug!(
            "Expired {} entries from bucket for node {}",
            entries_to_remove.len(),
            self.node_id
        );
        // Remove expired entries from the requests vector
        for &idx in entries_to_remove.iter().rev() {
            self.requests.remove(idx);
        }
    }
    pub fn to_external(&self, client_id: &str) -> DistributedBucketExternal {
        DistributedBucketExternal {
            client_id: client_id.to_string(),
            node_id: self.node_id,
            counter: self.counter.clone(),
        }
    }

    pub fn vclock(&self) -> VClock<NodeId> {
        self.counter.vclock.clone()
    }
}

impl Bucket for DistributedBucket {
    fn new(max_calls: u32, node_id: NodeId) -> Self {
        // Local creation: this node is both the origin (for GC) and the writer
        // (for CRDT actor slot). The two become different only after adoption
        // via `DistributedBucketExternal::bucket`.
        //
        // Do NOT write max_calls into the CRDT as a seed refill. The
        // capacity is stored as the local `max_calls` field and added as a
        // constant offset in `tokens_to_u32`. This prevents the seed from
        // living forever in gossip state as untracked, un-ageable CRDT tokens.
        Self {
            node_id,
            writer_node_id: node_id,
            max_calls,
            counter: DistributedRequestCounter::new(node_id),
            requests: Vec::new(),
            last_call: monotonic_ms(),
            last_gossiped_vclock: VClock::new(),
        }
    }

    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self {
        // clear out old entries first: these decs will get shared via CRDT merge
        let expiration_threshold_ms =
            (rate_limit_settings.rate_limit_interval_seconds * 1000 + 1) as i64;

        self.expire_entries(expiration_threshold_ms);

        // Same token replenishment logic as TokenBucket
        let now_ms = monotonic_ms();
        let diff_ms: i64 = now_ms - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        debug!("Token bucket diff_ms: {}", diff_ms);
        if diff_ms < 5i64 {
            // no-op
            debug!("Not adding tokens to bucket: diff_ms < 5ms");
            return self;
        }
        // Tokens are added at the token rate,
        // but for distributed bucket, we only add whole tokens
        // let participants: usize = self.vclock().dots.len();
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * (diff_ms as f64);
        let steps = tokens_to_add.trunc() as u64;
        debug!(
            "Adding tokens to bucket: diff_ms={}, tokens_to_add={} as steps={}",
            diff_ms, tokens_to_add, steps
        );
        // Stamp the fill entry with the current time, not the previous last_call.
        // Using self.last_call here would give the entry a stale timestamp, causing
        // expire_entries to immediately remove fills that were just recorded when
        // the elapsed gap exceeds the expiration threshold.
        self.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Fills(steps),
            timestamp_ms: now_ms,
            vclock: self.vclock(),
        });
        // Writes must go to the local writer slot. Using `counter.node_id` here
        // would attribute writes to a remote origin on adopted buckets, which
        // GCounter::merge would then silently drop via its per-actor max.
        self.counter.inc_refills(self.writer_node_id, steps);
        debug!("Updated bucket after adding tokens: {:?}", self.counter);
        self.last_call = now_ms;
        self
    }

    fn decrement(&mut self) -> &mut Self {
        self.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: monotonic_ms(),
            vclock: self.vclock(),
        });
        // See `add_tokens_to_bucket` — mutate the local writer slot, not the origin.
        self.counter.inc_request(self.writer_node_id);
        self
    }

    fn check_if_allowed(&self) -> bool {
        let tokens = self.tokens_to_u32();
        debug!("Checking if allowed: tokens={}", tokens);
        tokens >= 1
    }

    fn tokens_to_u32(&self) -> u32 {
        // Add max_calls as a local constant offset. The CRDT only tracks
        // *changes* (periodic refills and consumption); the initial capacity is
        // not gossiped and never lives in PNCounter state.
        (self.counter.tokens() + BigInt::from(self.max_calls))
            .clamp(BigInt::from(0), BigInt::from(u32::MAX))
            .to_u32()
            .unwrap_or(0)
    }
}

/// Distributed rate limiter using CRDT buckets
#[derive(Clone, Debug)]
pub struct DistributedBucketLimiter {
    pub node_id: NodeId,
    node_counters: HashMap<String, DistributedBucket>,
    rate_limit_settings: settings::RateLimitSettings,
}

/// Question: how to garbage collect old entries?
/// We can only safely remove entries that belong to this node,
/// because we can only trust our own timestamps.
impl DistributedBucketLimiter {
    pub fn new(node_id: NodeId, rate_limit_settings: settings::RateLimitSettings) -> Self {
        Self {
            node_id,
            node_counters: HashMap::new(),
            rate_limit_settings,
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

    pub fn expire_keys(&mut self) {
        let expiration_threshold_ms =
            (self.rate_limit_settings.rate_limit_interval_seconds * 1000) as i64 * 2;
        let keys_to_expire: Vec<String> = self
            .node_counters
            .pin()
            .iter()
            .filter_map(|(key, bucket)| {
                // We only feel comfortable expiring entries that belong to this node.
                // because we are looking at *local* timestamps only. This means
                // ultimately that *this node* hasn't seen an update for this client beyond the threshold
                // for our rate-limiting implementation: thus, old entries should be ignored in future calls.
                if bucket.node_id == self.node_id && bucket.can_expire(expiration_threshold_ms) {
                    // Never expire null nodes
                    Some(key.to_string())
                } else {
                    None
                }
            })
            .collect();

        for client in keys_to_expire {
            self.node_counters.pin().remove(&client);
        }
    }

    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        // Update the map with the modified bucket
        let guard = self.node_counters.pin();
        let bucket = guard.update_or_insert_with(
            key.clone(),
            |_counter: &DistributedBucket| {
                let mut counter = _counter.clone();
                debug!("Existing bucket found for client {}", key);
                counter.add_tokens_to_bucket(&self.rate_limit_settings);
                counter
            },
            || {
                debug!("Creating new bucket for client {}", key);
                DistributedBucket::new(
                    self.rate_limit_settings.rate_limit_max_calls_allowed,
                    self.node_id,
                )
            },
        );
        // Now check if allowed
        if bucket.check_if_allowed() {
            guard
                .update(key, |b| {
                    let mut b = b.clone();
                    b.decrement();
                    b
                })
                .map(|b| b.tokens_to_u32())
        } else {
            None
        }
    }

    /// Create state suitable for gossiping to other nodes.
    /// Only includes buckets with local changes since the last gossip batch,
    /// then advances each included bucket's gossip watermark so they are
    /// not re-sent until they have new operations.
    pub fn gossip_delta_state(&self) -> Vec<DistributedBucketExternal> {
        let guard = self.node_counters.pin();

        // 1: collect buckets that have changed since last gossip
        let result: Vec<DistributedBucketExternal> = guard
            .iter()
            .filter_map(|(client_id, bucket)| {
                if bucket.has_updates_since_last_gossip() {
                    Some(bucket.to_external(client_id))
                } else {
                    None
                }
            })
            .collect();

        // 2: advance the gossip watermark for each included bucket.
        // The || branch is the "insert if absent" path; in practice it should
        // not fire here because we just iterated over the live entries. If it
        // does, adopt the bucket with *this* limiter as the writer.
        let local_node_id = self.node_id;
        for ext in &result {
            guard.update_or_insert_with(
                ext.client_id.clone(),
                |bucket| {
                    let mut b = bucket.clone();
                    b.mark_gossiped();
                    b
                },
                || {
                    ext.bucket(
                        local_node_id,
                        self.rate_limit_settings.rate_limit_max_calls_allowed,
                    )
                },
            );
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

    /// Return the full CRDT state for all known clients, regardless of whether
    /// each bucket has been gossiped since its last update.
    ///
    /// Used for anti-entropy: when a peer requests a full sync, we send everything
    /// we have so they can merge it idempotently. Unlike `gossip_delta_state`, this
    /// does NOT advance the gossip watermark — the regular delta loop continues
    /// normally after an anti-entropy exchange.
    pub fn full_state_dump(&self) -> Vec<DistributedBucketExternal> {
        self.node_counters
            .pin()
            .iter()
            .map(|(client_id, bucket)| bucket.to_external(client_id))
            .collect()
    }

    pub fn accept_delta_state(&mut self, delta: &[DistributedBucketExternal]) {
        let local_node_id = self.node_id;
        for incoming_bucket in delta.iter() {
            self.node_counters.pin().update_or_insert_with(
                incoming_bucket.client_id.clone(),
                |_existing_counter| {
                    let mut existing_counter = _existing_counter.clone();
                    existing_counter
                        .counter
                        .merge(incoming_bucket.counter.clone());
                    // Advance the watermark to the post-merge vclock so this node DOES NOT
                    // re-broadcast state it just received from a peer.
                    // Only local writes that happen *after* this merge will advance
                    // counter.vclock past last_gossiped_vclock and trigger
                    // the next delta push.
                    existing_counter.last_gossiped_vclock = existing_counter.counter.vclock.clone();
                    existing_counter
                },
                || {
                    incoming_bucket.bucket(
                        local_node_id,
                        self.rate_limit_settings.rate_limit_max_calls_allowed,
                    )
                },
            );
        }
    }

    /// Age out expired operations on every live bucket.
    ///
    /// Addresses the Case where origin alive but idle: without traffic on a given
    /// bucket, `add_tokens_to_bucket` is never called, so `expire_entries` is
    /// never driven and the CRDT counter keeps the un-aged fill/request ops
    /// forever. Calling this on every gossip tick ensures expirations happen
    /// even for quiescent buckets.
    ///
    /// Preserves the single-writer-per-actor invariant: `expire_entries`
    /// only writes to `self.writer_node_id`, which is always the local node's
    /// slot, never a remote origin's. Case B (origin dead with un-aged ops at
    /// remote replicas) is not addressed here; see the review doc.
    pub fn tick_expirations(&self) {
        let expiration_threshold_ms =
            (self.rate_limit_settings.rate_limit_interval_seconds * 1000 + 1) as i64;
        let guard = self.node_counters.pin();
        let keys: Vec<String> = guard.iter().map(|(k, _)| k.clone()).collect();
        for key in keys {
            guard.update(key, |b| {
                let mut b = b.clone();
                b.expire_entries(expiration_threshold_ms);
                b
            });
        }
    }

    /// Return the join (element-wise maximum) of every bucket's vclock.
    ///
    /// This is the correct summary of "all state this node has seen": a single
    /// vclock whose value at each actor slot is the highest dot observed across
    /// any bucket. Using `>` (strict dominance) instead of merge would silently
    /// drop dots from concurrent vclocks whose iteration order is non-deterministic,
    /// causing heartbeat comparisons to be spuriously `None` (concurrent) and
    /// triggering permanent anti-entropy state requests.
    pub fn get_latest_updated_vclock(&self) -> VClock<NodeId> {
        self.node_counters
            .pin()
            .values()
            .fold(VClock::new(), |mut acc, bucket| {
                acc.merge(bucket.vclock());
                acc
            })
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

        // Add tokens and consume them
        counter.inc_refills(node_id, 5);
        assert_eq!(counter.tokens(), BigInt::from(5));

        counter.inc_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(4)); // 5 - 1
    }

    #[test]
    fn test_counter_crdt_merge() {
        let node1 = node_id();
        let node2 = NodeName::from("b").node_id();

        let mut counter1 = DistributedRequestCounter::new(node1);
        let mut counter2 = DistributedRequestCounter::new(node2);

        // Each counter operates independently
        counter1.inc_refills(node1, 3);
        counter1.inc_request(node1);

        counter2.inc_refills(node2, 2);
        counter2.inc_request(node2);
        counter2.inc_request(node2);

        // Before merge: counter1 = 3 (1+3-1), counter2 = 1 (1+2-2)
        assert_eq!(counter1.tokens(), BigInt::from(2));
        assert_eq!(counter2.tokens(), BigInt::from(0));

        // After merge: should see combined state = 2 (2 + 0)
        counter1.merge(counter2.clone());
        assert_eq!(counter1.tokens(), BigInt::from(2));
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
    fn test_bucket_token_replenishment() {
        let node_id = node_id();
        let mut bucket = DistributedBucket::new(1000, node_id);
        let settings = settings::RateLimitSettings {
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 1,
        };

        let initial_tokens = bucket.counter.tokens();

        // Sleep to ensure time passes for token calculation
        thread::sleep(Duration::from_millis(100));

        bucket.add_tokens_to_bucket(&settings);

        // Should have added tokens
        assert!(bucket.counter.tokens() > initial_tokens);
    }

    #[test]
    fn test_bucket_expiration() {
        let node_id = node_id();
        let mut bucket = DistributedBucket::new(1000, node_id);

        // Add entries with different ages
        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: monotonic_ms() - 2000, // Old
            vclock: bucket.vclock(),
        });

        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Fills(1),
            timestamp_ms: monotonic_ms() - 200, // Recent
            vclock: bucket.vclock(),
        });

        assert_eq!(bucket.requests.len(), 2);

        // Expire entries older than 1 second
        bucket.expire_entries(1000);

        // Should have removed the old entry only
        assert_eq!(bucket.requests.len(), 1);

        // Empty bucket should be expirable
        let empty_bucket = DistributedBucket::new(1000, node_id);
        assert!(empty_bucket.can_expire(1000));

        // Bucket with recent activity should not be expirable
        assert!(!bucket.can_expire(1000));
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
                vclock: bucket.vclock(),
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

        counter_a.inc_refills(node1, 5);
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
        let restored_bucket = external.bucket(node_id, 10);
        assert_eq!(restored_bucket.node_id, node_id);
        assert_eq!(restored_bucket.writer_node_id, node_id);
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
        // Use the bucket API directly (not the limiter) so wall-clock refills
        // don't contaminate the exact-count assertion below.
        let mut bucket_a = DistributedBucket::new(100, node_a);
        bucket_a.decrement(); // A local: 1 request

        // Simulate B adopting A's bucket via gossip. Before the safety bug fix this
        // step would copy A's `counter.node_id` into B's bucket and silently
        // route all of B's future writes into slot A of the PNCounter.
        let external = bucket_a.to_external("test_client");
        let mut bucket_b = external.bucket(node_b, 100);

        // Post-adoption invariants: origin preserved for GC, writer is local.
        assert_eq!(bucket_b.node_id, node_a, "origin must be preserved");
        assert_eq!(
            bucket_b.writer_node_id, node_b,
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
        // With seed refills = 100, net tokens after merge must be 100 - 4 = 96.
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
        let adopted = external.bucket(node_b, 100);
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
        let mut bucket_b = external_a.bucket(node_b, 100);
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
            !matches!(summary.partial_cmp(&b_vc), None),
            "summary must not be concurrent with node_b's vclock after convergence"
        );
        assert!(
            !matches!(summary.partial_cmp(&c_vc), None),
            "summary must not be concurrent with node_c's vclock after convergence"
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
                        for j in 0..NUM_NODES {
                            if i != j {
                                limiters[j].accept_delta_state(delta);
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
