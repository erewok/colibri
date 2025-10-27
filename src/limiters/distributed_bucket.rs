/// What does a state-based CRDT implementation of a token bucket look like?
/// Here we implement the distributed token bucket using a PN-counter.
/// We rely heavily on the `crdts` crate for CRDT data structures and operations instead of reimplementing them.
/// Our goals are CRDT goals:
/// - Associative: (a + b) + c = a + (b + c)
/// - Commutative: a + b = b + a
/// - Idempotent: a + a = a
/// 1. Semilattice join: merge(a, b) = least upper bound of a and b
/// 2. Monotonicity: state only grows (no deletions*)
/// * Deletions are handled via tombstones with Map::reset_remove().
///
/// Goals:
/// - Support distributed token bucket rate limiting across multiple nodes.
/// - Handle concurrent updates and network partitions gracefully.
/// - *WEAK* eventual consistency: allow temporary divergence but ensure convergence over time.
/// - Ensure convergence of token counts across nodes using CRDT principles
///   (associative, commutative, semilattice join); state-based CRDT (CvRDT).
/// - Thus, allow nodes to gossip their token bucket state and *merge* updates.

use chrono::Utc;
use crdts::{CmRDT, CvRDT, Map, PNCounter, ResetRemove, VClock};
use papaya::HashMap;
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::limiters::token_bucket::Bucket;
use crate::node::NodeId;
use crate::settings;

// Delta state for gossiping: list of (client_id, DistributedRequestCounter, is_tombstone)
pub type ClientDelta = (String, DistributedRequestCounter, bool);
pub type DeltaState = Vec<ClientDelta>;


/// `TokenBucket` implementation is inspired by a PN-counter with a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedRequestCounter {
    pub node_id: NodeId,
    // Tracks number of refills and consumes with causality per NodeId
    pub counter: PNCounter<NodeId>,

    // Timestamps in unix milliseconds.
    // These is NOT COMPARABLE ACROSS NODES. Use for refills and GC only.
    pub last_call: i64,

    // in order to track causality between `last_call` and merges in the counter
    // This VClock is incremented on each local update to `last_call`
    // When we refill, we look for all the vector clocks greater than this one.
    pub vclock: VClock<NodeId>,
}

// Here we implement operation-based CRDT for DistributedRequestCounter
// Because we plan to gossip state around the cluster, we will primarily use CvRDT merge,
// but we implement CmRDT as required for `Map` below.
impl CmRDT for DistributedRequestCounter {
    type Op = crdts::pncounter::Op<NodeId>;
    type Validation = <PNCounter<NodeId> as CmRDT>::Validation;

    fn apply(&mut self, op: Self::Op) {
        self.vclock.apply(op.dot.clone());
        self.counter.apply(op)
    }

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        self.counter.validate_op(op)
    }
}

// Here we implement state-based CRDT merge for DistributedRequestCounter
impl CvRDT for DistributedRequestCounter {
    type Validation = <PNCounter<NodeId> as CvRDT>::Validation;

    fn merge(&mut self, other: Self) {
        // Apply other's state
        self.vclock.merge(other.vclock);
        self.counter.merge(other.counter.clone());
        self.last_call = Utc::now().timestamp_millis();
    }
    fn validate_merge(&self, other: &Self) -> Result<(), Self::Validation> {
        self.vclock.validate_merge(&other.vclock)?;
        self.counter.validate_merge(&other.counter)?;
        Ok(())
    }
}

impl ResetRemove<NodeId> for DistributedRequestCounter {
    fn reset_remove(&mut self, clock: &VClock<NodeId>) {
        self.counter.reset_remove(clock);
    }
}

impl Default for DistributedRequestCounter {
    /// Null node check; null-value is not used for NodeId
    /// If this is a null node, it indicates a bucket that will never get updated.
    /// However, this is required for the CRDT Default implementation.
    fn default() -> Self {
        Self {
            node_id: NodeId::default(),
            counter: PNCounter::new(),
            last_call: Utc::now().timestamp_millis(),
            vclock: VClock::new(),
        }
    }
}

impl DistributedRequestCounter {
    // If it was updated more than expiration_threshold_ms ago, it can be expired.
    pub fn can_expire(&self, expiration_threshold_ms: i64) -> bool {
        let now = Utc::now().timestamp_millis();
        now - self.last_call > expiration_threshold_ms
    }
}

impl Bucket for DistributedRequestCounter {
    fn new(node_id: NodeId) -> Self {
        let new_counter = PNCounter::new();
        Self {
            node_id,
            counter: new_counter,
            last_call: Utc::now().timestamp_millis(),
            vclock: VClock::new(),
        }
    }
    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self{
        // Same token replenishment logic as TokenBucket
        let diff_ms: i64 = Utc::now().timestamp_millis() - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        let diff_ms: i32 = diff_ms as i32;
        if diff_ms < 5i32 {
            // no-op
            return self;
        }
        // Remove entries older than last local time
        self.counter.reset_remove(&self.vclock);

        // Tokens are added at the token rate
        let tokens_to_add = rate_limit_settings.token_rate_milliseconds();
        // Max calls is limited to: rate_limit_settings.rate_limit_max_calls_allowed
        let steps = tokens_to_add.trunc() as u64;
        self.counter.inc_many(self.node_id, steps);
        // These two must always be updated together: self.last_call for calculating time deltas and tokens
        // and vlock for causality tracking with other nodes' updates.
        self.last_call = Utc::now().timestamp_millis();
        self.vclock.inc(self.node_id);
        self
    }

    fn decrement(&mut self) -> &mut Self {
        if self.check_if_allowed() {
            // no-op
            self.counter.dec_many(self.node_id, 0);
        } else {
            self.counter.dec(self.node_id);
        }
        self
    }

    fn check_if_allowed(&self) -> bool {
        let tokens = self.counter.read();
        tokens >= 1.into()
    }

    fn tokens_to_u32(&self) -> u32 {
        let tokens = self.counter.read();
        if tokens >= BigInt::from(u32::MAX) {
            return u32::MAX;
        }
        tokens.clamp(BigInt::from(0), BigInt::from(u32::MAX)).to_u32().unwrap_or(u32::MAX)
    }
}

/// Finally, we define the distributed token bucket map, which holds per-node, removable buckets
/// Distributed token bucket that can be gossiped and merged across cluster nodes.
/// The CRDT states we *send* to other nodes are RemovableBuckets.
#[derive(Clone, Debug)]
pub struct DistributedBucketLimiter {
    /// Node ID of this bucket map owner
    pub node_id: NodeId,
    /// Tombstones for removed clients (vector clock tracks nodes performing removals)
    pub tombstones: Map<String, VClock<NodeId>, NodeId>,
    /// Per-node consumption counters for each client with add/remove semantics.
    pub node_counters: HashMap<String, DistributedRequestCounter>,
    /// Rate limit settings for token replenishment
    rate_limit_settings: settings::RateLimitSettings,
}

/// Question: how to garbage collect old entries?
/// We can only safely remove entries that belong to this node,
/// because we can only trust our own timestamps.
impl DistributedBucketLimiter {
    pub fn new(
        node_id: NodeId,
        rate_limit_settings: settings::RateLimitSettings,
    ) -> Self {
        Self {
            node_id,
            node_counters: HashMap::new(),
            tombstones: Map::new(),
            rate_limit_settings,
        }
    }

    pub fn check_calls_remaining_for_client(&self, key: &String) -> u32 {
        self.node_counters.pin().get(key)
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
                if bucket.node_id == self.node_id && bucket.can_expire(expiration_threshold_ms){
                    // Never expire null nodes
                    Some(key.to_string())
                } else { None }
            })
            .collect();

        for client in keys_to_expire {
            let add_ctx = self.tombstones.read_ctx().derive_add_ctx(self.node_id);
            self.tombstones.update(client.to_owned(), add_ctx, |v,a| v.inc(self.node_id));
            self.node_counters.pin().remove(&client);
        }
    }

    pub fn limit_calls_for_client(&mut self, key: String) -> Option<u32> {
        // Update the map with the modified bucket
        self.node_counters.pin().update_or_insert_with(
            key.clone(),
            |_counter| {
                let mut counter = _counter.clone();
                counter.add_tokens_to_bucket(&self.rate_limit_settings);
                if counter.check_if_allowed() {
                    counter.decrement();
                    counter
                } else {
                    counter
                }
            }, || {
                let mut counter = DistributedRequestCounter::new(self.node_id);
                counter.add_tokens_to_bucket(&self.rate_limit_settings);
                counter
            },
        );
        self.node_counters.pin().get(&key).and_then(|counter| {
            if counter.check_if_allowed() {
                Some(counter.tokens_to_u32())
            } else {
                None
            }
        })
    }

    /// Create state suitable for gossiping to other nodes
    pub fn gossip_delta_state(&self) -> DeltaState {
        // The problem here is selecting only the relevant entries to gossip.
        // We also don't want to send a massive amount of packets.
        // We break it into a vector in order to send smaller chunks.
        // If we do not broadcast and we have a lot of delta states to send
        // We may not end up gossiping everything we need to.
        todo!()
    }
    pub fn client_delta_state_for_gossip(&self, client_id: &String) -> ClientDelta {
        let counter = self.node_counters.pin().get(client_id).cloned().unwrap_or_default();
        let is_tombstone = self.tombstones.get(client_id).val.is_some();
        (client_id.clone(), counter, is_tombstone)
    }

    pub fn accept_delta_state(&mut self, delta: DeltaState) {
        for (client_id, incoming_counter, is_tombstone) in delta.iter() {
            if *is_tombstone {
                let add_ctx = self.tombstones.read_ctx().derive_add_ctx(self.node_id);
                self.tombstones.update(client_id.clone(), add_ctx, |v, _a| {
                    v.inc(incoming_counter.node_id)
                });
            };
            self.node_counters.pin().update_or_insert_with(client_id.clone(), |_existing_counter| {
                let mut existing_counter = _existing_counter.clone();
                existing_counter.merge(incoming_counter.clone());
                existing_counter
            }, || incoming_counter.clone());
        }
    }

    pub fn get_latest_updated_vclock(&self) -> VClock<NodeId> {
        let largest = self.node_counters.pin().values()
            .fold(VClock::new(), |acc, counter| {
                if acc > counter.vclock {
                    acc
                } else {
                    counter.vclock.clone()
                }
            });
            let ctx = self.tombstones.read_ctx();
            if largest < ctx.add_clock {
                ctx.add_clock.clone()
            } else {
                largest
            }
    }

    pub fn is_empty(&self) -> bool {
        self.node_counters.pin().is_empty()
    }

    pub fn len(&self) -> usize {
        self.node_counters.pin().len()
    }
}
