//! Distributed token bucket using CRDT for eventual consistency
use chrono::Utc;
use crdts::{CmRDT, CvRDT, PNCounter, ResetRemove, VClock};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use papaya::HashMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::limiters::token_bucket::Bucket;
use crate::node::NodeId;
use crate::settings;

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
    fn bucket(&self, local_node_id: NodeId) -> DistributedBucket {
        DistributedBucket {
            node_id: self.node_id,
            writer_node_id: local_node_id,
            counter: self.counter.clone(),
            requests: Vec::new(),
            last_call: Utc::now().timestamp_millis(),
            // Initialise the watermark to the incoming state rather than VClock::new().
            // A freshly-adopted bucket has nothing local to re-broadcast: the
            // original sender already gossiped it. Only local writes that arrive
            //  *after* adoption should trigger a push.
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
    pub counter: DistributedRequestCounter,
    // internal state only: timestamps
    requests: Vec<InternalRequestEntry>,
    last_call: i64,
    // vclock at the time this bucket was last included in a gossip batch
    last_gossiped_vclock: VClock<NodeId>,
}

impl DistributedBucket {
    pub fn can_expire(&self, expiration_threshold_ms: i64) -> bool {
        let now_ms = Utc::now().timestamp_millis();
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
        let now_ms = Utc::now().timestamp_millis();
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
        let mut instance = Self {
            node_id,
            writer_node_id: node_id,
            counter: DistributedRequestCounter::new(node_id),
            requests: Vec::new(),
            last_call: Utc::now().timestamp_millis(),
            last_gossiped_vclock: VClock::new(),
        };
        instance.counter.inc_refills(node_id, max_calls as u64);
        instance
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
        let diff_ms: i64 = Utc::now().timestamp_millis() - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        let diff_ms: i32 = diff_ms as i32;
        debug!("Token bucket diff_ms: {}", diff_ms);
        if diff_ms < 5i32 {
            // no-op
            debug!("Not adding tokens to bucket: diff_ms < 5ms");
            return self;
        }
        // Tokens are added at the token rate,
        // but for distributed bucket, we only add whole tokens
        // let participants: usize = self.vclock().dots.len();
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * f64::from(diff_ms);
        let steps = tokens_to_add.trunc() as u64;
        debug!(
            "Adding tokens to bucket: diff_ms={}, tokens_to_add={} as steps={}",
            diff_ms, tokens_to_add, steps
        );
        self.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Fills(steps),
            timestamp_ms: self.last_call,
            vclock: self.vclock(),
        });
        // Writes must go to the local writer slot. Using `counter.node_id` here
        // would attribute writes to a remote origin on adopted buckets, which
        // GCounter::merge would then silently drop via its per-actor max.
        self.counter.inc_refills(self.writer_node_id, steps);
        debug!("Updated bucket after adding tokens: {:?}", self.counter);
        self.last_call = Utc::now().timestamp_millis();
        self
    }

    fn decrement(&mut self) -> &mut Self {
        self.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: Utc::now().timestamp_millis(),
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
        self.counter
            .tokens()
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
                || ext.bucket(local_node_id),
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
                || incoming_bucket.bucket(local_node_id),
            );
        }
    }

    pub fn get_latest_updated_vclock(&self) -> VClock<NodeId> {
        self.node_counters
            .pin()
            .values()
            .fold(VClock::new(), |acc, bucket| {
                if acc > bucket.vclock() {
                    acc
                } else {
                    bucket.vclock()
                }
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

        // Start with 1 token - should be allowed
        assert!(bucket.check_if_allowed());

        // Consume the token
        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(0));

        // Should not be allowed anymore
        assert!(!bucket.check_if_allowed());

        // Decrementing goes below 0 (consistent with TokenBucket behavior)
        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(-1));
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
            timestamp_ms: Utc::now().timestamp_millis() - 2000, // Old
            vclock: bucket.vclock(),
        });

        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Fills(1),
            timestamp_ms: Utc::now().timestamp_millis() - 200, // Recent
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
    } // === CRDT Properties Tests ===

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

        // Full mesh gossip - each node receives updates from all others
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
        let restored_bucket = external.bucket(node_id);
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
        let mut bucket_b = external.bucket(node_b);

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
}
