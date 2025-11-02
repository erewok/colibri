/// What does a state-based CRDT implementation of a token bucket look like?
/// Here we implement the distributed token bucket using two PN-counters: one for refills and one for requests.
/// We rely heavily on the `crdts` crate for CRDT data structures and operations instead of reimplementing them.
/// Our goals are CRDT goals:
/// - Associative: (a + b) + c = a + (b + c)
/// - Commutative: a + b = b + a
/// - Idempotent: a + a = a
/// 1. Semilattice join: merge(a, b) = least upper bound of a and b
/// 2. Monotonicity: state only grows (no deletions*)
///
/// Goals:
/// - Support distributed token bucket rate limiting across multiple nodes.
/// - Handle concurrent updates and network partitions gracefully.
/// - *WEAK* eventual consistency: allow temporary divergence but ensure convergence over time.
/// - Ensure convergence of token counts across nodes using CRDT principles
///   (associative, commutative, semilattice join); state-based CRDT (CvRDT).
/// - Thus, allow nodes to gossip their token bucket state and *merge* updates.
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

/// `TokenBucket` implementation is inspired by a PN-counter.
/// We use two PNCounters: one for refills and one for requests.
/// In the RateLimiter, we *also* track last_call timestamps
/// and **all requests and refills** in order to expire old entries
/// and decrement from this counter when entries pass the expiry window.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DistributedRequestCounter {
    pub node_id: NodeId,
    // Tracks number of refills per NodeId
    refills: PNCounter<NodeId>,
    // Tracks number of requests per NodeId
    requests: PNCounter<NodeId>,
    // Tracks causal-order timing so we can see times *between* requests
    pub vclock: VClock<NodeId>,
}

impl DistributedRequestCounter {
    /// Create a new DistributedRequestCounter for a given node
    pub fn new(node_id: NodeId) -> Self {
        let mut fills = PNCounter::new();
        // start with 1: matches initial token bucket state
        let op = fills.inc(node_id);
        fills.apply(op);

        let mut vclock = VClock::new();
        let op = vclock.inc(node_id);
        vclock.apply(op);

        Self {
            node_id,
            refills: fills,
            requests: PNCounter::new(),
            vclock,
        }
    }
    fn expire_op_steps(&mut self, op: &InternalPnCounterOp) {
        match op {
            InternalPnCounterOp::Fills(steps) => {
                let op = self.refills.dec_many(self.node_id, *steps);
                self.refills.apply(op);
            }
            InternalPnCounterOp::Requests(steps) => {
                let op = self.requests.dec_many(self.node_id, *steps);
                self.requests.apply(op);
            }
        }
        let op = self.vclock.inc(self.node_id);
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

    pub fn dec_request(&mut self, node_id: NodeId) {
        let op = self.requests.dec(node_id);
        self.requests.apply(op);
        let op = self.vclock.inc(node_id);
        self.vclock.apply(op);
    }

    pub fn tokens(&self) -> BigInt {
        let refills = self.refills.read();
        let requests = self.requests.read();
        debug!(
            "Calculating tokens: refills={}, requests={}",
            refills, requests
        );
        refills - requests
    }
}

// Here we implement operation-based CRDT for DistributedRequestCounter
// Because we plan to gossip state around the cluster, we will primarily use CvRDT merge,
// but we implement CmRDT as required for `Map` below.
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

// Here we implement state-based CRDT merge for DistributedRequestCounter
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
    fn bucket(&self) -> DistributedBucket {
        DistributedBucket {
            node_id: self.node_id,
            counter: self.counter.clone(),
            requests: Vec::new(),
            last_call: Utc::now().timestamp_millis(),
        }
    }
}

#[derive(Clone, Debug)]
struct DistributedBucket {
    pub node_id: NodeId,
    pub counter: DistributedRequestCounter,
    // internal state only: timestamps
    requests: Vec<InternalRequestEntry>,
    last_call: i64,
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
        true
        // let entry = self.requests.iter().last();
        // if let Some(entry) = entry {
        //     if entry.vclock > self.counter.vclock {
        //         return true;
        //     }
        // }
        // false
    }

    pub fn expire_entries(&mut self, expiration_threshold_ms: i64) {
        let now_ms = Utc::now().timestamp_millis();
        let mut entries_to_remove = Vec::new();
        for (idx, entry) in self.requests.iter().enumerate() {
            if now_ms - (entry.timestamp_ms) > expiration_threshold_ms {
                // Expire this entry
                self.counter.expire_op_steps(&entry.op);
                entries_to_remove.push(idx);
            }
        }
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
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            counter: DistributedRequestCounter::new(node_id),
            requests: Vec::new(),
            last_call: Utc::now().timestamp_millis(),
        }
    }

    fn add_tokens_to_bucket(
        &mut self,
        rate_limit_settings: &settings::RateLimitSettings,
    ) -> &mut Self {
        // clear out old entries first: these decs will get shared via CRDT merge
        let expiration_threshold_ms =
            (rate_limit_settings.rate_limit_interval_seconds * 1000 + 25) as i64;

        self.expire_entries(expiration_threshold_ms);

        // Same token replenishment logic as TokenBucket
        let diff_ms: i64 = Utc::now().timestamp_millis() - self.last_call;
        // For this algorithm we arbitrarily do not trust intervals less than 5ms,
        // so we only *add* tokens if the diff is greater than that.
        let diff_ms: i32 = diff_ms as i32;
        if diff_ms < 5i32 {
            // no-op
            return self;
        }
        // Tokens are added at the token rate,
        // but for distributed bucket, we only add whole tokens
        let tokens_to_add: f64 = rate_limit_settings.token_rate_milliseconds() * f64::from(diff_ms)
            / rate_limit_settings.cluster_participant_count as f64;
        // Max calls is limited to
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
        self.counter.inc_refills(self.counter.node_id, steps);
        self.last_call = Utc::now().timestamp_millis();
        self
    }

    fn decrement(&mut self) -> &mut Self {
        if !self.check_if_allowed() {
            return self;
        } else {
            self.requests.push(InternalRequestEntry {
                op: InternalPnCounterOp::Requests(1),
                timestamp_ms: Utc::now().timestamp_millis(),
                vclock: self.vclock(),
            });
            self.counter.inc_request(self.counter.node_id);
        }
        self
    }

    fn check_if_allowed(&self) -> bool {
        let tokens = self.counter.tokens();
        debug!("Checking if allowed: tokens={}", tokens);
        tokens >= 1.into()
    }

    fn tokens_to_u32(&self) -> u32 {
        let tokens = self.counter.tokens();
        if tokens >= BigInt::from(u32::MAX) {
            return u32::MAX;
        }
        tokens
            .clamp(BigInt::from(0), BigInt::from(u32::MAX))
            .to_u32()
            .unwrap_or(u32::MAX)
    }
}

/// Finally, we define the distributed token bucket map, which holds per-node, removable buckets
/// Distributed token bucket that can be gossiped and merged across cluster nodes.
/// The CRDT states we *send* to other nodes are RemovableBuckets.
#[derive(Clone, Debug)]
pub struct DistributedBucketLimiter {
    /// Node ID of this bucket map owner
    pub node_id: NodeId,
    // /// Tombstones for removed clients (vector clock tracks nodes performing removals)
    // pub tombstones: Map<String, VClock<NodeId>, NodeId>,
    /// Client request counters distributed: shared state-based CRDT around the cluster
    node_counters: HashMap<String, DistributedBucket>,

    /// Rate limit settings for token replenishment
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
        self.node_counters.pin().update_or_insert_with(
            key.clone(),
            |_counter: &DistributedBucket| {
                let mut counter = _counter.clone();
                counter.add_tokens_to_bucket(&self.rate_limit_settings);
                if counter.check_if_allowed() {
                    counter.decrement();
                    counter
                } else {
                    counter
                }
            },
            || {
                let mut counter = DistributedBucket::new(self.node_id);
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
    pub fn gossip_delta_state(&self) -> Vec<DistributedBucketExternal> {
        // The problem here is selecting only the relevant entries to gossip.
        // We also don't want to send a massive amount of packets.
        // We break it into a vector in order to send smaller chunks.
        // If we do not broadcast and we have a lot of delta states to send
        // We may not end up gossiping everything we need to.
        self.node_counters
            .pin()
            .iter()
            .filter_map(|(client_id, bucket)| {
                if bucket.has_updates_since_last_gossip() {
                    Some(bucket.to_external(client_id))
                } else {
                    None
                }
            })
            .collect()
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

    pub fn accept_delta_state(&mut self, delta: Vec<DistributedBucketExternal>) {
        for incoming_bucket in delta.into_iter() {
            self.node_counters.pin().update_or_insert_with(
                incoming_bucket.client_id.clone(),
                |_existing_counter| {
                    let mut existing_counter = _existing_counter.clone();
                    existing_counter
                        .counter
                        .merge(incoming_bucket.counter.clone());
                    existing_counter
                },
                || incoming_bucket.bucket(),
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

    fn test_settings() -> settings::RateLimitSettings {
        settings::RateLimitSettings {
            cluster_participant_count: 3,
            rate_limit_max_calls_allowed: 10,
            rate_limit_interval_seconds: 1,
        }
    }

    #[test]
    fn test_distributed_request_counter_new() {
        let node_id = NodeId::new(1);
        let counter = DistributedRequestCounter::new(node_id);

        assert_eq!(counter.node_id, node_id);
        // Should start with 1 refill (initial token bucket state)
        assert_eq!(counter.tokens(), BigInt::from(1));
        assert!(counter.vclock.get(&node_id) > 0);
    }

    #[test]
    fn test_distributed_request_counter_inc_refills() {
        let node_id = NodeId::new(1);
        let mut counter = DistributedRequestCounter::new(node_id);

        counter.inc_refills(node_id, 5);
        assert_eq!(counter.tokens(), BigInt::from(6)); // 1 initial + 5 added

        counter.inc_refills(node_id, 3);
        assert_eq!(counter.tokens(), BigInt::from(9)); // 6 + 3
    }

    #[test]
    fn test_distributed_request_counter_inc_and_dec_request() {
        let node_id = NodeId::new(1);
        let mut counter = DistributedRequestCounter::new(node_id);

        counter.inc_refills(node_id, 5);
        assert_eq!(counter.tokens(), BigInt::from(6));

        // Increment requests (consume tokens)
        counter.inc_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(5)); // 6 - 1

        counter.inc_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(4)); // 5 - 1

        // Decrement requests (restore tokens, like undoing a request)
        counter.dec_request(node_id);
        assert_eq!(counter.tokens(), BigInt::from(5)); // 4 + 1
    }
    #[test]
    fn test_distributed_request_counter_merge() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let mut counter1 = DistributedRequestCounter::new(node1);
        let mut counter2 = DistributedRequestCounter::new(node2);

        // Add some operations on each counter
        counter1.inc_refills(node1, 3);
        counter1.inc_request(node1);

        counter2.inc_refills(node2, 2);
        counter2.inc_request(node2);
        counter2.inc_request(node2);

        // Before merge
        assert_eq!(counter1.tokens(), BigInt::from(3)); // 1 + 3 - 1
        assert_eq!(counter2.tokens(), BigInt::from(1)); // 1 + 2 - 2

        // Merge counter2 into counter1
        counter1.merge(counter2.clone());

        // After merge, should see operations from both nodes
        // Node1: 1 initial + 3 refills - 1 request = 3
        // Node2: 1 initial + 2 refills - 2 requests = 1
        // Total: 3 + 1 = 4
        assert_eq!(counter1.tokens(), BigInt::from(4));
    }

    #[test]
    fn test_distributed_bucket_new() {
        let node_id = NodeId::new(1);
        let bucket = DistributedBucket::new(node_id);

        assert_eq!(bucket.node_id, node_id);
        assert_eq!(bucket.counter.tokens(), BigInt::from(1));
        assert!(bucket.requests.is_empty());
    }

    #[test]
    fn test_distributed_bucket_check_if_allowed() {
        let node_id = NodeId::new(1);
        let bucket = DistributedBucket::new(node_id);

        // Should be allowed with initial token
        assert!(bucket.check_if_allowed());

        // Create bucket with no tokens by consuming the initial token
        let mut bucket_empty = DistributedBucket::new(node_id);
        bucket_empty.counter.inc_request(node_id);
        assert!(!bucket_empty.check_if_allowed());
    }

    #[test]
    fn test_distributed_bucket_decrement() {
        let node_id = NodeId::new(1);
        let mut bucket = DistributedBucket::new(node_id);

        // Add some tokens first
        bucket.counter.inc_refills(node_id, 2);
        assert_eq!(bucket.counter.tokens(), BigInt::from(3)); // 1 initial + 2

        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(2));
        assert_eq!(bucket.requests.len(), 1);

        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(1));
        assert_eq!(bucket.requests.len(), 2);

        // Try to decrement when not allowed (should not change)
        bucket.decrement(); // This should consume the last token
        assert_eq!(bucket.counter.tokens(), BigInt::from(0));

        // This should not decrement further
        bucket.decrement();
        assert_eq!(bucket.counter.tokens(), BigInt::from(0));
        assert_eq!(bucket.requests.len(), 3); // Only 3 successful decrements
    }

    #[test]
    fn test_distributed_bucket_tokens_to_u32() {
        let node_id = NodeId::new(1);
        let mut bucket = DistributedBucket::new(node_id);

        // Test normal case
        bucket.counter.inc_refills(node_id, 10);
        assert_eq!(bucket.tokens_to_u32(), 11);

        // Test large number (should cap at u32::MAX)
        bucket.counter.inc_refills(node_id, u64::MAX - 100);
        assert_eq!(bucket.tokens_to_u32(), u32::MAX);

        // Test negative (should be 0) - consume more tokens than available
        let mut empty_bucket = DistributedBucket::new(node_id);
        empty_bucket.counter.inc_request(node_id);
        empty_bucket.counter.inc_request(node_id);
        assert_eq!(empty_bucket.tokens_to_u32(), 0);
    }

    #[test]
    fn test_distributed_bucket_add_tokens() {
        let node_id = NodeId::new(1);
        let mut bucket = DistributedBucket::new(node_id);
        let settings = settings::RateLimitSettings {
            cluster_participant_count: 1,       // Use 1 to make calculation easier
            rate_limit_max_calls_allowed: 1000, // Higher rate to ensure tokens are added
            rate_limit_interval_seconds: 1,
        };

        // Sleep to ensure time difference (100ms should be enough to add tokens)
        thread::sleep(Duration::from_millis(100));

        bucket.add_tokens_to_bucket(&settings);

        // Should have added some tokens (initial + calculated tokens)
        let tokens = bucket.counter.tokens();
        assert!(tokens > BigInt::from(1));
        assert!(!bucket.requests.is_empty());
    }

    #[test]
    fn test_distributed_bucket_expire_entries() {
        let node_id = NodeId::new(1);
        let mut bucket = DistributedBucket::new(node_id);

        // Add some entries with different timestamps
        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: Utc::now().timestamp_millis() - 2000, // 2 seconds ago
            vclock: bucket.vclock(),
        });

        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Fills(1),
            timestamp_ms: Utc::now().timestamp_millis() - 500, // 0.5 seconds ago
            vclock: bucket.vclock(),
        });

        assert_eq!(bucket.requests.len(), 2);

        // Expire entries older than 1 second
        bucket.expire_entries(1000);

        // Should have removed the old entry
        assert_eq!(bucket.requests.len(), 1);
    }

    #[test]
    fn test_distributed_bucket_can_expire() {
        let node_id = NodeId::new(1);
        let mut bucket = DistributedBucket::new(node_id);

        // Empty bucket can expire
        assert!(bucket.can_expire(1000));

        // Add recent entry
        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: Utc::now().timestamp_millis(),
            vclock: bucket.vclock(),
        });

        // Should not be able to expire with recent entry
        assert!(!bucket.can_expire(1000));

        // Add old entry
        bucket.requests.push(InternalRequestEntry {
            op: InternalPnCounterOp::Requests(1),
            timestamp_ms: Utc::now().timestamp_millis() - 2000,
            vclock: bucket.vclock(),
        });

        // Still should not expire because of the recent entry
        assert!(!bucket.can_expire(1000));

        // Clear recent entries and keep only old ones
        bucket
            .requests
            .retain(|entry| Utc::now().timestamp_millis() - entry.timestamp_ms > 1500);

        // Now should be able to expire
        assert!(bucket.can_expire(1000));
    }

    #[test]
    fn test_distributed_bucket_to_external() {
        let node_id = NodeId::new(1);
        let bucket = DistributedBucket::new(node_id);

        let external = bucket.to_external("test_client");

        assert_eq!(external.client_id, "test_client");
        assert_eq!(external.node_id, node_id);
        assert_eq!(external.counter.tokens(), bucket.counter.tokens());
    }

    #[test]
    fn test_distributed_bucket_external_to_bucket() {
        let node_id = NodeId::new(1);
        let external = DistributedBucketExternal {
            client_id: "test_client".to_string(),
            node_id,
            counter: DistributedRequestCounter::new(node_id),
        };

        let bucket = external.bucket();

        assert_eq!(bucket.node_id, node_id);
        assert_eq!(bucket.counter.tokens(), external.counter.tokens());
        assert!(bucket.requests.is_empty());
    }

    #[test]
    fn test_distributed_bucket_limiter_new() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let limiter = DistributedBucketLimiter::new(node_id, settings.clone());

        assert_eq!(limiter.node_id, node_id);
        assert_eq!(limiter.rate_limit_settings.rate_limit_max_calls_allowed, 10);
        assert!(limiter.is_empty());
    }

    #[test]
    fn test_distributed_bucket_limiter_limit_calls() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let mut limiter = DistributedBucketLimiter::new(node_id, settings);

        let client_id = "test_client".to_string();

        // First call should succeed and return remaining tokens
        let remaining = limiter.limit_calls_for_client(client_id.clone());
        assert!(remaining.is_some());
        assert!(!limiter.is_empty());
        assert_eq!(limiter.len(), 1);

        // Check remaining calls
        let check_remaining = limiter.check_calls_remaining_for_client(&client_id);
        assert!(check_remaining > 0);
    }

    #[test]
    fn test_distributed_bucket_limiter_gossip_and_merge() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let settings = test_settings();

        let mut limiter1 = DistributedBucketLimiter::new(node1, settings.clone());
        let mut limiter2 = DistributedBucketLimiter::new(node2, settings);

        let client_id = "test_client".to_string();

        // Make requests on both limiters
        limiter1.limit_calls_for_client(client_id.clone());
        limiter2.limit_calls_for_client(client_id.clone());

        // Get gossip state from limiter2
        let gossip_state = limiter2.gossip_delta_state();
        assert!(!gossip_state.is_empty());

        // Apply gossip state to limiter1
        limiter1.accept_delta_state(gossip_state);

        // Both limiters should now have merged state
        let tokens1 = limiter1.check_calls_remaining_for_client(&client_id);

        // The merged state should reflect operations from both nodes
        assert!(tokens1 > 0);
    }

    #[test]
    fn test_distributed_bucket_limiter_expire_keys() {
        let node_id = NodeId::new(1);
        // Use settings that will actually add tokens
        let settings = settings::RateLimitSettings {
            cluster_participant_count: 1,
            rate_limit_max_calls_allowed: 1000,
            rate_limit_interval_seconds: 1,
        };
        let mut limiter = DistributedBucketLimiter::new(node_id, settings.clone());

        let client_id = "test_client".to_string();

        // First, create the client bucket
        limiter.limit_calls_for_client(client_id.clone());
        assert_eq!(limiter.len(), 1);

        // Wait, then call it again to trigger token addition (this should add entries)
        thread::sleep(Duration::from_millis(50));
        limiter.limit_calls_for_client(client_id.clone());
        assert_eq!(limiter.len(), 1);

        // Expire keys (should not expire recent entries)
        limiter.expire_keys();
        assert_eq!(limiter.len(), 1);

        // Test expiration with a different node (should not expire)
        let mut foreign_limiter = DistributedBucketLimiter::new(NodeId::new(999), settings);
        let foreign_client = "foreign_client".to_string();
        foreign_limiter.limit_calls_for_client(foreign_client.clone());

        // Add the foreign bucket to our limiter via gossip
        let gossip_state = foreign_limiter.gossip_delta_state();
        limiter.accept_delta_state(gossip_state);

        assert_eq!(limiter.len(), 2);

        // Expiration should not remove foreign node entries
        limiter.expire_keys();
        assert_eq!(limiter.len(), 2); // Both should remain
    }

    #[test]
    fn test_distributed_bucket_limiter_client_delta_state() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let mut limiter = DistributedBucketLimiter::new(node_id, settings);

        let client_id = "test_client".to_string();

        // No delta state for non-existent client
        assert!(limiter.client_delta_state_for_gossip(&client_id).is_none());

        // Add client
        limiter.limit_calls_for_client(client_id.clone());

        // Should now have delta state
        let delta = limiter.client_delta_state_for_gossip(&client_id);
        assert!(delta.is_some());

        let delta = delta.unwrap();
        assert_eq!(delta.client_id, client_id);
        assert_eq!(delta.node_id, node_id);
    }

    #[test]
    fn test_distributed_bucket_limiter_get_latest_vclock() {
        let node_id = NodeId::new(1);
        let settings = test_settings();
        let mut limiter = DistributedBucketLimiter::new(node_id, settings);

        // Empty limiter should return empty vclock
        let vclock = limiter.get_latest_updated_vclock();
        assert!(vclock.is_empty());

        // Add some clients
        limiter.limit_calls_for_client("client1".to_string());
        limiter.limit_calls_for_client("client2".to_string());

        let vclock = limiter.get_latest_updated_vclock();
        assert!(!vclock.is_empty());
        assert!(vclock.get(&node_id) > 0);
    }

    #[test]
    fn test_crdt_properties_associative() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let node3 = NodeId::new(3);

        let mut counter_a = DistributedRequestCounter::new(node1);
        let mut counter_b = DistributedRequestCounter::new(node2);
        let mut counter_c = DistributedRequestCounter::new(node3);

        counter_a.inc_refills(node1, 2);
        counter_b.inc_refills(node2, 3);
        counter_c.inc_refills(node3, 1);

        // Test (a + b) + c = a + (b + c)
        let mut left = counter_a.clone();
        left.merge(counter_b.clone());
        left.merge(counter_c.clone());

        let mut right = counter_a.clone();
        let mut bc = counter_b.clone();
        bc.merge(counter_c.clone());
        right.merge(bc);

        assert_eq!(left.tokens(), right.tokens());
    }

    #[test]
    fn test_crdt_properties_commutative() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let mut counter_a = DistributedRequestCounter::new(node1);
        let mut counter_b = DistributedRequestCounter::new(node2);

        counter_a.inc_refills(node1, 5);
        counter_b.inc_request(node2);

        // Test a + b = b + a
        let mut ab = counter_a.clone();
        ab.merge(counter_b.clone());

        let mut ba = counter_b.clone();
        ba.merge(counter_a.clone());

        assert_eq!(ab.tokens(), ba.tokens());
        assert_eq!(ab.refills, ba.refills);
        assert_eq!(ab.requests, ba.requests);
    }

    #[test]
    fn test_crdt_properties_idempotent() {
        let node_id = NodeId::new(1);
        let mut counter = DistributedRequestCounter::new(node_id);
        counter.inc_refills(node_id, 3);
        counter.inc_request(node_id);

        let original_tokens = counter.tokens();
        let original_counter = counter.clone();

        // Test a + a = a (merge with itself)
        counter.merge(original_counter.clone());

        assert_eq!(counter.tokens(), original_tokens);
    }

    #[test]
    fn test_multi_node_scenario() {
        // Simulate a 3-node cluster scenario
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let node3 = NodeId::new(3);
        let settings = test_settings();

        let mut limiter1 = DistributedBucketLimiter::new(node1, settings.clone());
        let mut limiter2 = DistributedBucketLimiter::new(node2, settings.clone());
        let mut limiter3 = DistributedBucketLimiter::new(node3, settings);

        let client_id = "shared_client".to_string();

        // Each node processes requests for the same client
        limiter1.limit_calls_for_client(client_id.clone());
        limiter2.limit_calls_for_client(client_id.clone());
        limiter3.limit_calls_for_client(client_id.clone());

        // Gossip between nodes
        let gossip1 = limiter1.gossip_delta_state();
        let gossip2 = limiter2.gossip_delta_state();
        let gossip3 = limiter3.gossip_delta_state();

        // Apply all gossip states to node1
        limiter1.accept_delta_state(gossip2.clone());
        limiter1.accept_delta_state(gossip3.clone());

        // Apply all gossip states to node2
        limiter2.accept_delta_state(gossip1.clone());
        limiter2.accept_delta_state(gossip3.clone());

        // Apply all gossip states to node3
        limiter3.accept_delta_state(gossip1);
        limiter3.accept_delta_state(gossip2);

        // All nodes should converge to the same state
        let tokens1 = limiter1.check_calls_remaining_for_client(&client_id);
        let tokens2 = limiter2.check_calls_remaining_for_client(&client_id);
        let tokens3 = limiter3.check_calls_remaining_for_client(&client_id);

        assert_eq!(tokens1, tokens2);
        assert_eq!(tokens2, tokens3);

        // The merged state should account for all three nodes' operations
        assert!(tokens1 > 0);
    }
}
