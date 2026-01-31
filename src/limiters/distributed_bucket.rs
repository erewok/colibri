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
        let entry = self.requests.iter().last();
        if let Some(entry) = entry {
            if entry.vclock > self.counter.vclock {
                return true;
            }
        }
        false
    }

    pub fn expire_entries(&mut self, expiration_threshold_ms: i64) {
        let now_ms = Utc::now().timestamp_millis();
        let mut entries_to_remove = Vec::new();
        for (idx, entry) in self.requests.iter().enumerate() {
            if now_ms - (entry.timestamp_ms) > expiration_threshold_ms {
                // Expire this entry
                debug!(
                    "Expiring entry {:?} from bucket for node {}: timestamp_ms={}, now_ms={}",
                    entry, self.node_id, entry.timestamp_ms, now_ms
                );
                self.counter.expire_op_steps(&entry.op);
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
        let mut instance = Self {
            node_id,
            counter: DistributedRequestCounter::new(node_id),
            requests: Vec::new(),
            last_call: Utc::now().timestamp_millis(),
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
        self.counter.inc_refills(self.counter.node_id, steps);
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
        self.counter.inc_request(self.counter.node_id);
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

    pub fn accept_delta_state(&mut self, delta: &[DistributedBucketExternal]) {
        for incoming_bucket in delta.iter() {
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

        // Get gossip state and merge
        let gossip_from_node2 = limiter2.gossip_delta_state();
        assert!(gossip_from_node2.is_empty());

        limiter1.accept_delta_state(&gossip_from_node2);

        // After merge, node1 should see combined state
        let tokens = limiter1.check_calls_remaining_for_client(&client_id);
        assert!(tokens > 0);
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
        limiter.accept_delta_state(&gossip_state);

        assert_eq!(limiter.len(), 1);

        // Should not expire foreign node buckets (only expires own node buckets)
        limiter.expire_keys();
        assert_eq!(limiter.len(), 1);
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

        // Convert back to internal format
        let restored_bucket = external.bucket();
        assert_eq!(restored_bucket.node_id, node_id);
        assert_eq!(restored_bucket.counter.tokens(), bucket.counter.tokens());
    }
}
