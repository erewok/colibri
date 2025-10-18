use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{event, info, Level};

use crate::error::Result;
use crate::gossip::{generate_node_id_from_system, VersionedTokenBucket};
use crate::node::{CheckCallsResponse, Node};
use crate::rate_limit::RateLimiter;
use crate::token_bucket::TokenBucket;

/// A pure gossip-based node that maintains all client state locally
/// and syncs with other nodes via gossip protocol. No consistent hashing,
/// no HTTP forwarding - every node can answer every request.
#[derive(Clone)]
pub struct PureGossipNode {
    node_id: u32,
    /// Local rate limiter for fallback/legacy support
    rate_limiter: Arc<RwLock<RateLimiter>>,
    /// All versioned token buckets maintained locally
    local_buckets: Arc<DashMap<String, VersionedTokenBucket>>,
    /// Configuration
    default_rate_limit: u32,
    default_window: Duration,
}

impl std::fmt::Debug for PureGossipNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PureGossipNode")
            .field("node_id", &self.node_id)
            .field("default_rate_limit", &self.default_rate_limit)
            .field("default_window", &self.default_window)
            .finish()
    }
}

impl PureGossipNode {
    pub fn new(_topology: Vec<String>, rate_limiter: Arc<RwLock<RateLimiter>>) -> Result<Self> {
        let node_id = generate_node_id_from_system(7946)?;
        info!("Created PureGossipNode with ID: {}", node_id);

        Ok(Self {
            node_id,
            rate_limiter,
            local_buckets: Arc::new(DashMap::new()),
            default_rate_limit: 1000, // Default from rate limiter settings
            default_window: Duration::from_secs(60), // Default window
        })
    }

    /// Get or create a versioned token bucket for a client
    fn get_or_create_bucket(&self, client_id: &str) -> VersionedTokenBucket {
        self.local_buckets
            .entry(client_id.to_string())
            .or_insert_with(|| {
                let bucket = TokenBucket::new(self.default_rate_limit, self.default_window);
                VersionedTokenBucket::new(bucket, self.node_id)
            })
            .clone()
    }

    /// Update a bucket locally (gossip will be added later)
    fn update_bucket(&self, client_id: String, mut bucket: VersionedTokenBucket) {
        // Increment our vector clock
        bucket.vector_clock.increment(self.node_id);
        bucket.last_updated_by = self.node_id;

        // Store the updated bucket
        self.local_buckets.insert(client_id, bucket);
    }
}

#[async_trait]
impl Node for PureGossipNode {
    /// Check remaining calls for a client using local state
    async fn check_limit(&self, client_id: String) -> Result<CheckCallsResponse> {
        let bucket = self.get_or_create_bucket(&client_id);
        Ok(CheckCallsResponse {
            client_id,
            calls_remaining: bucket.bucket.tokens_to_u32(),
        })
    }

    /// Apply rate limiting using local state only
    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let mut bucket = self.get_or_create_bucket(&client_id);

        // Try to consume one token
        if bucket.bucket.try_consume(1) {
            // Success - update our state and trigger gossip
            let response = CheckCallsResponse {
                client_id: client_id.clone(),
                calls_remaining: bucket.bucket.tokens_to_u32(),
            };

            // Update bucket with new state
            self.update_bucket(client_id, bucket);

            Ok(Some(response))
        } else {
            // Rate limit exceeded
            Ok(None)
        }
    }

    /// Expire keys from local buckets
    fn expire_keys(&self) {
        // Expire from the legacy rate limiter
        match self.rate_limiter.write() {
            Ok(mut rate_limiter) => {
                rate_limiter.expire_keys();
            }
            Err(err) => {
                event!(
                    Level::ERROR,
                    message = "Failed expiring keys from rate_limiter",
                    err = format!("{:?}", err)
                );
            }
        }

        // TODO: Implement expiry for local_buckets based on TokenBucket's last_refill time
        // For now, we'll rely on the existing rate_limiter for expiry logic
    }
}
