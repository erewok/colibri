//! Model-based state-machine tests for the single-node `TokenBucketLimiter`.
//!
//! Hegel drives random interleavings of requests, clock advances, key expiry
//! and cluster-migration export/import against the real limiter and a small
//! reference model, checking after every step that the two agree and that
//! the limiter never admits more traffic than a token bucket allows.
//!
//! The model encodes the limiter's *specified* behaviour, including its
//! documented quirk of ignoring refill intervals shorter than 5 ms.

mod common;

use std::collections::HashMap;

use colibri::clock::virtual_clock::{self, VirtualClockGuard};
use colibri::limiters::TokenBucketLimiter;
use colibri::settings::RateLimitSettings;
use hegel::TestCase;

use common::{CLIENTS, assert_within_bucket_bound, draw_advance_ms, draw_client, draw_settings};

/// Refill intervals shorter than this are ignored by the limiter.
const MIN_REFILL_INTERVAL_MS: i64 = 5;

#[derive(Debug, Clone)]
struct ModelBucket {
    tokens: f64,
    last_refill_ms: i64,
}

/// Reference token bucket per client.
struct Model {
    settings: RateLimitSettings,
    buckets: HashMap<String, ModelBucket>,
    /// Times at which each client's requests were admitted.
    admitted_at: HashMap<String, Vec<i64>>,
}

impl Model {
    fn capacity(&self) -> f64 {
        f64::from(self.settings.rate_limit_max_calls_allowed)
    }

    /// Credit refill for the time since the last refill, ignoring intervals
    /// under `MIN_REFILL_INTERVAL_MS`.
    fn refill(&self, bucket: &mut ModelBucket, now_ms: i64) {
        let elapsed = now_ms - bucket.last_refill_ms;
        if elapsed >= MIN_REFILL_INTERVAL_MS {
            let rate = self.settings.token_rate_milliseconds();
            bucket.tokens = (bucket.tokens + rate * elapsed as f64).clamp(0.0, self.capacity());
            bucket.last_refill_ms = now_ms;
        }
    }

    fn request(&mut self, client: &str, now_ms: i64) -> Option<u32> {
        let mut bucket = match self.buckets.get(client) {
            Some(existing) => existing.clone(),
            None => ModelBucket {
                tokens: self.capacity(),
                last_refill_ms: now_ms,
            },
        };
        self.refill(&mut bucket, now_ms);
        let bucket = self
            .buckets
            .entry(client.to_string())
            .insert_entry(bucket)
            .into_mut();

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            self.admitted_at
                .entry(client.to_string())
                .or_default()
                .push(now_ms);
            Some(bucket.tokens.trunc() as u32)
        } else {
            None
        }
    }

    /// Forget buckets idle for two intervals. A bucket idle that long has
    /// refilled to capacity, so forgetting it must be unobservable.
    fn expire(&mut self, now_ms: i64) {
        let threshold_ms = i64::from(self.settings.rate_limit_interval_seconds) * 1000 * 2;
        self.buckets
            .retain(|_, b| b.last_refill_ms > now_ms - threshold_ms);
    }

    /// What the next request would find: stored tokens plus refill so far.
    fn remaining(&self, client: &str, now_ms: i64) -> u32 {
        self.buckets
            .get(client)
            .map(|b| {
                let mut projected = b.clone();
                self.refill(&mut projected, now_ms);
                projected.tokens.trunc() as u32
            })
            .unwrap_or(self.settings.rate_limit_max_calls_allowed)
    }
}

struct TokenBucketMachine {
    clock: VirtualClockGuard,
    limiter: TokenBucketLimiter,
    model: Model,
}

#[hegel::state_machine]
impl TokenBucketMachine {
    #[rule]
    fn request(&mut self, tc: TestCase) {
        let client = draw_client(&tc);
        let now_ms = self.clock.now_ms();
        let actual = self.limiter.limit_calls_for_client(client.clone());
        let expected = self.model.request(&client, now_ms);
        tc.note(&format!("t={now_ms} request {client}: {actual:?}"));
        match expected {
            Some(_) if self.model.admitted_at[&client].len() > self.model.capacity() as usize => {
                tc.event("admitted beyond initial capacity (refill exercised)")
            }
            Some(_) => {}
            None => tc.event("request denied"),
        }
        assert_eq!(
            actual, expected,
            "request for {client} at t={now_ms}: limiter said {actual:?}, model said {expected:?}"
        );
    }

    #[rule]
    fn advance_time(&mut self, tc: TestCase) {
        let ms = draw_advance_ms(&tc, &self.model.settings);
        self.clock.advance(ms);
    }

    #[rule]
    fn expire_keys(&mut self, tc: TestCase) {
        let before = self.model.buckets.len();
        self.limiter.expire_keys();
        self.model.expire(self.clock.now_ms());
        if self.model.buckets.len() < before {
            tc.event("bucket expired");
        }
    }

    /// Hand every bucket to a fresh limiter, as a hashring node does when the
    /// cluster topology changes. Rate-limit decisions must carry on unchanged.
    #[rule]
    fn migrate_to_new_node(&mut self, tc: TestCase) {
        if !self.model.buckets.is_empty() {
            tc.event("migrated live buckets");
        }
        let successor = TokenBucketLimiter::new(self.model.settings.clone());
        successor.import_buckets(self.limiter.export_all_buckets());
        self.limiter = successor;
    }

    #[invariant(always_run)]
    fn remaining_matches_model(&mut self, _tc: TestCase) {
        let now_ms = self.clock.now_ms();
        for client in CLIENTS {
            assert_eq!(
                self.limiter.check_calls_remaining_for_client(client),
                self.model.remaining(client, now_ms),
                "calls remaining for {client}"
            );
        }
    }

    /// Token-bucket conformance: over any window of length `w` ending at the
    /// latest admitted request, at most `capacity + rate * w` requests are
    /// admitted. This is checked against the admission history, independently
    /// of how the model computes tokens.
    /// Checked against the admission history alone, independently of how
    /// the model computes tokens.
    #[invariant(always_run)]
    fn admissions_within_bucket_bound(&mut self, _tc: TestCase) {
        for (client, times) in &self.model.admitted_at {
            assert_within_bucket_bound(
                client,
                times,
                self.model.capacity(),
                self.model.settings.token_rate_milliseconds(),
            );
        }
    }
}

#[hegel::test]
fn token_bucket_limiter_matches_model(tc: TestCase) {
    let settings = draw_settings(&tc);
    let machine = TokenBucketMachine {
        clock: virtual_clock::install(),
        limiter: TokenBucketLimiter::new(settings.clone()),
        model: Model {
            settings,
            buckets: HashMap::new(),
            admitted_at: HashMap::new(),
        },
    };
    hegel::stateful::machine(machine).run(tc);
}
