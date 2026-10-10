//! State-machine tests for a single `DistributedBucketLimiter` in isolation.
//!
//! With no peers, the CRDT-backed limiter is a sliding window: each admitted
//! request holds one of `max_calls` tokens until it ages out of the window,
//! and ageing is the only way tokens come back. Ageing is lazy: it runs for a
//! client when that client makes a request, and for every client on a gossip
//! tick, so the model tracks which requests have actually been aged.

mod common;

use std::collections::HashMap;

use colibri::clock::virtual_clock::{self, VirtualClockGuard};
use colibri::limiters::distributed_bucket::DistributedBucketLimiter;
use colibri::settings::RateLimitSettings;
use hegel::TestCase;

use common::{
    CLIENTS, ageing_window_ms, assert_within_window_bound, draw_advance_ms, draw_client,
    draw_settings, node_ids,
};

/// Reference sliding window for one node.
struct Model {
    settings: RateLimitSettings,
    /// Admission times of requests still holding a token, per client.
    live: HashMap<String, Vec<i64>>,
    /// Every admission time, per client, for the window bound.
    admitted_at: HashMap<String, Vec<i64>>,
}

impl Model {
    fn capacity(&self) -> u32 {
        self.settings.rate_limit_max_calls_allowed
    }

    fn age(&mut self, client: &str, now_ms: i64) {
        let window = ageing_window_ms(&self.settings);
        if let Some(live) = self.live.get_mut(client) {
            live.retain(|&t| now_ms - t <= window);
        }
    }

    fn request(&mut self, client: &str, now_ms: i64) -> Option<u32> {
        self.age(client, now_ms);
        let capacity = self.capacity();
        let live = self.live.entry(client.to_string()).or_default();
        let remaining = capacity.saturating_sub(live.len() as u32);
        if remaining == 0 {
            return None;
        }
        live.push(now_ms);
        self.admitted_at
            .entry(client.to_string())
            .or_default()
            .push(now_ms);
        Some(remaining - 1)
    }

    fn remaining(&self, client: &str) -> u32 {
        let live = self.live.get(client).map_or(0, Vec::len) as u32;
        self.capacity().saturating_sub(live)
    }
}

struct SingleNode {
    clock: VirtualClockGuard,
    limiter: DistributedBucketLimiter,
    model: Model,
}

#[hegel::state_machine]
impl SingleNode {
    #[rule]
    fn request(&mut self, tc: TestCase) {
        let client = draw_client(&tc);
        let now_ms = self.clock.now_ms();
        let actual = self.limiter.limit_calls_for_client(client.clone());
        let expected = self.model.request(&client, now_ms);
        tc.note(&format!("t={now_ms} request {client}: {actual:?}"));
        if actual.is_none() {
            tc.event("request denied");
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

    /// The gossip tick's ageing pass.
    #[rule]
    fn tick_expirations(&mut self, _tc: TestCase) {
        self.limiter.tick_expirations();
        let now_ms = self.clock.now_ms();
        for client in CLIENTS {
            self.model.age(client, now_ms);
        }
    }

    #[invariant(always_run)]
    fn remaining_matches_model(&mut self, _tc: TestCase) {
        for client in CLIENTS {
            assert_eq!(
                self.limiter
                    .check_calls_remaining_for_client(&client.to_string()),
                self.model.remaining(client),
                "calls remaining for {client}"
            );
        }
    }

    /// Checked against the admission history alone, independently of the
    /// model's token accounting.
    #[invariant(always_run)]
    fn admissions_within_window_bound(&mut self, _tc: TestCase) {
        let window = ageing_window_ms(&self.model.settings);
        let capacity = self.model.capacity() as usize;
        for (client, times) in &self.model.admitted_at {
            assert_within_window_bound(client, times, capacity, window);
        }
    }
}

#[hegel::test]
fn single_distributed_bucket_is_a_sliding_window(tc: TestCase) {
    let settings = draw_settings(&tc);
    let node = SingleNode {
        clock: virtual_clock::install(),
        limiter: DistributedBucketLimiter::new(node_ids(1)[0], settings.clone()),
        model: Model {
            settings,
            live: HashMap::new(),
            admitted_at: HashMap::new(),
        },
    };
    hegel::stateful::machine(node).run(tc);
}
