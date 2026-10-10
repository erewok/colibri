//! Generators and fixtures shared by the hegel property-test suites.
#![allow(dead_code)]

use colibri::node::{NodeId, NodeName};
use colibri::settings::RateLimitSettings;
use hegel::TestCase;
use hegel::generators as gs;

/// A small, fixed client population: collisions between rules are what make
/// state-machine tests interesting, so we want few clients, not many.
pub const CLIENTS: [&str; 3] = ["alice", "bob", "carol"];

/// Rate-limit settings small enough that the bucket empties and refills
/// within a few dozen steps.
pub fn draw_settings(tc: &TestCase) -> RateLimitSettings {
    RateLimitSettings {
        rate_limit_max_calls_allowed: tc.draw(gs::integers::<u32>().min_value(1).max_value(10)),
        rate_limit_interval_seconds: tc.draw(gs::integers::<u32>().min_value(1).max_value(5)),
    }
}

pub fn draw_client(tc: &TestCase) -> String {
    tc.draw(gs::sampled_from(CLIENTS.to_vec())).to_string()
}

/// A time step that is usually short relative to the rate-limit interval but
/// occasionally long enough to refill the bucket or expire state outright.
pub fn draw_advance_ms(tc: &TestCase, settings: &RateLimitSettings) -> u32 {
    let interval_ms = settings.rate_limit_interval_seconds * 1000;
    tc.draw(
        gs::integers::<u32>()
            .min_value(1)
            .max_value(interval_ms * 3),
    )
}

pub fn node_ids(n: usize) -> Vec<NodeId> {
    (0..n)
        .map(|i| NodeName::from(format!("node-{i}").as_str()).node_id())
        .collect()
}

/// Token-bucket conformance: over any window ending at the latest admission,
/// a bucket of `capacity` refilling at `rate_per_ms` admits at most
/// `capacity + rate_per_ms * window` requests. `admitted_at` must be sorted.
pub fn assert_within_bucket_bound(
    label: &str,
    admitted_at: &[i64],
    capacity: f64,
    rate_per_ms: f64,
) {
    let Some(&end) = admitted_at.last() else {
        return;
    };
    for (i, &start) in admitted_at.iter().enumerate() {
        let admitted = (admitted_at.len() - i) as f64;
        let allowance = capacity + rate_per_ms * (end - start) as f64;
        assert!(
            admitted <= allowance + 1e-9,
            "{label}: {admitted} admissions in [{start}, {end}] ms exceeds bucket allowance {allowance:.3}"
        );
    }
}

/// Sliding-window conformance: no window spanning at most `window_ms`
/// milliseconds (inclusive) holds more than `limit` admissions.
/// `admitted_at` must be sorted.
pub fn assert_within_window_bound(label: &str, admitted_at: &[i64], limit: usize, window_ms: i64) {
    for (i, &start) in admitted_at.iter().enumerate() {
        let in_window = admitted_at[i..]
            .iter()
            .take_while(|&&t| t - start <= window_ms)
            .count();
        assert!(
            in_window <= limit,
            "{label}: {in_window} admissions within {window_ms} ms of t={start}, limit is {limit}"
        );
    }
}

/// Requests age out of a `DistributedBucket` once they are strictly older than
/// this, so a request admitted at `t` holds its token through `t + window`.
pub fn ageing_window_ms(settings: &RateLimitSettings) -> i64 {
    i64::from(settings.rate_limit_interval_seconds) * 1000 + 1
}
