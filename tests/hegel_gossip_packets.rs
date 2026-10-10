//! Splitting gossip updates into datagrams: every packet fits the size limit
//! (unless it holds one bucket that alone is larger), and together the
//! packets carry exactly the buckets given, each under its rule and version.

use colibri::limiters::RuleName;
use colibri::limiters::distributed_bucket::DistributedBucketLimiter;
use colibri::limiters::rule_config::RuleVersion;
use colibri::node::gossip::messages::{GossipMessage, delta_packets};
use colibri::node::gossip::state::RuleBuckets;
use colibri::node::{ActorId, NodeName};
use colibri::settings::RateLimitSettings;
use hegel::TestCase;
use hegel::generators as gs;

const SETTINGS: RateLimitSettings = RateLimitSettings {
    rate_limit_max_calls_allowed: 1000,
    rate_limit_interval_seconds: 60,
};

/// One rule's buckets, each client written by between one and `writers` actors.
fn draw_rule_buckets(tc: &TestCase, rule: usize) -> RuleBuckets {
    let writers: Vec<ActorId> = (0..tc.draw(gs::integers::<usize>().min_value(1).max_value(8)))
        .map(|w| ActorId {
            node: NodeName::from(format!("node-{w}").as_str()).node_id(),
            incarnation: tc.draw(gs::integers::<u64>()),
        })
        .collect();
    let mut limiters: Vec<_> = writers
        .iter()
        .map(|&w| DistributedBucketLimiter::new(w, SETTINGS))
        .collect();
    for c in 0..tc.draw(gs::integers::<usize>().max_value(200)) {
        let client = format!(
            "{c}-{}",
            "x".repeat(tc.draw(gs::integers::<usize>().max_value(80)))
        );
        let by = tc.draw(
            gs::integers::<usize>()
                .min_value(1)
                .max_value(writers.len()),
        );
        for limiter in limiters.iter_mut().take(by) {
            limiter.limit_calls_for_client(client.clone());
        }
    }
    let mut merged = DistributedBucketLimiter::new(writers[0], SETTINGS);
    for limiter in &limiters {
        merged.accept_delta_state(&limiter.full_state_dump());
    }
    RuleBuckets {
        rule: RuleName::from(format!("rule-{rule}")),
        version: tc.draw(gs::booleans()).then(|| RuleVersion {
            ms: tc.draw(gs::integers::<i64>()),
            actor: writers[0],
        }),
        buckets: merged.full_state_dump(),
    }
}

#[hegel::test]
fn packets_fit_and_carry_every_bucket(tc: TestCase) {
    let updates: Vec<RuleBuckets> = (0..tc.draw(gs::integers::<usize>().max_value(3)))
        .map(|r| draw_rule_buckets(&tc, r))
        .collect();
    let max_bytes = tc.draw(gs::integers::<usize>().min_value(64).max_value(9000));
    let sender = NodeName::from("sender").node_id();
    let addr = "127.0.0.1:7946".parse().unwrap();

    let packets = delta_packets(updates.clone(), sender, addr, 1, max_bytes);

    let mut carried: Vec<(RuleName, Option<RuleVersion>, String)> = Vec::new();
    for packet in &packets {
        let size = packet.serialize().unwrap().len();
        let GossipMessage::DeltaStateSync { updates, .. } = &packet.message else {
            panic!("expected a delta packet");
        };
        let buckets: usize = updates.iter().map(|u| u.buckets.len()).sum();
        assert!(buckets > 0, "empty packet");
        assert!(
            size <= max_bytes || buckets == 1,
            "{size}-byte packet with {buckets} buckets exceeds {max_bytes}"
        );
        for u in updates {
            carried.extend(
                u.buckets
                    .iter()
                    .map(|b| (u.rule.clone(), u.version, b.client_id.clone())),
            );
        }
    }
    let mut expected: Vec<_> = updates
        .iter()
        .flat_map(|u| {
            u.buckets
                .iter()
                .map(|b| (u.rule.clone(), u.version, b.client_id.clone()))
        })
        .collect();
    carried.sort();
    expected.sort();
    assert_eq!(carried, expected);
}
