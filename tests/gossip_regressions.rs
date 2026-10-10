//! Deterministic regressions for gossip scenarios found in review.

use colibri::clock::virtual_clock;
use colibri::limiters::RuleName;
use colibri::node::NodeName;
use colibri::node::gossip::state::{GossipState, RuleBuckets, TickOutput};
use colibri::settings::RateLimitSettings;

fn settings(max_calls: u32, interval_seconds: u32) -> RateLimitSettings {
    RateLimitSettings {
        rate_limit_max_calls_allowed: max_calls,
        rate_limit_interval_seconds: interval_seconds,
    }
}

fn delta(state: &mut GossipState) -> Vec<RuleBuckets> {
    match state.tick() {
        TickOutput::Delta(d) => d,
        TickOutput::Heartbeat(_) => panic!("expected a delta"),
    }
}

/// A node restarts with its wall clock stepped back. Its new requests must
/// still count in full at its peers, however their old copies line up.
#[test]
fn restart_with_clock_step_back_never_over_admits() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let w_id = NodeName::from("w").node_id();
    let mut w = GossipState::new(w_id, settings(10, 60));
    let mut p = GossipState::new(NodeName::from("p").node_id(), settings(10, 60));

    // W's first incarnation: one request, gossiped, aged out, gossiped.
    w.limit(&rule, "c").unwrap().unwrap();
    p.accept(&delta(&mut w));
    clock.advance(60_002);
    p.accept(&delta(&mut w));

    // W restarts with its clock back where it started, and admits 3.
    clock.advance(1);
    clock.step_wall(-60_003);
    let mut w = GossipState::new(w_id, settings(10, 60));
    for _ in 0..3 {
        w.limit(&rule, "c").unwrap().unwrap();
    }
    p.accept(&delta(&mut w));

    let mut admitted_by_p = 0;
    while p.limit(&rule, "c").unwrap().is_some() {
        admitted_by_p += 1;
    }
    assert_eq!(admitted_by_p, 7, "W holds 3 of the 10 calls");
}

/// A node that admits requests and then disappears for good must not keep
/// them counted forever: once nothing about them has changed for longer than
/// the window, they have aged out and peers release them.
#[test]
fn requests_of_a_vanished_node_are_released_after_the_window() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut gone = GossipState::new(NodeName::from("gone").node_id(), settings(5, 1));
    let mut p = GossipState::new(NodeName::from("p").node_id(), settings(5, 1));

    for _ in 0..3 {
        gone.limit(&rule, "c").unwrap().unwrap();
    }
    p.accept(&delta(&mut gone));
    drop(gone);
    assert_eq!(p.check(&rule, "c").unwrap(), 2);

    // Two windows later, with p ticking as usual.
    for _ in 0..30 {
        clock.advance(100);
        p.tick();
    }
    assert_eq!(
        p.check(&rule, "c").unwrap(),
        5,
        "the vanished node's requests are released"
    );
    p.expire_keys();
    assert_eq!(p.bucket_count(), 0, "and the idle bucket is forgotten");
}

fn heartbeat(state: &mut GossipState) -> colibri::node::gossip::state::HeartbeatDigest {
    match state.tick() {
        TickOutput::Heartbeat(h) => h,
        TickOutput::Delta(_) => panic!("expected a heartbeat"),
    }
}

/// A node whose copy of a rule change was lost, and that is too busy to send
/// heartbeats, still learns the change: when a peer's heartbeat makes it push
/// its stale config, the peer replies with the newer one.
#[test]
fn stale_rule_config_is_corrected_without_heartbeats_from_the_stale_node() {
    let _clock = virtual_clock::install();
    let rule = RuleName::from("r");
    let mut x = GossipState::new(NodeName::from("x").node_id(), settings(5, 1));
    let mut y = GossipState::new(NodeName::from("y").node_id(), settings(5, 1));
    x.create_rule(rule.clone(), settings(100, 1)).unwrap();
    y.merge_rules(x.rule_config());

    // X tightens the rule; its broadcast to Y is lost.
    x.create_rule(rule.clone(), settings(10, 1)).unwrap();

    // Y only ever sends deltas. X's heartbeat reaches Y; Y pushes its config back.
    let reply = y.on_heartbeat(&heartbeat(&mut x));
    assert!(reply.push_rules);
    let back = x.merge_rules(y.rule_config());
    assert!(
        back.send_rules,
        "X must answer Y's stale config with its own"
    );
    y.merge_rules(x.rule_config());

    let at_y = y
        .get_rule(&rule)
        .unwrap()
        .settings
        .rate_limit_max_calls_allowed;
    assert_eq!(at_y, 10);
}

/// A node that learns a rule after its buckets were gossiped asks for them.
#[test]
fn a_node_that_learns_a_rule_late_fetches_its_buckets() {
    let _clock = virtual_clock::install();
    let rule = RuleName::from("r");
    let mut x = GossipState::new(NodeName::from("x").node_id(), settings(5, 60));
    let mut y = GossipState::new(NodeName::from("y").node_id(), settings(5, 60));
    x.create_rule(rule.clone(), settings(5, 60)).unwrap();
    x.limit(&rule, "c").unwrap().unwrap();
    x.limit(&rule, "c").unwrap().unwrap();
    y.accept(&delta(&mut x)); // Y doesn't know the rule yet: ignored.

    let reply = y.merge_rules(x.rule_config());
    assert!(
        reply.request_state,
        "Y should ask for the new rule's buckets"
    );
    y.accept(&x.full_state());
    assert_eq!(y.check(&rule, "c").unwrap(), 3);
}

/// Deleting and recreating a rule starts it from zero everywhere, even when
/// another node still gossips buckets from the old rule.
#[test]
fn a_recreated_rule_does_not_inherit_old_counts() {
    let _clock = virtual_clock::install();
    let rule = RuleName::from("r");
    let mut x = GossipState::new(NodeName::from("x").node_id(), settings(5, 60));
    let mut y = GossipState::new(NodeName::from("y").node_id(), settings(5, 60));
    x.create_rule(rule.clone(), settings(5, 60)).unwrap();
    y.merge_rules(x.rule_config());
    x.limit(&rule, "c").unwrap().unwrap();
    x.limit(&rule, "c").unwrap().unwrap();
    y.accept(&delta(&mut x));

    x.delete_rule(rule.clone()).unwrap();
    x.create_rule(rule.clone(), settings(5, 60)).unwrap();
    assert_eq!(x.check(&rule, "c").unwrap(), 5);

    // Y, not yet told, admits under the old rule and gossips its buckets.
    y.limit(&rule, "c").unwrap().unwrap();
    x.accept(&delta(&mut y));
    assert_eq!(
        x.check(&rule, "c").unwrap(),
        5,
        "old-rule buckets must be ignored"
    );

    y.merge_rules(x.rule_config());
    assert_eq!(
        y.check(&rule, "c").unwrap(),
        5,
        "Y starts the new rule from zero"
    );
}

/// Deliver a peer's tick output, pulling `peer_state` if its heartbeat shows
/// state `to` lacks.
fn exchange(output: TickOutput, to: &mut GossipState, peer_state: &[RuleBuckets]) {
    match output {
        TickOutput::Delta(updates) => to.accept(&updates),
        TickOutput::Heartbeat(h) => {
            if to.on_heartbeat(&h).pull_state {
                to.accept(peer_state);
            }
        }
    }
}

/// A dead process's requests are retired by peers that received them at
/// different times. The first to retire must not pull them back from a peer
/// that hasn't yet, restarting its own countdown (an endless ping-pong).
#[test]
fn a_dead_process_is_retired_everywhere_despite_skewed_arrival() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut d = GossipState::new(NodeName::from("d").node_id(), settings(5, 1));
    let mut p = GossipState::new(NodeName::from("p").node_id(), settings(5, 1));
    let mut r = GossipState::new(NodeName::from("r").node_id(), settings(5, 1));
    for _ in 0..3 {
        d.limit(&rule, "c").unwrap().unwrap();
    }
    let sent = delta(&mut d);
    p.accept(&sent);
    drop(d);
    clock.advance(400);
    r.accept(&sent);

    // Ten seconds of gossip: tick, forget idle clients, exchange.
    for _ in 0..100 {
        clock.advance(100);
        let (from_p, from_r) = (p.tick(), r.tick());
        p.expire_keys();
        r.expire_keys();
        let (full_p, full_r) = (p.full_state(), r.full_state());
        exchange(from_r, &mut p, &full_r);
        exchange(from_p, &mut r, &full_p);
    }
    assert_eq!(p.check(&rule, "c").unwrap(), 5);
    assert_eq!(r.check(&rule, "c").unwrap(), 5);
    assert_eq!((p.bucket_count(), r.bucket_count()), (0, 0));
}

/// Incarnations started later compare greater, so a node can tell its own
/// older processes apart.
#[test]
fn later_incarnations_are_ordered_after_earlier_ones() {
    let clock = virtual_clock::install();
    let node = NodeName::from("n").node_id();
    let first = colibri::node::ActorId::fresh(node);
    clock.advance(1);
    let second = colibri::node::ActorId::fresh(node);
    assert!(second > first);
}
