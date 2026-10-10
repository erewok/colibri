//! Deterministic regressions for gossip scenarios found in review.

use std::collections::BTreeMap;

use colibri::clock::virtual_clock;
use colibri::limiters::RuleName;
use colibri::node::gossip::messages::{GossipMessage, GossipPacket};
use colibri::node::gossip::state::{
    GossipState, HeartbeatDigest, StateSync, TickOutput, partition_of,
};
use colibri::node::{ActorId, NodeName};
use colibri::settings::RateLimitSettings;

fn settings(max_calls: u32, interval_seconds: u32) -> RateLimitSettings {
    RateLimitSettings {
        rate_limit_max_calls_allowed: max_calls,
        rate_limit_interval_seconds: interval_seconds,
    }
}

fn delta(state: &mut GossipState) -> StateSync {
    state.tick().delta.expect("expected a delta")
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

fn heartbeat(state: &mut GossipState) -> HeartbeatDigest {
    state.tick().heartbeat.expect("expected a heartbeat")
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

/// Deliver `from`'s tick output to `to`. On a heartbeat whose partitions
/// differ, `to` pulls those partitions and pushes its own; returns whether
/// they differed.
fn exchange(output: TickOutput, from: &mut GossipState, to: &mut GossipState) -> bool {
    if let Some(updates) = output.delta {
        to.accept(&updates);
    }
    let Some(h) = output.heartbeat else {
        return false;
    };
    let differ = to.on_heartbeat(&h).partitions;
    if !differ.is_empty() {
        to.accept(&from.partition_state(&differ));
        from.accept(&to.partition_state(&differ));
    }
    !differ.is_empty()
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
        exchange(from_r, &mut r, &mut p);
        exchange(from_p, &mut p, &mut r);
    }
    assert_eq!(p.check(&rule, "c").unwrap(), 5);
    assert_eq!(r.check(&rule, "c").unwrap(), 5);
    assert_eq!((p.bucket_count(), r.bucket_count()), (0, 0));
}

/// Incarnations started a second or more later compare greater, so a node can tell its own
/// older processes apart.
#[test]
fn later_incarnations_are_ordered_after_earlier_ones() {
    let clock = virtual_clock::install();
    let node = NodeName::from("n").node_id();
    let first = ActorId::fresh(node);
    clock.advance(1_000);
    let second = ActorId::fresh(node);
    assert!(second > first);
}

/// A client that never goes idle sheds the slot of a node's dead process once
/// it is retired, so its gossiped state doesn't grow with every restart.
#[test]
fn a_hot_client_sheds_a_restarted_process_slot() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let node = NodeName::from("a").node_id();
    let old = ActorId {
        node,
        incarnation: 1,
    };
    let mut a = GossipState::with_actor(old, settings(100, 1));
    let mut b = GossipState::new(NodeName::from("b").node_id(), settings(100, 1));
    a.limit(&rule, "c").unwrap().unwrap();
    b.accept(&delta(&mut a));

    // The process restarts; both nodes keep serving the client.
    let mut a = GossipState::with_actor(
        ActorId {
            node,
            incarnation: 2,
        },
        settings(100, 1),
    );
    for _ in 0..50 {
        clock.advance(100);
        a.limit(&rule, "c").unwrap().unwrap();
        b.limit(&rule, "c").unwrap().unwrap();
        let (from_a, from_b) = (a.tick(), b.tick());
        exchange(from_b, &mut b, &mut a);
        exchange(from_a, &mut a, &mut b);
    }
    for (name, state) in [("a", &a), ("b", &b)] {
        let actors = state.limiter(&rule).unwrap().bucket_actors("c").unwrap();
        assert!(
            !actors.contains(&old),
            "{name} still holds {old}: {actors:?}"
        );
    }
}

/// After a restart, the new process compacts its predecessor's slot. A peer
/// that still holds it in a live bucket must not make it pull state forever.
#[test]
fn compaction_after_a_restart_settles() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let node = NodeName::from("a").node_id();
    let mut a = GossipState::with_actor(
        ActorId {
            node,
            incarnation: 1,
        },
        settings(5, 10),
    );
    let mut b = GossipState::new(NodeName::from("b").node_id(), settings(5, 10));
    a.limit(&rule, "c").unwrap().unwrap();
    b.accept(&delta(&mut a));

    // The process restarts and stays quiet. The peer admits a request that
    // keeps its bucket, and the dead slot in it, live past retirement.
    let mut a = GossipState::with_actor(
        ActorId {
            node,
            incarnation: 2,
        },
        settings(5, 10),
    );
    let mut late_pulls = 0;
    for round in 0..150 {
        clock.advance(100);
        if round == 50 {
            b.limit(&rule, "c").unwrap().unwrap();
        }
        let (from_a, from_b) = (a.tick(), b.tick());
        a.expire_keys();
        b.expire_keys();
        let differed = exchange(from_b, &mut b, &mut a) | exchange(from_a, &mut a, &mut b);
        late_pulls += usize::from(round >= 110 && differed);
    }
    assert_eq!(
        late_pulls, 0,
        "still pulling state after the cluster settled"
    );
}

/// A dead process's requests keep counting until they age out, even once its
/// slot is known to be dead and compactable.
#[test]
fn a_dead_process_requests_count_until_they_age_out() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let node = NodeName::from("a").node_id();
    let mut a = GossipState::with_actor(
        ActorId {
            node,
            incarnation: 1,
        },
        settings(5, 10),
    );
    let mut b = GossipState::new(NodeName::from("b").node_id(), settings(5, 10));
    for _ in 0..3 {
        a.limit(&rule, "c").unwrap().unwrap();
    }
    b.accept(&delta(&mut a));

    let mut a = GossipState::with_actor(
        ActorId {
            node,
            incarnation: 2,
        },
        settings(5, 10),
    );
    a.limit(&rule, "c").unwrap().unwrap();
    b.accept(&delta(&mut a));
    a.accept(&b.full_state());
    clock.advance(1_000);
    a.tick();
    b.tick();
    assert_eq!(b.check(&rule, "c").unwrap(), 1);
    assert_eq!(a.check(&rule, "c").unwrap(), 1);
}

/// Two in-sync nodes; then `a` updates `client` and the delta is lost.
fn one_missed_update(client: &str) -> (GossipState, GossipState) {
    let rule = RuleName::default();
    let mut a = GossipState::new(NodeName::from("a").node_id(), settings(5, 60));
    let mut b = GossipState::new(NodeName::from("b").node_id(), settings(5, 60));
    for c in 0..300 {
        a.limit(&rule, &format!("client-{c}")).unwrap().unwrap();
    }
    b.accept(&delta(&mut a));
    a.limit(&rule, client).unwrap().unwrap();
    let _lost = delta(&mut a);
    (a, b)
}

/// A node that missed one update pulls only that client's partition.
#[test]
fn a_peer_that_missed_one_update_pulls_only_its_partition() {
    let _clock = virtual_clock::install();
    let (mut a, mut b) = one_missed_update("client-7");
    let differ = b.on_heartbeat(&heartbeat(&mut a)).partitions;
    assert_eq!(differ, vec![partition_of("client-7")]);

    let pulled = a.partition_state(&differ);
    let clients: usize = pulled.updates.iter().map(|r| r.buckets.len()).sum();
    assert!(clients < 300 / 8, "pulled {clients} of 300 clients");
    b.accept(&pulled);
    assert_eq!(b.check(&RuleName::default(), "client-7").unwrap(), 3);
    assert!(b.on_heartbeat(&heartbeat(&mut a)).partitions.is_empty());
}

/// The node that is ahead also sees the difference in a behind peer's
/// heartbeat, so a busy node (which only sends deltas) can push the repair.
#[test]
fn a_node_ahead_sees_a_behind_peers_partition() {
    let _clock = virtual_clock::install();
    let (mut a, mut b) = one_missed_update("client-7");
    let differ = a.on_heartbeat(&heartbeat(&mut b)).partitions;
    assert_eq!(differ, vec![partition_of("client-7")]);
    b.accept(&a.partition_state(&differ));
    assert_eq!(b.check(&RuleName::default(), "client-7").unwrap(), 3);
}

/// A heartbeat fits one datagram however many clients a node holds.
#[test]
fn a_heartbeat_fits_one_datagram() {
    let _clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut a = GossipState::new(NodeName::from("a").node_id(), settings(5, 60));
    for c in 0..10_000 {
        a.limit(&rule, &format!("client-{c}")).unwrap().unwrap();
    }
    let _ = delta(&mut a);
    let h = heartbeat(&mut a);
    let packet = GossipPacket::new(GossipMessage::Heartbeat {
        response_addr: "127.0.0.1:7946".parse().unwrap(),
        timestamp: 0,
        node_id: NodeName::from("a").node_id(),
        incarnations: h.incarnations,
        partitions: h.partitions,
        rules_digest: h.rules,
    });
    let size = packet.serialize().unwrap().len();
    assert!(size <= 1400, "{size}-byte heartbeat");
}

/// One reliable gossip round: in `order`, each node ticks, forgets idle
/// clients, and delivers to every other.
/// Returns whether any partitions differed.
fn gossip_round(nodes: &mut [GossipState], order: &[usize]) -> bool {
    let mut differed = false;
    for &sender in order {
        let output = nodes[sender].tick();
        nodes[sender].expire_keys();
        for receiver in (0..nodes.len()).filter(|&r| r != sender) {
            let (from, to) = if sender < receiver {
                let (left, right) = nodes.split_at_mut(receiver);
                (&mut left[sender], &mut right[0])
            } else {
                let (left, right) = nodes.split_at_mut(sender);
                (&mut right[0], &mut left[receiver])
            };
            differed |= exchange(output.clone(), from, to);
        }
    }
    differed
}

/// Nodes retire and compact a dead process's slot at different ticks. One
/// that has compacted must not take the slot back from one that hasn't yet.
#[test]
fn a_compacted_slot_is_not_taken_back_from_a_peer() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let dead = NodeName::from("d").node_id();
    let mut d = GossipState::with_actor(
        ActorId {
            node: dead,
            incarnation: 1,
        },
        settings(5, 1),
    );
    let mut nodes: Vec<GossipState> = ["p", "q", "r"]
        .iter()
        .map(|n| GossipState::new(NodeName::from(*n).node_id(), settings(5, 1)))
        .collect();
    d.limit(&rule, "c").unwrap().unwrap();
    let sent = delta(&mut d);
    let mut d = GossipState::with_actor(
        ActorId {
            node: dead,
            incarnation: 2,
        },
        settings(5, 1),
    );
    let restarted = heartbeat(&mut d);
    for node in &mut nodes {
        node.accept(&sent);
        node.on_heartbeat(&restarted);
    }

    clock.advance(1_002);
    let still_moving = (0..10)
        .filter(|_| gossip_round(&mut nodes, &[0, 1, 2]))
        .count();
    assert!(
        still_moving <= 2,
        "state moved in {still_moving} of 10 rounds"
    );
    for node in &nodes {
        assert_eq!(node.check(&rule, "c").unwrap(), 5);
    }
}

/// A lost delta stays visible in the digest after a later delta for another
/// client from the same writer is delivered.
#[test]
fn a_lost_delta_is_not_masked_by_a_later_one() {
    let _clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut writer = GossipState::new(NodeName::from("w").node_id(), settings(5, 60));
    let mut peer = GossipState::new(NodeName::from("p").node_id(), settings(5, 60));
    writer.limit(&rule, "alice").unwrap().unwrap();
    let _lost = delta(&mut writer);
    writer.limit(&rule, "bob").unwrap().unwrap();
    peer.accept(&delta(&mut writer));

    let differ = peer.on_heartbeat(&heartbeat(&mut writer)).partitions;
    assert!(differ.contains(&partition_of("alice")));
}

/// A process that hears of a newer incarnation of its own node (a duplicate
/// name, or a clock behind at restart) must not compact its own slot: it
/// would restart below what peers hold, and they would miss its requests.
#[test]
fn a_process_never_compacts_its_own_slot() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let w = NodeName::from("w").node_id();
    let mut newer = GossipState::with_actor(
        ActorId {
            node: w,
            incarnation: 2,
        },
        settings(10, 1),
    );
    let mut live = GossipState::with_actor(
        ActorId {
            node: w,
            incarnation: 1,
        },
        settings(10, 1),
    );
    let mut p = GossipState::new(NodeName::from("p").node_id(), settings(10, 1));
    live.on_heartbeat(&heartbeat(&mut newer));

    for _ in 0..3 {
        live.limit(&rule, "c").unwrap().unwrap();
    }
    p.accept(&delta(&mut live));
    clock.advance(1_002);
    if let Some(aged) = live.tick().delta {
        p.accept(&aged);
    }
    p.tick();
    for _ in 0..3 {
        live.limit(&rule, "c").unwrap().unwrap();
    }
    p.accept(&delta(&mut live));
    assert_eq!(live.check(&rule, "c").unwrap(), 7);
    assert_eq!(
        p.check(&rule, "c").unwrap(),
        7,
        "p must count live's 3 requests"
    );
}

/// Two buckets in one partition both missed by a peer: their hash changes
/// must not cancel out in the partition's sum.
#[test]
fn partition_hashes_do_not_cancel() {
    let _clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut by_partition: BTreeMap<u16, Vec<String>> = BTreeMap::new();
    for c in 0..4000 {
        let client = format!("client-{c}");
        by_partition
            .entry(partition_of(&client))
            .or_default()
            .push(client);
    }
    let mut masked = 0;
    for (partition, clients) in by_partition.iter().take(20) {
        for pair in clients.chunks_exact(2).take(10) {
            for (n1, n2) in [
                (1, 1),
                (2, 3),
                (4, 4),
                (5, 7),
                (10, 2),
                (6, 6),
                (3, 9),
                (8, 8),
            ] {
                let mut w = GossipState::new(NodeName::from("w").node_id(), settings(1000, 60));
                let mut p = GossipState::new(NodeName::from("p").node_id(), settings(1000, 60));
                for (client, n) in [(&pair[0], n1), (&pair[1], n2)] {
                    for _ in 0..n {
                        w.limit(&rule, client).unwrap().unwrap();
                    }
                }
                p.accept(&delta(&mut w));
                w.limit(&rule, &pair[0]).unwrap().unwrap();
                w.limit(&rule, &pair[1]).unwrap().unwrap();
                let _lost = delta(&mut w);
                masked += usize::from(
                    !p.on_heartbeat(&heartbeat(&mut w))
                        .partitions
                        .contains(partition),
                );
            }
        }
    }
    assert_eq!(
        masked, 0,
        "{masked} lost updates hidden by cancelling hashes"
    );
}

/// A busy node (deltas every tick) knows of a restart its peer doesn't. Its
/// deltas carry what it knows, so their partition hashes agree.
#[test]
fn incarnation_knowledge_travels_with_deltas() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let n = NodeName::from("n").node_id();
    let mut n1 = GossipState::with_actor(
        ActorId {
            node: n,
            incarnation: 1,
        },
        settings(10, 1),
    );
    let mut n2 = GossipState::with_actor(
        ActorId {
            node: n,
            incarnation: 2,
        },
        settings(10, 1),
    );
    let mut a = GossipState::new(NodeName::from("a").node_id(), settings(10, 1));
    let mut b = GossipState::new(NodeName::from("b").node_id(), settings(10, 1));
    n1.limit(&rule, "c").unwrap().unwrap();
    let sent = delta(&mut n1);
    a.accept(&sent);
    b.accept(&sent);
    a.on_heartbeat(&heartbeat(&mut n2)); // only A hears of N2
    clock.advance(1_100);
    a.tick();
    b.tick();

    let mut differing_rounds = 0;
    for _ in 0..20 {
        clock.advance(50);
        a.limit(&rule, "c").unwrap();
        let from_a = a.tick();
        differing_rounds += usize::from(exchange(from_a, &mut a, &mut b));
        let from_b = b.tick();
        differing_rounds += usize::from(exchange(from_b, &mut b, &mut a));
    }
    assert!(
        differing_rounds <= 1,
        "partitions differed in {differing_rounds} exchanges"
    );
}

/// A node with changes on every tick still sends heartbeats, so anti-entropy
/// keeps running under constant load.
#[test]
fn a_busy_node_still_sends_heartbeats() {
    let _clock = virtual_clock::install();
    let rule = RuleName::default();
    let mut a = GossipState::new(NodeName::from("a").node_id(), settings(1000, 60));
    let heartbeats = (0..20)
        .filter(|_| {
            a.limit(&rule, "c").unwrap().unwrap();
            let output = a.tick();
            assert!(output.delta.is_some());
            output.heartbeat.is_some()
        })
        .count();
    assert!(heartbeats >= 2, "{heartbeats} heartbeats in 20 busy ticks");
}

/// With gossip ticks longer than the window, a compacted slot's tombstone
/// expires between ticks. A dead slot must still settle rather than circulate.
#[test]
fn a_dead_slot_settles_when_ticks_outlast_the_window() {
    let clock = virtual_clock::install();
    let rule = RuleName::default();
    let names = ["n0", "n1", "n2"];
    let node = |i: usize, incarnation| ActorId {
        node: NodeName::from(names[i]).node_id(),
        incarnation,
    };
    let mut nodes: Vec<GossipState> = (0..3)
        .map(|i| GossipState::with_actor(node(i, 0), settings(1, 1)))
        .collect();
    nodes[1].limit(&rule, "c").unwrap().unwrap();
    gossip_round(&mut nodes, &[0, 1, 2]);
    nodes[1] = GossipState::with_actor(node(1, 1), settings(1, 1));

    for order in [
        [1, 0, 2],
        [0, 1, 2],
        [0, 1, 2],
        [0, 1, 2],
        [0, 1, 2],
        [0, 1, 2],
    ] {
        clock.advance(2_001);
        gossip_round(&mut nodes, &order);
    }
    let held: Vec<usize> = nodes.iter().map(GossipState::bucket_count).collect();
    assert_eq!(held, vec![0, 0, 0], "buckets still held");
}
