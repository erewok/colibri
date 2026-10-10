//! State-machine tests for a gossip cluster, driving each node's
//! `GossipState`: the same code the gossip controller runs, minus the network.
//!
//! Each rule mirrors one step of the production protocol in
//! `node::gossip::controller`, over an in-memory network whose deliveries
//! Hegel chooses, so any message can be dropped:
//!
//! - `request`: a client request lands on one node, under the default rule or
//!   the named rule.
//! - `gossip_tick`: the node ages entries, then pushes a delta, or sends a
//!   heartbeat when there is nothing to push. A peer whose state is behind
//!   pulls the sender's full state; a peer whose rule configuration differs
//!   pushes its own, and the sender answers with its own if that differs.
//! - `change_named_rule` / `delete_named_rule`: a node changes the named rule
//!   and broadcasts its configuration. A change or recreation starts a new
//!   *version* of the rule, which starts from zero.
//! - `expire_keys`: a node forgets clients with nothing live anywhere.
//! - `restart_node`: a node loses all its state, including the rules it had
//!   learned, and starts a new *incarnation* (a new CRDT actor).
//! - `settle`: gossip runs on a reliable network until quiescent; every node
//!   must then agree on the rules and on every client.
//! - `wait_out_the_window`: time passes until every request has aged out or,
//!   if its process is gone, been retired; everything must be back to full.
//!
//! Every guarantee is checked per rule version, so named rules must behave
//! exactly like the default rule. The invariants are the implementation-level
//! counterparts of `tla/Gossip.tla`, plus the rate-limit guarantee: within any
//! ageing window, each node incarnation admits at most `max_calls` per client
//! and rule version.

mod common;

use std::collections::{BTreeSet, HashMap};

use colibri::clock::virtual_clock::{self, VirtualClockGuard};
use colibri::limiters::rule_config::RuleVersion;
use colibri::limiters::{DistributedBucketExternal, RuleName};
use colibri::node::gossip::state::{GossipState, TickOutput};
use colibri::node::{ActorId, NodeId};
use colibri::settings::RateLimitSettings;
use hegel::TestCase;
use hegel::generators as gs;

use common::{
    CLIENTS, ageing_window_ms, assert_within_window_bound, draw_advance_ms, draw_client,
    draw_settings, node_ids,
};

const NUM_NODES: usize = 3;
const NAMED_RULE: &str = "named";

/// Upper bound on reliable gossip rounds `settle` may take.
const MAX_SETTLE_ROUNDS: usize = 2 * NUM_NODES + 4;

/// A rule version: `None` for the default rule, which has only one.
type Version = Option<RuleVersion>;

struct GossipCluster {
    clock: VirtualClockGuard,
    /// When false, the clock stays frozen, so nothing ages and every run
    /// spends its steps on gossip interleavings rather than on time.
    time_flows: bool,
    default_settings: RateLimitSettings,
    /// The settings of every version of the named rule ever created.
    named_versions: HashMap<RuleVersion, RateLimitSettings>,
    ids: Vec<NodeId>,
    nodes: Vec<GossipState>,
    /// Current incarnation of each node; a restart increments it.
    incarnation: Vec<u64>,
    /// Admission times per (rule, version, node, incarnation, client).
    admitted_at: HashMap<(RuleName, Version, usize, u64, String), Vec<i64>>,
    /// The highest value each actor has had in its own slot, per rule
    /// version and client: no replica can have seen more.
    writer_high: HashMap<(RuleName, Version, ActorId, String), u64>,
    /// Requests by earlier incarnations admitted before this time are known to
    /// be retired everywhere (`wait_out_the_window` ran since).
    retired_before: i64,
}

fn named() -> RuleName {
    RuleName::from(NAMED_RULE)
}

impl GossipCluster {
    fn draw_node(&self, tc: &TestCase) -> usize {
        tc.draw(gs::integers::<usize>().max_value(NUM_NODES - 1))
    }

    fn draw_rule(&self, tc: &TestCase) -> RuleName {
        tc.draw(gs::sampled_from(vec![RuleName::default(), named()]))
    }

    fn rules() -> [RuleName; 2] {
        [RuleName::default(), named()]
    }

    /// The version of `rule` that `node` enforces, if it knows the rule.
    fn version_at(&self, node: usize, rule: &RuleName) -> Option<Version> {
        if *rule == RuleName::default() {
            Some(None)
        } else {
            self.nodes[node].rule_version(rule).map(Some)
        }
    }

    fn settings_of(&self, version: Version) -> RateLimitSettings {
        match version {
            None => self.default_settings.clone(),
            Some(v) => self.named_versions[&v].clone(),
        }
    }

    fn capacity(&self, version: Version) -> u32 {
        self.settings_of(version).rate_limit_max_calls_allowed
    }

    fn actor(&self, node: usize, incarnation: u64) -> ActorId {
        ActorId {
            node: self.ids[node],
            incarnation,
        }
    }

    /// Every actor so far, with the node it belongs to.
    fn all_actors(&self) -> Vec<(usize, ActorId)> {
        (0..NUM_NODES)
            .flat_map(|n| (0..=self.incarnation[n]).map(move |i| (n, i)))
            .map(|(n, i)| (n, self.actor(n, i)))
            .collect()
    }

    /// Note a new version of the named rule just made on `node`.
    fn record_named_version(&mut self, node: usize, settings: RateLimitSettings) {
        if let Some(v) = self.nodes[node].rule_version(&named()) {
            self.named_versions.entry(v).or_insert(settings);
        }
    }

    /// `node`'s rule configuration reaches `peer`, as a broadcast does: `peer`
    /// answers with its own config if it differs, and pulls state for any rule
    /// it just started. `reply_delivered` decides whether those replies arrive.
    fn send_rules(&mut self, node: usize, peer: usize, reply_delivered: bool) -> bool {
        let rules = self.nodes[node].rule_config().clone();
        let reply = self.nodes[peer].merge_rules(&rules);
        if reply_delivered {
            if reply.send_rules {
                let back = self.nodes[peer].rule_config().clone();
                self.nodes[node].merge_rules(&back);
            }
            if reply.request_state {
                let state = self.nodes[node].full_state();
                self.nodes[peer].accept(&state);
            }
        }
        reply.send_rules || reply.request_state
    }

    /// Send `node`'s rule configuration to each peer over a lossy network.
    fn broadcast_rules(&mut self, tc: &TestCase, node: usize) {
        for peer in (0..NUM_NODES).filter(|&p| p != node) {
            let delivered = tc.draw(gs::booleans());
            let reply_delivered = tc.draw(gs::booleans());
            if delivered {
                self.send_rules(node, peer, reply_delivered);
            }
        }
    }

    /// Deliver one node's tick output to another, running the receiver's
    /// anti-entropy checks for a heartbeat. Returns true if any state moved.
    fn deliver(
        &mut self,
        output: &TickOutput,
        sender: usize,
        receiver: usize,
        response_delivered: bool,
    ) -> bool {
        match output {
            TickOutput::Delta(updates) => {
                self.nodes[receiver].accept(updates);
                true
            }
            TickOutput::Heartbeat(digest) => {
                let reply = self.nodes[receiver].on_heartbeat(digest);
                let differ = !reply.partitions.is_empty();
                if response_delivered {
                    if differ {
                        // The receiver pushes its partitions and pulls the sender's.
                        let pushed = self.nodes[receiver].partition_state(&reply.partitions);
                        let pulled = self.nodes[sender].partition_state(&reply.partitions);
                        self.nodes[sender].accept(&pushed);
                        self.nodes[receiver].accept(&pulled);
                    }
                    if reply.push_rules {
                        self.send_rules(receiver, sender, true);
                    }
                }
                differ || reply.push_rules
            }
        }
    }

    /// Admission times of `node`'s current incarnation for `client`.
    fn admissions(&self, rule: &RuleName, v: Version, node: usize, client: &str) -> &[i64] {
        self.admitted_at
            .get(&(
                rule.clone(),
                v,
                node,
                self.incarnation[node],
                client.to_string(),
            ))
            .map_or(&[], Vec::as_slice)
    }

    /// Admission times for `client` by current incarnations, sorted.
    fn cluster_admissions(&self, rule: &RuleName, v: Version, client: &str) -> Vec<i64> {
        let mut times: Vec<i64> = (0..NUM_NODES)
            .flat_map(|n| self.admissions(rule, v, n, client).iter().copied())
            .collect();
        times.sort_unstable();
        times
    }

    /// Every admission for `client`, as (time, node, incarnation), sorted.
    fn all_admissions(&self, rule: &RuleName, v: Version, client: &str) -> Vec<(i64, usize, u64)> {
        let mut all: Vec<(i64, usize, u64)> = self
            .admitted_at
            .iter()
            .filter(|((r, rv, _, _, c), _)| r == rule && *rv == v && c == client)
            .flat_map(|(&(_, _, node, inc, _), times)| times.iter().map(move |&t| (t, node, inc)))
            .collect();
        all.sort_unstable();
        all
    }

    /// Requests by earlier incarnations that may still be counted: the dead
    /// process never aged them, and peers retire them only once they have sat
    /// unchanged for a window, which `wait_out_the_window` guarantees.
    fn dead_pending(&self, rule: &RuleName, v: Version, client: &str) -> u32 {
        self.all_admissions(rule, v, client)
            .iter()
            .filter(|&&(t, n, inc)| inc != self.incarnation[n] && t >= self.retired_before)
            .count() as u32
    }

    /// Admissions recent enough that no node can have aged them yet.
    fn count_unexpired(&self, v: Version, times: &[i64]) -> u32 {
        let now_ms = self.clock.now_ms();
        let window = ageing_window_ms(&self.settings_of(v));
        times.iter().filter(|&&t| now_ms - t <= window).count() as u32
    }

    fn remaining(&self, node: usize, rule: &RuleName, client: &str) -> Option<u32> {
        self.nodes[node].check(rule, client).ok()
    }

    fn holds(&self, node: usize, rule: &RuleName, client: &str) -> bool {
        self.nodes[node].limiter(rule).is_some_and(|l| {
            l.client_delta_state_for_gossip(&client.to_string())
                .is_some()
        })
    }

    fn bucket_states(
        &self,
        node: usize,
        rule: &RuleName,
    ) -> HashMap<String, DistributedBucketExternal> {
        self.nodes[node]
            .limiter(rule)
            .map(|l| {
                l.full_state_dump()
                    .into_iter()
                    .map(|b| (b.client_id.clone(), b))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every node's view of every actor's slot for `client`, for failure
    /// messages, as (requests, expiries).
    fn describe_slots(&self, rule: &RuleName, client: &str) -> String {
        (0..NUM_NODES)
            .map(|n| {
                let slots: Vec<String> = match self.bucket_states(n, rule).get(client) {
                    None => vec!["no bucket".to_string()],
                    Some(b) => self
                        .all_actors()
                        .iter()
                        .map(|(_, a)| format!("{a}:{:?}", b.counter.slot(*a)))
                        .collect(),
                };
                format!("  node {n}: {}", slots.join(" "))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Record each current writer's slots, before anything discards them.
    fn observe_writers(&mut self) {
        let high_of = |b: &DistributedBucketExternal, a: &ActorId| {
            let (requests, expiries) = b.counter.slot(*a);
            requests.max(expiries).max(b.counter.vclock.get(a))
        };
        for rule in Self::rules() {
            for writer in 0..NUM_NODES {
                let Some(v) = self.version_at(writer, &rule) else {
                    continue;
                };
                let actor = self.actor(writer, self.incarnation[writer]);
                for (client, bucket) in self.bucket_states(writer, &rule) {
                    let high = self
                        .writer_high
                        .entry((rule.clone(), v, actor, client))
                        .or_insert(0);
                    *high = (*high).max(high_of(&bucket, &actor));
                }
            }
        }
    }

    /// Forget a node's idle clients, recording what its writer held first.
    fn expire_keys_at(&mut self, node: usize) {
        self.observe_writers();
        self.nodes[node].expire_keys();
    }

    /// Run reliable gossip rounds until no state moves. Each round also
    /// forgets idle clients, as the server's key-expiry loop does.
    fn run_until_quiescent(&mut self) -> usize {
        let mut rounds = 0;
        loop {
            for node in 0..NUM_NODES {
                self.expire_keys_at(node);
            }
            let mut transferred = false;
            for sender in 0..NUM_NODES {
                let output = self.nodes[sender].tick();
                for receiver in (0..NUM_NODES).filter(|&r| r != sender) {
                    transferred |= self.deliver(&output, sender, receiver, true);
                }
            }
            if !transferred {
                return rounds;
            }
            rounds += 1;
            assert!(
                rounds <= MAX_SETTLE_ROUNDS,
                "gossip still transferring state after {MAX_SETTLE_ROUNDS} reliable rounds"
            );
        }
    }

    /// The version of `rule` every node agrees on after settling, or `None`
    /// if the rule is unknown everywhere (deleted, or lost with a restart).
    fn settled_version(&self, rule: &RuleName, when: &str) -> Option<Version> {
        let versions: BTreeSet<Option<Version>> =
            (0..NUM_NODES).map(|n| self.version_at(n, rule)).collect();
        assert_eq!(
            versions.len(),
            1,
            "{when}: nodes disagree on the version of {rule}: {versions:?}"
        );
        versions.into_iter().next().flatten()
    }

    /// After settling, every node agrees on every rule, and each view counts
    /// the current incarnations' requests still in the window, plus any
    /// earlier incarnations' requests not yet retired.
    fn assert_settled_views(&self, when: &str) {
        for rule in Self::rules() {
            let Some(v) = self.settled_version(&rule, when) else {
                continue;
            };
            let capacity = self.capacity(v);
            for client in CLIENTS {
                let views: Vec<u32> = (0..NUM_NODES)
                    .map(|n| self.remaining(n, &rule, client).expect("rule is known"))
                    .collect();
                let live = self.count_unexpired(v, &self.cluster_admissions(&rule, v, client));
                let pending = self.dead_pending(&rule, v, client);
                // Each node retires a dead incarnation's requests on its own timer,
                // so nodes agree only once none are pending.
                assert!(
                    pending > 0 || views.iter().all(|&view| view == views[0]),
                    "{when}: nodes disagree on {rule} for {client}: {views:?}\n{}",
                    self.describe_slots(&rule, client)
                );
                let (low, high) = (
                    capacity.saturating_sub(live + pending),
                    capacity.saturating_sub(live),
                );
                for view in views {
                    assert!(
                        low <= view && view <= high,
                        "{when}: {rule} view of {client} is {view}; expected {low}..={high} \
                         ({live} live, {pending} from earlier incarnations)\n{}",
                        self.describe_slots(&rule, client)
                    );
                }
            }
        }
    }
}

#[hegel::state_machine]
impl GossipCluster {
    #[rule]
    fn request(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        let rule = self.draw_rule(&tc);
        let client = draw_client(&tc);
        let version = self.version_at(node, &rule);
        let result = self.nodes[node].limit(&rule, &client);
        tc.note(&format!("node {node} request {rule} {client}: {result:?}"));
        match (result, version) {
            (Ok(Some(_)), Some(v)) => {
                let now_ms = self.clock.now_ms();
                self.admitted_at
                    .entry((
                        rule.clone(),
                        v,
                        node,
                        self.incarnation[node],
                        client.clone(),
                    ))
                    .or_default()
                    .push(now_ms);
                let live = self.count_unexpired(v, &self.cluster_admissions(&rule, v, &client));
                if live > self.capacity(v) {
                    tc.event("cluster admitted beyond max_calls within one window");
                }
                if v.is_some() && self.named_versions.len() > 1 {
                    tc.event("admitted under a changed or recreated named rule");
                }
            }
            (Ok(None), _) => tc.event("request denied"),
            (Err(_), _) => tc.event("named rule not known on this node"),
            (Ok(Some(_)), None) => unreachable!("admitted under a rule the node doesn't know"),
        }
    }

    #[rule]
    fn advance_time(&mut self, tc: TestCase) {
        if self.time_flows {
            let longest = std::iter::once(self.default_settings.clone())
                .chain(self.named_versions.values().cloned())
                .max_by_key(|s| s.rate_limit_interval_seconds)
                .expect("there is a default rule");
            let ms = draw_advance_ms(&tc, &longest);
            self.clock.advance(ms);
        }
    }

    /// One node's gossip tick over a lossy network: each peer independently
    /// receives the message or not.
    #[rule]
    fn gossip_tick(&mut self, tc: TestCase) {
        let sender = self.draw_node(&tc);
        let output = self.nodes[sender].tick();
        for receiver in (0..NUM_NODES).filter(|&r| r != sender) {
            let delivered = tc.draw(gs::booleans());
            let response_delivered = tc.draw(gs::booleans());
            if delivered
                && self.deliver(&output, sender, receiver, response_delivered)
                && matches!(output, TickOutput::Heartbeat(_))
            {
                tc.event("heartbeat triggered anti-entropy");
            }
        }
    }

    /// A node creates, changes or recreates the named rule, and broadcasts its
    /// configuration over a lossy network.
    #[rule]
    fn change_named_rule(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        let settings = draw_settings(&tc);
        self.nodes[node]
            .create_rule(named(), settings.clone())
            .expect("a named rule can be created");
        self.record_named_version(node, settings);
        tc.event("named rule changed");
        self.broadcast_rules(&tc, node);
    }

    /// A node deletes the named rule, and broadcasts its configuration.
    #[rule]
    fn delete_named_rule(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        self.nodes[node]
            .delete_rule(named())
            .expect("the named rule can be deleted");
        tc.event("named rule deleted");
        self.broadcast_rules(&tc, node);
    }

    /// A node forgets clients that have nothing live anywhere.
    #[rule]
    fn expire_keys(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        let before = self.nodes[node].bucket_count();
        self.expire_keys_at(node);
        if self.nodes[node].bucket_count() < before {
            tc.event("expire_keys dropped a bucket");
        }
    }

    /// A node crashes and comes back under the same name with no state, as a
    /// new incarnation. A restart takes time, so the clock moves at least a
    /// millisecond; the wall clock may also have been stepped back, which must
    /// not matter.
    #[rule]
    fn restart_node(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        self.observe_writers();
        self.clock.advance(1);
        self.incarnation[node] += 1;
        if tc.draw(gs::weighted_booleans(0.2)) {
            let step_back = tc.draw(gs::integers::<i64>().min_value(1).max_value(5_000));
            self.clock.step_wall(-step_back);
            tc.event("wall clock stepped back across a restart");
        }
        let actor = self.actor(node, self.incarnation[node]);
        self.nodes[node] = GossipState::with_actor(actor, self.default_settings.clone());
        tc.event("node restarted");
    }

    /// With no new traffic and a reliable network, gossip must spread every
    /// rule to every node and bring every node to the same view of every
    /// client. Then every node forgets its idle clients, and further gossip
    /// must not bring them back.
    #[rule]
    fn settle(&mut self, tc: TestCase) {
        self.run_until_quiescent();
        self.assert_settled_views("after gossip settled");

        for node in 0..NUM_NODES {
            self.expire_keys_at(node);
        }
        for rule in Self::rules() {
            let Some(v) = self.settled_version(&rule, "after expiring") else {
                continue;
            };
            for client in CLIENTS {
                let live = self.count_unexpired(v, &self.cluster_admissions(&rule, v, client));
                if self.dead_pending(&rule, v, client) > 0 {
                    continue;
                }
                for node in 0..NUM_NODES {
                    assert_eq!(
                        self.holds(node, &rule, client),
                        live > 0,
                        "after expire_keys, node {node} should hold {rule} {client} only if it \
                         has live requests ({live} live)"
                    );
                }
                if live == 0 && !self.all_admissions(&rule, v, client).is_empty() {
                    tc.event("settled cluster forgot an idle client");
                }
            }
        }

        self.run_until_quiescent();
        self.assert_settled_views("after expiring and settling again");
        for rule in Self::rules() {
            let Some(v) = self.settled_version(&rule, "after settling again") else {
                continue;
            };
            for client in CLIENTS {
                let live = self.count_unexpired(v, &self.cluster_admissions(&rule, v, client));
                let pending = self.dead_pending(&rule, v, client);
                for node in 0..NUM_NODES {
                    assert!(
                        live > 0 || pending > 0 || !self.holds(node, &rule, client),
                        "gossip resurrected idle client {rule} {client} on node {node}"
                    );
                }
            }
        }
    }

    /// Let every window pass with the cluster gossiping: every request ages out
    /// or, if its process is gone, is retired by the peers; then every count
    /// is back to full and every bucket is forgotten.
    #[rule]
    fn wait_out_the_window(&mut self, tc: TestCase) {
        if !self.time_flows {
            return;
        }
        let window = std::iter::once(&self.default_settings)
            .chain(self.named_versions.values())
            .map(ageing_window_ms)
            .max()
            .expect("there is a default rule");
        // Gossip as running nodes do: each tick, in a drawn order, every node
        // ticks, forgets idle clients and delivers to its peers. A node that
        // dropped a client may pull it back from a peer that hasn't yet.
        let step = tc.draw(
            gs::integers::<u32>()
                .min_value(window as u32 / 20 + 1)
                .max_value(window as u32 / 2 + 1),
        );
        let mut elapsed = 0;
        while elapsed < 4 * window + 4 {
            self.clock.advance(step);
            elapsed += i64::from(step);
            let order = tc.draw(gs::permutations((0..NUM_NODES).collect::<Vec<_>>()));
            for sender in order {
                let output = self.nodes[sender].tick();
                self.expire_keys_at(sender);
                for receiver in (0..NUM_NODES).filter(|&r| r != sender) {
                    self.deliver(&output, sender, receiver, true);
                }
            }
        }
        self.run_until_quiescent();
        for node in 0..NUM_NODES {
            self.expire_keys_at(node);
        }
        for rule in Self::rules() {
            let Some(v) = self.settled_version(&rule, "after waiting") else {
                continue;
            };
            for client in CLIENTS {
                for node in 0..NUM_NODES {
                    assert_eq!(
                        self.remaining(node, &rule, client),
                        Some(self.capacity(v)),
                        "{rule} {client} not back to full on node {node}\n{}",
                        self.describe_slots(&rule, client)
                    );
                    assert!(
                        !self.holds(node, &rule, client),
                        "node {node} still holds idle {rule} {client}"
                    );
                }
            }
        }
        if self.incarnation.iter().any(|&i| i > 0) {
            tc.event("waited out the window after a restart");
        }
        self.retired_before = self.clock.now_ms();
    }

    /// GCounter single-writer discipline (`WriterHighWater` and
    /// `WriterNotBehind` in `tla/Gossip.tla`), per actor and rule version:
    ///
    /// - No replica has seen more of an actor's slot than it ever wrote.
    /// - While an actor is live and has requests outstanding, no replica has
    ///   seen more of its requests than it holds: max-merge would hide them.
    ///   (Replicas may count more of its expiries, having retired stale ones.)
    #[invariant(always_run)]
    fn writer_slot_never_goes_backwards(&mut self, _tc: TestCase) {
        self.observe_writers();
        let high_of = |b: &DistributedBucketExternal, a: &ActorId| {
            let (requests, expiries) = b.counter.slot(*a);
            requests.max(expiries).max(b.counter.vclock.get(a))
        };
        let actors = self.all_actors();
        for rule in Self::rules() {
            let versions: Vec<Option<Version>> =
                (0..NUM_NODES).map(|n| self.version_at(n, &rule)).collect();
            let states: Vec<_> = (0..NUM_NODES)
                .map(|n| self.bucket_states(n, &rule))
                .collect();
            for &(writer, actor) in &actors {
                let Some(v) = versions[writer] else {
                    continue;
                };
                let current = actor.incarnation == self.incarnation[writer];
                for replica in (0..NUM_NODES).filter(|&r| versions[r] == Some(v)) {
                    for (client, bucket) in &states[replica] {
                        let seen = high_of(bucket, &actor);
                        let high = self
                            .writer_high
                            .get(&(rule.clone(), v, actor, client.clone()))
                            .copied()
                            .unwrap_or(0);
                        assert!(
                            seen <= high,
                            "node {replica} has seen {seen} in {actor}'s {rule} slot for \
                             {client}, but {actor} only ever wrote up to {high}"
                        );
                        let own = states[writer]
                            .get(client)
                            .filter(|b| current && b.counter.outstanding(actor) > 0);
                        if let Some(own) = own {
                            let (own_req, _) = own.counter.slot(actor);
                            let (seen_req, _) = bucket.counter.slot(actor);
                            assert!(
                                seen_req <= own_req
                                    && bucket.counter.vclock.get(&actor)
                                        <= own.counter.vclock.get(&actor),
                                "{actor}'s {rule} slot for {client} went backwards: it holds \
                                 {own_req} requests, but node {replica} has seen {seen_req}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// A node's own slot counts as live only requests it still holds a record
    /// of, and so will age out (`OwnLiveTracked` in `tla/Gossip.tla`). It may
    /// count fewer: peers retire requests older than the window before a node
    /// that has not ticked ages them itself.
    #[invariant(always_run)]
    fn own_live_tracked(&mut self, _tc: TestCase) {
        for (node, state) in self.nodes.iter().enumerate() {
            for rule in Self::rules() {
                let Some(limiter) = state.limiter(&rule) else {
                    continue;
                };
                for client in CLIENTS {
                    if let Some((outstanding, tracked)) = limiter.own_slot_audit(client) {
                        assert!(
                            outstanding <= tracked,
                            "node {node}'s {rule} slot for {client} counts {outstanding} live, \
                             but it tracks {tracked}"
                        );
                    }
                }
            }
        }
    }

    /// No replica counts more expiries than requests for any actor, so ageing
    /// never gives back a token that was not taken (`ExpiriesWithinRequests`).
    #[invariant(always_run)]
    fn expiries_within_requests(&mut self, _tc: TestCase) {
        let actors = self.all_actors();
        for replica in 0..NUM_NODES {
            for rule in Self::rules() {
                for bucket in self.bucket_states(replica, &rule).values() {
                    for (_, actor) in &actors {
                        let (requests, expiries) = bucket.counter.slot(*actor);
                        assert!(
                            expiries <= requests,
                            "node {replica} counts {expiries} expiries but only {requests} \
                             requests for {actor}'s {rule} slot for {}",
                            bucket.client_id
                        );
                    }
                }
            }
        }
    }

    /// A node's view must count at least its current incarnation's requests
    /// that are too recent to have aged, and can never count more than every
    /// request ever admitted under that rule version.
    #[invariant(always_run)]
    fn views_bounded_by_admissions(&mut self, _tc: TestCase) {
        for rule in Self::rules() {
            for node in 0..NUM_NODES {
                let Some(v) = self.version_at(node, &rule) else {
                    continue;
                };
                let capacity = self.capacity(v);
                for client in CLIENTS {
                    let view = self.remaining(node, &rule, client).expect("rule is known");
                    let ever = self.all_admissions(&rule, v, client).len() as u32;
                    let own_live = self.count_unexpired(v, self.admissions(&rule, v, node, client));
                    assert!(
                        capacity.saturating_sub(ever) <= view
                            && view <= capacity.saturating_sub(own_live),
                        "node {node} sees {view} remaining for {rule} {client}; expected between \
                         {} (all {ever} admissions) and {} (its own {own_live} unexpired)",
                        capacity.saturating_sub(ever),
                        capacity.saturating_sub(own_live),
                    );
                }
            }
        }
    }

    /// The rate-limit guarantee, per rule version: each node incarnation stays
    /// within `max_calls` per window, so the cluster stays within `max_calls`
    /// times the incarnations admitting in that window.
    #[invariant(always_run)]
    fn admissions_within_cluster_guarantee(&mut self, _tc: TestCase) {
        let keys: BTreeSet<(RuleName, Version, String)> = self
            .admitted_at
            .keys()
            .map(|(r, v, _, _, c)| (r.clone(), *v, c.clone()))
            .collect();
        for (rule, v, client) in keys {
            let window = ageing_window_ms(&self.settings_of(v));
            let capacity = self.capacity(v) as usize;
            for ((r, rv, node, inc, c), times) in &self.admitted_at {
                if *r == rule && *rv == v && *c == client {
                    assert_within_window_bound(
                        &format!("{rule}, node {node} incarnation {inc}, {client}"),
                        times,
                        capacity,
                        window,
                    );
                }
            }
            let all = self.all_admissions(&rule, v, &client);
            for (i, &(start, _, _)) in all.iter().enumerate() {
                let in_window: Vec<_> = all[i..]
                    .iter()
                    .take_while(|&&(t, _, _)| t - start <= window)
                    .collect();
                let incarnations: BTreeSet<(usize, u64)> =
                    in_window.iter().map(|&&(_, n, inc)| (n, inc)).collect();
                assert!(
                    in_window.len() <= capacity * incarnations.len(),
                    "{rule}, cluster, {client}: {} admissions within {window} ms of t={start} \
                     from {} node incarnation(s), limit is {}",
                    in_window.len(),
                    incarnations.len(),
                    capacity * incarnations.len()
                );
            }
        }
    }
}

fn run_cluster(tc: TestCase, time_flows: bool) {
    // Install the clock first: creating a rule stamps it with the time.
    let clock = virtual_clock::install();
    let default_settings = draw_settings(&tc);
    let named_settings = draw_settings(&tc);
    let ids = node_ids(NUM_NODES);
    let nodes: Vec<GossipState> = ids
        .iter()
        .map(|&id| GossipState::with_actor(id.into(), default_settings.clone()))
        .collect();
    let mut cluster = GossipCluster {
        clock,
        time_flows,
        default_settings,
        named_versions: HashMap::new(),
        ids,
        nodes,
        incarnation: vec![0; NUM_NODES],
        admitted_at: HashMap::new(),
        writer_high: HashMap::new(),
        retired_before: i64::MIN,
    };
    // The named rule starts on node 0 only; the others learn it by gossip.
    cluster.nodes[0]
        .create_rule(named(), named_settings.clone())
        .expect("a new named rule can be created");
    cluster.record_named_version(0, named_settings);
    hegel::stateful::machine(cluster).run(tc);
}

#[hegel::test]
fn gossip_cluster_with_frozen_time(tc: TestCase) {
    run_cluster(tc, false);
}

#[hegel::test]
fn gossip_cluster_with_ageing(tc: TestCase) {
    run_cluster(tc, true);
}
