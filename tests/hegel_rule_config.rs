//! State-machine tests for `RuleConfig`, the replicated map of named rate-limit
//! rules that every cluster node keeps.
//!
//! Each node creates, changes and deletes rules locally, stamping every
//! change with a version: wall-clock time, raised past any version the node
//! has already seen for that rule, then the node id. Hegel delivers the
//! nodes' maps to each other in any order, or not at all. Whatever happens,
//! once every node has heard from every other one, they must all hold the
//! same map, and each rule must be in the state its newest change left it.

mod common;

use std::collections::BTreeMap;

use colibri::clock::virtual_clock::{self, VirtualClockGuard};
use colibri::limiters::RuleName;
use colibri::limiters::rule_config::{RuleConfig, RuleVersion};
use colibri::node::NodeId;
use colibri::settings::RateLimitSettings;
use hegel::TestCase;
use hegel::generators as gs;

use common::node_ids;

const NUM_NODES: usize = 3;
const RULES: [&str; 3] = ["checkout", "login", "search"];

struct RuleConfigCluster {
    clock: VirtualClockGuard,
    ids: Vec<NodeId>,
    configs: Vec<RuleConfig>,
    /// The newest change any node has made to each rule: its version, and
    /// the settings it set (`None` for a delete).
    newest: BTreeMap<String, (RuleVersion, Option<RateLimitSettings>)>,
}

impl RuleConfigCluster {
    fn draw_node(&self, tc: &TestCase) -> usize {
        tc.draw(gs::integers::<usize>().max_value(NUM_NODES - 1))
    }

    fn draw_rule(&self, tc: &TestCase) -> RuleName {
        RuleName::from(tc.draw(gs::sampled_from(RULES.to_vec())))
    }

    /// Note the change a node just made, if its version is the newest yet.
    /// It may not be: a node whose clock is behind, and that has not yet
    /// heard of another node's change, makes an older one.
    fn record(&mut self, node: usize, rule: &RuleName) {
        let entry = self.configs[node]
            .entry(rule)
            .expect("a change leaves an entry, even a delete");
        let change = (entry.version, entry.settings.clone());
        let newest = self
            .newest
            .entry(rule.as_str().to_string())
            .or_insert_with(|| change.clone());
        if change.0 > newest.0 {
            *newest = change;
        }
    }
}

#[hegel::state_machine]
impl RuleConfigCluster {
    #[rule]
    fn set_rule(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        let rule = self.draw_rule(&tc);
        let settings = RateLimitSettings {
            rate_limit_max_calls_allowed: tc.draw(gs::integers::<u32>().min_value(1).max_value(5)),
            rate_limit_interval_seconds: tc.draw(gs::integers::<u32>().min_value(1).max_value(3)),
        };
        self.configs[node].set(rule.clone(), settings, self.ids[node]);
        self.record(node, &rule);
        tc.note(&format!("node {node} sets {rule}"));
    }

    #[rule]
    fn delete_rule(&mut self, tc: TestCase) {
        let node = self.draw_node(&tc);
        let rule = self.draw_rule(&tc);
        self.configs[node].delete(rule.clone(), self.ids[node]);
        self.record(node, &rule);
        tc.note(&format!("node {node} deletes {rule}"));
    }

    /// Clocks are not synchronised: a node may change a rule at the same wall
    /// time as another, or with its clock behind. The version still orders
    /// every change, and a node's own changes always move forward.
    #[rule]
    fn move_clock(&mut self, tc: TestCase) {
        if tc.draw(gs::booleans()) {
            self.clock
                .advance(tc.draw(gs::integers::<u32>().max_value(50)));
        } else {
            self.clock
                .step_wall(-tc.draw(gs::integers::<i64>().min_value(0).max_value(50)));
        }
    }

    /// One node's map reaches another.
    #[rule]
    fn deliver(&mut self, tc: TestCase) {
        let from = self.draw_node(&tc);
        let to = self.draw_node(&tc);
        if from != to {
            let sent = self.configs[from].clone();
            self.configs[to].merge(&sent);
        }
    }

    /// Every node hears from every other node: all must now agree, on the
    /// newest change to every rule, and on the digest.
    #[rule]
    fn settle(&mut self, _tc: TestCase) {
        for _ in 0..2 {
            for from in 0..NUM_NODES {
                let sent = self.configs[from].clone();
                for to in (0..NUM_NODES).filter(|&t| t != from) {
                    self.configs[to].merge(&sent);
                }
            }
        }
        for node in 1..NUM_NODES {
            assert_eq!(
                self.configs[node], self.configs[0],
                "node {node} disagrees with node 0"
            );
            assert_eq!(self.configs[node].digest(), self.configs[0].digest());
        }
        for rule in RULES {
            let expected = self
                .newest
                .get(rule)
                .and_then(|(_, settings)| settings.clone());
            assert_eq!(
                self.configs[0].get(&RuleName::from(rule)).cloned(),
                expected,
                "{rule} is not in the state its newest change left it"
            );
        }
    }

    /// Merging is idempotent and order-independent, so receiving the same
    /// map twice, or two maps in either order, gives the same result.
    #[invariant]
    fn merge_is_idempotent_and_commutative(&mut self, _tc: TestCase) {
        let (a, b) = (&self.configs[0], &self.configs[1]);
        let mut ab = a.clone();
        ab.merge(b);
        let mut ba = b.clone();
        ba.merge(a);
        assert_eq!(ab, ba, "merge order changed the result");
        let mut twice = ab.clone();
        twice.merge(b);
        assert_eq!(twice, ab, "merging the same map twice changed the result");
    }

    /// Equal maps have equal digests, so a heartbeat digest mismatch is the
    /// only signal needed to start a sync.
    #[invariant]
    fn equal_maps_have_equal_digests(&mut self, _tc: TestCase) {
        for i in 0..NUM_NODES {
            for j in 0..NUM_NODES {
                if self.configs[i] == self.configs[j] {
                    assert_eq!(self.configs[i].digest(), self.configs[j].digest());
                }
            }
        }
    }
}

#[hegel::test]
fn rule_configs_converge_on_the_newest_change(tc: TestCase) {
    let cluster = RuleConfigCluster {
        clock: virtual_clock::install(),
        ids: node_ids(NUM_NODES),
        configs: vec![RuleConfig::default(); NUM_NODES],
        newest: BTreeMap::new(),
    };
    hegel::stateful::machine(cluster).run(tc);
}
