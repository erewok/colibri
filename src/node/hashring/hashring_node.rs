use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use super::HashringController;
use crate::error::{ColibriError, Result};
use crate::limiters::{RuleList, RuleName, SerializableRule};
use crate::node::messages::{
    CheckCallsRequest, CheckCallsResponse, Message, StatusResponse, TopologyChangeRequest,
    TopologyResponse,
};
use crate::node::{Node, NodeName};
use crate::settings;

/// Consistent hash ring distributed rate limiter node
#[derive(Clone)]
pub struct HashringNode {
    pub node_name: NodeName,

    /// The hashring controller (manages all TCP communication)
    pub controller: Arc<HashringController>,
}

impl HashringNode {
    /// Stop the hashring receiver task
    pub fn stop_receiver(&self) {
        self.controller.stop_receiver();
    }

    /// Stop all background tasks
    pub fn stop_all_tasks(&self) {
        self.stop_receiver();
    }
}

impl Drop for HashringNode {
    fn drop(&mut self) {
        // Clean up receiver task via controller when the node is dropped
        self.controller.stop_receiver();
    }
}

impl std::fmt::Debug for HashringNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashringNode")
            .field("node_name", &self.node_name)
            .finish()
    }
}

#[async_trait]
impl Node for HashringNode {
    async fn new(settings: settings::Settings) -> Result<Self> {
        let node_name: NodeName = settings.node_name();
        let listen_api = format!(
            "{}:{}",
            settings.client_listen_address, settings.client_listen_port
        );
        info!(
            "[Node<{}>] Hashring node starting at {} with {} nodes in topology: {:?}",
            &node_name,
            listen_api,
            settings.topology.len(),
            settings.topology
        );

        // Create controller
        let controller = Arc::new(HashringController::new(settings.clone()).await?);

        // Start the controller's TCP receiver
        controller.start_receiver().await?;
        controller.start_rule_sync()?;

        Ok(Self {
            node_name,
            controller,
        })
    }

    async fn check_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id,
            rule_name: None,
            consume_token: false,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CheckLimit".to_string(),
            )),
        }
    }

    async fn rate_limit(&self, client_id: String) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id,
            rule_name: None,
            consume_token: true,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for RateLimit".to_string(),
            )),
        }
    }

    async fn expire_keys(&self) -> Result<()> {
        self.controller.expire_keys()
    }

    async fn create_named_rule(&self, rule: SerializableRule) -> Result<()> {
        let message = Message::CreateRateLimitRule(rule);
        match self.controller.handle_message(message).await? {
            Message::CreateRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CreateRule".to_string(),
            )),
        }
    }

    async fn delete_named_rule(&self, rule_name: RuleName) -> Result<()> {
        let message = Message::DeleteRateLimitRule { rule_name };
        match self.controller.handle_message(message).await? {
            Message::DeleteRateLimitRuleResponse => Ok(()),
            _ => Err(ColibriError::Api(
                "Unexpected response type for DeleteRule".to_string(),
            )),
        }
    }

    async fn list_named_rules(&self) -> Result<RuleList> {
        let message = Message::ListRateLimitRules;
        match self.controller.handle_message(message).await? {
            Message::ListRateLimitRulesResponse(rules) => Ok(rules),
            _ => Err(ColibriError::Api(
                "Unexpected response type for ListRules".to_string(),
            )),
        }
    }

    async fn get_named_rule(&self, rule_name: RuleName) -> Result<Option<SerializableRule>> {
        let message = Message::GetRateLimitRule { rule_name };
        match self.controller.handle_message(message).await? {
            Message::GetRateLimitRuleResponse(rule) => Ok(rule),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetRule".to_string(),
            )),
        }
    }

    async fn rate_limit_custom(
        &self,
        rule_name: RuleName,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key,
            rule_name: Some(rule_name),
            consume_token: true,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for RateLimitCustom".to_string(),
            )),
        }
    }

    async fn check_limit_custom(
        &self,
        rule_name: RuleName,
        key: String,
    ) -> Result<Option<CheckCallsResponse>> {
        let request = CheckCallsRequest {
            client_id: key,
            rule_name: Some(rule_name),
            consume_token: false,
            forwarding_depth: 0,
        };
        let message = Message::RateLimitRequest(request);

        match self.controller.handle_message(message).await? {
            Message::RateLimitResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for CheckLimitCustom".to_string(),
            )),
        }
    }
}

// Cluster-specific methods for HashringNode
impl HashringNode {
    pub async fn handle_cluster_health(&self) -> Result<StatusResponse> {
        let message = Message::GetStatus;
        match self.controller.handle_message(message).await? {
            Message::StatusResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetStatus".to_string(),
            )),
        }
    }

    pub async fn handle_get_topology(&self) -> Result<TopologyResponse> {
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetTopology".to_string(),
            )),
        }
    }

    pub async fn handle_new_topology(
        &self,
        _request: TopologyChangeRequest,
    ) -> Result<TopologyResponse> {
        warn!("Topology changes not yet implemented for hashring nodes");

        // For now, just return current topology
        let message = Message::GetTopology;
        match self.controller.handle_message(message).await? {
            Message::TopologyResponse(response) => Ok(response),
            _ => Err(ColibriError::Api(
                "Unexpected response type for GetTopology".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limiters::rule_config::RuleConfig;
    use crate::node::hashring::consistent_hashing::jump_consistent_hash;
    use crate::settings::{self, RunMode};

    /// Regression (found by the demo scripts): the request that takes the last
    /// token is admitted with 0 remaining, not reported as rate limited.
    #[tokio::test]
    async fn test_last_token_is_admitted() {
        let mut settings = settings::tests::sample();
        settings.rate_limit_max_calls_allowed = 1;
        settings.run_mode = RunMode::Hashring;
        // A one-node ring: this node owns every client, so nothing is forwarded.
        settings
            .topology
            .insert(settings.server_name.clone(), "127.0.0.1:0".to_string());
        let node = HashringNode::new(settings).await.unwrap();
        let client_id = "last_token_client".to_string();

        let first = node.rate_limit(client_id.clone()).await.unwrap();
        assert_eq!(first.map(|r| r.calls_remaining), Some(0));
        assert!(node.rate_limit(client_id).await.unwrap().is_none());
    }

    fn bucket_count(node: &HashringNode) -> usize {
        node.controller
            .named_rate_limiters
            .pin()
            .values()
            .map(|limiter| limiter.lock().unwrap().bucket_count())
            .sum()
    }

    /// Idle clients are forgotten after two intervals, as in single-node mode.
    /// By then their bucket has refilled to capacity, so forgetting it cannot
    /// change any rate-limit decision.
    #[tokio::test]
    async fn test_expire_keys_drops_idle_buckets() {
        let clock = crate::clock::virtual_clock::install();
        let mut settings = settings::tests::sample();
        settings.run_mode = RunMode::Hashring;
        settings
            .topology
            .insert(settings.server_name.clone(), "127.0.0.1:0".to_string());
        let interval_ms = settings.rate_limit_interval_seconds * 1000;
        let node = HashringNode::new(settings).await.unwrap();

        node.rate_limit("idle-client".to_string()).await.unwrap();
        clock.advance(interval_ms);
        node.rate_limit("recent-client".to_string()).await.unwrap();
        clock.advance(interval_ms + 1);
        node.expire_keys().await.unwrap();

        assert_eq!(bucket_count(&node), 1, "only the recent client is kept");
        let check = node.check_limit("recent-client".to_string()).await.unwrap();
        assert!(check.is_some());
    }

    fn checkout_rule(max_calls: u32) -> SerializableRule {
        SerializableRule {
            name: RuleName::from("checkout"),
            settings: settings::RateLimitSettings {
                rate_limit_max_calls_allowed: max_calls,
                rate_limit_interval_seconds: 60,
            },
        }
    }

    /// Settings for node `index` of a ring whose nodes use `peer_ports`.
    fn ring_settings(index: usize, peer_ports: &[u16]) -> settings::Settings {
        let mut settings = settings::tests::sample();
        settings.run_mode = RunMode::Hashring;
        settings.server_name = format!("ring-node-{index}");
        settings.peer_listen_port = peer_ports[index];
        settings.topology = peer_ports
            .iter()
            .enumerate()
            .map(|(i, port)| (format!("ring-node-{i}"), format!("127.0.0.1:{port}")))
            .collect();
        settings
    }

    fn free_ports(n: usize) -> Vec<u16> {
        let listeners: Vec<_> = (0..n)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect()
    }

    /// A rule configuration received from a peer creates its rules here.
    #[tokio::test]
    async fn test_rule_config_sync_creates_rules() {
        let ports = free_ports(1);
        let node = HashringNode::new(ring_settings(0, &ports)).await.unwrap();
        let rule = checkout_rule(3);
        let mut config = RuleConfig::default();
        config.set(
            rule.name.clone(),
            rule.settings.clone(),
            NodeName::from("peer").node_id(),
        );

        node.controller
            .handle_message(Message::RuleConfigSync(config))
            .await
            .unwrap();

        assert_eq!(
            node.get_named_rule(rule.name.clone()).await.unwrap(),
            Some(rule)
        );
    }

    /// A rule created or deleted on one node reaches every node, and a client
    /// gets one limit under it whichever node its calls arrive at.
    #[tokio::test]
    async fn test_named_rules_reach_every_node_and_share_one_limit() {
        let ports = free_ports(2);
        let a = HashringNode::new(ring_settings(0, &ports)).await.unwrap();
        let b = HashringNode::new(ring_settings(1, &ports)).await.unwrap();
        let rule = checkout_rule(3);

        a.create_named_rule(rule.clone()).await.unwrap();
        assert_eq!(
            b.get_named_rule(rule.name.clone()).await.unwrap(),
            Some(rule.clone())
        );

        let mut admitted = 0;
        for i in 0..8 {
            let node = if i % 2 == 0 { &a } else { &b };
            let result = node
                .rate_limit_custom(rule.name.clone(), "shopper".to_string())
                .await
                .unwrap();
            admitted += usize::from(result.is_some());
        }
        assert_eq!(admitted, 3, "one limit for the client across both nodes");

        b.delete_named_rule(rule.name.clone()).await.unwrap();
        assert_eq!(a.get_named_rule(rule.name.clone()).await.unwrap(), None);
    }

    /// A node that was down when a rule was created learns it once it is up,
    /// from the periodic rule sync.
    #[tokio::test]
    async fn test_a_node_that_missed_a_rule_change_catches_up() {
        let ports = free_ports(2);
        let a = HashringNode::new(ring_settings(0, &ports)).await.unwrap();
        let rule = checkout_rule(3);
        a.create_named_rule(rule.clone()).await.unwrap();

        let b = HashringNode::new(ring_settings(1, &ports)).await.unwrap();
        let mut learned = None;
        for _ in 0..40 {
            learned = b.get_named_rule(rule.name.clone()).await.unwrap();
            if learned.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert_eq!(learned, Some(rule), "node b never learned the rule");
    }

    /// After a node joins, every node's idea of which clients it owns follows
    /// the new ring: no client is owned twice, and none bounces between nodes.
    #[tokio::test]
    async fn test_ownership_follows_topology_changes() {
        let ports = free_ports(3);
        let node = HashringNode::new(ring_settings(1, &ports)).await.unwrap();
        let joiner = NodeName::from("ring-node-0a");
        node.controller
            .handle_message(Message::AddNode {
                name: joiner.clone(),
                address: "127.0.0.1:1".parse().unwrap(),
            })
            .await
            .unwrap();

        let mut ring: Vec<String> = (0..3).map(|i| format!("ring-node-{i}")).collect();
        ring.push(joiner.to_string());
        ring.sort();
        for i in 0..200 {
            let client = format!("client-{i}");
            let owner = &ring[jump_consistent_hash(&client, ring.len() as u32) as usize];
            let claims = node
                .controller
                .find_owner_if_not_self(&client)
                .await
                .unwrap();
            assert_eq!(
                claims.is_none(),
                owner == "ring-node-1",
                "{client} belongs to {owner}, but ring-node-1 says {claims:?}"
            );
        }
    }

    /// A rule deleted and recreated elsewhere starts from zero here, even when
    /// this node only hears the final configuration (same settings, new version).
    #[tokio::test]
    async fn test_a_recreated_rule_starts_from_zero() {
        let ports = free_ports(1);
        let node = HashringNode::new(ring_settings(0, &ports)).await.unwrap();
        let rule = checkout_rule(3);
        let peer = NodeName::from("peer").node_id();
        let mut config = RuleConfig::default();
        config.set(rule.name.clone(), rule.settings.clone(), peer);
        node.controller
            .handle_message(Message::RuleConfigSync(config.clone()))
            .await
            .unwrap();
        for _ in 0..2 {
            node.rate_limit_custom(rule.name.clone(), "c".to_string())
                .await
                .unwrap();
        }

        config.delete(rule.name.clone(), peer);
        config.set(rule.name.clone(), rule.settings.clone(), peer);
        node.controller
            .handle_message(Message::RuleConfigSync(config))
            .await
            .unwrap();

        let check = node
            .check_limit_custom(rule.name.clone(), "c".to_string())
            .await
            .unwrap();
        assert_eq!(check.map(|c| c.calls_remaining), Some(3));
    }

    /// Rule names are trimmed, as in gossip mode.
    #[tokio::test]
    async fn test_rule_names_are_trimmed() {
        let ports = free_ports(1);
        let node = HashringNode::new(ring_settings(0, &ports)).await.unwrap();
        let mut rule = checkout_rule(3);
        rule.name = RuleName::from("  checkout ");
        node.create_named_rule(rule).await.unwrap();
        assert!(
            node.get_named_rule(RuleName::from("checkout"))
                .await
                .unwrap()
                .is_some()
        );
    }
}
