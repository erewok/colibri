use colibri::api::cluster::{
    ClusterStatusResponse, PrepareChangeRequest, PrepareChangeResponse, TopologyResponse,
};

/// Tests that validate the admin tool's expectations of the cluster API
#[tokio::test]
async fn test_admin_tool_expected_responses() {
    // Test that the admin tool can handle the cluster status response format
    let status = ClusterStatusResponse {
        node_id: "node_123".to_string(),
        operational_state: "normal".to_string(),
        active_change_id: None,
        last_topology_change: None,
        data_consistency_ok: true,
    };

    // Admin tool expects this to serialize/deserialize cleanly
    let json = serde_json::to_string(&status).expect("Admin tool expects serializable status");
    let parsed: ClusterStatusResponse =
        serde_json::from_str(&json).expect("Admin tool expects parseable status");

    assert_eq!(parsed.operational_state, "normal");
    assert!(parsed.data_consistency_ok);
}

#[tokio::test]
async fn test_admin_tool_topology_validation() {
    // Test the topology response format that admin tool expects
    let topology = TopologyResponse {
        node_id: "node_456".to_string(),
        total_buckets: 4,
        owned_bucket: Some(2),
        replica_buckets: vec![1, 3],
        peer_nodes: vec!["192.168.1.100:8001".to_string()],
        topology_version: 1,
    };

    // Admin tool parses this to understand cluster state
    let json = serde_json::to_string(&topology).expect("Admin tool expects serializable topology");
    let parsed: TopologyResponse =
        serde_json::from_str(&json).expect("Admin tool expects parseable topology");

    assert_eq!(parsed.node_id, "node_456");
    assert_eq!(parsed.total_buckets, 4);
    assert_eq!(parsed.owned_bucket, Some(2));
    assert_eq!(parsed.peer_nodes.len(), 1);
}

#[tokio::test]
async fn test_admin_tool_prepare_change_workflow() {
    // Test the prepare change request/response cycle
    let request = PrepareChangeRequest {
        new_topology: vec![
            "192.168.1.100:8001".to_string(),
            "192.168.1.101:8001".to_string(),
            "192.168.1.102:8001".to_string(),
        ],
        change_id: "resize_001".to_string(),
        timeout_seconds: 300,
    };

    // Admin tool sends this request
    let _request_json =
        serde_json::to_string(&request).expect("Admin tool should serialize request");

    // Simulate successful response
    let response = PrepareChangeResponse {
        node_id: "node_789".to_string(),
        ready: true,
        error: None,
        data_exported: false,
    };

    // Admin tool expects to parse this response
    let response_json = serde_json::to_string(&response).expect("API should serialize response");
    let parsed_response: PrepareChangeResponse =
        serde_json::from_str(&response_json).expect("Admin tool should parse response");

    assert!(parsed_response.ready);
    assert_eq!(parsed_response.error, None);

    // Test error response handling
    let error_response = PrepareChangeResponse {
        node_id: "node_789".to_string(),
        ready: false,
        error: Some("New topology cannot be empty".to_string()),
        data_exported: false,
    };

    let error_json =
        serde_json::to_string(&error_response).expect("API should serialize error response");
    let parsed_error: PrepareChangeResponse =
        serde_json::from_str(&error_json).expect("Admin tool should parse error response");

    assert_eq!(parsed_error.ready, false);
    assert_eq!(
        parsed_error.error,
        Some("New topology cannot be empty".to_string())
    );
}

#[tokio::test]
async fn test_admin_tool_health_check_expectations() {
    // Test that health responses match what admin tool expects to see
    use colibri::api::cluster::HealthResponse;

    let health = HealthResponse {
        node_id: "test_node".to_string(),
        status: "healthy".to_string(),
        replication_active: true,
        buckets_owned: vec![0, 1],
        buckets_replicated: vec![2, 3],
    };

    // Admin tool needs to parse health responses for validation
    let json = serde_json::to_string(&health).expect("Health should serialize");
    let parsed: HealthResponse =
        serde_json::from_str(&json).expect("Admin tool should parse health");

    assert_eq!(parsed.status, "healthy");
    assert!(parsed.replication_active);
    assert_eq!(parsed.buckets_owned.len(), 2);
    assert_eq!(parsed.buckets_replicated.len(), 2);
}
