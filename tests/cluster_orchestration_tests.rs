use colibri::api::cluster::{
    BucketExport, ClientBucketData, ClusterStatusResponse, PrepareChangeRequest,
    PrepareChangeResponse, TopologyResponse,
};

#[tokio::test]
async fn test_topology_response_serialization() {
    let topology = TopologyResponse {
        node_id: "node1".to_string(),
        total_buckets: 4,
        owned_bucket: Some(2),
        replica_buckets: vec![1, 3],
        peer_nodes: vec![
            "http://127.0.0.1:8001".to_string(),
            "http://127.0.0.1:8002".to_string(),
        ],
        topology_version: 1,
    };

    // Test JSON serialization/deserialization
    let json = serde_json::to_string(&topology).expect("Should serialize");
    let deserialized: TopologyResponse = serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.node_id, "node1");
    assert_eq!(deserialized.total_buckets, 4);
    assert_eq!(deserialized.owned_bucket, Some(2));
    assert_eq!(deserialized.replica_buckets, vec![1, 3]);
    assert_eq!(deserialized.peer_nodes.len(), 2);
}

#[tokio::test]
async fn test_prepare_change_request_serialization() {
    let request = PrepareChangeRequest {
        new_topology: vec![
            "127.0.0.1:8001".to_string(),
            "127.0.0.1:8002".to_string(),
            "127.0.0.1:8003".to_string(),
        ],
        change_id: "change_123".to_string(),
        timeout_seconds: 300,
    };

    let json = serde_json::to_string(&request).expect("Should serialize");
    let deserialized: PrepareChangeRequest =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.new_topology.len(), 3);
    assert_eq!(deserialized.change_id, "change_123");
    assert_eq!(deserialized.timeout_seconds, 300);
}

#[tokio::test]
async fn test_prepare_change_response_serialization() {
    let response = PrepareChangeResponse {
        node_id: "node1".to_string(),
        ready: true,
        error: None,
        data_exported: true,
    };

    let json = serde_json::to_string(&response).expect("Should serialize");
    let deserialized: PrepareChangeResponse =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.node_id, "node1");
    assert!(deserialized.ready);
    assert_eq!(deserialized.error, None);
    assert!(deserialized.data_exported);
}

#[tokio::test]
async fn test_cluster_status_response_serialization() {
    let status = ClusterStatusResponse {
        node_id: "node1".to_string(),
        operational_state: "preparing".to_string(),
        active_change_id: Some("change_123".to_string()),
        last_topology_change: Some(1640995200),
        data_consistency_ok: true,
    };

    let json = serde_json::to_string(&status).expect("Should serialize");
    let deserialized: ClusterStatusResponse =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.node_id, "node1");
    assert_eq!(deserialized.operational_state, "preparing");
    assert_eq!(
        deserialized.active_change_id,
        Some("change_123".to_string())
    );
    assert_eq!(deserialized.last_topology_change, Some(1640995200));
    assert_eq!(deserialized.data_consistency_ok, true);
}

#[test]
fn test_bucket_export_with_topology_change() {
    // Test that BucketExport can handle large datasets during topology changes
    let export = BucketExport {
        bucket_id: 42,
        client_data: (0..1000)
            .map(|i| ClientBucketData {
                client_id: format!("client_{}", i),
                tokens: 95.5 - (i as f64 * 0.1),
                last_call: 1640995200 + i,
            })
            .collect(),
        export_timestamp: 1640995200,
    };

    // Verify large export can be serialized
    let json = serde_json::to_string(&export).expect("Should serialize large export");
    assert!(json.len() > 10000); // Should be substantial JSON

    // Verify deserialization
    let deserialized: BucketExport =
        serde_json::from_str(&json).expect("Should deserialize large export");
    assert_eq!(deserialized.bucket_id, 42);
    assert_eq!(deserialized.client_data.len(), 1000);
    assert_eq!(deserialized.client_data[0].client_id, "client_0");
    assert_eq!(deserialized.client_data[999].client_id, "client_999");
}

#[tokio::test]
async fn test_prepare_change_validation() {
    // Test empty topology validation
    let empty_request = PrepareChangeRequest {
        new_topology: vec![],
        change_id: "test_change".to_string(),
        timeout_seconds: 300,
    };

    // Verify it serializes correctly (the validation happens in the API endpoint)
    let json = serde_json::to_string(&empty_request).expect("Should serialize");
    let deserialized: PrepareChangeRequest =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.new_topology.len(), 0);
    assert_eq!(deserialized.change_id, "test_change");

    // Test valid topology request
    let valid_request = PrepareChangeRequest {
        new_topology: vec!["127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()],
        change_id: "valid_change".to_string(),
        timeout_seconds: 300,
    };

    let json = serde_json::to_string(&valid_request).expect("Should serialize");
    let deserialized: PrepareChangeRequest =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.new_topology.len(), 2);
    assert_eq!(deserialized.change_id, "valid_change");
}

#[tokio::test]
async fn test_prepare_change_response_error_handling() {
    // Test response with error
    let error_response = PrepareChangeResponse {
        node_id: "node1".to_string(),
        ready: false,
        error: Some("New topology cannot be empty".to_string()),
        data_exported: false,
    };

    let json = serde_json::to_string(&error_response).expect("Should serialize");
    let deserialized: PrepareChangeResponse =
        serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.ready, false);
    assert_eq!(
        deserialized.error,
        Some("New topology cannot be empty".to_string())
    );
    assert_eq!(deserialized.data_exported, false);

    // Test successful response
    let success_response = PrepareChangeResponse {
        node_id: "node1".to_string(),
        ready: true,
        error: None,
        data_exported: false,
    };

    let json = serde_json::to_string(&success_response).expect("Should serialize");
    let deserialized: PrepareChangeResponse =
        serde_json::from_str(&json).expect("Should deserialize");

    assert!(deserialized.ready);
    assert_eq!(deserialized.error, None);
}

#[test]
fn test_gossip_node_export_behavior() {
    // Test that gossip nodes handle export differently
    // (This tests the data structure behavior that would be returned
    // from a gossip node export - empty client data)
    let gossip_export = BucketExport {
        bucket_id: 0,        // Gossip nodes don't use bucket concept
        client_data: vec![], // Empty because gossip uses different sync
        export_timestamp: 1640995200,
    };

    // Verify it serializes correctly
    let json = serde_json::to_string(&gossip_export).expect("Should serialize");
    let deserialized: BucketExport = serde_json::from_str(&json).expect("Should deserialize");

    assert_eq!(deserialized.bucket_id, 0);
    assert_eq!(deserialized.client_data.len(), 0);
    assert_eq!(deserialized.export_timestamp, 1640995200);
}

#[test]
fn test_cluster_status_operational_states() {
    // Test different operational states that our cluster API returns
    let states = vec!["normal", "preparing", "changing", "error"];

    for state in states {
        let status = ClusterStatusResponse {
            node_id: "node1".to_string(),
            operational_state: state.to_string(),
            active_change_id: if state == "normal" {
                None
            } else {
                Some("change_123".to_string())
            },
            last_topology_change: Some(1640995200),
            data_consistency_ok: state != "error",
        };

        // Verify serialization works for all states
        let json = serde_json::to_string(&status).expect("Should serialize");
        let deserialized: ClusterStatusResponse =
            serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.operational_state, state);
        assert_eq!(deserialized.data_consistency_ok, state != "error");
    }
}

// Integration test for the topology change workflow would go here
// but requires actual running nodes, so it's commented out:
/*
#[tokio::test]
#[ignore] // Only run with actual cluster
async fn test_topology_change_workflow() {
    // This would test the full workflow:
    // 1. Start 3 nodes
    // 2. Validate current topology
    // 3. Prepare change to 4 nodes
    // 4. Export data
    // 5. Update configuration
    // 6. Restart with new topology
    // 7. Import data
    // 8. Commit change
    // 9. Verify health
}
*/
