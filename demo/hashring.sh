#!/bin/bash -eu
export RUST_LOG=debug
export mode="hashring"
export max_calls=5
export interval_seconds=3

# Helper function to check cluster health
check_cluster_health() {
    local nodes="$1"
    echo "Checking cluster health..."
    cargo run --bin colibri-admin -- health --nodes "$nodes"
    if [ $? -ne 0 ]; then
        echo "❌ Cluster health check failed!"
        return 1
    fi
    echo "✅ Cluster is healthy"
    return 0
}

# Helper function to validate topology change
validate_topology_change() {
    local current="$1"
    local new="$2"
    echo "Validating topology change..."
    echo "  Current: $current"
    echo "  New: $new"
    cargo run --bin colibri-admin -- validate-topology --current "$current" --new "$new"
    if [ $? -ne 0 ]; then
        echo "❌ Topology validation failed!"
        return 1
    fi
    echo "✅ Topology change is valid"
    return 0
}

# Helper function to perform cluster resize with admin tool
resize_cluster_with_admin() {
    local current="$1"
    local new="$2"
    echo "Performing cluster resize with colibri-admin..."
    cargo run --bin colibri-admin -- resize --current "$current" --new "$new"
    if [ $? -ne 0 ]; then
        echo "❌ Cluster resize validation failed!"
        return 1
    fi
    echo "✅ Cluster resize plan validated"
    return 0
}

echo "Starting 3-node cluster with ${max_calls} max calls and ${interval_seconds} seconds interval..."
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
echo "Press Ctrl+C to stop all nodes"

# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8001 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8002 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8003 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE3_PID=$!

sleep 7

echo -e "All ${mode} nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID \e"
echo -e "Test with: curl -X POST http://localhost:8001/rl/test-client \n"

# Define topologies
INITIAL_TOPOLOGY="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
EXPANDED_TOPOLOGY="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003,127.0.0.1:8004"
SHRUNK_TOPOLOGY="127.0.0.1:8001,127.0.0.1:8002"

# Check initial cluster health
echo -e "\n=== INITIAL CLUSTER HEALTH CHECK ==="
check_cluster_health "$INITIAL_TOPOLOGY"

echo -e "\n=== Test request demo ==="
./demo/cluster-request-tests.sh

# CLUSTER RESIZE OPERATIONS WITH COLIBRI-ADMIN
echo -e "\n=== CLUSTER RESIZE OPERATIONS ==="

# 1. EXPAND: Add a 4th node (port 8004)
echo -e "\n--- EXPANDING CLUSTER: Adding node on port 8004 ---"
# Validate the expansion
validate_topology_change "$INITIAL_TOPOLOGY" "$EXPANDED_TOPOLOGY"
if [ $? -ne 0 ]; then
    echo "Skipping expansion due to validation failure"
else
    # Start the new node
    echo "Starting node 4 on port 8004..."
    cargo run -- \
        --run-mode "${mode}" \
        --rate-limit-max-calls-allowed ${max_calls} \
        --rate-limit-interval-seconds ${interval_seconds} \
        --listen-port 8004 \
        --topology "http://127.0.0.1:8001" \
        --topology "http://127.0.0.1:8002" \
        --topology "http://127.0.0.1:8003" \
        --topology "http://127.0.0.1:8004" &
    NODE4_PID=$!
    sleep 3

    # Use admin tool to resize
    resize_cluster_with_admin "$INITIAL_TOPOLOGY" "$EXPANDED_TOPOLOGY"

    sleep 5

    # Check health of expanded cluster
    echo "Checking expanded cluster health..."
    check_cluster_health "$EXPANDED_TOPOLOGY" || echo "⚠️  New node may still be starting up"

    echo "✅ Cluster expanded to 4 nodes (PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID, $NODE4_PID)"
fi

sleep 3

# 2. SHRINK: Remove a node (port 8003)
echo -e "\n--- SHRINKING CLUSTER: Removing node on port 8003 ---"

# Validate the shrinking (from expanded or original topology)
if [ ! -z "${NODE4_PID:-}" ]; then
    SOURCE_TOPOLOGY="$EXPANDED_TOPOLOGY"
    echo "Shrinking from 4-node topology"
else
    SOURCE_TOPOLOGY="$INITIAL_TOPOLOGY"
    echo "Shrinking from 3-node topology (expansion was skipped)"
fi

validate_topology_change "$SOURCE_TOPOLOGY" "$SHRUNK_TOPOLOGY"
if [ $? -ne 0 ]; then
    echo "Skipping shrinking due to validation failure"
else
    # Use admin tool to prepare resize
    resize_cluster_with_admin "$SOURCE_TOPOLOGY" "$SHRUNK_TOPOLOGY"

    # Export data before stopping nodes (demo - normally you'd coordinate this carefully)
    echo "Exporting cluster data..."
    mkdir -p ./cluster_exports
    cargo run --bin colibri-admin -- export-data --nodes "$SOURCE_TOPOLOGY" --output-dir "./cluster_exports" || echo "⚠️  Export failed - continuing anyway"

    # Stop node 3
    echo "Stopping node 3 (port 8003)..."
    kill $NODE3_PID 2>/dev/null || true

    # Stop node 4
    echo "Stopping node 4 (port 8004)..."
    kill $NODE4_PID 2>/dev/null || true

    sleep 3

    # Check health of remaining nodes
    echo "Checking remaining cluster health..."
    check_cluster_health "$SHRUNK_TOPOLOGY"

    echo "✅ Cluster shrunk to 2 nodes (PIDs: $NODE1_PID, $NODE2_PID)"
fi

sleep 5


# Final cluster status
echo -e "\n=== FINAL CLUSTER STATUS ==="
echo "Remaining active nodes:"
ps -p $NODE1_PID > /dev/null 2>&1 && echo "  ✅ Node 1 (port 8001): Running (PID $NODE1_PID)"
ps -p $NODE2_PID > /dev/null 2>&1 && echo "  ✅ Node 2 (port 8002): Running (PID $NODE2_PID)"
ps -p $NODE3_PID > /dev/null 2>&1 && echo "  ✅ Node 3 (port 8003): Running (PID $NODE3_PID)" || echo "  ❌ Node 3 (port 8003): Stopped"
if [ ! -z "${NODE4_PID:-}" ]; then
    ps -p $NODE4_PID > /dev/null 2>&1 && echo "  ✅ Node 4 (port 8004): Running (PID $NODE4_PID)" || echo "  ❌ Node 4 (port 8004): Stopped"
fi

echo -e "\nDemo complete! The cluster has been through expansion and contraction cycles."
echo "Export data is available in: ./cluster_exports/"
echo -e "\nPress Ctrl+C to stop all remaining nodes"

# Wait for interrupt and cleanup
trap "echo '\nStopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID ${NODE4_PID:-} 2>/dev/null || true; rm -rf ./cluster_exports; exit 0" INT
wait