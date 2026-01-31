#!/bin/bash -eu
export RUST_LOG=info
export mode="hashring"
export max_calls=5
export interval_seconds=3

# Kill any existing colibri processes
echo "Cleaning up any existing colibri processes..."
pkill -9 colibri 2>/dev/null || true
sleep 1

# Helper function to check cluster health
check_cluster_health() {
    echo "Checking cluster health..."
    cargo run --bin colibri-admin -- --target "127.0.0.1:8411" get-status
    if [ $? -ne 0 ]; then
        echo "❌ Cluster health check failed!"
        return 1
    fi
    echo "✅ Cluster is healthy"
    return 0
}

echo "Starting 3-node cluster with ${max_calls} max calls and ${interval_seconds} seconds interval..."
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
echo "Press Ctrl+C to stop all nodes"

# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --name "node-1" \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8001 \
    --peer-listen-port 8411 \
    -t "node-1=127.0.0.1:8411" \
    -t "node-2=127.0.0.1:8421" \
    -t "node-3=127.0.0.1:8431" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --name "node-2" \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8002 \
    --peer-listen-port 8421 \
    -t "node-1=127.0.0.1:8411" \
    -t "node-2=127.0.0.1:8421" \
    -t "node-3=127.0.0.1:8431" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --name "node-3" \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8003 \
    --peer-listen-port 8431 \
    -t "node-1=127.0.0.1:8411" \
    -t "node-2=127.0.0.1:8421" \
    -t "node-3=127.0.0.1:8431" &
NODE3_PID=$!

sleep 7

echo -e "All ${mode} nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID \e"
echo -e "Test with: curl -X POST http://localhost:8001/rl/test-client \n"

# Check initial cluster health
echo -e "\n=== INITIAL CLUSTER HEALTH CHECK ==="
check_cluster_health

echo -e "\n=== Comprehensive Rate Limit Validation ==="
export max_calls
export interval_seconds
export mode

echo -e "=== Basic Rate Limit Validation ==="
./demo/rate-limit-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== Timing-Based Validation ==="
./demo/timing-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== Distributed Consistency Validation ==="
./demo/consistency-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== FINAL STATUS ==="
echo "All nodes running:"
ps -p $NODE1_PID > /dev/null 2>&1 && echo "  ✅ Node 1 (port 8001): Running (PID $NODE1_PID)"
ps -p $NODE2_PID > /dev/null 2>&1 && echo "  ✅ Node 2 (port 8002): Running (PID $NODE2_PID)"
ps -p $NODE3_PID > /dev/null 2>&1 && echo "  ✅ Node 3 (port 8003): Running (PID $NODE3_PID)"

echo -e "\nDemo complete! Press Ctrl+C to stop all nodes"

# Wait for interrupt and cleanup
trap "echo '\nStopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
wait