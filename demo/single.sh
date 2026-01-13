#!/bin/bash -eu
export mode="single"
export max_calls=5
export interval_seconds=3

echo "Starting single-node cluster with ${max_calls} max calls and ${interval_seconds} seconds interval..."
echo "Node 1 on port 8001"
echo "Press Ctrl+C to stop node"

export RUST_LOG=info

# Start node 1
cargo run -- \
    --name "node-1" \
    --run-mode "single" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8001 &
NODE1_PID=$!

echo "${mode}-mode node started. PID: $NODE1_PID"
sleep 3 # Give the server a moment to start

echo -e "\nRunning single-node validation tests...\n"
export max_calls
export interval_seconds

echo -e "=== Single Node Validation ==="
./demo/single-node-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== Timing-Based Validation ==="
./demo/timing-validation.sh 2>&1 | ./demo/log-filter.sh

# Wait for interrupt and cleanup
trap "echo 'Stopping node...'; kill $NODE1_PID 2>/dev/null || true; exit 0" INT
wait