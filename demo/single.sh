#!/bin/bash -eu
export mode="single"
echo "Starting single-node cluster..."
echo "Node 1 on port 8001"
echo "Press Ctrl+C to stop node"

export RUST_LOG=info

# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --run-mode "single" \
    --rate-limit-max-calls-allowed 4 \
    --rate-limit-interval-seconds 3 \
    --listen-port 8001 &
NODE1_PID=$!

echo "${mode}-mode node started. PIDs: $NODE1_PID"
echo "Test with: curl -X POST http://localhost:8001/rl/test-client"
sleep 2 # Give the server a moment to start

echo "Sending test requests to node 1 (port 8001):"

res1=$(curl -iX POST http://localhost:8001/rl/test-client)
echo "Response: $res1"
sleep 1

res2=$(curl -iX POST http://localhost:8001/rl/test-client)
echo "Response: $res2"

res3=$(curl -iX POST http://localhost:8001/rl/test-client)
echo "Response: $res3... sleeping to reset rate limit interval"
sleep 4 # Wait for rate limit interval to reset

res4=$(curl -iX POST http://localhost:8001/rl/test-client)
echo "Response: $res4"

# Wait for interrupt and cleanup
trap "echo 'Stopping node...'; kill $NODE1_PID 2>/dev/null || true; exit 0" INT
wait