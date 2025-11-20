#!/bin/bash -eux
export mode="hashring"
echo "Starting 3-node ${mode} cluster..."
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
echo "Press Ctrl+C to stop all nodes"

export RUST_LOG=debug

# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed 4 \
    --rate-limit-interval-seconds 3 \
    --listen-port 8001 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed 4 \
    --rate-limit-interval-seconds 3 \
    --listen-port 8002 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --run-mode "${mode}" \
    --rate-limit-max-calls-allowed 4 \
    --rate-limit-interval-seconds 3 \
    --listen-port 8003 \
    --topology "http://127.0.0.1:8001" \
    --topology "http://127.0.0.1:8002" \
    --topology "http://127.0.0.1:8003" &
NODE3_PID=$!

sleep 7

echo -e "All ${mode} nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID \e"
echo -e "Test with: curl -X POST http://localhost:8001/rl/test-client \n"
./demo/cluster-request-tests.sh

# Wait for interrupt and cleanup
trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
wait