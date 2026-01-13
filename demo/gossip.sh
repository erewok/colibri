#!/bin/bash -eux
export RUST_LOG=info
export max_calls=5
export interval_seconds=3

export srv1_udp=8401
export srv2_udp=8402
export srv3_udp=8403

echo "Starting 3-node cluster with ${max_calls} max calls and ${interval_seconds} seconds interval..."
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
echo "Press Ctrl+C to stop all nodes"

# export RUSTFLAGS="--cfg tokio_unstable"
# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --name "node-1" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8001 \
    --peer-listen-port $srv1_udp \
    -t "node-2=127.0.0.1:$srv2_udp" \
    -t "node-3=127.0.0.1:$srv3_udp" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --name "node-2" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8002 \
    --peer-listen-port $srv2_udp \
    -t "node-1=127.0.0.1:$srv1_udp" \
    -t "node-3=127.0.0.1:$srv3_udp" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --name "node-3" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --client-listen-port 8003 \
    --peer-listen-port $srv3_udp \
    -t "node-1=127.0.0.1:$srv1_udp" \
    -t "node-2=127.0.0.1:$srv2_udp" &
NODE3_PID=$!

sleep 7

echo -e "\nRunning comprehensive validation tests...\n"
export max_calls
export interval_seconds
export mode="gossip"

echo -e "=== Basic Rate Limit Validation ==="
./demo/rate-limit-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== Timing-Based Validation ==="
./demo/timing-validation.sh 2>&1 | ./demo/log-filter.sh

echo -e "\n=== Distributed Consistency Validation ==="
./demo/consistency-validation.sh 2>&1 | ./demo/log-filter.sh

# Wait for interrupt and cleanup
trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
wait