#!/bin/bash -eux
export RUST_LOG=debug
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
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8001 \
    --listen-port-udp $srv1_udp \
    --topology "127.0.0.1:$srv2_udp" \
    --topology "127.0.0.1:$srv3_udp" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8002 \
    --listen-port-udp $srv2_udp \
    --topology "127.0.0.1:$srv1_udp" \
    --topology "127.0.0.1:$srv3_udp" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed ${max_calls} \
    --rate-limit-interval-seconds ${interval_seconds} \
    --listen-port 8003 \
    --listen-port-udp $srv3_udp \
    --topology "127.0.0.1:$srv1_udp" \
    --topology "127.0.0.1:$srv2_udp" &
NODE3_PID=$!

sleep 7

echo -e "Test with: curl -X POST http://localhost:8001/rl/test-client \n"
./demo/cluster-request-tests.sh

# Wait for interrupt and cleanup
trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
wait