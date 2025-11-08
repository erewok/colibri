#!/bin/bash -eux
echo "Starting 3-node cluster..."
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
echo "Press Ctrl+C to stop all nodes"

export srv1_udp=8401
export srv2_udp=8402
export srv3_udp=8403
export RUST_LOG=debug
# export RUSTFLAGS="--cfg tokio_unstable"
# Start node 1 (knows about nodes 2 and 3)
cargo run -- \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed 10 \
    --listen-port 8001 \
    --listen-port-udp $srv1_udp \
    --topology "127.0.0.1:$srv2_udp" \
    --topology "127.0.0.1:$srv3_udp" &
NODE1_PID=$!

# Start node 2 (knows about nodes 1 and 3)
cargo run -- \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed 10 \
    --listen-port 8002 \
    --listen-port-udp $srv2_udp \
    --topology "127.0.0.1:$srv1_udp" \
    --topology "127.0.0.1:$srv3_udp" &
NODE2_PID=$!

# Start node 3 (knows about nodes 1 and 2)
cargo run -- \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed 10 \
    --listen-port 8003 \
    --listen-port-udp $srv3_udp \
    --topology "127.0.0.1:$srv1_udp" \
    --topology "127.0.0.1:$srv2_udp" &
NODE3_PID=$!

sleep 7

echo -e "All ${mode} nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID \e"
echo -e "Test with: curl -X POST http://localhost:8001/rl/test-client \n"

echo -e "\nSending test requests to node 1 (port 8001):"

res1=$(curl -iX POST http://localhost:8001/rl/test-client)
echo -e "\nResponse: $res1\n"
sleep 1

res2=$(curl -iX POST http://localhost:8002/rl/test-client)
echo -e "\nResponse: $res2\n"

res3=$(curl -iX POST http://localhost:8003/rl/test-client)
echo -e "\nResponse: $res3... sleeping to reset rate limit interval\n"
sleep 4 # Wait for rate limit interval to reset

res4=$(curl -iX POST http://localhost:8002/rl/test-client)
echo -e "\nResponse: $res4\n"

res5=$(curl -iX POST http://localhost:8003/rl/test-client)
echo -e "\nResponse: $res5\n"

# Wait for interrupt and cleanup
trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
wait