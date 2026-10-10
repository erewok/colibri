#!/usr/bin/env bash
# Gossip demo: three nodes, each with a full CRDT replica, gossiping over UDP.
cd "$(dirname "$0")/.." || exit 1
source demo/run-utils.sh
export RUST_LOG=${RUST_LOG:-info}
export mode="gossip"
export ports="8001 8002 8003"

# Gossip must spread a change to every node well within the rate-limit
# window, or other nodes cannot enforce the shared limit in time. Each tick
# reaches one random peer, so tick faster than the 1000 ms default.
gossip_interval_ms=${gossip_interval_ms:-250}

srv1_udp=8401
srv2_udp=8402
srv3_udp=8403

echo "3-node gossip cluster with ${max_calls} max calls per ${interval_seconds}s, gossiping every ${gossip_interval_ms}ms"
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
build_server

start_node \
    --name "node-1" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed "${max_calls}" \
    --rate-limit-interval-seconds "${interval_seconds}" \
    --gossip-interval-ms "${gossip_interval_ms}" \
    --client-listen-port 8001 \
    --peer-listen-port $srv1_udp \
    -t "node-2=127.0.0.1:$srv2_udp" \
    -t "node-3=127.0.0.1:$srv3_udp"

start_node \
    --name "node-2" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed "${max_calls}" \
    --rate-limit-interval-seconds "${interval_seconds}" \
    --gossip-interval-ms "${gossip_interval_ms}" \
    --client-listen-port 8002 \
    --peer-listen-port $srv2_udp \
    -t "node-1=127.0.0.1:$srv1_udp" \
    -t "node-3=127.0.0.1:$srv3_udp"

start_node \
    --name "node-3" \
    --run-mode "gossip" \
    --rate-limit-max-calls-allowed "${max_calls}" \
    --rate-limit-interval-seconds "${interval_seconds}" \
    --gossip-interval-ms "${gossip_interval_ms}" \
    --client-listen-port 8003 \
    --peer-listen-port $srv3_udp \
    -t "node-1=127.0.0.1:$srv1_udp" \
    -t "node-2=127.0.0.1:$srv2_udp"
wait_for_health 8001 8002 8003

run_validation rate-limit-validation.sh
run_validation timing-validation.sh
run_validation consistency-validation.sh
run_named_rule_validations rate-limit-validation.sh timing-validation.sh consistency-validation.sh
conclude
