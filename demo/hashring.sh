#!/usr/bin/env bash
# Hashring demo: three nodes; each client is owned by one node, and the
# others forward its requests there over TCP.
cd "$(dirname "$0")/.." || exit 1
source demo/run-utils.sh
export RUST_LOG=${RUST_LOG:-info}
export mode="hashring"
export ports="8001 8002 8003"

check_cluster_health() {
    echo "Checking cluster health..."
    if cargo run --quiet --bin colibri-admin -- --target "127.0.0.1:8411" get-status; then
        echo "✅ Cluster is healthy"
    else
        echo "❌ Cluster health check failed!"
        FAILED_SCRIPTS="$FAILED_SCRIPTS cluster-health"
    fi
}

echo "3-node hashring cluster with ${max_calls} max calls per ${interval_seconds}s"
echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
build_server

for n in 1 2 3; do
    start_node \
        --name "node-$n" \
        --run-mode "hashring" \
        --rate-limit-max-calls-allowed "${max_calls}" \
        --rate-limit-interval-seconds "${interval_seconds}" \
        --client-listen-port "800$n" \
        --peer-listen-port "84${n}1" \
        -t "node-1=127.0.0.1:8411" \
        -t "node-2=127.0.0.1:8421" \
        -t "node-3=127.0.0.1:8431"
done
wait_for_health 8001 8002 8003

echo -e "\n=== Cluster Health ==="
check_cluster_health

run_validation rate-limit-validation.sh
run_validation timing-validation.sh
run_validation consistency-validation.sh
run_named_rule_validations rate-limit-validation.sh timing-validation.sh consistency-validation.sh
conclude
