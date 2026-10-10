#!/usr/bin/env bash
# Single-node demo: one token-bucket node on port 8001.
cd "$(dirname "$0")/.." || exit 1
source demo/run-utils.sh
export RUST_LOG=${RUST_LOG:-info}
export mode="single"
export ports="8001"

echo "Single node with ${max_calls} max calls per ${interval_seconds}s on port 8001"
build_server

start_node \
    --name "node-1" \
    --run-mode "single" \
    --rate-limit-max-calls-allowed "${max_calls}" \
    --rate-limit-interval-seconds "${interval_seconds}" \
    --client-listen-port 8001
wait_for_health 8001

run_validation rate-limit-validation.sh
run_validation timing-validation.sh
run_named_rule_validations rate-limit-validation.sh timing-validation.sh
conclude
