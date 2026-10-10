#!/usr/bin/env bash
# Shared lifecycle for the demo entry scripts (single.sh, gossip.sh,
# hashring.sh). Source this file; do not run it.
#
# Nodes keep running after the validations so you can explore with curl;
# press Ctrl+C to stop them. Set DEMO_EXIT=1 to stop the nodes and exit with
# the validations' verdict instead.

export max_calls=${max_calls:-5}
export interval_seconds=${interval_seconds:-3}

SERVER_BIN="${CARGO_TARGET_DIR:-target}/debug/colibri-server"
NODE_PIDS=""
FAILED_SCRIPTS=""

stop_nodes() {
    if [ -n "$NODE_PIDS" ]; then
        echo "Stopping nodes..."
        kill $NODE_PIDS 2>/dev/null || true
        wait $NODE_PIDS 2>/dev/null || true
        NODE_PIDS=""
    fi
}
trap 'stop_nodes; exit 130' INT TERM

build_server() {
    echo "Cleaning up any existing colibri processes..."
    pkill -9 colibri-server 2>/dev/null || true
    cargo build --quiet --bin colibri-server || exit 1
}

# start_node <args...>: run a node in the background.
start_node() {
    "$SERVER_BIN" "$@" &
    NODE_PIDS="$NODE_PIDS $!"
}

# wait_for_health <port>...: wait up to 30s for each node's /health.
wait_for_health() {
    local port waited
    for port in "$@"; do
        waited=0
        until curl -sf "http://localhost:${port}/health" >/dev/null 2>&1; do
            sleep 0.25
            waited=$((waited + 1))
            if [ $waited -ge 120 ]; then
                echo "Node on port ${port} did not become healthy within 30s"
                stop_nodes
                exit 1
            fi
        done
    done
    echo "All nodes healthy: $*"
}

# run_validation <script> [args]: run one validation script through the log filter
# and remember whether it failed.
run_validation() {
    ./demo/"$@" 2>&1 | ./demo/log-filter.sh
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        FAILED_SCRIPTS="$FAILED_SCRIPTS $1${rule:+($rule)}"
    fi
}

# run_named_rule_validations <script>...: create a named rule with its own
# settings, run the given validations under it, then delete it. Named rules
# must behave exactly like the default rule.
run_named_rule_validations() {
    local script
    export rule=${named_rule:-demo-rule}
    local default_max=$max_calls default_interval=$interval_seconds
    export max_calls=${named_max_calls:-4}
    export interval_seconds=${named_interval_seconds:-2}
    run_validation rule-propagation-validation.sh create
    for script in "$@"; do
        run_validation "$script"
    done
    run_validation rule-propagation-validation.sh delete
    unset rule
    export max_calls=$default_max interval_seconds=$default_interval
}

# Report the overall verdict, then either exit (DEMO_EXIT=1) or keep the
# nodes running until Ctrl+C.
conclude() {
    echo
    if [ -z "$FAILED_SCRIPTS" ]; then
        echo -e "\033[0;32m=== ALL VALIDATIONS PASSED ===\033[0m"
        status=0
    else
        echo -e "\033[0;31m=== VALIDATIONS FAILED:${FAILED_SCRIPTS} ===\033[0m"
        status=1
    fi
    if [ "${DEMO_EXIT:-0}" = "1" ]; then
        stop_nodes
        exit $status
    fi
    echo "Nodes are still running for exploration, e.g.:"
    echo "  curl -X POST http://localhost:8001/rl/my-client"
    echo "  curl http://localhost:8001/rl-check/my-client"
    echo "Press Ctrl+C to stop them."
    wait
}
