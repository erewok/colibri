#!/usr/bin/env bash
# How nodes share a client's limit. This differs by mode:
#
# - hashring: each client belongs to one owner node, and every node forwards
#   both requests and checks to it. All nodes therefore report the same
#   count at once, and the client gets exactly max_calls however its requests
#   are spread across nodes.
# - gossip: every node keeps its own replica and gossips changes. A node
#   always counts its own requests, so it admits at most max_calls per window.
#   Nodes can admit concurrently before hearing of each other, so the cluster
#   admits at most (number of nodes) * max_calls per window. Once gossip has
#   spread, every node enforces the shared limit.
#
# Uses: max_calls, interval_seconds, mode, ports

source "$(dirname "$0")/validation-utils.sh"
check_dependencies

FIRST=${PORTS%% *}
OTHERS=${PORTS#* }
NODE_COUNT=$(echo "$PORTS" | wc -w | tr -d ' ')

echo -e "${BLUE}=== Distributed Consistency Validation (${MODE} mode, $(rule_label)) ===${NC}"
echo "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s across ports ${PORTS}"

if [ "$MODE" = "hashring" ]; then
    section "Phase 1: Every node reports the owner's count"
    client=$(fresh_client routing)
    post "$FIRST" "$client"
    post "$FIRST" "$client"
    for port in $PORTS; do
        expect_eq "port $port calls remaining after 2 requests" "$(remaining "$port" "$client")" "$((MAX_CALLS - 2))"
    done

    section "Phase 2: One limit per client, wherever requests land"
    client=$(fresh_client shared)
    admitted=0
    for i in $(seq 1 $((NODE_COUNT * MAX_CALLS))); do
        set -- $PORTS
        shift $(((i - 1) % NODE_COUNT))
        post "$1" "$client"
        [ "$HTTP_CODE" = "200" ] && admitted=$((admitted + 1))
    done
    expect_eq "admitted out of $((NODE_COUNT * MAX_CALLS)) spread round-robin" "$admitted" "$MAX_CALLS"

    section "Phase 3: Capacity returns after the interval on every node"
    sleep "$INTERVAL_SECONDS"
    sleep 0.25
    for port in $PORTS; do
        expect_eq "port $port calls remaining" "$(remaining "$port" "$client")" "$MAX_CALLS"
    done

elif [ "$MODE" = "gossip" ]; then
    section "Phase 1: A change on one node reaches every node"
    client=$(fresh_client propagation)
    post "$FIRST" "$client"
    post "$FIRST" "$client"
    info "right after: $(remaining_everywhere "$client")"
    if await_everywhere "$client" "$((MAX_CALLS - 2))" "$CONVERGE_TIMEOUT_SECONDS"; then
        pass "all nodes report $((MAX_CALLS - 2)) remaining after ${WAITED}s"
    else
        fail "nodes did not converge within ${CONVERGE_TIMEOUT_SECONDS}s: $(remaining_everywhere "$client")"
    fi

    section "Phase 2: Once gossip has spread, every node enforces the shared limit"
    client=$(fresh_client enforcement)
    burst "$FIRST" "$client" "$MAX_CALLS"
    expect_eq "admitted on port $FIRST" "$ADMITTED" "$MAX_CALLS"
    if await_everywhere "$client" 0 "$CONVERGE_TIMEOUT_SECONDS"; then
        pass "all nodes report 0 remaining after ${WAITED}s"
    else
        fail "nodes did not converge within ${CONVERGE_TIMEOUT_SECONDS}s: $(remaining_everywhere "$client")"
    fi
    for port in $OTHERS; do
        post "$port" "$client"
        expect_eq "request on port $port" "$HTTP_CODE" "429"
    done

    section "Phase 3: Concurrent admission is bounded"
    echo "Sending $((NODE_COUNT * MAX_CALLS + NODE_COUNT)) requests round-robin, faster than gossip..."
    client=$(fresh_client concurrent)
    total=0
    for port in $PORTS; do
        eval "admitted_$port=0"
    done
    for i in $(seq 1 $((NODE_COUNT * MAX_CALLS + NODE_COUNT))); do
        set -- $PORTS
        shift $(((i - 1) % NODE_COUNT))
        post "$1" "$client"
        if [ "$HTTP_CODE" = "200" ]; then
            total=$((total + 1))
            eval "admitted_$1=\$((admitted_$1 + 1))"
        fi
    done
    for port in $PORTS; do
        eval "count=\$admitted_$port"
        expect_between "admitted on port $port" "$count" 0 "$MAX_CALLS"
    done
    expect_between "admitted across the cluster" "$total" "$MAX_CALLS" "$((NODE_COUNT * MAX_CALLS))"
    info "the cluster admitted $total; anything above ${MAX_CALLS} is nodes admitting before they heard of each other"

    section "Phase 4: Nodes converge after concurrent admission"
    if await_everywhere "$client" 0 "$CONVERGE_TIMEOUT_SECONDS"; then
        pass "all nodes report 0 remaining after ${WAITED}s"
    else
        fail "nodes did not converge within ${CONVERGE_TIMEOUT_SECONDS}s: $(remaining_everywhere "$client")"
    fi

    section "Phase 5: Capacity returns on every node after the window"
    echo "Waiting ${INTERVAL_SECONDS}s for the window to pass..."
    sleep "$INTERVAL_SECONDS"
    # Each node ages its own requests on its next tick, then gossip spreads
    # the expiries; nothing here races the window, so allow a little longer.
    recovery_timeout=$((CONVERGE_TIMEOUT_SECONDS + 2))
    if await_everywhere "$client" "$MAX_CALLS" "$recovery_timeout"; then
        pass "all nodes report $MAX_CALLS remaining ${WAITED}s after the window"
    else
        fail "capacity not restored everywhere within ${recovery_timeout}s: $(remaining_everywhere "$client")"
    fi

else
    info "nothing to check across nodes in ${MODE} mode"
fi

finish
