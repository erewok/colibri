#!/usr/bin/env bash
# How capacity comes back after a client is exhausted. This differs by mode:
#
# - single and hashring use a token bucket that refills continuously, at
#   max_calls per interval_seconds.
# - gossip uses a sliding window: each admitted request holds its token for
#   interval_seconds, then the token comes back all at once. Expiry runs on
#   the node's gossip tick (once a second), so it can lag by up to a tick.
#
# Uses: max_calls, interval_seconds, mode, ports (the first port is the target)

source "$(dirname "$0")/validation-utils.sh"
check_dependencies

PORT=${PORTS%% *}
HALF=$(seconds_times "$INTERVAL_SECONDS" 0.5)

echo -e "${BLUE}=== Timing Validation (${MODE} mode, $(rule_label), port ${PORT}) ===${NC}"
echo "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"

client=$(fresh_client timing)

section "Phase 1: Exhaust a new client"
burst "$PORT" "$client" $((MAX_CALLS + 1))
expect_eq "admitted out of $((MAX_CALLS + 1))" "$ADMITTED" "$MAX_CALLS"

if [ "$MODE" = "gossip" ]; then
    section "Phase 2: No gradual refill inside the window"
    echo "Waiting ${HALF}s (half the interval)..."
    sleep "$HALF"
    expect_eq "calls remaining at half the interval" "$(remaining "$PORT" "$client")" "0"

    section "Phase 3: Full capacity returns once the window has passed"
    echo "Waiting the rest of the interval..."
    sleep "$HALF"
    # Expiry runs on the next gossip tick after the window closes.
    if await_on "$PORT" "$client" "$MAX_CALLS" 3; then
        pass "full capacity back ${WAITED}s after the interval ended"
    else
        fail "capacity not restored within 3s after the interval: $(remaining "$PORT" "$client") remaining"
    fi
else
    # Refill since exhaustion, allowing up to half a second of request and
    # scheduling overhead.
    LOW=$(awk "BEGIN { printf \"%d\", $MAX_CALLS * 0.5 }")
    HIGH=$(awk "BEGIN { v = int($MAX_CALLS * ($HALF + 0.5) / $INTERVAL_SECONDS); print (v > $MAX_CALLS ? $MAX_CALLS : v) }")

    section "Phase 2: Tokens refill gradually"
    echo "Waiting ${HALF}s (half the interval)..."
    sleep "$HALF"
    expect_between "calls remaining at half the interval" "$(remaining "$PORT" "$client")" "$LOW" "$HIGH"

    section "Phase 3: Full capacity after the whole interval"
    echo "Waiting the rest of the interval..."
    sleep "$HALF"
    sleep 0.25
    expect_eq "calls remaining after the interval" "$(remaining "$PORT" "$client")" "$MAX_CALLS"
fi

section "Phase 4: The restored capacity is a full burst, and no more"
burst "$PORT" "$client" $((MAX_CALLS + 2))
expect_eq "admitted out of $((MAX_CALLS + 2))" "$ADMITTED" "$MAX_CALLS"

finish
