#!/usr/bin/env bash
# Rate-limit basics against one node: exact capacity, the last token, the
# check endpoint, and client isolation. Holds in every mode, because a node
# always counts its own requests.
#
# Uses: max_calls, interval_seconds, mode, ports (the first port is the target)

source "$(dirname "$0")/validation-utils.sh"
check_dependencies

PORT=${PORTS%% *}

echo -e "${BLUE}=== Rate Limit Validation (${MODE} mode, $(rule_label), port ${PORT}) ===${NC}"
echo "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"

section "Test 1: A new client starts with full capacity"
client=$(fresh_client basics)
expect_eq "calls remaining for a new client" "$(remaining "$PORT" "$client")" "$MAX_CALLS"

section "Test 2: Exactly ${MAX_CALLS} requests are admitted, counting down to 0"
for i in $(seq 1 "$MAX_CALLS"); do
    post "$PORT" "$client"
    expect_eq "request $i" "$HTTP_CODE/$REMAINING" "200/$((MAX_CALLS - i))"
done

section "Test 3: Further requests are rate limited"
for i in 1 2; do
    post "$PORT" "$client"
    expect_eq "extra request $i" "$HTTP_CODE" "429"
done

section "Test 4: The check endpoint agrees and does not consume"
expect_eq "calls remaining after exhaustion" "$(remaining "$PORT" "$client")" "0"
expect_eq "calls remaining on a second check" "$(remaining "$PORT" "$client")" "0"

section "Test 5: Clients are isolated"
other=$(fresh_client isolation)
expect_eq "another client's calls remaining" "$(remaining "$PORT" "$other")" "$MAX_CALLS"
post "$PORT" "$other"
expect_eq "another client's first request" "$HTTP_CODE/$REMAINING" "200/$((MAX_CALLS - 1))"

finish
