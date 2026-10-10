#!/usr/bin/env bash
# Named rules reach every node: `create` makes RULE on the first node and
# checks every node has it; `delete` deletes it on the last node and checks
# every node drops it.
#
# Uses: rule, max_calls, interval_seconds, mode, ports

source "$(dirname "$0")/validation-utils.sh"
check_dependencies

ACTION=${1:-create}
FIRST=${PORTS%% *}
LAST=${PORTS##* }
TIMEOUT_SECONDS=10

# rule_status <port>: HTTP status of GET /rl-config/RULE.
rule_status() {
    curl -s -o /dev/null -w '%{http_code}' "http://localhost:$1/rl-config/$RULE" 2>/dev/null
}

# await_rule_status <status>: poll every port until all report <status>.
await_rule_status() {
    local port all polls=0
    while :; do
        all=1
        for port in $PORTS; do
            [ "$(rule_status "$port")" = "$1" ] || all=0
        done
        WAITED=$(seconds_times "$polls" 0.25)
        [ $all -eq 1 ] && return 0
        [ $polls -ge $((TIMEOUT_SECONDS * 4)) ] && return 1
        sleep 0.25
        polls=$((polls + 1))
    done
}

echo -e "${BLUE}=== Rule Propagation Validation (${MODE} mode, rule ${RULE}: ${ACTION}) ===${NC}"

if [ "$ACTION" = "create" ]; then
    section "Create ${RULE} (${MAX_CALLS} calls per ${INTERVAL_SECONDS}s) on port ${FIRST}"
    body="{\"name\":\"$RULE\",\"settings\":{\"rate_limit_max_calls_allowed\":$MAX_CALLS,\"rate_limit_interval_seconds\":$INTERVAL_SECONDS}}"
    status=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' \
        -d "$body" "http://localhost:$FIRST/rl-config")
    expect_eq "create on port $FIRST" "$status" "201"
    if await_rule_status 200; then
        pass "every node has ${RULE} after ${WAITED}s"
    else
        fail "not every node has ${RULE} after ${TIMEOUT_SECONDS}s"
    fi
else
    section "Delete ${RULE} on port ${LAST}"
    status=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://localhost:$LAST/rl-config/$RULE")
    expect_eq "delete on port $LAST" "$status" "204"
    if await_rule_status 404; then
        pass "every node dropped ${RULE} after ${WAITED}s"
    else
        fail "not every node dropped ${RULE} after ${TIMEOUT_SECONDS}s"
    fi
fi

finish
