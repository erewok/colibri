#!/usr/bin/env bash
# Shared helpers for the demo validation scripts. Source this file; do not run it.
#
# Every check counts as a pass or a failure, and `finish` exits non-zero if
# anything failed, so a run's verdict is its exit status.

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

MAX_CALLS=${max_calls:-5}
INTERVAL_SECONDS=${interval_seconds:-3}
MODE=${mode:-single}
# Space-separated client ports of the nodes under test.
PORTS=${ports:-"8001 8002 8003"}
# Named rule to validate; empty means the default rule.
RULE=${rule:-}
# How long gossip may take to spread a change to every node. A change must
# spread before its requests age out of the window, or the other nodes never
# see it, so the default stays a second inside the window.
DEFAULT_CONVERGE=$((INTERVAL_SECONDS > 1 ? INTERVAL_SECONDS - 1 : 1))
CONVERGE_TIMEOUT_SECONDS=${converge_timeout_seconds:-$DEFAULT_CONVERGE}

PASS_COUNT=0
FAIL_COUNT=0

check_dependencies() {
    local missing_deps=()
    command -v curl >/dev/null 2>&1 || missing_deps+=("curl")
    command -v jq >/dev/null 2>&1 || missing_deps+=("jq")
    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo "Error: Missing required dependencies: ${missing_deps[*]}"
        echo "  macOS: brew install ${missing_deps[*]}"
        echo "  Ubuntu/Debian: sudo apt-get install ${missing_deps[*]}"
        exit 1
    fi
}

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo -e "${GREEN}✓${NC} $*"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo -e "${RED}✗${NC} $*"
}

info() {
    echo -e "${BLUE}ℹ${NC} $*"
}

section() {
    echo -e "\n${YELLOW}$*${NC}"
}

# expect_eq <description> <actual> <expected>
expect_eq() {
    if [ "$2" = "$3" ]; then
        pass "$1: $2"
    else
        fail "$1: got $2, expected $3"
    fi
}

# expect_between <description> <actual> <low> <high>   (inclusive)
expect_between() {
    if [[ "$2" =~ ^[0-9]+$ ]] && [ "$2" -ge "$3" ] && [ "$2" -le "$4" ]; then
        pass "$1: $2 (expected $3..$4)"
    else
        fail "$1: got $2, expected $3..$4"
    fi
}

# A client id no earlier phase or run has used, so phases cannot leak state
# into each other.
fresh_client() {
    echo "$1-$$-$RANDOM"
}

# Rate-limit and check paths for a client, under RULE if one is set.
limit_path() {
    if [ -n "$RULE" ]; then echo "/rl/$RULE/$1"; else echo "/rl/$1"; fi
}
check_path() {
    if [ -n "$RULE" ]; then echo "/rl-check/$RULE/$1"; else echo "/rl-check/$1"; fi
}

# The rule being validated, for headings.
rule_label() {
    if [ -n "$RULE" ]; then echo "rule $RULE"; else echo "default rule"; fi
}

# post <port> <client>: consume a token. Sets HTTP_CODE, and REMAINING for an
# admitted request ("-" otherwise).
post() {
    local body
    body=$(curl -s -w $'\n%{http_code}' -X POST "http://localhost:$1$(limit_path "$2")" 2>/dev/null)
    HTTP_CODE=${body##*$'\n'}
    REMAINING="-"
    if [ "$HTTP_CODE" = "200" ]; then
        REMAINING=$(echo "${body%$'\n'*}" | jq -r '.calls_remaining' 2>/dev/null)
    fi
}

# remaining <port> <client>: calls remaining without consuming, or "ERR".
remaining() {
    local value
    value=$(curl -s -X GET "http://localhost:$1$(check_path "$2")" 2>/dev/null | jq -r '.calls_remaining' 2>/dev/null)
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "$value"
    else
        echo "ERR"
    fi
}

# remaining_everywhere <client>: "port:remaining" for every port.
remaining_everywhere() {
    local port out=""
    for port in $PORTS; do
        out="$out $port:$(remaining "$port" "$1")"
    done
    echo "${out# }"
}

# burst <port> <client> <count>: send <count> requests back to back and set
# ADMITTED to how many succeeded.
burst() {
    local i
    ADMITTED=0
    for i in $(seq 1 "$3"); do
        post "$1" "$2"
        if [ "$HTTP_CODE" = "200" ]; then
            ADMITTED=$((ADMITTED + 1))
        fi
    done
}

# await_on "<ports>" <client> <expected> <timeout_seconds>: poll the given
# ports every 0.25s until all report <expected> calls remaining. Returns
# non-zero on timeout. Sets WAITED to the seconds it took (e.g. "0.75").
await_on() {
    local port all_match
    local polls=0
    local max_polls=$(($4 * 4))
    while :; do
        all_match=1
        for port in $1; do
            if [ "$(remaining "$port" "$2")" != "$3" ]; then
                all_match=0
                break
            fi
        done
        WAITED=$(seconds_times "$polls" 0.25)
        [ $all_match -eq 1 ] && return 0
        [ $polls -ge $max_polls ] && return 1
        sleep 0.25
        polls=$((polls + 1))
    done
}

# await_everywhere <client> <expected> <timeout_seconds>: await_on every port.
await_everywhere() {
    await_on "$PORTS" "$@"
}

# Seconds as a decimal string: seconds_times <seconds> <factor>
seconds_times() {
    awk "BEGIN { printf \"%.2f\", $1 * $2 }"
}

# Print the tally and exit non-zero if any check failed.
finish() {
    echo
    if [ "$FAIL_COUNT" -eq 0 ]; then
        echo -e "${GREEN}PASS${NC}: $PASS_COUNT checks passed"
        exit 0
    fi
    echo -e "${RED}FAIL${NC}: $FAIL_COUNT of $((PASS_COUNT + FAIL_COUNT)) checks failed"
    exit 1
}
