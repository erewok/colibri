#!/bin/bash -eu

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MAX_CALLS=${max_calls:-5}
INTERVAL_SECONDS=${interval_seconds:-3}
TEST_CLIENT="timing-test-client"

echo -e "${BLUE}=== Timing-Based Rate Limit Validation ===${NC}"
echo -e "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"
echo -e "This test validates token bucket refill timing"
echo

# Function to make request and extract remaining tokens
get_remaining() {
    local port=$1
    local consume_token=$2  # true for POST, false for GET

    if [ "$consume_token" = "true" ]; then
        response=$(curl -s -X POST "http://localhost:${port}/rl/${TEST_CLIENT}" 2>/dev/null)
    else
        response=$(curl -s -X GET "http://localhost:${port}/rl-check/${TEST_CLIENT}" 2>/dev/null)
    fi

    remaining=$(echo "$response" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")
    # Ensure we have a valid number
    if ! [[ "$remaining" =~ ^[0-9]+$ ]]; then
        remaining="0"
    fi
    echo $remaining
}

# Function to make request and check if it succeeds
make_request_check() {
    local port=$1
    response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:${port}/rl/${TEST_CLIENT}" 2>/dev/null)
    http_code=$(echo "$response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)

    if [ "$http_code" = "200" ]; then
        echo "SUCCESS"
    else
        echo "RATE_LIMITED"
    fi
}

echo -e "\n${YELLOW}Phase 1: Consume all tokens at once${NC}"
echo "Initial tokens: $(get_remaining 8001 false)"

# Consume all tokens quickly
for i in $(seq 1 $MAX_CALLS); do
    result=$(make_request_check 8001)
    remaining=$(get_remaining 8001 false)
    echo "Request $i: $result (${remaining} remaining)"
done

# Try one more - should fail
result=$(make_request_check 8001)
echo "Extra request: $result (should be RATE_LIMITED)"

echo -e "\n${YELLOW}Phase 2: Wait for partial refill${NC}"
half_interval=$((INTERVAL_SECONDS / 2))
echo "Waiting ${half_interval}s (half interval)..."
sleep $half_interval

remaining=$(get_remaining 8001 false)
echo "Tokens after half interval: $remaining"

# Ensure remaining is a valid number for comparison
if ! [[ "$remaining" =~ ^[0-9]+$ ]]; then
    remaining=0
fi

if [ $remaining -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Tokens are refilling gradually (token bucket working)"

    # Test a request at half interval
    result=$(make_request_check 8001)
    echo "Request at half-interval: $result"
else
    echo -e "${YELLOW}⚠${NC} No tokens yet - may use strict interval-based refill"
fi

echo -e "\n${YELLOW}Phase 3: Wait for full interval${NC}"
echo "Waiting remaining ${half_interval}s for full interval..."
sleep $half_interval

remaining=$(get_remaining 8001 false)
echo "Tokens after full interval: $remaining"

# Ensure remaining is a valid number for comparison
if ! [[ "$remaining" =~ ^[0-9]+$ ]]; then
    remaining=0
fi

if [ $remaining -eq $MAX_CALLS ]; then
    echo -e "${GREEN}✓${NC} Full token bucket restored"
elif [ $remaining -gt 0 ]; then
    echo -e "${YELLOW}⚠${NC} Partial restore - tokens: $remaining/$MAX_CALLS"
else
    echo -e "${RED}✗${NC} No tokens restored - rate limiter may not be working"
fi

echo -e "\n${YELLOW}Phase 4: Burst capacity test${NC}"
echo "Testing if we can make ${MAX_CALLS} requests immediately after refill..."

success_count=0
for i in $(seq 1 $MAX_CALLS); do
    result=$(make_request_check 8001)
    if [ "$result" = "SUCCESS" ]; then
        ((success_count++))
    fi
    echo "Burst request $i: $result"
done

if [ $success_count -eq $MAX_CALLS ]; then
    echo -e "${GREEN}✓${NC} Full burst capacity available ($success_count/$MAX_CALLS)"
else
    echo -e "${YELLOW}⚠${NC} Limited burst capacity ($success_count/$MAX_CALLS)"
fi

echo -e "\n${YELLOW}Phase 5: Rate limit enforcement validation${NC}"
# Try to make more requests than allowed
echo "Attempting $(($MAX_CALLS + 3)) requests in rapid succession..."

success_count=0
fail_count=0

for i in $(seq 1 $(($MAX_CALLS + 3))); do
    result=$(make_request_check 8001)
    if [ "$result" = "SUCCESS" ]; then
        ((success_count++))
    else
        ((fail_count++))
    fi
done

echo "Results: ${success_count} successes, ${fail_count} rate-limited"

if [ $success_count -eq $MAX_CALLS ] && [ $fail_count -eq 3 ]; then
    echo -e "${GREEN}✓${NC} Complete rate limiting enforcement"
elif [ $success_count -le $MAX_CALLS ] && [ $fail_count -gt 0 ]; then
    echo -e "${YELLOW}⚠${NC} Rate limiting working but with some variance"
else
    echo -e "${RED}✗${NC} Rate limiting not working correctly"
fi

echo -e "\n${BLUE}=== Timing Validation Complete ===${NC}"