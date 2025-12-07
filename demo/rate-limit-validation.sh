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
TEST_CLIENT="test-client-validation"

echo -e "${BLUE}=== Rate Limit Validation Test ===${NC}"
echo -e "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"
echo -e "Test client: ${TEST_CLIENT}"
echo

# Function to make a rate limit request and parse response
make_request() {
    local port=$1
    local expected_remaining=$2
    local should_succeed=$3

    response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:${port}/rl/${TEST_CLIENT}" 2>/dev/null)
    http_code=$(echo "$response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
    body=$(echo "$response" | sed 's/HTTPCODE:[0-9]*$//')

    # Parse remaining calls from response body
    remaining=$(echo "$body" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")

    if [ "$should_succeed" = "true" ]; then
        if [ "$http_code" = "200" ]; then
            echo -e "${GREEN}✓${NC} Port ${port}: SUCCESS (${remaining} remaining)"
            return 0
        else
            echo -e "${RED}✗${NC} Port ${port}: FAILED (expected success, got HTTP ${http_code})"
            return 1
        fi
    else
        if [ "$http_code" = "429" ] || [ "$http_code" = "503" ]; then
            echo -e "${GREEN}✓${NC} Port ${port}: RATE_LIMITED (${remaining} remaining) - HTTP ${http_code}"
            return 0
        else
            echo -e "${RED}✗${NC} Port ${port}: FAILED (expected rate limit, got HTTP ${http_code})"
            return 1
        fi
    fi
}

# Function to check remaining tokens without consuming
check_remaining() {
    local port=$1
    response=$(curl -s -X GET "http://localhost:${port}/rl-check/test-client-validation" 2>/dev/null)
    remaining=$(echo "$response" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")
    echo -e "${BLUE}ℹ${NC} Port ${port}: ${remaining} tokens remaining"
}

# Test 1: Initial state - all nodes should have full tokens
echo -e "\n${YELLOW}Test 1: Initial Token State${NC}"
for port in 8001 8002 8003; do
    check_remaining $port
done

# Test 2: Exhaust rate limit on one node
echo -e "\n${YELLOW}Test 2: Rate Limit Exhaustion${NC}"
success_count=0
fail_count=0

echo "Making ${MAX_CALLS} + 2 requests to port 8001..."
for i in $(seq 1 $((MAX_CALLS + 2))); do
    if [ $i -le $MAX_CALLS ]; then
        # Should succeed
        if make_request 8001 "" "true"; then
            ((success_count++))
        fi
    else
        # Should fail (rate limited)
        if make_request 8001 "" "false"; then
            ((fail_count++))
        fi
    fi
    sleep 0.1  # Small delay between requests
done

echo -e "Summary: ${success_count} successes, ${fail_count} rate-limited"

# Test 3: Check consistency across nodes (should vary based on mode)
echo -e "\n${YELLOW}Test 3: Cross-Node Consistency${NC}"
for port in 8001 8002 8003; do
    check_remaining $port
done

# Test 4: Wait for token refresh and verify recovery
echo -e "\n${YELLOW}Test 4: Token Refresh Verification${NC}"
echo "Waiting ${INTERVAL_SECONDS}s for token bucket refresh..."
sleep $((INTERVAL_SECONDS + 1))

echo "Checking token recovery:"
for port in 8001 8002 8003; do
    check_remaining $port
done

# Test 5: Verify requests work again after refresh
echo -e "\n${YELLOW}Test 5: Post-Refresh Request Validation${NC}"
for port in 8001 8002 8003; do
    if make_request $port "" "true"; then
        echo -e "${GREEN}✓${NC} Port ${port}: Token refresh successful"
    else
        echo -e "${RED}✗${NC} Port ${port}: Token refresh failed"
    fi
done

# Test 6: Different client should have separate rate limit
echo -e "\n${YELLOW}Test 6: Client Isolation Test${NC}"
different_client_response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:8001/rl/different-client" 2>/dev/null)
http_code=$(echo "$different_client_response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
if [ "$http_code" = "200" ]; then
    echo -e "${GREEN}✓${NC} Different client has separate rate limit"
else
    echo -e "${RED}✗${NC} Client isolation failed"
fi

echo -e "\n${BLUE}=== Validation Test Complete ===${NC}"