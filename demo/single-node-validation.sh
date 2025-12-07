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
TEST_CLIENT="single-node-test"
PORT=8001

echo -e "${BLUE}=== Single Node Rate Limit Validation ===${NC}"
echo -e "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"
echo -e "Test client: ${TEST_CLIENT}"
echo

# Function to make a rate limit request and parse response
make_request() {
    local expected_remaining=$1
    local should_succeed=$2

    response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:${PORT}/rl/${TEST_CLIENT}" 2>/dev/null)
    http_code=$(echo "$response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
    body=$(echo "$response" | sed 's/HTTPCODE:[0-9]*$//')

    # Parse remaining calls from response body
    remaining=$(echo "$body" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")

    if [ "$should_succeed" = "true" ]; then
        if [ "$http_code" = "200" ]; then
            echo -e "${GREEN}✓${NC} Request SUCCESS (${remaining} remaining)"
            return 0
        else
            echo -e "${RED}✗${NC} Request FAILED (expected success, got HTTP ${http_code})"
            return 1
        fi
    else
        if [ "$http_code" = "429" ] || [ "$http_code" = "503" ]; then
            echo -e "${GREEN}✓${NC} Request RATE_LIMITED (${remaining} remaining) - HTTP ${http_code}"
            return 0
        else
            echo -e "${RED}✗${NC} Request FAILED (expected rate limit, got HTTP ${http_code})"
            return 1
        fi
    fi
}

# Function to check remaining tokens without consuming
check_remaining() {
    response=$(curl -s -X GET "http://localhost:${PORT}/rl-check/${TEST_CLIENT}" 2>/dev/null)
    remaining=$(echo "$response" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")
    echo -e "${BLUE}ℹ${NC} ${remaining} tokens remaining"
}

# Test 1: Initial state
echo -e "\n${YELLOW}Test 1: Initial Token State${NC}"
check_remaining

# Test 2: Exhaust rate limit
echo -e "\n${YELLOW}Test 2: Rate Limit Exhaustion${NC}"
success_count=0
fail_count=0

echo "Making ${MAX_CALLS} + 2 requests..."
for i in $(seq 1 $((MAX_CALLS + 2))); do
    if [ $i -le $MAX_CALLS ]; then
        # Should succeed
        if make_request "" "true"; then
            ((success_count++))
        fi
    else
        # Should fail (rate limited)
        if make_request "" "false"; then
            ((fail_count++))
        fi
    fi
    sleep 0.1  # Small delay between requests
done

echo -e "Summary: ${success_count} successes, ${fail_count} rate-limited"

if [ $success_count -eq $MAX_CALLS ] && [ $fail_count -eq 2 ]; then
    echo -e "${GREEN}✓${NC} Complete single-node rate limiting"
elif [ $success_count -le $MAX_CALLS ] && [ $fail_count -gt 0 ]; then
    echo -e "${YELLOW}⚠${NC} Rate limiting working with some variance"
else
    echo -e "${RED}✗${NC} Rate limiting not working correctly"
fi

# Test 3: Wait for token refresh and verify recovery
echo -e "\n${YELLOW}Test 3: Token Refresh Verification${NC}"
echo "Waiting ${INTERVAL_SECONDS}s for token bucket refresh..."
sleep $((INTERVAL_SECONDS + 1))

echo "Checking token recovery:"
check_remaining

# Test 4: Verify requests work again after refresh
echo -e "\n${YELLOW}Test 4: Post-Refresh Request Validation${NC}"
if make_request "" "true"; then
    echo -e "${GREEN}✓${NC} Token refresh successful"
else
    echo -e "${RED}✗${NC} Token refresh failed"
fi

# Test 5: Different client should have separate rate limit
echo -e "\n${YELLOW}Test 5: Client Isolation Test${NC}"
different_client_response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:${PORT}/rl/different-client" 2>/dev/null)
http_code=$(echo "$different_client_response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
if [ "$http_code" = "200" ]; then
    echo -e "${GREEN}✓${NC} Different client has separate rate limit"
else
    echo -e "${RED}✗${NC} Client isolation failed"
fi

echo -e "\n${BLUE}=== Single Node Validation Complete ===${NC}"