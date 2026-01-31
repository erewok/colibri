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
MODE=${mode:-"gossip"}  # gossip or hashring
TEST_CLIENT="consistency-test"

echo -e "${BLUE}=== Distributed Consistency Validation (${MODE} mode) ===${NC}"
echo -e "Rate limit: ${MAX_CALLS} calls per ${INTERVAL_SECONDS}s"
echo

# Function to get remaining tokens from specific port
get_remaining() {
    local port=$1
    response=$(curl -s -X GET "http://localhost:${port}/rl-check/${TEST_CLIENT}" 2>/dev/null)
    remaining=$(echo "$response" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")
    echo $remaining
}

# Function to make rate limit request
make_request() {
    local port=$1
    response=$(curl -s -w "HTTPCODE:%{http_code}" -X POST "http://localhost:${port}/rl/${TEST_CLIENT}" 2>/dev/null)
    http_code=$(echo "$response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
    echo $http_code
}

echo -e "\n${YELLOW}Phase 1: Initial consistency check${NC}"
echo "Checking initial token state across all nodes:"
for port in 8001 8002 8003; do
    remaining=$(get_remaining $port)
    echo "Node ${port}: ${remaining} tokens"
done

echo -e "\n${YELLOW}Phase 2: Single node consumption${NC}"
echo "Consuming 2 tokens on node 8001..."
make_request 8001 > /dev/null
make_request 8001 > /dev/null

echo "Token state after consumption on node 8001:"
node1_tokens=$(get_remaining 8001)
node2_tokens=$(get_remaining 8002)
node3_tokens=$(get_remaining 8003)

echo "Node 8001: ${node1_tokens} tokens"
echo "Node 8002: ${node2_tokens} tokens"
echo "Node 8003: ${node3_tokens} tokens"

if [ "$MODE" = "gossip" ]; then
    echo -e "\n${BLUE}GOSSIP MODE EXPECTATIONS:${NC}"
    echo "- All nodes should eventually have same reduced token count"
    echo "- May take a few seconds for gossip propagation"

    # Wait for gossip propagation
    echo "Waiting 3s for gossip propagation..."
    sleep 3

    echo "Token state after gossip propagation:"
    node1_tokens=$(get_remaining 8001)
    node2_tokens=$(get_remaining 8002)
    node3_tokens=$(get_remaining 8003)

    echo "Node 8001: ${node1_tokens} tokens"
    echo "Node 8002: ${node2_tokens} tokens"
    echo "Node 8003: ${node3_tokens} tokens"

    if [ $node1_tokens -eq $node2_tokens ] && [ $node2_tokens -eq $node3_tokens ]; then
        echo -e "${GREEN}✓${NC} Complete gossip consistency achieved"
    elif [ $((node1_tokens + node2_tokens + node3_tokens)) -lt $((MAX_CALLS * 3 - 2)) ]; then
        echo -e "${YELLOW}⚠${NC} Partial gossip consistency (some state shared)"
    else
        echo -e "${RED}✗${NC} No gossip consistency detected"
    fi

elif [ "$MODE" = "hashring" ]; then
    echo -e "\n${BLUE}HASHRING MODE EXPECTATIONS:${NC}"
    echo "- Only the bucket owner should show token consumption"
    echo "- Other nodes should maintain full tokens"

    # In hashring mode, requests to different nodes should route to bucket owners
    total_tokens=$((node1_tokens + node2_tokens + node3_tokens))
    consumed_tokens=$((MAX_CALLS * 3 - total_tokens))

    if [ $consumed_tokens -eq 2 ]; then
        echo -e "${GREEN}✓${NC} Correct hashring behavior: ${consumed_tokens} tokens consumed total"
    else
        echo -e "${YELLOW}⚠${NC} Unexpected token consumption pattern: ${consumed_tokens} tokens consumed"
    fi
fi

echo -e "\n${YELLOW}Phase 3: Cross-node request distribution${NC}"
echo "Making requests to different nodes to test routing..."

# Make requests to each node and track responses
for port in 8001 8002 8003; do
    http_code=$(make_request $port)
    if [ "$http_code" = "200" ]; then
        result="SUCCESS"
    else
        result="RATE_LIMITED"
    fi
    echo "Request to node ${port}: $result"
done

echo -e "\n${YELLOW}Phase 4: Token exhaustion test${NC}"
echo "Attempting to exhaust rate limit across all nodes..."

# Keep making requests until we hit rate limits
total_requests=0
success_requests=0

for attempt in $(seq 1 $((MAX_CALLS * 2))); do
    port=$((8001 + (attempt % 3)))  # Round-robin across nodes
    http_code=$(make_request $port)
    ((total_requests++))

    if [ "$http_code" = "200" ]; then
        ((success_requests++))
        echo "Request ${total_requests} to ${port}: SUCCESS"
    else
        echo "Request ${total_requests} to ${port}: RATE_LIMITED"
        break  # Stop once we hit rate limit
    fi
done

echo -e "\nExhaustion test results:"
echo "Total requests made: ${total_requests}"
echo "Successful requests: ${success_requests}"

if [ "$MODE" = "gossip" ]; then
    # In gossip mode, we expect around MAX_CALLS successful requests total
    if [ $success_requests -le $((MAX_CALLS + 1)) ] && [ $success_requests -ge $((MAX_CALLS - 1)) ]; then
        echo -e "${GREEN}✓${NC} Correct gossip behavior: ~${MAX_CALLS} requests allowed"
    else
        echo -e "${YELLOW}⚠${NC} Unexpected gossip behavior: ${success_requests} requests allowed"
    fi
elif [ "$MODE" = "hashring" ]; then
    # In hashring mode, we might see different behavior depending on consistent hashing
    echo -e "${BLUE}ℹ${NC} Hashring behavior: ${success_requests}/${MAX_CALLS} requests succeeded"
fi

echo -e "\n${YELLOW}Phase 5: Recovery validation${NC}"
echo "Waiting ${INTERVAL_SECONDS}s for token recovery..."
sleep $((INTERVAL_SECONDS + 1))

echo "Token state after recovery period:"
for port in 8001 8002 8003; do
    remaining=$(get_remaining $port)
    echo "Node ${port}: ${remaining} tokens"
done

# Test that requests work again
echo -e "\nTesting post-recovery requests:"
recovery_success=0
for port in 8001 8002 8003; do
    http_code=$(make_request $port)
    if [ "$http_code" = "200" ]; then
        ((recovery_success++))
        echo "Node ${port}: SUCCESS"
    else
        echo "Node ${port}: STILL_LIMITED"
    fi
done

if [ $recovery_success -eq 3 ]; then
    echo -e "${GREEN}✓${NC} Full recovery achieved on all nodes"
elif [ $recovery_success -gt 0 ]; then
    echo -e "${YELLOW}⚠${NC} Partial recovery (${recovery_success}/3 nodes)"
else
    echo -e "${RED}✗${NC} No recovery detected"
fi

echo -e "\n${BLUE}=== Distributed Consistency Validation Complete ===${NC}"