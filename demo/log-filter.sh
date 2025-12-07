#!/bin/bash

# Log filter to show only key rate limiting validation messages
# Usage: ./your-demo-script.sh 2>&1 | ./demo/log-filter.sh

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

while IFS= read -r line; do
    # Rate limiting validation messages
    if echo "$line" | grep -q '\[RATE_CHECK\]'; then
        echo -e "${GREEN}$line${NC}"
    elif echo "$line" | grep -q '\[RATE_LIMIT\]'; then
        if echo "$line" | grep -q 'allowed:false'; then
            echo -e "${RED}$line${NC}"
        else
            echo -e "${GREEN}$line${NC}"
        fi
    elif echo "$line" | grep -q '\[GOSSIP_CHECK\]'; then
        echo -e "${CYAN}$line${NC}"
    elif echo "$line" | grep -q '\[GOSSIP_LIMIT\]'; then
        if echo "$line" | grep -q 'allowed:false'; then
            echo -e "${RED}$line${NC}"
        else
            echo -e "${CYAN}$line${NC}"
        fi
    elif echo "$line" | grep -q '\[GOSSIP_SYNC\]'; then
        echo -e "${BLUE}$line${NC}"
    elif echo "$line" | grep -q '\[TCP_RESPONSE\]'; then
        echo -e "${YELLOW}$line${NC}"
    elif echo "$line" | grep -q '\[ROUTE_FALLBACK\]'; then
        echo -e "${YELLOW}$line${NC}"
    elif echo "$line" | grep -q '\[BUCKET_MISSING\]'; then
        echo -e "${RED}$line${NC}"
    # Error messages
    elif echo "$line" | grep -q -i 'error'; then
        echo -e "${RED}$line${NC}"
    # Warning messages about missing implementations
    elif echo "$line" | grep -q 'not yet.*implemented'; then
        echo -e "${YELLOW}$line${NC}"
    # Validation script output (pass through with color)
    elif echo "$line" | grep -q -E '✓|SUCCESS|Phase [0-9]|Test [0-9]'; then
        echo -e "$line"
    elif echo "$line" | grep -q -E '✗|FAILED|RATE_LIMITED'; then
        echo -e "$line"
    elif echo "$line" | grep -q -E '⚠|WARNING'; then
        echo -e "$line"
    elif echo "$line" | grep -q -E '===.*==='; then
        echo -e "$line"
    elif echo "$line" | grep -q -E 'Summary:|tokens remaining|PIDs:'; then
        echo -e "$line"
    # Filter out most other debug/info noise
    elif echo "$line" | grep -v -q -E 'DEBUG|debug|Starting|Stopping|received|processing|handling'; then
        echo "$line"
    fi
done