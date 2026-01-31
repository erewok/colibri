#!/bin/bash -eu

# Example of how to use the improved validation system
# This shows a quick demo run with filtered logging

export RUST_LOG=info  # Reduced log level
export max_calls=3
export interval_seconds=2

echo "=== Quick Validation Demo ==="
echo "Rate limit: ${max_calls} calls per ${interval_seconds}s"
echo "Choose demo mode:"
echo "1) Single node"
echo "2) Gossip cluster"
echo "3) Hashring cluster"
echo

read -p "Enter choice (1-3): " choice

case $choice in
    1)
        echo "Starting single node demo with filtered logs..."
        ./demo/single.sh 2>&1 | ./demo/log-filter.sh
        ;;
    2)
        echo "Starting gossip cluster demo with filtered logs..."
        ./demo/gossip.sh 2>&1 | ./demo/log-filter.sh
        ;;
    3)
        echo "Starting hashring cluster demo with filtered logs..."
        ./demo/hashring.sh 2>&1 | ./demo/log-filter.sh
        ;;
    *)
        echo "Invalid choice. Running single node demo..."
        ./demo/single.sh 2>&1 | ./demo/log-filter.sh
        ;;
esac