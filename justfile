# just manual: https://github.com/casey/just#readme

_default:
    just --list

# Install cargo plugins used by this project
bootstrap:
    cargo install cargo-nextest
    cargo install cargo-udeps

# Install cargo plugins for building docs
bootstrap-docs:
    cargo install mdbook
    cargo install mdbook-mermaid
    mdbook-mermaid install docs

# Build the project (cargo build)
build *args:
    cargo build {{args}}

# Run code quality checks
check:
    #!/bin/bash -eux
    cargo clippy -- -D warnings
    cargo fmt --all -- --check

# Run code formatting
fmt:
    cargo fmt

# Run API Server (single-node mode)
run:
    cargo run

# Run API Server on specific port (single-node mode)
run-port port:
    cargo run -- --listen-port {{port}}

# Run multi-node cluster locally (3 nodes on ports 8001, 8002, 8003)
run-cluster:
    #!/bin/bash -eux
    echo "Starting 3-node cluster..."
    echo "Node 1 on port 8001, Node 2 on port 8002, Node 3 on port 8003"
    echo "Press Ctrl+C to stop all nodes"

    # Start node 1 (knows about nodes 2 and 3)
    cargo run -- --listen-port 8001 \
        --topology "http://localhost:8002" \
        --topology "http://localhost:8003" &
    NODE1_PID=$!

    # Start node 2 (knows about nodes 1 and 3)
    cargo run -- --listen-port 8002 \
        --topology "http://localhost:8001" \
        --topology "http://localhost:8003" &
    NODE2_PID=$!

    # Start node 3 (knows about nodes 1 and 2)
    cargo run -- --listen-port 8003 \
        --topology "http://localhost:8001" \
        --topology "http://localhost:8002" &
    NODE3_PID=$!

    echo "All nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID"
    echo "Test with: curl -X POST http://localhost:8001/rl/test-client"

    # Wait for interrupt and cleanup
    trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
    wait

# Run a specific node in multi-node mode
run-node port other_nodes:
    cargo run -- --listen-port {{port}} --topology {{other_nodes}}

# Run node 1 of a 3-node cluster
run-node1:
    cargo run -- --listen-port 8001 \
        --topology "http://localhost:8002" \
        --topology "http://localhost:8003"

# Run node 2 of a 3-node cluster
run-node2:
    cargo run -- --listen-port 8002 \
        --topology "http://localhost:8001" \
        --topology "http://localhost:8003"

# Run node 3 of a 3-node cluster
run-node3:
    cargo run -- --listen-port 8003 \
        --topology "http://localhost:8001" \
        --topology "http://localhost:8002"

# Test multi-node cluster (assumes cluster is running)
test-cluster:
    #!/bin/bash -eux
    echo "Testing multi-node cluster..."

    echo "Testing node 1 (port 8001):"
    curl -X POST http://localhost:8001/rl/client1 || echo "Node 1 not responding"

    echo "Testing node 2 (port 8002):"
    curl -X POST http://localhost:8002/rl/client2 || echo "Node 2 not responding"

    echo "Testing node 3 (port 8003):"
    curl -X POST http://localhost:8003/rl/client3 || echo "Node 3 not responding"

    echo "Testing consistent hashing (same client on different nodes):"
    curl -X POST http://localhost:8001/rl/consistent-test
    curl -X POST http://localhost:8002/rl/consistent-test
    curl -X POST http://localhost:8003/rl/consistent-test

# Run all tests locally
test *args:
    # Run unit tests
    cargo nextest run {{args}}
    # Doctests next
    cargo test --doc

# Run benchmarks (full execution)
bench:
    cargo bench