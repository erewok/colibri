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
    cargo check
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
    export srv1_udp=8401
    export srv2_udp=8402
    export srv3_udp=8403
    export RUST_LOG=debug

    # Start node 1 (knows about nodes 2 and 3)
    cargo run -- \
        --run-mode "gossip" \
        --rate-limit-max-calls-allowed 10 \
        --listen-port 8001 \
        --listen-port-udp $srv1_udp \
        --topology "127.0.0.1:$srv2_udp" \
        --topology "127.0.0.1:$srv3_udp" &
    NODE1_PID=$!

    # Start node 2 (knows about nodes 1 and 3)
    cargo run -- \
        --run-mode "gossip" \
        --rate-limit-max-calls-allowed 10 \
        --listen-port 8002 \
        --listen-port-udp $srv2_udp \
        --topology "127.0.0.1:$srv1_udp" \
        --topology "127.0.0.1:$srv3_udp" &
    NODE2_PID=$!

    # Start node 3 (knows about nodes 1 and 2)
    cargo run -- \
        --run-mode "gossip" \
        --rate-limit-max-calls-allowed 10 \
        --listen-port 8003 \
        --listen-port-udp $srv3_udp \
        --topology "127.0.0.1:$srv1_udp" \
        --topology "127.0.0.1:$srv2_udp" &
    NODE3_PID=$!

    echo "All nodes started. PIDs: $NODE1_PID, $NODE2_PID, $NODE3_PID"
    echo "Test with: curl -X POST http://localhost:8001/rl/test-client"

    # Wait for interrupt and cleanup
    trap "echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true; exit 0" INT
    wait

# Run a specific node in multi-node mode
run-node port udp_port other_nodes run_mode:
    #!/bin/bash -eux
    cargo run -- \
        --listen-port {{port}} \
        --listen-port-udp {{udp_port}} \
        --topology {{other_nodes}} \
        --run-mode {{run_mode}}

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

transport_demo:
    cargo run --example transport_demo