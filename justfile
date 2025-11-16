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

# Run a demo: single, hashring, or gossip
demo mode="single":
    bash demo/{{mode}}.sh

# Test multi-node cluster (assumes cluster is running)
test-cluster:
    #!/bin/bash -eux
    echo "Testing multi-node cluster..."

    echo "Sending 3 requests to node 1 (port 8001):"
    gtimeout 4 curl -X POST http://localhost:8001/rl/client1 || echo "Node 1 not responding"
    gtimeout 4 curl -X POST http://localhost:8001/rl/client1 || echo "Node 1 not responding"
    gtimeout 4 curl -X POST http://localhost:8001/rl/client1 || echo "Node 1 not responding"

    echo "Testing node 2 (port 8002):"
    gtimeout 4 curl -X POST http://localhost:8002/rl/client2 || echo "Node 2 not responding"

    echo "Back to node 1 (port 8001):"
    gtimeout 4 curl -X POST http://localhost:8001/rl/client1 || echo "Node 1 not responding"

    echo "Testing node 3 (port 8003):"
    gtimeout 4 curl -X POST http://localhost:8003/rl/client3 || echo "Node 3 not responding"


# Run all tests locally
test *args:
    # Run unit tests
    cargo nextest run {{args}}
    # Doctests next
    cargo test --doc

# Run benchmarks (full execution)
bench:
    cargo bench

example name="gossip":
    cargo run --example {{name}}