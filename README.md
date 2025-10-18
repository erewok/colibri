# Colibri: Rate-Limiting as a Service

Colibri is a simple HTTP service built in Rust that implements an in-memory data structure for rate-limiting services. Rate counts are stored _in memory_ so that Colibri can respond quickly.

**Note**: Restarting a Colibri node will restart any rate-limit counts.

## Design

Colibri implements the [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) for rate-limiting clients. Currently, all Colibri data structures are held in memory without persistence so that it can quickly respond to incoming requests.

Colibri can be run in single-node mode or in multi-node mode. In single-node mode each Colibri node will keep track of rate-limits individually without using any distributed properties. This strategy could potentially work behind a round-robin load balancer that fairly distributes traffic but it gets quickly confusing with interleaved client requests.

In multi-node mode Colibri functions as a distributed hash table, assigning responsibility for distinct client IDs to individual nodes using consistent hashing. This is experimental; for instance, it's not currently designed to work around network partitions or dynamic cluster resizing.

## Quick Start

After cloning this repo, you can quickly launch Colibri using the provided justfile recipes:

### Single-Node Mode (Simplest)

```sh
# Start a single node on port 8000
❯ just run
```

### Multi-Node Cluster (3 nodes)

```sh
# Start a 3-node cluster on ports 8001, 8002, 8003
❯ just run-cluster
```

Then test the rate limiting:

```sh
❯ curl -XPOST -i http://localhost:8002/rl/test-client
HTTP/1.1 200 OK
content-type: application/json
content-length: 50

{"client_id":"test-client","calls_remaining":999}

❯ curl -XPOST -i http://localhost:8001/rl/test-client
HTTP/1.1 200 OK
content-type: application/json
content-length: 50

{"client_id":"test-client","calls_remaining":998}
```

### Available Development Recipes

Use `just --list` to see all available recipes:

- `just run` - Single-node mode on port 8000
- `just run-cluster` - 3-node cluster for testing distributed features
- `just run-nodeN` - Individual nodes (1, 2, 3) for custom cluster setup
- `just test-cluster` - Automated testing of multi-node functionality
- `just test` - Run all unit and integration tests

[Click here for a terminal demo](./rate-limiting-demo.gif).

## Binary Arguments

The following configuration options are available for running Colibri:

```sh
Usage: colibri [OPTIONS]

Options:
      --listen-address <LISTEN_ADDRESS>
          IP Address to listen on [env: LISTEN_ADDRESS=] [default: 0.0.0.0]
      --listen-port <LISTEN_PORT>
          Port to bind Colibri server to [env: LISTEN_PORT=] [default: 8000]
      --rate-limit-max-calls-allowed <RATE_LIMIT_MAX_CALLS_ALLOWED>
          Max calls allowed per interval [env: RATE_LIMIT_MAX_CALLS_ALLOWED=] [default: 1000]
      --rate-limit-interval-seconds <RATE_LIMIT_INTERVAL_SECONDS>
          Interval in seconds to check limit [env: RATE_LIMIT_INTERVAL_SECONDS=] [default: 60]
      --topology <TOPOLOGY>
          Other node addresses in the cluster (e.g., http://node1:8000,http://node2:8000). If empty, runs in single-node mode. [env: TOPOLOGY=]
  -h, --help
          Print help
```

## Manual Single-Node

```sh
$ cargo run
```

### Manual Multi-Node Cluster

```sh
# Run each in separate terminals
$ cargo run -- --listen-port 8001 --topology http://localhost:8002 --topology http://localhost:8003
$ cargo run -- --listen-port 8002 --topology http://localhost:8001 --topology http://localhost:8003
$ cargo run -- --listen-port 8003 --topology http://localhost:8001 --topology http://localhost:8002
```

Note: The `--topology` flag specifies OTHER nodes in the cluster (not including the current node).
