# Colibri: Rate-Limiting as a Service

HTTP service for distributed rate limiting with in-memory token bucket storage.

**Note**: Rate limit counts reset when nodes restart.

## Design

Implements [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) with in-memory storage for fast response times.

Colibri supports three distinct operational modes:

1. **Single-Node Mode**: Each Colibri node tracks rate limits independently. Simple but isolated behavior across distributed requests (such as behind a load balancer)

2. **Gossip Mode**: Nodes share rate limiting state through a gossip protocol, eventually converging to consistent token counts across all nodes. Provides eventual consistency with resilience to network partitions.

3. **Hashring Mode**: Uses consistent hashing to assign client responsibility to specific nodes, functioning as a distributed hash table. Provides strong consistency but requires all nodes to be reachable.

## Quick Start

After cloning this repo, you can quickly launch Colibri using the provided justfile recipes:

### Single-Node Mode

```sh
# Start a single node on port 8000
❯ just run
```

### Multi-Node Clusters

```sh
# Gossip mode: 3-node cluster with eventual consistency
❯ just demo gossip

# Hashring mode: 3-node cluster with consistent hashing
❯ just demo hashring

# Single node with comprehensive validation
❯ just demo single
```

### Quick Manual Test

```sh
# Test rate limiting (consumes tokens)
❯ curl -XPOST -i http://localhost:8001/rl/test-client

# Check remaining tokens (doesn't consume)
❯ curl -XGET -i http://localhost:8001/rl-check/test-client
```

## Demo Scripts & Validation

The `demo/` directory contains comprehensive validation scripts that test all three operational modes:

### Interactive Demo

```sh
# Choose between single, gossip, or hashring mode
❯ ./demo/quick-validation-demo.sh
```

### Validation Features

- **Rate Limiting**: Token consumption, exhaustion, and recovery
- **Timing**: Token bucket refill and burst capacity
- **Consistency**: Distributed state management across nodes
- **Logging**: Filtered output with essential information only

### Manual Commands

```sh
❯ just demo single     # Single node validation
❯ just demo gossip     # 3-node gossip cluster
❯ just demo hashring   # 3-node hashring cluster
```

### Available Development Recipes

Use `just --list` to see all available recipes:

- `just run` - Single-node mode on port 8000
- `just run-cluster` - 3-node cluster for testing distributed features
- `just run-nodeN` - Individual nodes (1, 2, 3) for custom cluster setup
- `just test-cluster` - Automated testing of multi-node functionality
- `just test` - Run all unit and integration tests

## API Endpoints

### Rate Limiting

- `POST /rl/{client_id}` - Apply rate limit (consumes tokens)
- `GET /rl-check/{client_id}` - Check remaining tokens (no consumption)

### Health & Status

- `GET /health` - Health check endpoint
- `GET /about` - Application version info

### Custom Rules

- `POST /rl-config` - Create named rate limit rule
- `GET /rl-config` - List all rules
- `GET /rl-config/{rule_name}` - Get specific rule
- `DELETE /rl-config/{rule_name}` - Delete rule
- `POST /rl/{rule_name}/{key}` - Apply custom rate limit
- `GET /rl-check/{rule_name}/{key}` - Check custom rule tokens

## Configuration Options

Key command-line options for running Colibri:

```sh
# Basic single-node mode
❯ cargo run

# Multi-node with custom settings
❯ cargo run -- --listen-port 8001 --rate-limit-max-calls-allowed 100 --rate-limit-interval-seconds 10 --run-mode gossip --topology "127.0.0.1:8401" --topology "127.0.0.1:8402"
```

### Important Options

- `--run-mode`: `single`, `gossip`, or `hashring`
- `--rate-limit-max-calls-allowed`: Token bucket size (default: 1000)
- `--rate-limit-interval-seconds`: Refill interval (default: 60)
- `--topology`: Other nodes in cluster (for distributed modes)
- `--listen-port`: HTTP port (default: 8000)
- `--listen-port-udp`: UDP port for gossip communication

Use `cargo run -- --help` for complete options list.

## Expected Behavior

- **Single Mode**: Each node maintains independent rate limits
- **Gossip Mode**: Nodes eventually converge to consistent token counts (~3s)
- **Hashring Mode**: Requests route to consistent bucket owners

## Performance Notes

- In-memory storage provides microsecond response times
- Token bucket refills occur at configured intervals
- UDP gossip minimizes network overhead
