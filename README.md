# Colibri: Rate-Limiting as a Service

HTTP service for distributed rate limiting with in-memory state.

**Colibri is a single binary with no other services to run.**

**Note**: Rate limit counts reset when nodes restart.

**Another Note**: I have generally asked Claude to implement the TLA+ specs **and** the hegel property-based tests. I wrote myself the primary code for this codebase and it was all an idea I had (which did not come from talking to an LLM...). Make of that what you will ¯\_(ツ)_/¯.

## Why I Made This

Most of the rate-limiting implementations I have worked on in the past have used Redis with a token-bucket or leaky-bucket implementation either in Lua or application code. Redis is completely fine, but something always bothered me about this setup: why couldn't I have a pure in-memory rate-limiting appliance?

I wanted something standalone, in other words: its only job is rate-limiting! It should be able to do this job quickly and purely in-memory. Of course, this request ends up being something like a single-function distributed database, but that's probably okay.

So that's what colibri is for. It has **one job**: you can ask it (over http) to rate-limit some key or some group of keys with a dedicated limit configuration.

One primary design goal for this project is as mentioned above: **Colibri remains a single binary with no other services to run.**

## Status: EXPERIMENTAL

This project's status is considered **experimental**. I do not run it in production yet. It has the following major issues to solve:

- Gossip: a node that starts or rejoins receives every active client in one burst of datagrams (see [deploy.md](deploy.md)).
- Crash-and-reload is not implemented
- No upgrade paths (we have planned an admin command).
- No authentication or TLS, no metrics, no graceful shutdown
- The config file I want to use is ignored.

## Design

Each client gets `max_calls` calls per `interval_seconds`. Colibri supports three operational modes, which share that limit in different ways:

1. **Single-Node Mode**: A [token bucket](https://en.wikipedia.org/wiki/Token_bucket) per client that refills continuously, at `max_calls` per `interval_seconds`. Each node tracks its clients independently, so behind a load balancer every node grants its own full limit.

2. **Gossip Mode**: Every node holds a replica of every client's state as a CRDT and gossips changes to its peers over UDP. The limit is a sliding window: each admitted call holds one of `max_calls` tokens for `interval_seconds`, then gives it back. What the cluster guarantees, per client and per window:
   - Each node admits at most `max_calls`, because a node always counts its own calls.
   - The cluster admits at most `(number of nodes) * max_calls`. Nodes can admit calls before hearing about each other's, so a burst spread across nodes faster than gossip can exceed `max_calls`.
   - Once a change has spread (usually within a few gossip ticks), every node enforces the shared limit.
   - After a lost message or a partition, heartbeats detect the gap and a full-state exchange repairs it, so nodes converge.

3. **Hashring Mode**: Each client is owned by one node, chosen by consistent hashing. Other nodes forward that client's calls and checks to the owner over TCP, and the owner runs a token bucket as in single-node mode. Every node therefore reports the same count, and a client gets exactly `max_calls` however its calls are spread. If a client's owner is unreachable, calls for that client fail with an error.

## Quick Start

After cloning this repo, launch a single node with the provided justfile recipe:

```sh
# Start a single node with its HTTP API on port 8411
❯ just run

# Apply the rate limit (consumes a token)
❯ curl -XPOST -i http://localhost:8411/rl/test-client
HTTP/1.1 200 OK
content-type: application/json

{"client_id":"test-client","rule_name":"<_default>","calls_remaining":999}

# Check remaining calls (doesn't consume)
❯ curl -XGET -i http://localhost:8411/rl-check/test-client
HTTP/1.1 200 OK
content-type: application/json

{"client_id":"test-client","rule_name":"<_default>","calls_remaining":999}
```

Once a client has used up its calls, `POST /rl/{client_id}` returns `429 Too Many Requests`.

## Demo Scripts & Validation

The `demo/` directory starts a cluster in each mode and validates its behavior:

```sh
# Gossip mode: 3-node cluster
❯ just demo gossip

# Hashring mode: 3-node cluster
❯ just demo hashring

# Single node
❯ just demo single
```

Each demo runs validation scripts that check the behavior described under [Design](#design) and reports pass or fail. The nodes then keep running so you can explore with `curl`; press Ctrl+C to stop them. To run the validations and exit with their verdict instead, use `just demo-check <mode>`.

See [demo/README.md](demo/README.md) for what each script checks.

### Available Development Recipes

Use `just --list` to see all available recipes:

- `just run` - Single node, HTTP API on port 8411
- `just run-port <port>` - Single node on another port
- `just demo <mode>` - Start a demo cluster and validate it (`single`, `gossip`, or `hashring`)
- `just demo-check <mode>` - Validate a demo cluster, stop it, and exit with the verdict
- `just test` - Run all unit, integration, and property tests
- `just test-deep` - Run the property tests with many more cases
- `just check` - Run `cargo check`, `clippy`, and `rustfmt`

## API Endpoints

### Rate Limiting

- `POST /rl/{client_id}` - Apply the rate limit (consumes a token). Returns `200` with `calls_remaining`, where `0` means this call used the last token, or `429` when the client is rate limited.
- `GET /rl-check/{client_id}` - Calls remaining without consuming. This is what the next call would find. In gossip mode, expired calls are released on the node's next gossip tick, so a check may lag by up to one tick.

### Health & Status

- `GET /health` - Health check endpoint
- `GET /about` - Application version info

### Custom Rules

A named rule has its own limit and gets the same guarantees as the default rule. In gossip and hashring mode, creating or deleting a rule on any node applies it on every node.

- `POST /rl-config` - Create named rate limit rule
- `GET /rl-config` - List all rules
- `GET /rl-config/{rule_name}` - Get specific rule
- `DELETE /rl-config/{rule_name}` - Delete rule
- `POST /rl/{rule_name}/{key}` - Apply custom rate limit
- `GET /rl-check/{rule_name}/{key}` - Check custom rule tokens

## Configuration Options

Key command-line options for running Colibri:

```sh
# Single node
❯ cargo run -- --name node-1

# A gossip node with custom settings, in a cluster with two other nodes
❯ cargo run -- --name node-1 --run-mode gossip \
    --client-listen-port 8001 --peer-listen-port 8401 \
    --rate-limit-max-calls-allowed 100 --rate-limit-interval-seconds 10 \
    -t "node-2=127.0.0.1:8402" -t "node-3=127.0.0.1:8403"
```

See `demo/gossip.sh` and `demo/hashring.sh` for complete three-node clusters.

### Important Options

- `--name`: Name of this node, unique within the cluster (required)
- `--run-mode`: `single`, `gossip`, or `hashring` (default: `single`)
- `--rate-limit-max-calls-allowed`: Calls allowed per interval (default: 1000)
- `--rate-limit-interval-seconds`: Length of the interval (default: 60)
- `--client-listen-port`: HTTP API port (default: 8411)
- `--peer-listen-port`: Port for node-to-node traffic: UDP in gossip mode, TCP in hashring mode (default: 8412)
- `-t, --topology`: Another node in the cluster as `name=address:port`; repeat for each node. Hashring nodes also list themselves.
- `--gossip-interval-ms`: How often a gossip node sends updates (default: 1000)
- `--gossip-fanout`: How many peers each gossip update goes to (default: 1)

Every option can also be set with an environment variable, such as `COLIBRI_RUN_MODE`. Use `cargo run -- --help` for the complete list.

## Deployment

See [deploy.md](deploy.md) for choosing a mode, network and port requirements, gossip tuning, and the current limitations of each mode.

## Verification

Beyond unit tests, two tools check the distributed behavior:

- **Property tests** (`tests/hegel_*.rs`) drive the limiters and a simulated gossip cluster through random sequences of requests, clock advances, and lost messages, using [Hegel](https://hegel.dev). Their invariants include the per-node and cluster-wide limits above.
- **TLA+ specifications** (`tla/`) model the gossip CRDT and hashring routing and are checked with TLC.
