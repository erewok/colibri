# Demo and Validation Scripts

Each demo starts Colibri in one of its modes then runs validation scripts against it, and finally reports a verdict. Every check either passes or fails, and a validation script exits non-zero if any of its checks failed.

## Requirements

- `curl` - HTTP client for API requests
- `jq` - JSON parsing (install with `brew install jq`)

## Running a Demo

```bash
just demo single      # one node on port 8001
just demo gossip      # three gossip nodes on ports 8001-8003
just demo hashring    # three hashring nodes on ports 8001-8003
```

Each demo builds `colibri-server`, starts the nodes, waits for their `/health` endpoints, and runs its validations twice: under the default rule, and under a named rule created during the run. It then prints `ALL VALIDATIONS PASSED` or lists the scripts that failed. The nodes keep running afterwards so you can explore:

```bash
curl -X POST http://localhost:8001/rl/my-client
curl http://localhost:8001/rl-check/my-client
```

Press Ctrl+C to stop them. To stop the nodes and exit with the verdict instead, for example in CI, run `just demo-check <mode>` or set `DEMO_EXIT=1`.

### Settings

The demos read these environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `max_calls` | `5` | Calls allowed per interval |
| `interval_seconds` | `3` | Length of the interval |
| `gossip_interval_ms` | `250` | Gossip tick (gossip demo only) |
| `DEMO_EXIT` | `0` | `1` stops the nodes and exits with the verdict |
| `named_max_calls` | `4` | Calls allowed per interval under the named rule |
| `named_interval_seconds` | `2` | Interval for the named rule |
| `RUST_LOG` | `info` | Server log level |

For example, `max_calls=3 interval_seconds=2 just demo-check gossip`.

The gossip demo ticks every 250 ms rather than the server's 1000 ms default. A change must reach every node before its calls leave the window, or the other nodes cannot enforce the shared limit in time, and each tick reaches only one random peer.

## Validation Scripts

`validation-utils.sh` and `run-utils.sh` hold the shared helpers. Every phase uses a new client ID, so phases cannot affect each other.

### `rate-limit-validation.sh` (all modes)

Runs against the first node:

- A new client has `max_calls` remaining.
- Exactly `max_calls` calls are admitted, counting down to `0`. The call that takes the last token succeeds.
- Further calls get `429`.
- `/rl-check` reports `0` and does not consume.
- Another client is unaffected.

### `timing-validation.sh` (all modes)

Checks how capacity comes back after a client is exhausted, which differs by mode:

- **single and hashring**: The token bucket refills continuously. About half the capacity is back at half the interval, and all of it after the full interval.
- **gossip**: The sliding window gives nothing back during the interval. All the capacity returns at once after it, within a gossip tick or two.

In both cases the restored capacity allows exactly `max_calls` more calls.

### `consistency-validation.sh` (hashring and gossip)

Checks how nodes share a client's limit:

- **hashring**: Every node reports the owner's count, and `max_calls` calls are admitted in total however they are spread across nodes. Capacity returns on every node after the interval.
- **gossip**:
  - A change on one node reaches every node.
  - Once it has, the other nodes reject calls for an exhausted client.
  - When calls arrive at all nodes faster than gossip, each node admits at most `max_calls` and the cluster at most `3 * max_calls`. The script reports how many the cluster actually admitted.
  - Nodes converge afterwards, and capacity returns on every node after the window.

### `rule-propagation-validation.sh` (all modes)

`create` makes the named rule on the first node and checks every node has it; `delete` deletes it on the last node and checks every node drops it.

## Other Scripts

- **`log-filter.sh`**: Colors validation output and the gossip log messages `[GOSSIP_SYNC]`, `[GOSSIP_RESP]`, and `[GOSSIP_ANTI_ENTROPY]`, and hides most other log noise. For example: `./demo/gossip.sh 2>&1 | ./demo/log-filter.sh`.
- **`quick-validation-demo.sh`**: Interactive menu that runs one of the demos with 3 calls per 2 seconds.
- **`cluster-request-tests.sh`**: Sends a few raw requests to a running three-node cluster, for manual inspection.
