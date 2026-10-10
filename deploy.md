# Deploying Colibri

Colibri is a single binary, `colibri-server`, with no external dependencies. All state lives in memory: a restart forgets every count, which for a rate limiter means a fresh window. You run one process per node, in one of three modes.

```sh
cargo build --release --bin colibri-server
./target/release/colibri-server --help
```

## Choosing a Mode

| | Single | Gossip | Hashring |
|---|---|---|---|
| Limit per client | `max_calls` per node | at most `max_calls` per node, `nodes × max_calls` per cluster | exactly `max_calls` per cluster |
| A node goes down | its clients start fresh elsewhere | the others carry on | calls for the clients it owns fail |
| Node-to-node traffic | none | UDP | TCP |
| Memory for idle clients | freed after two intervals | freed once nothing is live | freed after two intervals |
| Best for | one limiter per app instance, or one shared instance | high availability when some overshoot is acceptable | an exact limit with a small, stable cluster |

Gossip mode has further limits, described [below](#current-limitations).

## What Every Deployment Needs

**A private network.** Colibri has no authentication and no TLS on any port. Anyone who can reach the HTTP port can spend any client's limit, and anyone who can reach the peer port can inject state. Put nodes where only your own services can reach them, such as a private subnet, a Kubernetes `NetworkPolicy`, or a service mesh that adds mTLS.

**Two ports per node.**

- `--client-listen-port` (default 8411, TCP): the HTTP API your services call.
- `--peer-listen-port` (default 8412): node-to-node traffic. It's UDP in gossip mode and TCP in hashring mode, and unused in single mode. Firewalls and Kubernetes `Service` objects must allow the right protocol.

Both bind to `--client-listen-address` (default `127.0.0.1`). In a container or on a shared network, set it to `0.0.0.0` or the host's address. Use `--peer-listen-address` to bind the peer port somewhere else.

**Configuration through flags or environment variables.** Every flag has an environment variable, such as `COLIBRI_RUN_MODE` or `COLIBRI_CLIENT_LISTEN_PORT`, which suits containers. The `--config-file` option exists, but currently any value it sets is overridden by the flag defaults, so don't rely on it.

**A unique, stable name per node.** `--name` is required. A node's identity in the cluster is derived from its name. For gossip mode, read [restarts](#restarting-a-gossip-node) before reusing names.

**A static topology.** List the other nodes with `-t name=host:peer_port`, once per node. Membership is fixed at startup: to add or remove a node, restart every node with the new list.

**Named rules reach every node.** Create or delete a rule on any node; gossip nodes spread it with their state, and hashring nodes send it to every peer and re-send it every 2 seconds, so a node that was down catches up. Changing a rule's settings starts its counts afresh. A restarted node relearns rules from its peers.

**Upgrade every node together.** Nodes refuse messages from a different protocol version, so a mixed-version cluster doesn't share state.

**Health checks.** `GET /health` returns `200` once the HTTP server is up; use it for liveness and readiness. In gossip mode, also see the readiness note under [restarts](#restarting-a-gossip-node).

**Logs.** Plain-text logs go to stdout. Set the level with `RUST_LOG`, for example `RUST_LOG=info` or `RUST_LOG=colibri=debug`. There's no metrics endpoint yet.

**A decision about failures.** A request that takes longer than 10 s gets `408`. An overloaded node returns `503` rather than queueing, and in hashring mode a request whose owner is down fails with a `5xx`. Your services must decide whether an error means allow (fail open) or deny (fail closed). For most rate limiting, failing open with a short client-side timeout is the safer default.

**No graceful shutdown.** On `SIGTERM` the process exits immediately, and in-flight requests fail. Remove a node from your load balancer before stopping it.

## Single Mode

Each node limits its clients independently. Two common layouts:

- **Sidecar**: one Colibri per application instance, reached over `localhost`. This needs no network exposure and gives each instance its own limit.
- **Shared instance**: one Colibri for a whole service. This gives one exact limit, but it's a single point of failure, so clients should fail open.

```sh
colibri-server --name limiter --client-listen-address 0.0.0.0 \
    --rate-limit-max-calls-allowed 100 --rate-limit-interval-seconds 60
```

Idle clients are dropped after two intervals, so memory tracks the number of active clients.

## Gossip Mode

Every node stores every client and admits calls on its own, then gossips the change to its peers. Any node can serve any client, and a node that loses its peers keeps working. The trade-off is the overshoot described in the README: calls spread across nodes faster than gossip can reach `nodes × max_calls` per window.

Each node lists all the *other* nodes:

```sh
colibri-server --name node-1 --run-mode gossip --client-listen-address 0.0.0.0 \
    --peer-listen-port 8412 \
    -t node-2=10.0.0.2:8412 -t node-3=10.0.0.3:8412
```

Put a load balancer in front of all nodes, or have each application instance call a nearby node.

### Tuning Gossip

On each tick (`--gossip-interval-ms`, default 1000), a node sends its changes to `--gossip-fanout` random peers (default 1). A change usually reaches every node within a few ticks. The other nodes can only enforce the shared limit once the change has arrived, and a call only counts until it ages out of the window. So:

- **Keep the tick well under the rate-limit interval.** With a 2-second interval and the default 1-second tick, changes sometimes arrive too late to matter. For example, the demo uses 250 ms ticks.
- **Raise the fanout as the cluster grows**, so changes still reach every node in a few ticks.

### Restarting a Gossip Node

Each process is a separate participant in the cluster's counts, so a restart is safe whatever the clocks do. Calls the old process admitted keep counting until they age out; once a peer's copy of them has sat unchanged for a full window, it releases them. The same happens when a node is removed for good.

Until a restarted node has heard from its peers, it doesn't know about their calls. To keep the overshoot small, keep it out of the load balancer for a few gossip ticks after it starts, for example by delaying readiness by 5 × `--gossip-interval-ms`. `/health` alone doesn't wait for this.

### Current Limitations

**Anti-entropy sends every client.** Gossip state is split into datagrams of at most `--gossip-max-packet-bytes` (default 1400, which fits a standard Ethernet frame without fragmenting). A client takes about 90 bytes in a 3-node cluster when one node serves it, and about 190 bytes when all three do, so a 1400-byte datagram carries 7 to 15 clients. A restarted process's entry is dropped about a window after the restart; a node removed for good keeps its entry until the client goes idle. A node that falls behind pulls the full state of every active client in one burst, for example about 700 datagrams for 10,000 clients. If the receiver's socket buffer overflows, the lost datagrams are retried on the next heartbeat. Raise the buffer (`net.core.rmem_max` and `rmem_default` on Linux) for large client counts.

Each node forgets a client once nothing for it is live, on any node it has heard from. That is usually within an interval plus a few gossip ticks of the client's last call, so memory and message size track active clients, not every client ever seen.

Gossip mode currently suits a small set of keys, such as per-tenant or per-route limits, rather than per-end-user limits.

## Hashring Mode

Each client is owned by one node, chosen by consistent hashing over the sorted node names. Any node accepts a call and forwards it to the owner over TCP, so every client gets exactly `max_calls`. Each node lists every node, *including itself*:

```sh
colibri-server --name node-1 --run-mode hashring --client-listen-address 0.0.0.0 \
    --peer-listen-port 8412 \
    -t node-1=10.0.0.1:8412 -t node-2=10.0.0.2:8412 -t node-3=10.0.0.3:8412
```

Operational notes:

- **A node going down takes its clients with it.** There is no replication: calls for the clients it owns fail until it returns, and then start from a fresh window.
- **Changing the node list moves clients to new owners**, which start them from a fresh window. Ownership follows each node's position in the sorted list of names, so how many clients move depends on where a new name sorts. Carrying counts across isn't supported yet.
- **Memory.** Like single mode, the owner forgets a client after two idle intervals.

### `colibri-admin`

`colibri-admin` talks to a hashring node's peer port over TCP. It doesn't work with gossip nodes, which use UDP.

```sh
colibri-admin --target 10.0.0.1:8412 get-status
colibri-admin --target 10.0.0.1:8412 get-topology
```

It also lists `export-buckets` and `import-buckets`, which aren't implemented yet.

## Kubernetes Sketch

There are no published images or manifests yet. These are the parts that matter:

- **Gossip or hashring**: use a `StatefulSet` with a headless `Service`, so pods get stable names (`colibri-0`, `colibri-1`, ...) and DNS addresses for `-t name=colibri-0.colibri:8412`. Set `--name` from the pod name. A topology change means updating every pod's arguments and rolling the set.
- **Ports**: expose the HTTP port through a normal `Service` for callers. Gossip needs `protocol: UDP` on the peer port; hashring needs TCP.
- **Probes**: use `GET /health` for liveness. For gossip, add the readiness delay described above.
- **Lifecycle**: add a short `preStop` sleep so the pod leaves the `Service` endpoints before it exits, since there's no graceful drain.
- **Network**: add a `NetworkPolicy` that only admits your services to the HTTP port and only Colibri pods to the peer port.
