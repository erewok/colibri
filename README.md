# Colibri: Rate-Limiting as a Service

Colibri is a simple HTTP service built in Rust that implements an in-memory data structure for rate-limiting services. Rate counts are stored _in memory_ so that Colibri can respond quickly.

**Note**: Restarting a Colibri node will restart any rate-limit counts.

## Design

Colibri implements the [Token Bucket algorithm](https://en.wikipedia.org/wiki/Token_bucket) for rate-limiting clients. Currently, all Colibri data structures are held in memory without persistence so that it can quickly respond to incoming requests. 

Colibri can be run in single-node mode or in multi-node mode. In single-node mode each Colibri node will keep track of rate-limits individually without using any distributed properties. This strategy could potentially work behind a round-robin load balancer that fairly distributes traffic but it gets quickly confusing with interleaved client requests.

In multi-node mode Colibri functions as a distributed hash table, assigning responsibility for distinct client IDs to individual nodes using consistent hashing. This is experimental; for instance, it's not currently designed to work around network partitions or dynamic cluster resizing.

## Demo

After cloning this repo, you can launch Colibri locally using `cargo`:

```sh
❯ cargo run -- --rate-limit-max-calls-allowed=2 --rate-limit-interval-seconds=10
   Compiling colibri v0.2.0 (/Users/erewok/open_source/colibri)
    Finished dev [unoptimized + debuginfo] target(s) in 3.61s
     Running `target/debug/colibri --rate-limit-max-calls-allowed=4 --rate-limit-interval-seconds=10`
2023-03-22T15:24:12.893173Z  INFO colibri: Starting Cache Expiry background task
2023-03-22T15:24:12.893274Z  INFO colibri: Starting Colibri on 0.0.0.0:8000
```

Then, in another terminal, you can send requests and see them rate-limited:

```sh
❯ curl -XPOST -i http://localhost:8000/rl/some-client-identifier
HTTP/1.1 200 OK
content-type: application/json
content-length: 58
date: Wed, 22 Mar 2023 15:26:17 GMT

{"client_id":"some-client-identifier","calls_remaining":1}

❯ curl -XPOST -i http://localhost:8000/rl/some-client-identifier
HTTP/1.1 200 OK
content-type: application/json
content-length: 58
date: Wed, 22 Mar 2023 15:26:20 GMT

{"client_id":"some-client-identifier","calls_remaining":0}

❯ curl -XPOST -i http://localhost:8000/rl/some-client-identifier
HTTP/1.1 429 Too Many Requests
content-length: 0
date: Wed, 22 Mar 2023 15:26:45 GMT
```

[Click here for a terminal demo](./rate-limiting-demo.gif).

## Single-Node Mode

You can run colibri as a single-node in the following way:

```sh
$ colibri
2023-03-24T23:22:26.690566Z  INFO colibri: Starting Cache Expiry background task
2023-03-24T23:22:26.690666Z  INFO colibri: Starting Colibri on 0.0.0.0:8000

```


## Multi-Node Mode

You can run a few colibri nodes as part of a cluster in the following way:

```sh
$ cargo run -- --listen-port 8000 --node-id 0 --topology http://localhost:8000 --topology http://localhost:8001 --topology http://localhost:8002 --topology http://localhost:8003

$ cargo run -- --listen-port 8001 --node-id 1 --topology http://localhost:8000 --topology http://localhost:8001 --topology http://localhost:8002 --topology http://localhost:8003

$ cargo run -- --listen-port 8002 --node-id 2 --topology http://localhost:8000 --topology http://localhost:8001 --topology http://localhost:8002 --topology http://localhost:8003

```

In another terminal, if you make some requests, you can see communicating going across the cluster:

```sh
❯ curl -i -XPOST  http://localhost:8000/rl/a
HTTP/1.1 200 OK
content-type: application/json
content-length: 37
date: Fri, 24 Mar 2023 23:42:19 GMT

{"client_id":"a","calls_remaining":1}

❯ curl -i -XPOST  http://localhost:8000/rl/a
HTTP/1.1 200 OK
content-type: application/json
content-length: 37
date: Fri, 24 Mar 2023 23:42:21 GMT

{"client_id":"a","calls_remaining":0}

❯ curl -i -XPOST  http://localhost:8000/rl/a
HTTP/1.1 429 Too Many Requests
content-length: 0
date: Fri, 24 Mar 2023 23:42:22 GMT

```

Back in the terminal of the node we're requesting, we'll see it's pulling from the other node:

```sh
2023-03-24T23:42:09.859385Z  INFO rate_limit{client_id="a"}: colibri::node: Requesting data from bucket 1

```


## Configuration

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
          In cluster mode, pass other node addresses: order matters! [env: TOPOLOGY=] [default: ]
      --hostname <HOSTNAME>
          An identifier for this node [env: HOSTNAME=] [default: ]
      --node-id <NODE_ID>
          An identifier for this node [env: HOSTNAME=] [default: 0]
  -h, --help
          Print help

```

