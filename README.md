# Colibri: Rate-Limiting as a Service

Colibri is a simple HTTP service built in Rust that implements an in-memory data structure for rate-limiting services. Rate counts are stored _in memory_ so that Colibri can respond quickly.

**Note**: Restarting any Colibri node will restart any rate-limit counts.

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
          In multi-node mode, pass other node hostnames [env: TOPOLOGY=] [default: ]
      --hostname <HOSTNAME>
          An identifier for this node [env: HOSTNAME=] [default: ]
  -h, --help
          Print help

```

