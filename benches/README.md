# Colibri Benchmarks

Performance benchmarks for the rate limiter core.

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Save baseline for comparison
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main
```

## Benchmark Groups

### Token Bucket

Tests the core rate limiting logic without network overhead:

- `single_client_check`: Check remaining tokens for one client
- `single_client_consume`: Consume tokens for one client
- `many_clients_rotating`: 1000 rotating clients

### Token Refill

Tests token bucket refill logic when tokens replenish over time.

### Export/Import

Tests serialization for cluster operations with 10, 100, and 1000 clients.

## Interpreting Results

- **Time**: Lower is better (nanoseconds or microseconds per operation)
- **Throughput**: Operations per second
- **Variance**: Lower is more consistent

## Adding Benchmarks

When adding benchmarks:

1. Test realistic scenarios
2. Use `black_box()` to prevent compiler optimization
3. Set throughput metrics with `group.throughput()`
4. Document what the benchmark measures
