# Demo and Validation Scripts

This directory contains improved demo scripts and validation tools to help you verify that your distributed rate limiter is working correctly.

## Key Improvements

### 1. Structured Logging

The application uses structured debug messages with these prefixes:

- `[RATE_CHECK]` - Rate limit check operations
- `[RATE_LIMIT]` - Rate limit enforcement operations
- `[GOSSIP_CHECK]` / `[GOSSIP_LIMIT]` - Gossip-based operations
- `[GOSSIP_SYNC]` - Gossip state synchronization
- `[TCP_RESPONSE]` - Hashring TCP communication
- `[ROUTE_FALLBACK]` / `[BUCKET_MISSING]` - Routing issues

### 2. Validation Scripts

#### `rate-limit-validation.sh`

Basic validation script that tests:

- Initial token state
- Rate limit exhaustion
- Cross-node consistency
- Token refresh after interval
- Client isolation

#### `timing-validation.sh`

Timing tests that validate:

- Token bucket refill behavior
- Burst capacity
- Gradual vs. strict interval refills
- Complete rate limit enforcement

#### `consistency-validation.sh`

Distributed system validation for:

- Gossip mode: eventual consistency across nodes
- Hashring mode: consistent request routing
- Cross-node request distribution
- Recovery behavior

#### `log-filter.sh`

Filters log output to show only essential rate limiting information with color coding.

## Usage

### Running with Validation

```bash
# Gossip mode with validation
./demo/gossip.sh

# Hashring mode with validation
./demo/hashring.sh

# With filtered logging to reduce noise
./demo/gossip.sh 2>&1 | ./demo/log-filter.sh
```

### Individual Test Scripts

```bash
# Set your rate limit parameters
export max_calls=5
export interval_seconds=3
export mode="gossip"  # or "hashring"

# Run specific validations
./demo/rate-limit-validation.sh
./demo/timing-validation.sh
./demo/consistency-validation.sh
```

## Understanding the Output

### Color Coding

- 🟢 **Green**: Successful operations and validations
- 🔴 **Red**: Rate limited requests and errors
- 🟡 **Yellow**: Warnings and fallback behaviors
- 🔵 **Blue**: Informational messages and gossip sync
- 🟦 **Cyan**: Gossip-specific operations

### Key Metrics to Watch

1. **Token Consistency**: All nodes should eventually have consistent token counts (gossip mode)
2. **Rate Limit Enforcement**: Exactly `max_calls` requests should succeed per interval
3. **Recovery Timing**: Tokens should refresh after `interval_seconds`
4. **Routing Behavior**: Hashring should route to consistent bucket owners

### Expected Behaviors

#### Gossip Mode

- Initial state: All nodes have full tokens
- After consumption: Tokens reduce on consuming node first
- After gossip propagation (~3s): All nodes converge to same token count
- Recovery: All nodes restore tokens simultaneously

#### Hashring Mode

- Initial state: All nodes have full tokens
- After consumption: Only bucket-owning node shows reduction
- Cross-node requests: Route to appropriate bucket owners
- Recovery: Each bucket owner recovers independently

## Troubleshooting

### Common Issues

1. **No rate limiting**: Check that `max_calls` and `interval_seconds` are set correctly
2. **Inconsistent behavior**: May indicate gossip propagation delays or bucket routing issues
3. **Tokens not recovering**: Check token bucket refill implementation
4. **Cross-node inconsistency**: May indicate network issues or improper distributed state management

### Debug Tips

1. Use `./demo/log-filter.sh` to reduce log noise
2. Check the structured log messages for specific client/bucket routing
3. Verify timing between token consumption and recovery
4. Test with different client names to verify isolation