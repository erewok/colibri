# Demo and Validation Scripts

Demo scripts and validation tools for testing distributed rate limiting behavior.

## Structured Logging

Structured debug messages use these prefixes:

- `[RATE_CHECK]` - Rate limit check operations
- `[RATE_LIMIT]` - Rate limit enforcement operations
- `[GOSSIP_CHECK]` / `[GOSSIP_LIMIT]` - Gossip-based operations
- `[GOSSIP_SYNC]` - Gossip state synchronization
- `[TCP_RESPONSE]` - Hashring TCP communication
- `[ROUTE_FALLBACK]` / `[BUCKET_MISSING]` - Routing issues

## Validation Scripts

**`rate-limit-validation.sh`** - Tests token consumption, exhaustion, recovery, and client isolation.

**`timing-validation.sh`** - Validates token bucket refill timing and burst capacity.

**`consistency-validation.sh`** - Tests distributed state management across gossip and hashring modes.

**`log-filter.sh`** - Filters logs to show essential rate limiting information with color coding.

## Requirements

- `curl` - HTTP client for API requests
- `jq` - JSON parsing (install with `brew install jq`)

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
