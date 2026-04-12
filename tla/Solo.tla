---------------------------- MODULE Solo ----------------------------
\* Spec for: Single-node token bucket rate limiter (no distribution).
\* Corresponds to: src/limiters/token_bucket.rs, src/node/single_node.rs
\* Verified: 2026-04-11, TLC 1.8
\*
\* Abstractions:
\*   - tokens (f64) -> Nat in 0..MaxTokens
\*   - wall-clock time -> discrete tick counter in 0..MaxTicks
\*   - refill: adds RefillPerTick tokens per tick (clamped at MaxTokens)
\*   - A single client "c1" with one bucket
\*   - Bucket presence is tracked separately (present[c] is TRUE or FALSE).
\*     When absent, tokens[c] is irrelevant.
\*   - expire_keys: bucket is removed when idle_ticks > ExpireAfterTicks

EXTENDS Naturals, TLC

CONSTANTS
    MaxTokens,        \* capacity of the bucket (rate_limit_max_calls_allowed)
    MaxTicks,         \* model bound on time
    RefillPerTick,    \* tokens added per tick
    ExpireAfterTicks  \* idle ticks before bucket is evicted

ASSUME MaxTokens        \in Nat /\ MaxTokens        >= 1
ASSUME MaxTicks         \in Nat /\ MaxTicks          >= 1
ASSUME RefillPerTick    \in Nat /\ RefillPerTick     >= 1
ASSUME ExpireAfterTicks \in Nat /\ ExpireAfterTicks  >= 1

Client == {"c1"}

VARIABLES
    present,     \* present[c] \in BOOLEAN — TRUE when bucket exists
    tokens,      \* tokens[c]  \in 0..MaxTokens (relevant only when present[c])
    tick,        \* current discrete time step  0..MaxTicks
    idle_ticks,  \* idle_ticks[c]: ticks since last request (for expiry)
    last_action  \* "init" | "tick" | "allowed" | "denied" | "expire"

vars == <<present, tokens, tick, idle_ticks, last_action>>

\* ---- Type invariant -------------------------------------------------------

TypeOK ==
    /\ present    \in [Client -> BOOLEAN]
    /\ tokens     \in [Client -> 0..MaxTokens]
    /\ tick       \in 0..MaxTicks
    /\ idle_ticks \in [Client -> 0..(MaxTicks + 1)]
    /\ last_action \in {"init", "tick", "allowed", "denied", "expire"}

\* ---- Safety invariants ----------------------------------------------------

TokensNonNegative ==
    \A c \in Client : tokens[c] >= 0

TokensCapped ==
    \A c \in Client : tokens[c] <= MaxTokens

\* After an allowed request every present bucket has tokens >= 0.
RequestOnlyWhenTokens ==
    last_action = "allowed" =>
        \A c \in Client : present[c] => tokens[c] >= 0

\* ---- Initial state --------------------------------------------------------

Init ==
    /\ present     = [c \in Client |-> TRUE]
    /\ tokens      = [c \in Client |-> MaxTokens]
    /\ tick        = 0
    /\ idle_ticks  = [c \in Client |-> 0]
    /\ last_action = "init"

\* ---- Helpers ---------------------------------------------------------------

RefillValue(c) ==
    IF tokens[c] + RefillPerTick > MaxTokens THEN MaxTokens
    ELSE tokens[c] + RefillPerTick

\* ---- Actions ---------------------------------------------------------------

\* Tick: advance time, refill all live buckets, increment idle counters.
Tick ==
    /\ tick < MaxTicks
    /\ tick' = tick + 1
    /\ tokens'     = [c \in Client |->
                          IF present[c] THEN RefillValue(c)
                          ELSE tokens[c]]
    /\ idle_ticks' = [c \in Client |->
                          IF present[c] THEN idle_ticks[c] + 1
                          ELSE idle_ticks[c]]
    /\ UNCHANGED <<present>>
    /\ last_action' = "tick"

\* Allowed request: bucket present and tokens >= 1, OR bucket absent (first call).
ConsumeAllowed(c) ==
    /\ (~present[c]) \/ (present[c] /\ tokens[c] >= 1)
    /\ present'    = [present    EXCEPT ![c] = TRUE]
    /\ tokens'     = [tokens     EXCEPT ![c] = IF ~present[c]
                                                THEN MaxTokens - 1
                                                ELSE tokens[c] - 1]
    /\ idle_ticks' = [idle_ticks EXCEPT ![c] = 0]
    /\ tick' = tick
    /\ last_action' = "allowed"

\* Denied request: bucket present but tokens < 1.
ConsumeDenied(c) ==
    /\ present[c]
    /\ tokens[c] < 1
    /\ UNCHANGED <<present, tokens, tick, idle_ticks>>
    /\ last_action' = "denied"

ConsumeToken ==
    \E c \in Client : ConsumeAllowed(c) \/ ConsumeDenied(c)

\* ExpireKeys: evict buckets idle for more than ExpireAfterTicks ticks.
ExpireKeys ==
    /\ \E c \in Client : present[c] /\ idle_ticks[c] > ExpireAfterTicks
    /\ present'    = [c \in Client |->
                          IF present[c] /\ idle_ticks[c] > ExpireAfterTicks
                          THEN FALSE ELSE present[c]]
    /\ idle_ticks' = [c \in Client |->
                          IF present[c] /\ idle_ticks[c] > ExpireAfterTicks
                          THEN 0 ELSE idle_ticks[c]]
    /\ UNCHANGED <<tokens, tick>>
    /\ last_action' = "expire"

\* ---- Spec ------------------------------------------------------------------

Next ==
    \/ Tick
    \/ ConsumeToken
    \/ ExpireKeys

Spec == Init /\ [][Next]_vars

====================================================================
