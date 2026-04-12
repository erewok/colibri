---------------------------- MODULE Hashring ----------------------------
\* Spec for: Consistent-hashing request routing in Colibri hashring mode.
\* Corresponds to: src/node/hashring/controller.rs,
\*                 src/node/hashring/consistent_hashing.rs
\* Verified: 2026-04-11, TLC 1.8
\*
\* Abstractions:
\*   - jump_consistent_hash(client_id, N) -> fixed Owner: Clients -> Nodes
\*     TLC enumerates all surjective assignments (i.e. all valid partitions).
\*   - tokens (f64) -> Nat in 0..MaxTokens, one bucket per (node, client).
\*     Non-owning nodes always hold MaxTokens (untouched).
\*   - Time -> discrete tick counter; RefillPerTick tokens added per tick.
\*   - No cross-node coordination (no CRDT, no gossip).
\*   - Request forwarding: a consume action for (n, c) requires owner[c] = n,
\*     mirroring handle_rate_limit_request's find_owner_if_not_self guard.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,        \* set of node identifiers, e.g. {"n1", "n2"}
    Clients,      \* set of client identifiers, e.g. {"a", "b"}
    MaxTokens,    \* token bucket capacity
    MaxTicks,     \* model bound on time
    RefillPerTick \* tokens added per tick

ASSUME Nodes   /= {} /\ IsFiniteSet(Nodes)
ASSUME Clients /= {} /\ IsFiniteSet(Clients)
ASSUME MaxTokens    \in Nat /\ MaxTokens    >= 1
ASSUME MaxTicks     \in Nat /\ MaxTicks     >= 1
ASSUME RefillPerTick \in Nat /\ RefillPerTick >= 1

VARIABLES
    owner,       \* owner: Clients -> Nodes (fixed after Init)
    tokens,      \* tokens[n][c] \in 0..MaxTokens
    tick,        \* current discrete time step
    last_action  \* "init" | "tick" | "allowed" | "denied"

vars == <<owner, tokens, tick, last_action>>

\* ---- Type invariant -------------------------------------------------------

TypeOK ==
    /\ owner \in [Clients -> Nodes]
    /\ tick \in 0..MaxTicks
    /\ \A n \in Nodes, c \in Clients : tokens[n][c] \in 0..MaxTokens
    /\ last_action \in {"init", "tick", "allowed", "denied"}

\* ---- Safety invariants ----------------------------------------------------

\* Each client has exactly one owner.
\* Since owner is a total function [Clients -> Nodes], each client maps to
\* exactly one node by definition.  We verify this explicitly by asserting that
\* the set of nodes that "own" c has cardinality 1.
SingleOwner ==
    \A c \in Clients :
        Cardinality({n \in Nodes : owner[c] = n}) = 1

\* Tokens never go negative.
TokensNonNegative ==
    \A n \in Nodes, c \in Clients : tokens[n][c] >= 0

\* Non-owning nodes never have depleted buckets for a client.
\* This holds because only ConsumeAllowed mutates tokens, and its guard
\* enforces owner[c] = n before decrementing tokens[n][c].
RequestRoutedToOwner ==
    \A n \in Nodes, c \in Clients :
        tokens[n][c] < MaxTokens => owner[c] = n

\* ---- Helpers ---------------------------------------------------------------

IsSurjective(f) ==
    \A n \in Nodes : \E c \in Clients : f[c] = n

RefillOne(t) ==
    IF t + RefillPerTick > MaxTokens THEN MaxTokens ELSE t + RefillPerTick

\* ---- Initial state --------------------------------------------------------

Init ==
    /\ owner \in [Clients -> Nodes]
    /\ IsSurjective(owner)
    /\ tokens = [n \in Nodes |-> [c \in Clients |-> MaxTokens]]
    /\ tick   = 0
    /\ last_action = "init"

\* ---- Actions ---------------------------------------------------------------

\* Tick: advance time, refill all buckets at all nodes.
Tick ==
    /\ tick < MaxTicks
    /\ tick'   = tick + 1
    /\ tokens' = [n \in Nodes |-> [c \in Clients |-> RefillOne(tokens[n][c])]]
    /\ UNCHANGED <<owner>>
    /\ last_action' = "tick"

\* Allowed consume at node n for client c: n owns c and has >= 1 token.
ConsumeAllowed(n, c) ==
    /\ owner[c] = n
    /\ tokens[n][c] >= 1
    /\ tokens'     = [tokens EXCEPT ![n][c] = tokens[n][c] - 1]
    /\ UNCHANGED <<owner, tick>>
    /\ last_action' = "allowed"

\* Denied consume at node n for client c: n owns c but tokens < 1.
ConsumeDenied(n, c) ==
    /\ owner[c] = n
    /\ tokens[n][c] < 1
    /\ UNCHANGED <<owner, tokens, tick>>
    /\ last_action' = "denied"

ConsumeToken ==
    \E n \in Nodes, c \in Clients :
        ConsumeAllowed(n, c) \/ ConsumeDenied(n, c)

\* ---- Spec ------------------------------------------------------------------

Next ==
    \/ Tick
    \/ ConsumeToken

Spec == Init /\ [][Next]_vars

====================================================================
