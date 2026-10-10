---------------------------- MODULE Gossip ----------------------------
\* Spec for: gossip-based CRDT rate limiter with sliding-window ageing.
\* Corresponds to: src/limiters/distributed_bucket.rs,
\*                 src/node/gossip/state.rs, src/node/gossip/controller.rs
\* Verified: 2026-10-10 with TLC 2026.08.21 (Gossip.cfg; Gossip3.cfg does not finish)
\*
\* What this models
\* ----------------
\* One client's bucket, replicated on every node. The CRDT has one slot per
\* actor: a node process, <<node, incarnation>>. A restart starts a new
\* incarnation, so a slot is never reused.
\*   reqs[n][a] = node n's view of actor a's request count
\*   exps[n][a] = node n's view of actor a's expiry count
\* Capacity is a local constant: Tokens(n) = MaxTokens - (live requests known).
\* Each actor writes only its own slot. Others learn it through GossipMerge
\* (element-wise max), except that RetireAt may expire a slot that has sat
\* unchanged for longer than the window: everything it counts is old enough.
\*
\* Actions
\* -------
\*   ConsumeAt(n)     admit a request (rebasing an unused own slot first)
\*   ExpireAt(n)      age out n's own requests: exps := reqs - still tracked
\*   RetireAt(n, a)   expire another actor's slot unchanged for a window
\*   CompactAt(n, a)  forget an idle slot of a dead incarnation (tombstoned)
\*   ExpireTombAt(n,a) drop a compaction tombstone after a window
\*   GossipMerge(m,n) merge m's state (with its change ticks) into n, or adopt it
\*   DropAt(n)        forget an idle bucket
\*   RestartAt(n)     lose all state and start a new incarnation
\*   Tick             advance the clock
\*
\* GossipMerge may happen between any pair at any time, or never; which
\* messages the implementation actually sends is checked by
\* tests/hegel_gossip.rs.
\*
\* Ghosts: hw[a] is the highest value actor a ever wrote; admits[a] holds a
\* <<value, tick>> pair for each of a's requests; lastWrite[a] is when a last wrote.

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Nodes,          \* e.g. {n1, n2}
    MaxTokens,      \* capacity, a local constant on every node
    AgeingWindow,   \* requests older than this many ticks age out
    MaxOps,         \* bound on ConsumeAt
    MaxTicks,       \* bound on Tick
    MaxRestarts     \* bound on RestartAt (and on incarnations per node)

ASSUME Nodes # {}
ASSUME MaxTokens    \in Nat /\ MaxTokens    >= 1
ASSUME AgeingWindow \in Nat /\ AgeingWindow >= 1
ASSUME MaxOps       \in Nat
ASSUME MaxTicks     \in Nat /\ MaxTicks     >= 1
ASSUME MaxRestarts  \in Nat

Actors == Nodes \X (0..MaxRestarts)

VARIABLES
    reqs,        \* reqs[n][a] : n's view of actor a's request count
    exps,        \* exps[n][a] : n's view of actor a's expiry count
    changed,     \* changed[n][a] : tick at which n's view of a last changed
    opLog,       \* opLog[n] : admission ticks of n's requests not yet aged out
    present,     \* present[n] : n holds the bucket
    inc,         \* inc[n] : n's current incarnation
    dropHigh,    \* dropHigh[n] : highest own value in a bucket this process dropped
    tomb,        \* tomb[n][a] : <<requests, tick>> of a compacted slot, or <<0, 0>>
    tick,
    totalOps,
    restarts,
    hw,          \* ghost: hw[a] = highest value actor a ever wrote
    admits,      \* ghost: admits[a] = {<<value, tick>>} for each of a's requests
    lastWrite,   \* ghost: lastWrite[a] = tick of actor a's last write
    seen         \* ghost: seen[n][a] = highest reqs[n][a] since n's process started

vars == <<reqs, exps, changed, opLog, present, inc, dropHigh, tomb, tick, totalOps,
          restarts, hw, admits, lastWrite, seen>>

\* ---------------------------------------------------------------------------
\* Helpers
\* ---------------------------------------------------------------------------

Max(x, y) == IF x >= y THEN x ELSE y
EWMax(f, g) == [a \in DOMAIN f |-> Max(f[a], g[a])]
ZeroRow == [a \in Actors |-> 0]
NoTombs == [a \in Actors |-> <<0, 0>>]

RECURSIVE SumOver(_, _)
SumOver(f, S) ==
    IF S = {} THEN 0
    ELSE LET a == CHOOSE x \in S : TRUE IN f[a] + SumOver(f, S \ {a})

Self(n) == <<n, inc[n]>>
KnownLive(n) == SumOver([a \in Actors |-> reqs[n][a] - exps[n][a]], Actors)
Tokens(n) == MaxTokens - KnownLive(n)
OwnLive(n) == reqs[n][Self(n)] - exps[n][Self(n)]
OwnHigh(n) == Max(reqs[n][Self(n)], exps[n][Self(n)])
Idle(r, e) == \A a \in Actors : r[a] = e[a]

\* When m's view of actor b last changed, as gossiped (quiet_ms in the code).
\* Its own slot last changed with its newest tracked request.
SentChanged(m, b) ==
    IF b # Self(m) THEN changed[m][b]
    ELSE IF opLog[m] = <<>> THEN 0 ELSE opLog[m][Len(opLog[m])]

IsExpired(t) == t + AgeingWindow < tick

RECURSIVE FilterActive(_)
FilterActive(sq) ==
    IF sq = <<>> THEN <<>>
    ELSE IF IsExpired(Head(sq)) THEN FilterActive(Tail(sq))
         ELSE <<Head(sq)>> \o FilterActive(Tail(sq))

\* ---------------------------------------------------------------------------
\* Init
\* ---------------------------------------------------------------------------
Init ==
    /\ reqs     = [n \in Nodes |-> ZeroRow]
    /\ exps     = [n \in Nodes |-> ZeroRow]
    /\ changed  = [n \in Nodes |-> ZeroRow]
    /\ opLog    = [n \in Nodes |-> <<>>]
    /\ present  = [n \in Nodes |-> FALSE]
    /\ inc      = [n \in Nodes |-> 0]
    /\ dropHigh = [n \in Nodes |-> 0]
    /\ tomb     = [n \in Nodes |-> NoTombs]
    /\ tick     = 0
    /\ totalOps = 0
    /\ restarts = 0
    /\ hw       = [a \in Actors |-> 0]
    /\ admits   = [a \in Actors |-> {}]
    /\ lastWrite = [a \in Actors |-> 0]
    /\ seen      = [n \in Nodes |-> ZeroRow]

\* ---------------------------------------------------------------------------
\* ConsumeAt(n): admit a request. With nothing live of its own, n first moves
\* its slot above anything it held before (requests = expiries = base).
\* ---------------------------------------------------------------------------
ConsumeAt(n) ==
    LET a    == Self(n)
        base == Max(dropHigh[n] + 1, OwnHigh(n) + 1)
        r0   == IF opLog[n] = <<>> THEN base ELSE reqs[n][a]
        e0   == IF opLog[n] = <<>> THEN base ELSE exps[n][a]
    IN
    /\ Tokens(n) >= 1
    /\ totalOps < MaxOps
    /\ reqs'     = [reqs EXCEPT ![n][a] = r0 + 1]
    /\ exps'     = [exps EXCEPT ![n][a] = e0]
    /\ opLog'    = [opLog EXCEPT ![n] = Append(@, tick)]
    /\ present'  = [present EXCEPT ![n] = TRUE]
    /\ totalOps' = totalOps + 1
    /\ hw'       = [hw EXCEPT ![a] = Max(@, r0 + 1)]
    /\ admits'   = [admits EXCEPT ![a] = @ \cup {<<r0 + 1, tick>>}]
    /\ lastWrite' = [lastWrite EXCEPT ![a] = tick]
    /\ UNCHANGED <<changed, inc, dropHigh, tomb, tick, restarts>>

\* ---------------------------------------------------------------------------
\* ExpireAt(n): age out n's own requests. Absolute: expiries become requests
\* minus those still tracked, so a peer's earlier retirement isn't counted twice.
\* ---------------------------------------------------------------------------
ExpireAt(n) ==
    LET a      == Self(n)
        kept   == FilterActive(opLog[n])
        target == reqs[n][a] - Len(kept)
    IN
    /\ kept # opLog[n]
    /\ opLog' = [opLog EXCEPT ![n] = kept]
    /\ exps'  = [exps EXCEPT ![n][a] = Max(@, target)]
    /\ hw'    = [hw EXCEPT ![a] = Max(@, Max(exps[n][a], target))]
    /\ lastWrite' = [lastWrite EXCEPT ![a] = tick]
    /\ UNCHANGED <<reqs, changed, present, inc, dropHigh, tomb, tick, totalOps, restarts, admits>>

\* ---------------------------------------------------------------------------
\* RetireAt(n, a): expire another actor's slot that has not changed here for
\* longer than the window.
\* ---------------------------------------------------------------------------
RetireAt(n, a) ==
    /\ present[n]
    /\ a # Self(n)
    /\ reqs[n][a] > exps[n][a]
    /\ changed[n][a] + AgeingWindow < tick
    /\ exps' = [exps EXCEPT ![n][a] = reqs[n][a]]
    /\ UNCHANGED <<reqs, changed, opLog, present, inc, dropHigh, tomb, tick, totalOps,
                   restarts, hw, admits, lastWrite>>

\* ---------------------------------------------------------------------------
\* CompactAt(n, a): forget the idle slot of an incarnation older than its
\* node's current one, keeping a tombstone so merges don't bring it back.
\* ---------------------------------------------------------------------------
CompactAt(n, a) ==
    /\ present[n]
    /\ a[2] < inc[a[1]]
    /\ reqs[n][a] > 0
    /\ reqs[n][a] = exps[n][a]
    /\ reqs' = [reqs EXCEPT ![n][a] = 0]
    /\ exps' = [exps EXCEPT ![n][a] = 0]
    /\ tomb' = [tomb EXCEPT ![n][a] = <<reqs[n][a], tick>>]
    /\ UNCHANGED <<changed, opLog, present, inc, dropHigh, tick, totalOps,
                   restarts, hw, admits, lastWrite>>

\* ExpireTombAt(n, a): drop a tombstone a window after the compaction.
ExpireTombAt(n, a) ==
    /\ tomb[n][a] # <<0, 0>>
    /\ tomb[n][a][2] + AgeingWindow < tick
    /\ tomb' = [tomb EXCEPT ![n][a] = <<0, 0>>]
    /\ UNCHANGED <<reqs, exps, changed, opLog, present, inc, dropHigh, tick, totalOps,
                   restarts, hw, admits, lastWrite>>

\* ---------------------------------------------------------------------------
\* GossipMerge(m, n): merge m's state into n, or adopt it if n doesn't hold
\* the bucket (ignoring idle state). n dates other actors' changed slots by the
\* sender's last change, and expires any own requests it no longer tracks.
\* ---------------------------------------------------------------------------
GossipMerge(m, n) ==
    LET a  == Self(n)
        Kept(b) == reqs[m][b] > tomb[n][b][1]
        rm == [b \in Actors |-> IF Kept(b) THEN reqs[m][b] ELSE 0]
        em == [b \in Actors |-> IF Kept(b) THEN exps[m][b] ELSE 0]
        r  == IF present[n] THEN EWMax(reqs[n], rm) ELSE rm
        e1 == IF present[n] THEN EWMax(exps[n], em) ELSE em
        e  == [e1 EXCEPT ![a] = Max(@, r[a] - Len(opLog[n]))]
    IN
    /\ m # n
    /\ present[m]
    /\ IF ~present[n] /\ Idle(rm, em)
       THEN UNCHANGED <<reqs, exps, changed, present, hw>>
       ELSE /\ reqs'    = [reqs EXCEPT ![n] = r]
            /\ exps'    = [exps EXCEPT ![n] = e]
            /\ changed' = [changed EXCEPT ![n] =
                             [b \in Actors |->
                                IF ~present[n] THEN SentChanged(m, b)
                                ELSE IF r[b] # reqs[n][b] \/ e[b] # exps[n][b]
                                THEN Max(changed[n][b], SentChanged(m, b))
                                ELSE changed[n][b]]]
            /\ present' = [present EXCEPT ![n] = TRUE]
            /\ hw'      = [hw EXCEPT ![a] = Max(@, e[a])]
    /\ UNCHANGED <<opLog, inc, dropHigh, tomb, tick, totalOps, restarts, admits, lastWrite>>

\* ---------------------------------------------------------------------------
\* DropAt(n): forget an idle bucket, remembering the own slot's high value.
\* ---------------------------------------------------------------------------
DropAt(n) ==
    /\ present[n]
    /\ opLog[n] = <<>>
    /\ Idle(reqs[n], exps[n])
    /\ tomb[n] = NoTombs
    /\ dropHigh' = [dropHigh EXCEPT ![n] = Max(@, OwnHigh(n))]
    /\ reqs'     = [reqs EXCEPT ![n] = ZeroRow]
    /\ exps'     = [exps EXCEPT ![n] = ZeroRow]
    /\ present'  = [present EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<changed, opLog, inc, tomb, tick, totalOps, restarts, hw, admits, lastWrite>>

\* ---------------------------------------------------------------------------
\* RestartAt(n): lose all state; the new process is a new actor.
\* ---------------------------------------------------------------------------
RestartAt(n) ==
    /\ restarts < MaxRestarts
    /\ inc[n] < MaxRestarts
    /\ inc'      = [inc EXCEPT ![n] = @ + 1]
    /\ reqs'     = [reqs EXCEPT ![n] = ZeroRow]
    /\ exps'     = [exps EXCEPT ![n] = ZeroRow]
    /\ changed'  = [changed EXCEPT ![n] = ZeroRow]
    /\ opLog'    = [opLog EXCEPT ![n] = <<>>]
    /\ present'  = [present EXCEPT ![n] = FALSE]
    /\ dropHigh' = [dropHigh EXCEPT ![n] = 0]
    /\ tomb'     = [tomb EXCEPT ![n] = NoTombs]
    /\ restarts' = restarts + 1
    /\ UNCHANGED <<tick, totalOps, hw, admits, lastWrite>>

Tick ==
    /\ tick < MaxTicks
    /\ tick' = tick + 1
    /\ UNCHANGED <<reqs, exps, changed, opLog, present, inc, dropHigh, tomb, totalOps,
                   restarts, hw, admits, lastWrite>>

Actions ==
    \/ \E n \in Nodes : ConsumeAt(n) \/ ExpireAt(n) \/ DropAt(n) \/ RestartAt(n)
    \/ \E n \in Nodes, a \in Actors : RetireAt(n, a) \/ CompactAt(n, a) \/ ExpireTombAt(n, a)
    \/ \E m, n \in Nodes : GossipMerge(m, n)
    \/ Tick

Next ==
    /\ Actions
    /\ seen' = [n \in Nodes |-> IF inc'[n] # inc[n] THEN ZeroRow ELSE EWMax(seen[n], reqs'[n])]

\* Safety only: no fairness, so TLC explores every reachable state.
Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ reqs     \in [Nodes -> [Actors -> Nat]]
    /\ exps     \in [Nodes -> [Actors -> Nat]]
    /\ changed  \in [Nodes -> [Actors -> Nat]]
    /\ opLog    \in [Nodes -> Seq(Nat)]
    /\ present  \in [Nodes -> BOOLEAN]
    /\ inc      \in [Nodes -> 0..MaxRestarts]
    /\ dropHigh \in [Nodes -> Nat]
    /\ tomb     \in [Nodes -> [Actors -> Nat \X Nat]]

\* No release before ageing: a node never counts one of an actor's requests
\* as expired (value <= its expiry count) before that request has aged out.
NoPrematureRelease ==
    \A n \in Nodes, a \in Actors :
        \A va \in admits[a] : va[1] <= exps[n][a] => IsExpired(va[2])

\* No forgetting: a node counts every request it has seen until it ages out.
SeenRequestsCounted ==
    \A n \in Nodes, a \in Actors :
        \A va \in admits[a] :
            (va[1] <= seen[n][a] /\ ~IsExpired(va[2])) => va[1] <= reqs[n][a]

\* No node has seen more of an actor's slot than the actor ever wrote.
WriterHighWater ==
    \A n \in Nodes, a \in Actors : reqs[n][a] <= hw[a] /\ exps[n][a] <= hw[a]

\* While a node has live requests, no node has seen more of its requests than
\* it holds: max-merge would hide them.
WriterNotBehind ==
    \A n \in Nodes : OwnLive(n) > 0 =>
        \A m \in Nodes : reqs[m][Self(n)] <= reqs[n][Self(n)]

\* A node's slot counts as live only requests it still tracks (and will age).
OwnLiveTracked ==
    \A n \in Nodes : OwnLive(n) <= Len(opLog[n])

\* Retirement can't be postponed: a replica's countdown for another actor
\* starts no later than that actor's last write, so it retires within a window.
RetirementDeadline ==
    \A n \in Nodes, a \in Actors :
        (present[n] /\ a # Self(n) /\ reqs[n][a] > exps[n][a]) => changed[n][a] <= lastWrite[a]

ExpiriesWithinRequests ==
    \A n \in Nodes, a \in Actors : exps[n][a] <= reqs[n][a]

TokensWithinCapacity ==
    \A n \in Nodes : Tokens(n) <= MaxTokens

NodeLiveWithinCapacity ==
    \A n \in Nodes : OwnLive(n) <= MaxTokens

ClusterLiveWithinBound ==
    SumOver([n \in Nodes |-> OwnLive(n)], Nodes) <= Cardinality(Nodes) * MaxTokens

\* When no merge would change anything, nodes holding the bucket agree, and
\* every node sees the same number of tokens.
AllNodesAgree ==
    /\ \A m, n \in Nodes :
           (present[m] /\ present[n]) => (reqs[m] = reqs[n] /\ exps[m] = exps[n])
    /\ \A m, n \in Nodes : Tokens(m) = Tokens(n)

GossipWouldChange(m, n) ==
    /\ m # n
    /\ present[m]
    /\ IF present[n]
       THEN reqs[n] # EWMax(reqs[n], reqs[m]) \/ exps[n] # EWMax(exps[n], exps[m])
       ELSE ~Idle(reqs[m], exps[m])

ConvergenceQuiescent ==
    (\A m, n \in Nodes : ~GossipWouldChange(m, n)) => AllNodesAgree

\* Nodes are interchangeable, so a config may declare SYMMETRY NodePerms.
NodePerms == Permutations(Nodes)

=============================================================================
