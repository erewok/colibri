---------------------------- MODULE Solo ----------------------------
\* Model-checks to validate (invariantly) that a single node with a local token bucket behaves as expected.
\* Corresponds to: src/limiters/local_bucket.rs, src/node/single_node.rs
\* Abstractions:
\*   ...