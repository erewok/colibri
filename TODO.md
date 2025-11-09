# Colibri Rate Limiter - Implementation Plan & TODOs

- [x] Add gossip-based system for propagating cluster state
- [x] Add tests
- [ ] Add docs
- [ ] Consistent hashing should offer replication
- [ ] Make it possible to setup a rate-limiter key that diverges from settings (see [CONFIGURABLE_RATE_LIMITS_DESIGN.md](./CONFIGURABLE_RATE_LIMITS_DESIGN.md))
- [ ] Add deployment infra: config-file and kubernetes resource definitions
- [ ] Snapshot node state to persistent storage and reload on crash