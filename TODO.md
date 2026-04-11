# Colibri Rate Limiter - Implementation Plan & TODOs

- [x] Add gossip-based system for propagating cluster state
- [x] Add tests
- [x] Make it possible to setup a rate-limiter key that diverges from settings
- [x] Consistent hashing should offer replication.
- [ ] Alternatives for config: config files and env vars. Make a config file and use that for both *launching* a node *and* admin commands for all nodes.
- [ ] Add deployment infra: config-file and kubernetes resource definitions
- [ ] Snapshot node state to persistent storage and reload on crash
- [ ] Add docs
