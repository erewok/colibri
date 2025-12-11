# Plan: Signal-Based Topology Management for Stateless Colibri

Build topology change notification mechanism using Unix signals and configuration file injection, enabling dynamic cluster scaling without service awareness of Kubernetes deployment.

## Pre-Steps

This plan requires first implementing the cluster-toplogy-change plan: ./cluster-changes-plan.md

## Steps

1. **Add configuration file support with signal-based reloading** - Extend [`src/settings.rs`](src/settings.rs) to load YAML/TOML config files and modify [`src/main.rs`](src/main.rs) to handle `SIGHUP` for graceful config reload without restart

2. **Implement topology change handlers** - Create signal handlers in [`src/main.rs`](src/main.rs) that rebuild [`NodeWrapper`](src/node/mod.rs) when topology changes, triggering hashring redistribution or gossip membership updates

3. **Design external Kubernetes configuration injection** - Build `k8s/` manifests with ConfigMaps mounted as files, using init-containers or sidecars to generate config and send `SIGHUP` when cluster topology changes

4. **Create graceful node state transition** - Add methods to [`NodeWrapper`](src/node/mod.rs) for clean shutdown/restart of transport layers in [`src/node/gossip/controller.rs`](src/node/gossip/controller.rs) and [`src/node/hashring/hashring_node.rs`](src/node/hashring/hashring_node.rs) while preserving in-memory rate limit data

5. **Build Helm chart with scaling automation** - Package manifests in `charts/colibri/` with hooks that update ConfigMaps and signal pods during scale operations, leveraging standard Kubernetes patterns

6. **Add topology validation and rollback** - Implement config validation in [`src/settings.rs`](src/settings.rs) with ability to reject invalid topology changes and revert to previous configuration on failure

## Further Considerations

1. **Signal handling approach** - Use `SIGHUP` for config reload or custom signal like `SIGUSR1`? Option A: Standard SIGHUP convention / Option B: SIGUSR1 for topology changes only / Option C: HTTP endpoint for config reload

2. **In-memory state preservation** - During topology changes, preserve all rate limit buckets or selective preservation? Option A: Full state preservation / Option B: Gradual bucket migration / Option C: Accept temporary state loss for simplicity

3. **Configuration change detection** - Monitor file changes via inotify or polling? Option A: Filesystem notifications (inotify/kqueue) / Option B: Periodic file polling / Option C: External signal-only approach

4. **Hashring replication strategy** - Implement virtual nodes and replica placement for fault tolerance before production deployment? Option A: Simple 2x replication / Option B: Configurable replica count / Option C: Defer until after basic deployment

5. **Configuration precedence strategy** - Should config files override CLI args or vice versa? Option A: CLI > Env > Config file / Option B: Config file > CLI > Env / Option C: Explicit precedence flags
