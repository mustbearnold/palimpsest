# Palimpsest

Temporal memory infrastructure for AI agents.

Palimpsest keeps recent information useful without erasing the past. It stores
resumable thread state, timestamped episodes, versioned facts, procedures, and
artifact references with provenance and authorization boundaries. Retrieval is
hybrid and time-aware; embeddings are rebuildable indexes, never the source of
truth. The repository is run under strict Spec-Driven Development: every
capability has a living spec, and code exists to satisfy it.

## Status

Active development on the sole `main` branch. Self-hosted, PostgreSQL-backed,
with temporal memory, hybrid retrieval, crash-safe checkpoints,
canonical-history exports, fenced subject deletion, fail-closed restore
replay, a local Codex MCP adapter, and dependency-free Python and TypeScript
clients. Not an official or production release; see the specs for exactly what
is claimed and the backlog for what is not.

## Start here

- [Constitution](specs/constitution.md) — project principles, the SDD loop,
  authority model, and domain vocabulary (read first)
- [Conventions](specs/conventions.md) — formatting and style law
- [Capability specs](specs/) — `001` memory service through `013` hermes
  memory plugin
- [Backlog](specs/BACKLOG.md) — known-but-unspecced gaps
- [Quickstart](docs/runbooks/quickstart.md) — run it locally
- [Architecture](docs/architecture.md) — system shape
- [Decisions](docs/decisions/) — architecture decision records
- [Contributing](docs/runbooks/contributing.md) · [Security policy](docs/runbooks/security.md)

## License

Apache-2.0.
