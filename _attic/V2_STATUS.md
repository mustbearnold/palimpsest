# Palimpsest v2 development status

Date: 2026-08-03

Status: defensible self-hosted development milestone; not an official release and not a production-readiness claim.

Palimpsest is v2-worthy for a developer running an agent against one self-hosted PostgreSQL service. The core promise is usable end to end: an agent can write attributable evidence, maintain bitemporal facts, retrieve an authorized current or historical view, resume a crashed thread, export a canonical history, and request scoped deletion without the client having to reimplement the governance rules.

## Working now

- PostgreSQL plus pgvector is the canonical store. Episodes are immutable; facts are attributable revision chains with valid-time and recorded-time views, supersession, provenance, retention, sensitivity, and policy data.
- Retrieval applies tenant, subject, lifecycle, retention, sensitivity, and temporal authorization before lexical or exact-vector candidate generation. Durable receipts preserve policy, ranking, provenance, and redacted decision evidence.
- Checkpoints are immutable full snapshots with optimistic concurrency, idempotent retries, prepared/completed effect receipts, and crash-recovery conformance over real PostgreSQL and HTTP.
- Export is an authorized durable operation that produces a deterministic, integrity-checked canonical-history package. Ready status uses a `303` redirect and scoped content lease semantics.
- Deletion is an authorized, idempotent subject-fence workflow. It purges the configured live targets, verifies absence, retains only the documented content-free tombstone, and has restore-fence replay evidence for a database copy.
- `scripts/dev-up.sh` provides a one-command local service with liveness and migration-aware readiness probes. The local Codex MCP adapter and the dependency-free Python client use the same HTTP authorization boundary.
- The Python client exposes `remember`, `recall`, `correct`, `forget`, bounded deletion polling, checkpoint read/save, export status, and binary export download. It reports partial episode/fact promotion instead of pretending that a two-request convenience helper is atomic.
- The dependency-free TypeScript client exposes the same governed HTTP seam, and both clients can return isolated recall bundles for multiple project namespaces.
- The opt-in ingestion bridge follows text-only Codex, Claude Code, and Hermes session data with resumable cursors, common credential redaction, stable project identities, and explicit `--discover` support for the conventional current-user stores. A separately installed Linux systemd user service can supervise that watcher continuously. It does not ingest tools, private thinking, or hidden system prompts.
- An operator can run a guarded PostgreSQL custom-format logical backup rehearsal into a separate empty database; the rehearsal compares only content-free schema, extension, migration, and row-count probes.

## Evidence bar

The repository gates include Rust formatting, clippy with warnings denied, the complete workspace suite against PostgreSQL 18.4 and pgvector 0.8.5, public HTTP conformance, retrieval evaluations, restore-fence replay, repository contract checks, MCP tests, and Python client tests. Every v2 commit must be green locally, pushed directly to `main`, checked against the exact remote SHA, and confirmed by push-triggered CI.

## Deliberately not claimed

These are real follow-on engineering areas, not hidden failures in the v2 milestone:

- external identity, credential rotation, and a production authorization control plane;
- Valkey/Redis cache and S3-compatible artifact adapters, including their deletion and recovery contracts;
- a backup/PITR provider, backup-disposition verification, or a full production-shaped PITR restore rehearsal. The checked-in logical dump rehearsal is narrower and does not change that boundary;
- million-revision performance, cost, capacity, availability, or SLA evidence; exact PostgreSQL retrieval remains the correctness oracle and is not an ANN scale claim;
- production release/security gates, independent high-risk review, and a first production deployment;
- public procedure/artifact APIs, embedded clients, automatic model-driven consolidation, native/provider ingestion hooks, multi-region writes, and a hosted control plane.

Those boundaries keep “v2” honest: it means a coherent, trustworthy, self-hostable development product, not that every future adapter or production operation has already been built.

## Next frontier

The next material milestone is operational portability: add production-shaped cache/object/backup adapters with deterministic failure and recovery evidence, then publish measured scale and release reports. Procedure memory, embedded mode, and broader SDKs should follow only when they advance the governed case-memory contract rather than widen it cosmetically.
