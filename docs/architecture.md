# Architecture

## System shape

Palimpsest is a self-hostable temporal memory service for AI agents. Canonical
memory lives in PostgreSQL 18 plus pgvector 0.8.5; the versioned HTTP API
(`api/openapi.yaml`) is the public seam. Optional S3-compatible object storage
holds large immutable artifacts; Valkey/Redis is a future optional hot cache
only.

```
agents (Codex/Claude Code/Hermes)
   │  MCP adapter (scripts/palimpsest_mcp.py)   thin clients (Python/TS)
   ▼                                                    │
Palimpsest HTTP service (crates/palimpsest-server)      │
   │  migrate · doctor · restore · serve                │
   ▼                                                    ▼
crates/palimpsest-http ─► crates/palimpsest-application ─► crates/palimpsest-domain
        │                                                     ▲
        └────────────► crates/palimpsest-postgres ────────────┘
                              │
                 PostgreSQL 18 + pgvector (memory schema, migrations 1–20)
                              │
              optional S3-compatible export/artifact store
```

## Crate responsibilities

- `palimpsest-domain` — deterministic domain rules: tenants, subjects,
  principals, cases, threads; bitemporal intervals; revision chains;
  authorization ordering; retrieval-policy vocabulary.
- `palimpsest-postgres` — persistence: episodes, fact/procedure revision
  chains, checkpoints, provenance, retrieval receipts and manifests, derived
  projections (current fact-revision projection + coverage marker), export and
  deletion operations, subject lifecycle fences and content leases, outbox and
  audit receipts; checked-in SQLx migrations; the conformance-critical SQL.
- `palimpsest-http` — the versioned HTTP surface: content-free probes,
  metrics, and the governed operation routes.
- `palimpsest-application` — wiring and orchestration between HTTP, domain,
  and persistence.
- `palimpsest-server` — the binary: `serve`, `migrate status|plan|apply`,
  `doctor`, `restore verify|apply`; operation-grant validation.

## Storage model

- Canonical records: `memory.episodes` (immutable), `memory.facts` +
  `memory.fact_revisions` (bitemporal revision chains), procedures likewise.
- Derived data: `memory.fact_revision_current` (scope-protected current
  projection) gated by `memory.fact_revision_current_coverage` (durable
  scope-local coverage marker); embedding projections and search documents
  rebuildable from canonical records.
- Retrieval: receipts + manifests make results explainable; authorization and
  temporal filters run before lexical (full-text) and vector (pgvector)
  candidate generation.
- Lifecycle: subject fences are monotonic; deletion purges canonical and
  derived rows and records content-free tombstones; export operations freeze
  immutable manifests; restore replays scoped purge from a verified
  content-free fence ledger.

## Security posture

- Forced row-level security on memory tables; the HTTP runtime uses a
  synthetic non-superuser role (`NOBYPASSRLS`); migrations run under a
  separate privileged identity.
- Logs, metrics, doctor, migrate, and probe output are content-free by design;
  private memory content is never a routine log field.
- Owner-only maintenance functions (e.g. projection rebuild) verify the
  session user against the table owner.

## Deployment profile

- Single-region, self-hosted: Docker Compose (pinned PostgreSQL 18.4 +
  pgvector 0.8.5 image) or a user-owned local cluster on port 55432
  (`scripts/dev-up.sh`); `PALIMPSEST_*` environment variables configure
  runtime, export stores, restore mode, and operation grants.
- Local tooling: ingestion watcher as an optional systemd user service,
  logical-backup rehearsal script, rollback-only scale probe.

## See also

- Capability specs: `specs/001`–`specs/010`
- Decisions: `docs/decisions/`
- Runbooks: `docs/runbooks/`
