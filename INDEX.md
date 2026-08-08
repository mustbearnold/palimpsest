# INDEX — living map of this repository

Founder directive 2026-08-08: an AI agent MUST refresh this file after every
turn of Palimpsest development in this repository.

## Refresh contract

A turn is one unit of agent work. It ends in a commit, a report, or a
handover.

At the end of each turn, the agent that did the work MUST:

1. Update every section below that the turn changed.
2. Set the "Last refresh" line in the status section to the current date,
   time, and agent.
3. Add one row to the refresh log: date, agent, and change.
4. Keep this file short. Link to the source of truth. Do not copy content.

`AGENTS.md` makes this refresh a duty of every agent. `scripts/check-repo.sh`
verifies that this file and `SOUL.md` exist and are not empty.

## Start here

- [SOUL.md](SOUL.md) — the identity of every AI agent in this repository
- [AGENTS.md](AGENTS.md) — the agent entry point and the list of law
- [specs/constitution.md](specs/constitution.md) — the highest authority in
  the repository
- [specs/conventions.md](specs/conventions.md) — formatting and style law
- [README.md](README.md) — product summary and status

## Capability specs (`specs/`)

| Number | Capability | Status |
| --- | --- | --- |
| 001 | Memory service core | active |
| 002 | Authorized retrieval | active |
| 003 | Subject lifecycle and deletion | active |
| 004 | Export operations | active |
| 005 | Restore and recovery | active |
| 006 | Agent session ingestion | active |
| 007 | Project comparison and governed review | active |
| 008 | Local MCP adapter | active |
| 009 | Governed clients | active |
| 010 | Operations and evidence tooling | active |
| 011 | Governed consolidation | active |
| 012 | Proactive surfacing | active |
| 013 | Hermes Agent memory plugin | active |
| 014 | Embedded/single-user mode | active |
| 015 | Optional hot cache (Valkey/Redis) | draft, review round 2 PASS |
| 016 | Provider-managed backup and PITR | active |

Known gaps: [specs/BACKLOG.md](specs/BACKLOG.md).

## Code map

### Rust workspace (`crates/`)

- `palimpsest-domain` — deterministic domain rules: tenants, subjects,
  principals, cases, threads; bitemporal intervals; revision chains;
  authorization order; retrieval-policy vocabulary.
- `palimpsest-postgres` — persistence: episodes, revision chains,
  checkpoints, provenance, retrieval receipts, projections, export and
  deletion operations, fences and leases, outbox and audit; checked-in SQLx
  migrations.
- `palimpsest-http` — the versioned HTTP surface: content-free probes,
  metrics, and the governed operation routes.
- `palimpsest-application` — wiring and orchestration between HTTP, domain,
  and persistence.
- `palimpsest-server` — the binary: `serve`, `migrate status|plan|apply`,
  `doctor`, `restore verify|apply`; operation-grant validation.
- `palimpsest-embedded` — library-first embedded mode (spec 014); optional
  loopback-only HTTP listener.
- `palimpsest-cache` — optional hot cache (spec 015): Valkey and in-process
  implementations of the `HotCache` trait.
- `palimpsest-conformance` — cross-cutting conformance tests.

### Clients (spec 009)

- `clients/python` — dependency-free Python client.
- `clients/typescript` — dependency-free TypeScript client.

### Agent tools (`tools/`)

- `palimpsest_mcp.py` — local Codex MCP adapter (spec 008).
- `palimpsest_ingest.py` — supervised ingestion watcher (spec 006).
- `generate-retrieval-corpus.py`, `generate-q63-exp2.py` — evidence and
  fixture generators (spec 010).

### Integrations

- `integrations/hermes` — Hermes Agent memory plugin (spec 013).

### Public contract and database

- `api/openapi.yaml` — the versioned HTTP contract, v0.1.0.
- `migrations/` — checked-in SQLx migrations `0001` through `0025`.

## Documentation

- [docs/architecture.md](docs/architecture.md) — the shape of the system.
  Read it before any architecture, governance, security, storage, or public
  contract change.
- `docs/decisions/` — 33 architecture decision records (`0001`–`0033`).
- `docs/runbooks/` — 7 runbooks: quickstart, contributing, issue tracker,
  triage labels, release gate, security, skills provenance.

## Operations

- `compose.yaml` — pinned PostgreSQL 18 + pgvector 0.8.5 for local use.
- `scripts/` — dev-up, repository contract check, backups, restore rehearsal,
  ingest service, scale probe.
- `.github/workflows/ci.yml` — Rust, PostgreSQL, and repository gates.
- `.github/workflows/repository-quality.yml` — repository contract and
  TypeScript client contract tests.

## Evidence and history

- `evaluations/` — retrieval corpus and evaluation data (spec 010).
- `_attic/` — archived V2/V3 documents. Not authoritative. Git and the specs
  hold the truth.

## Current status

- Sole branch: `main`. Direct commits per the constitution.
- Product status: see `README.md`. Known gaps: `specs/BACKLOG.md`.
- Work frontier: GitHub issues labelled `ready-for-agent`.
- Last refresh: 2026-08-08 21:35 UTC by the AI CEO (prime agent session).

## Refresh log

| Date (UTC) | Agent | Change |
| --- | --- | --- |
| 2026-08-08 | AI CEO (prime agent) | Created `SOUL.md` and `INDEX.md`. Bound the per-turn refresh in `AGENTS.md` and `scripts/check-repo.sh`. |
