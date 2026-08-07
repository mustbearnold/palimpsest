# ADR-0033: Embedded-mode storage substrate

Date: 2026-08-08 · Status: accepted

## Context

Spec 014 requires an embedded substrate with the same invariants as the
server (spec 014 R3). Spec 001 excluded SQLite in the first vertical slice.
Issue #40 requires the spec to revisit that decision with evidence.

Two candidates exist. Embedded PostgreSQL is a single-user local cluster
with the same SQLx migrations, the same RLS policies, and the same recovery
semantics. SQLite needs a new storage layer. That layer must reproduce
RLS-equivalent isolation, migration parity, and reproducible derived
indexes.

The constitution binds the choice: canonical memory is durable structured
data (principle 7); derived indexes are reproducible from canonical records
(principle 12); retrieval, isolation, and recovery claims need scenario or
benchmark evidence (principle 14); no framework bias exists (principle 15).
ADR-0015 binds the migration lifecycle to Postgres semantics (advisory
locks, embedded SQLx migrations).

## Decision

Adopt embedded PostgreSQL as the v1 substrate. The embedded runtime manages
a private single-user local cluster. It applies the same migrations and RLS
policies. Recovery semantics stay identical. This gives zero semantic
divergence and maximum code reuse.

SQLite adoption stays conditional. It happens only if a benchmark
demonstrates a named failure of the embedded-Postgres default. The benchmark
must record its digests and reproduce on demand (spec 014 A1, A6). This
mirrors the conditional patterns of spec 002 (vector DB) and spec 008
(packaging).

## Alternatives considered

- **SQLite**: rejected for v1. It needs a new storage layer with RLS parity
  and migration divergence. The named-failure condition does not exist yet.
  Revisit only with benchmark evidence.
- **Local server binary only**: rejected. A library-first surface keeps the
  domain boundary first-class for Rust hosts.
- **HTTP service only (status quo)**: rejected. It fails user story 27
  (offline single-user agent).

## Consequences

- Easier: no new storage engine; the conformance suites run unchanged (spec
  002 A1–A3); migrations stay single-sourced.
- Harder: the embedded runtime must manage the cluster lifecycle (init,
  start, stop, upgrade). No external database service is required. Packaging
  must carry the cluster.
- Locked in: spec 014 (decision D1); the conditional gate binds any future
  SQLite adoption to benchmark evidence.

## Links

Spec: `specs/014-embedded-mode/spec.md`
Issue: #40
