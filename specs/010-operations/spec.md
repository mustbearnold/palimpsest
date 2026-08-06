# 010 — Operations and evidence tooling

Status: active
Owner: AI CEO

## Purpose

The operator and developer tooling that makes the service operable by a small
team or an autonomous engineering agent: doctor, migration lifecycle,
content-free metrics, the local development profile, and the rollback-only
scale probe.

## Requirements

- R1. `doctor` MUST be a read-only diagnostic: it MUST verify PostgreSQL 18+,
  pgvector 0.8.5, the complete checked-in migration set, and a non-superuser
  runtime role that does not bypass row-level security, and MUST print
  content-free JSON without echoing the database URL.
- R2. `migrate status` and `migrate plan` MUST be read-only; `migrate apply`
  MUST take the Palimpsest advisory lock, run only forward, checked-in SQLx
  migrations, and report pending, failed, unknown, and checksum-mismatched
  versions as content-free JSON.
- R3. `/metrics` MUST be unauthenticated, cache-free, database-free, and
  content-free: fixed Prometheus text with build/schema identity, content-lease
  cleanup counters, a request-latency histogram (cumulative `le` buckets plus
  sum), the deployed embedding-projection lease policy gauges (recorded at
  startup), and PostgreSQL pool size/idle gauges. Adding a metric family is a
  spec change (this requirement must be amended in the same change); the
  content-free test asserts every family and the absence of tenant, subject,
  memory, or credential text.
- R4. The dev profile MUST use a user-owned local PostgreSQL cluster (or the
  pinned Docker image) with a synthetic non-superuser runtime role so forced
  row-level security remains active; `dev-up.sh` MUST never touch the system
  PostgreSQL service.
- R5. The scale probe MUST be rollback-only, repeatable, and content-free,
  printing only counts, latency percentiles, a plan digest, and a bounded
  EXPLAIN node profile; it MUST NOT print synthetic values or raw SQL.

## Acceptance criteria

- [ ] A1. `doctor_postgres18.rs` passes: ready runtime database, connection
      failure without echoing credentials, missing configuration classified
      as not-ready.
- [ ] A2. `migrate_postgres18.rs` and `metrics.rs` pass; migration status is
      machine-readable without memory content.
- [ ] A3. `test_palimpsest_scale_probe.sh` passes; the probe runs against a
      real local database via `scripts/palimpsest-scale-probe.sh`.
- [ ] A4. `scripts/check-repo.sh` passes: repository contract plus the 41
      pinned official skills, lock, and tree digest.

## Out of scope

- Hosted control planes, external identity/credential rotation, official
  production release gates.

## Open questions

- None.

## Links

Code: `crates/palimpsest-server` (doctor, migrate, metrics) ·
`scripts/dev-up.sh` · `scripts/palimpsest-scale-probe.sh` ·
`scripts/check-repo.sh`
Tests: `doctor_postgres18.rs` · `migrate_postgres18.rs` · `metrics.rs` ·
`scripts/test_palimpsest_scale_probe.sh`
Decisions: 0014, 0015, 0016, 0009
