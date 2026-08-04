# Quickstart

Operational how-to for running, configuring, and using Palimpsest locally.

## Prerequisites

The pinned Rust toolchain (`rust-toolchain.toml`) is required. Docker Compose
2.20.0+ may provide the pinned PostgreSQL 18.4 plus pgvector 0.8.5 image;
otherwise the launcher uses a user-owned local PostgreSQL cluster on port
55432 (PostgreSQL 18.4 plus pgvector 0.8.5 must already be installed). It
never touches the system PostgreSQL service or another local cluster.

## Run locally

```bash
bash scripts/dev-up.sh
```

The launcher applies checked-in migrations before starting the service. The
server process itself never changes the schema on startup. Stop the service
with `Ctrl+C`; stop the dependency without deleting its volume:

```bash
docker compose stop postgres                                   # Docker profile
pg_ctl --pgdata="$HOME/.local/state/palimpsest/postgres" stop  # local cluster
```

Startup detects an incompatible legacy local volume and exits without
deleting it. Preserve or back up needed local data before explicitly
recreating a volume.

## Migrations

Inspect or apply migrations explicitly with the same binary. `status` and
`plan` are read-only; `apply` takes the Palimpsest PostgreSQL advisory lock,
runs only forward, checked-in SQLx migrations, and reports pending, failed,
unknown, and checksum-mismatched versions as content-free JSON. Use a
privileged migration identity for `apply` and keep the runtime identity for
the HTTP process. When those identities differ, grant the runtime identity the
scope-filtered `SELECT` privileges required by the checked-in derived tables,
including `memory.fact_revision_current_coverage`.

```bash
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:***@db/palimpsest' \
  cargo run --locked -- migrate status
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:***@db/palimpsest' \
  cargo run --locked -- migrate plan
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:***@db/palimpsest' \
  cargo run --locked -- migrate apply
```

## Doctor

Read-only operator diagnostic; never starts HTTP or applies migrations; prints
content-free JSON and exits nonzero when a prerequisite is not ready. Checks
PostgreSQL 18+, pgvector 0.8.5, the complete migration set, required lifecycle
tables, and a non-superuser role that does not bypass row-level security.
Never prints the database URL.

```bash
PALIMPSEST_DATABASE_URL='postgresql://runtime-user:***@db/palimpsest' \
  cargo run --locked -- doctor
```

## Service endpoints and configuration

The service listens on `http://127.0.0.1:8080`. `GET /healthz` is a
content-free liveness probe; `GET /readyz` checks database connectivity plus
the exact successful SQLx migration set shipped by this binary; both require
no authentication and disclose no memory data. `GET /metrics` is also
unauthenticated and emits fixed Prometheus text with only build/schema
identity and content-lease cleanup counters; it performs no database query and
carries no scope labels.

The HTTP service uses a synthetic, non-superuser PostgreSQL role so forced
row-level security remains active. The local launcher allows the `internal`
sensitivity by default. Override the `PALIMPSEST_*` environment variables when
needed. Set `PALIMPSEST_EXPORT_ROOT` to a durable private filesystem path when
enabling canonical-history exports; the development default is
`var/palimpsest/exports`.

`PALIMPSEST_OPERATION_GRANTS` is empty by default. Trusted deployments may set
the comma-separated closed vocabulary `canonical_history_export` and/or
`subject_delete`; unknown grants fail startup. The grants do not add public
export or deletion endpoints by themselves.

## S3-compatible export store

For a self-hosted object-store deployment, set all of
`PALIMPSEST_EXPORT_S3_ENDPOINT`, `PALIMPSEST_EXPORT_S3_BUCKET`,
`PALIMPSEST_EXPORT_S3_REGION`, `PALIMPSEST_EXPORT_S3_ACCESS_KEY_ID`, and
`PALIMPSEST_EXPORT_S3_SECRET_ACCESS_KEY`. `PALIMPSEST_EXPORT_S3_PREFIX` and
`PALIMPSEST_EXPORT_S3_SESSION_TOKEN` are optional. A partial configuration
fails startup instead of silently falling back to local files. See
`docs/decisions/0024-s3-compatible-export-package-store.md`.

## Restore automation

Restore automation must set `PALIMPSEST_RESTORE_MODE=1`,
`PALIMPSEST_RESTORE_DATABASE_URL`, `PALIMPSEST_RESTORE_FENCE_LEDGER_PATH`, and
`PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256`. The restore database URL must
identify a separately privileged database authority with the current schema;
normal serving credentials are not enough. The process verifies the
content-free ledger, replays every matching scope's canonical and derived-data
purge, records an idempotent content-free receipt, checks the returned counts
and ledger digest internally, and exits without binding HTTP. Missing, stale,
corrupt, or unmatched ledger evidence fails closed. See spec 005; a
backup/PITR adapter is not yet provided.

Operators can preflight the independent ledger without database access, then
run the explicit replay command:

```bash
PALIMPSEST_RESTORE_FENCE_LEDGER_PATH=/secure/fences/current.json \
PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256='...' \
  cargo run --locked -- restore verify

PALIMPSEST_RESTORE_DATABASE_URL='postgresql://restore-authority:***@db/palimpsest' \
PALIMPSEST_RESTORE_FENCE_LEDGER_PATH=/secure/fences/current.json \
PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256='...' \
  cargo run --locked -- restore apply
```

`restore verify` reports only the profile, schema, entry count, generation
time, and ledger digest. `restore apply` is the mutating, privileged replay
operation and never starts HTTP.

## Logical backup rehearsal

Guarded rehearsal for an operator with an isolated empty restore database.
Uses PostgreSQL's custom dump format, verifies the archive, restores it, and
compares only content-free schema/extension/row-count probes. Refuses to
restore over a database that already has the `memory` schema and does not
print either connection URL. This is logical dump/restore evidence, not a
base-backup, WAL-archive, PITR, expiry, or production RPO/RTO claim.

```bash
export PALIMPSEST_BACKUP_SOURCE_URL='postgresql://.../palimpsest'
export PALIMPSEST_BACKUP_RESTORE_URL='postgresql://.../palimpsest_restore'
bash scripts/palimpsest-logical-backup-rehearsal.sh
```

## Scale probe

Rollback-only measurement of the authorized lexical retrieval core on a
synthetic scope without retaining any rows. Prints only counts, latency
percentiles, a plan digest, and a bounded plan summary (node timings, row
counts, cache/temp block counts, relation names); never prints synthetic
values or raw SQL. The local 100,000-revision baseline and coverage-gated
profiles are recorded in the attic scale evaluation; the coverage-gated
profile (p95 1.747 s) is materially faster but still misses the proposed
million-revision release latency target, so no SLA claim is made.

```bash
PALIMPSEST_SCALE_DATABASE_URL='postgresql://runtime-user:***@db/palimpsest' \
PALIMPSEST_SCALE_REVISIONS=100000 \
PALIMPSEST_SCALE_QUERIES=20 \
  bash scripts/palimpsest-scale-probe.sh
```

## Use Palimpsest from Codex

Once the service is running, register its local MCP adapter once:

```bash
codex mcp add palimpsest \
  --env PALIMPSEST_MCP_BASE_URL=http://127.0.0.1:8080 \
  --env PALIMPSEST_BEARER_TOKEN=palimpsest-local-development-token \
  --env PALIMPSEST_TENANT_ID=019be000-0000-7000-8000-000000000010 \
  --env PALIMPSEST_SUBJECT_ID=019be000-0000-7000-8000-000000000020 \
  --env PALIMPSEST_CASE_ID=019be000-0000-7000-8000-000000000030 \
  -- python3 "$(pwd)/scripts/palimpsest_mcp.py"
```

Codex then has `palimpsest_retrieve`, `palimpsest_recall_by_project`,
`palimpsest_compare_by_project`, `palimpsest_validate_project_review`,
`palimpsest_consolidate_project_review`, and `palimpsest_remember`. The
comparison tool does not infer semantic conflicts; consolidation requires
caller-supplied values, temporal fields, and a registered write policy. The
adapter uses the HTTP API, keeps the configured tenant and subject scope, and
never exposes delete or export operations. Verify registration with
`codex mcp list`.

## Use Palimpsest from Python

```bash
python3 -m pip install ./clients/python
```

`PalimpsestClient` provides `remember`, `recall`, `correct`, and `forget`,
plus the lower-level episode, fact, temporal as-of, checkpoint,
retrieval-page, export, and deletion-status methods. It uses the same
authorized HTTP boundary as MCP and never connects directly to PostgreSQL.
See `clients/python/README.md`.

## Use Palimpsest from TypeScript or JavaScript

```bash
npm install ./clients/typescript
```

A dependency-free ESM client with TypeScript declarations exposing the same
governed helpers. See `clients/typescript/README.md`.

## Ingest coding-agent sessions

Palimpsest includes an opt-in project-aware session ingestion path for Codex,
Claude Code, and Hermes (`clients/python/README.md#ingest-coding-agent-sessions`).
It can poll explicitly selected local source paths or the exact conventional
locations for the current user with `--discover`; it redacts common
credential-shaped values, excludes tools and private thinking, and writes
through the authorized HTTP API. Each repository gets a stable retrieval
namespace. See `docs/decisions/0019-project-aware-agent-session-ingestion.md`
and `docs/decisions/0021-local-agent-source-discovery.md`.

On Linux, `bash scripts/install-palimpsest-ingest-service.sh` installs the
discovery watcher as an owner-only systemd user service for continuous local
ingestion; see `docs/decisions/0022-supervised-local-ingestion-service.md`.

## Validation

```bash
bash scripts/check-repo.sh
```

## License

Apache-2.0.
