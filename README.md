# Palimpsest

Temporal memory infrastructure for AI agents.

Palimpsest keeps recent information useful without erasing the past. It stores
resumable thread state, timestamped episodes, versioned facts, procedures, and
artifact references with provenance and authorization boundaries. Retrieval is
hybrid and time-aware; embeddings are rebuildable indexes, never the source of
truth.

## Status

Palimpsest is active v3 development on the sole local and remote `main` branch.
It is a self-hosted,
PostgreSQL-backed agent memory service. It includes temporal memory, hybrid
retrieval, crash-safe checkpoints, canonical-history exports, fenced subject
deletion, fail-closed restore replay, a local Codex MCP adapter, and a
dependency-free Python client for the governed lifecycle. It is not an
official or production release. See [V3_STATUS.md](docs/V3_STATUS.md) for the
current evidence and deliberately unclaimed boundaries; [V2_STATUS.md](docs/V2_STATUS.md)
remains the baseline milestone record.

## Product commitments

- PostgreSQL plus pgvector is the durable source of truth.
- Long-term memory is temporal, versioned, attributable, and queryable as-of a
  point in time.
- Recent information can rank higher without deleting older evidence.
- Tenant and subject authorization filters run before semantic retrieval.
- Agent autonomy remains subordinate to the human founder's explicit charter.

## Start here

- [Product specification](docs/PRODUCT_SPEC.md)
- [Domain glossary](CONTEXT.md)
- [AI CEO operating rules](AGENTS.md)
- [Architecture decisions](docs/adr/)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Run locally

The pinned Rust toolchain is required. If Docker Compose 2.20.0 or newer is
available, the launcher uses the pinned PostgreSQL 18.4 plus pgvector 0.8.5
image. Otherwise it uses a user-owned local PostgreSQL cluster on port 55432;
the fallback requires PostgreSQL 18.4 plus pgvector 0.8.5 to already be
installed. It never touches the system PostgreSQL service or another local
cluster.

Start the dependency and the Rust HTTP service with one command:

```bash
bash scripts/dev-up.sh
```

The launcher applies checked-in migrations before starting the service. The
server process itself never changes the schema on startup. Operators can
inspect or apply migrations explicitly with the same binary:

```bash
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:password@db/palimpsest' cargo run --locked -- migrate status
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:password@db/palimpsest' cargo run --locked -- migrate plan
PALIMPSEST_MIGRATION_DATABASE_URL='postgresql://migrator:password@db/palimpsest' cargo run --locked -- migrate apply
```

status and plan are read-only. apply takes the Palimpsest PostgreSQL advisory
lock and runs only forward, checked-in SQLx migrations; it reports pending,
failed, unknown, and checksum-mismatched versions as content-free JSON. Use a
privileged migration identity for apply and keep the runtime identity for the
HTTP process.

The same binary has a read-only operator diagnostic. It never starts HTTP or
applies migrations; it prints content-free JSON and exits nonzero when a
prerequisite is not ready:

```bash
PALIMPSEST_DATABASE_URL='postgresql://runtime-user:password@db/palimpsest' \
  cargo run --locked -- doctor
```

`doctor` checks PostgreSQL 18+, pgvector 0.8.5, the complete migration set,
required lifecycle tables, and that the connected role is a non-superuser that
does not bypass row-level security. It never prints the database URL.

The service listens on `http://127.0.0.1:8080`. Docker PostgreSQL listens only
on `127.0.0.1:5432`; the local fallback uses `127.0.0.1:55432`. `GET /healthz` is a content-free liveness probe and
`GET /readyz` checks database connectivity plus the exact successful SQLx
migration set shipped by this binary. Both probes require no authentication
and disclose no memory data. `GET /metrics` is also unauthenticated and emits
fixed Prometheus text with only build/schema identity and content-lease cleanup
counters; it does not perform a database query or include scope labels. The
HTTP service uses a synthetic, non-superuser
PostgreSQL role so forced row-level security remains active. The local launcher
allows the `internal` sensitivity by default so retrieval has a useful but
explicitly narrow development scope. Override the `PALIMPSEST_*`
environment variables when needed. Set `PALIMPSEST_EXPORT_ROOT` to a durable
private filesystem path when enabling canonical-history exports; the development
default is `var/palimpsest/exports`. Stop the service with `Ctrl+C`, then stop
PostgreSQL without deleting its volume:

For a self-hosted object-store deployment, set all of
`PALIMPSEST_EXPORT_S3_ENDPOINT`, `PALIMPSEST_EXPORT_S3_BUCKET`,
`PALIMPSEST_EXPORT_S3_REGION`, `PALIMPSEST_EXPORT_S3_ACCESS_KEY_ID`, and
`PALIMPSEST_EXPORT_S3_SECRET_ACCESS_KEY`. `PALIMPSEST_EXPORT_S3_PREFIX` and
`PALIMPSEST_EXPORT_S3_SESSION_TOKEN` are optional. The server then uses the
signed, conditional-write S3-compatible export store; a partial configuration
fails startup instead of silently falling back to local files. See
[ADR-0024](docs/adr/0024-s3-compatible-export-package-store.md).

`PALIMPSEST_OPERATION_GRANTS` is empty by default. Trusted deployments may set
the comma-separated closed vocabulary `canonical_history_export` and/or
`subject_delete`; unknown grants fail startup. The grants do not add public
export or deletion endpoints by themselves.

Restore automation must set `PALIMPSEST_RESTORE_MODE=1`,
`PALIMPSEST_RESTORE_DATABASE_URL`, `PALIMPSEST_RESTORE_FENCE_LEDGER_PATH`,
and `PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256`. The restore database URL must
identify a separately privileged database authority with the current schema;
normal serving credentials are not enough. The process verifies the
content-free ledger, replays every matching scope's canonical and derived-data
purge, records an idempotent content-free receipt, checks the returned counts
and ledger digest internally, and exits without binding HTTP. Missing, stale, corrupt, or
unmatched ledger evidence fails closed. This repository still does not provide
a backup/PITR adapter, backup disposition check, or the broad export/deletion
negative HTTP conformance gate for every configured external target.

Operators can preflight the independent ledger without database access, then
run the explicit replay command:

```bash
PALIMPSEST_RESTORE_FENCE_LEDGER_PATH=/secure/fences/current.json \
PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256='...' \
  cargo run --locked -- restore verify

PALIMPSEST_RESTORE_DATABASE_URL='postgresql://restore-authority:password@db/palimpsest' \
PALIMPSEST_RESTORE_FENCE_LEDGER_PATH=/secure/fences/current.json \
PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256='...' \
  cargo run --locked -- restore apply
```

`restore verify` reports only the profile, schema, entry count, generation
time, and ledger digest. `restore apply` is the mutating, privileged replay
operation and never starts HTTP; the environment-driven restore mode remains
available for automation that already uses it.

The repository also includes a guarded logical-backup rehearsal for an
operator with an isolated empty restore database. It uses PostgreSQL's custom
dump format, verifies the archive, restores it, and compares only content-free
schema/extension/row-count probes:

```bash
export PALIMPSEST_BACKUP_SOURCE_URL='postgresql://.../palimpsest'
export PALIMPSEST_BACKUP_RESTORE_URL='postgresql://.../palimpsest_restore'
bash scripts/palimpsest-logical-backup-rehearsal.sh
```

The script refuses to restore over a database that already has the `memory`
schema and does not print either connection URL. This is logical dump/restore
evidence, not a PostgreSQL base-backup, WAL-archive, PITR, expiry, or production
RPO/RTO claim.

The rollback-only scale probe measures the authorized lexical retrieval core on
a synthetic scope without retaining any rows:

```bash
PALIMPSEST_SCALE_DATABASE_URL='postgresql://runtime-user:password@db/palimpsest' \
PALIMPSEST_SCALE_REVISIONS=100000 \
PALIMPSEST_SCALE_QUERIES=20 \
  bash scripts/palimpsest-scale-probe.sh
```

It prints only counts, latency percentiles, and a plan digest. The first local
100,000-revision profile is recorded in
[the scale evaluation](docs/evaluations/2026-08-03-authorized-lexical-scale-probe.md);
it misses the proposed release latency target, so no million-revision or SLA
claim is made.

With Docker, stop PostgreSQL without deleting its volume:

```bash
docker compose stop postgres
```

With the local fallback, stop its user-owned cluster without deleting its
data:

```bash
pg_ctl --pgdata="$HOME/.local/state/palimpsest/postgres" stop
```

### Use Palimpsest from Codex

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

Codex will then have `palimpsest_retrieve` for authorized current-memory
searches, `palimpsest_recall_by_project` for isolated side-by-side project
evidence, `palimpsest_compare_by_project` for deterministic key/value-digest
review candidates plus bounded token-difference hints, and
`palimpsest_remember` for explicitly requested saves.
The comparison tool does not infer semantic conflicts or consolidate memories.
The adapter uses the HTTP API, keeps the configured tenant and subject scope,
and never exposes delete or export operations. Verify registration with
`codex mcp list`.

Startup detects an incompatible legacy local volume and exits without deleting
it. Preserve or back up needed local data before explicitly recreating a volume.

### Use Palimpsest from Python

Install the first-party client from this checkout:

```bash
python3 -m pip install ./clients/python
```

`PalimpsestClient` provides `remember`, `recall`, `correct`, and `forget`, plus
the lower-level episode, fact, temporal as-of, checkpoint, retrieval-page,
export, and deletion status methods. It uses the same authorized HTTP boundary
as MCP and never connects directly to PostgreSQL. See the [Python client guide](clients/python/README.md)
and [client boundary ADR](docs/adr/0013-python-client-boundary.md).

### Use Palimpsest from TypeScript or JavaScript

The checkout also includes a dependency-free ESM client with TypeScript
declarations:

```bash
npm install ./clients/typescript
```

It exposes the same governed `remember`, `recall`, `correct`, `forget`,
checkpoint, export, deletion, and lower-level HTTP helpers as the Python
client. See the [TypeScript client guide](clients/typescript/README.md) and
[client boundary ADR](docs/adr/0018-typescript-client-boundary.md).

### Ingest coding-agent sessions

Palimpsest includes an opt-in [project-aware session ingestion guide](clients/python/README.md#ingest-coding-agent-sessions)
for Codex, Claude Code, and Hermes. It can poll explicitly selected local
source paths or the exact conventional locations for the current user with
`--discover`; it redacts common credential-shaped values, excludes tools and
private thinking, and writes through the authorized HTTP API. Each repository
gets a stable retrieval namespace so memories from multiple projects do not
have to share one undifferentiated search pool. See [ADR-0019](docs/adr/0019-project-aware-agent-session-ingestion.md)
and [ADR-0021](docs/adr/0021-local-agent-source-discovery.md).
The Python and TypeScript clients also expose per-project recall helpers that
return isolated evidence bundles for deliberate comparison; they do not
silently mix namespaces. Their comparison helpers additionally identify exact
key/value matches and same-key/different-value review candidates using
deterministic digests, plus bounded lexical-overlap candidates for differently
keyed session messages, without making a semantic claim or durable write.
On Linux, `bash scripts/install-palimpsest-ingest-service.sh` installs the
discovery watcher as an owner-only systemd user service for continuous local
ingestion; see [ADR-0022](docs/adr/0022-supervised-local-ingestion-service.md).

## Validation

```bash
bash scripts/check-repo.sh
```

## License

Apache-2.0.
