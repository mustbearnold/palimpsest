# Palimpsest

Temporal memory infrastructure for AI agents.

Palimpsest keeps recent information useful without erasing the past. It stores
resumable thread state, timestamped episodes, versioned facts, procedures, and
artifact references with provenance and authorization boundaries. Retrieval is
hybrid and time-aware; embeddings are rebuildable indexes, never the source of
truth.

## Status

Palimpsest is a defensible v2 development milestone for a self-hosted,
PostgreSQL-backed agent memory service. It includes temporal memory, hybrid
retrieval, crash-safe checkpoints, canonical-history exports, fenced subject
deletion, fail-closed restore replay, a local Codex MCP adapter, and a
dependency-free Python client for the governed lifecycle. It is not an
official or production release. See the [v2 status](docs/V2_STATUS.md) for the
evidence and the deliberately unclaimed deployment boundaries.

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
and disclose no memory data. The HTTP service uses a synthetic, non-superuser
PostgreSQL role so forced row-level security remains active. The local launcher
allows the `internal` sensitivity by default so retrieval has a useful but
explicitly narrow development scope. Override the `PALIMPSEST_*`
environment variables when needed. Set `PALIMPSEST_EXPORT_ROOT` to a durable
private filesystem path when enabling canonical-history exports; the development
default is `var/palimpsest/exports`. Stop the service with `Ctrl+C`, then stop
PostgreSQL without deleting its volume:

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
searches and `palimpsest_remember` for explicitly requested saves. The adapter
uses the HTTP API, keeps the configured tenant and subject scope, and never
exposes delete or export operations. Verify registration with `codex mcp list`.

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

## Validation

```bash
bash scripts/check-repo.sh
```

## License

Apache-2.0.
