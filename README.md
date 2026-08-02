# Palimpsest

Temporal memory infrastructure for AI agents.

Palimpsest keeps recent information useful without erasing the past. It stores
resumable thread state, timestamped episodes, versioned facts, procedures, and
artifact references with provenance and authorization boundaries. Retrieval is
hybrid and time-aware; embeddings are rebuildable indexes, never the source of
truth.

## Status

The repository contains a checked development slice for the PostgreSQL-backed
HTTP service, including temporal memory, hybrid retrieval, canonical-history
exports, fenced subject deletion, and an executable fail-closed restore replay
for an independent deletion-fence ledger. It is not a production release:
cache, artifact, backup/PITR adapters, external identity, SDK, complete restore
rehearsal, and operational release gates remain deployment work.

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

Docker with Compose 2.20.0 or newer and the pinned Rust toolchain are required.
Start the pinned PostgreSQL 18.4 plus pgvector 0.8.5 dependency and the Rust
HTTP service with one command:

```bash
bash scripts/dev-up.sh
```

The service listens on `http://127.0.0.1:8080` and PostgreSQL listens only on
`127.0.0.1:5432`. `GET /healthz` is a content-free liveness probe and
`GET /readyz` checks database connectivity plus the exact successful SQLx
migration set shipped by this binary. Both probes require no authentication
and disclose no memory data. The HTTP service uses a synthetic, non-superuser
PostgreSQL role so forced row-level security remains active. Override the
`PALIMPSEST_*`
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
a backup/PITR adapter, backup disposition check, or the complete black-box
restore rehearsal and negative HTTP conformance gate.

```bash
docker compose stop postgres
```

Startup detects an incompatible legacy local volume and exits without deleting
it. Preserve or back up needed local data before explicitly recreating a volume.

## Validation

```bash
bash scripts/check-repo.sh
```

## License

Apache-2.0.
