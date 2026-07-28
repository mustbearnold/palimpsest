# Palimpsest

Temporal memory infrastructure for AI agents.

Palimpsest keeps recent information useful without erasing the past. It stores
resumable thread state, timestamped episodes, versioned facts, procedures, and
artifact references with provenance and authorization boundaries. Retrieval is
hybrid and time-aware; embeddings are rebuildable indexes, never the source of
truth.

## Status

Specification and repository bootstrap. No production service has shipped yet.

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
`127.0.0.1:5432`. The HTTP service uses a synthetic, non-superuser PostgreSQL
role so forced row-level security remains active. Override the `PALIMPSEST_*`
environment variables when needed. Stop the service with `Ctrl+C`, then stop
PostgreSQL without deleting its volume:

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
