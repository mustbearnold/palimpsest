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

## Validation

```bash
bash scripts/check-repo.sh
```

## License

Apache-2.0.
