# Bitemporal lifecycle evaluation — 2026-07-29

## Scope

Issue #2's first public MemoryService lifecycle was evaluated through an
implementation-neutral HTTP client over a real TCP listener and an isolated
PostgreSQL 18.4 database with pgvector 0.8.5. The portable conformance crate has
no dependency on the HTTP, application, domain, PostgreSQL, or server crates.

## Scenarios

| Scenario | Evidence | Result |
| --- | --- | --- |
| Immutable episode | Append, exact idempotent replay, and read through `Location` | Pass |
| Attributable fact | Create revision 1 with an authorized evidence episode and write-policy identity | Pass |
| Explicit supersession | Strong `If-Match`, named predecessor, revision 2, monotonic recorded time, changed ETag | Pass |
| Valid-time reconstruction | January remains revision 1 after February evidence; February boundary selects revision 2 | Pass |
| Recorded-time reconstruction | March at revision-1 cutoff returns revision 1; at revision-2 cutoff returns revision 2 | Pass |
| Tenant isolation | Principal A receives a redacted RFC 9457 `404` for a real tenant-B fact that principal B can read | Pass |
| Subject isolation | Principal A receives a redacted `404` for a real second-subject fact in tenant A that its principal can read | Pass |
| Cross-subject idempotency | One principal reuses a key against another authorized subject and receives stable `422 idempotency-key-reused` | Pass |
| Contract validation | Non-UTC and over-precision timestamps return `400`; an empty valid-time interval returns stable `422 invalid-valid-time` | Pass |
| Governed writes | Each append, create, and supersede has exactly one audit/outbox pair; exact replays create none | Pass |
| Outbox publication | A scoped transaction can perform the sole permitted one-way `published_at` transition | Pass |

The isolation fixtures contain unique private marker values and resource IDs.
Neither marker nor hidden ID appears in the unauthorized response.

## Commands and gates

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash scripts/check-repo.sh
```

All gates passed locally. The PostgreSQL conformance test creates and removes a
fresh database per run and validates the PostgreSQL and pgvector versions before
starting the HTTP server.

## Boundaries

- Local native PostgreSQL evidence used an installed PostgreSQL 18.4 server and
  pgvector 0.8.5. Docker is unavailable on this machine, so Compose was validated
  structurally but its pinned image will receive runtime proof in GitHub CI.
- The local default database role is a superuser and therefore does not prove RLS.
  CI runs the same conformance suite with a `NOSUPERUSER NOBYPASSRLS` role; the
  migration evaluation separately proved unscoped and cross-subject RLS failures.
- Static bearer credentials are a local composition adapter. Tenant and subject
  values in paths never grant authority; production OAuth/OIDC integration remains
  a future adapter behind the same authentication boundary.
