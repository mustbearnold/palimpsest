# Authorized lexical retrieval evaluation — 2026-07-29

## Scope

Issue #19's lexical retrieval contract was evaluated through the portable conformance client over a real TCP listener and an isolated PostgreSQL 18.4 database with pgvector 0.8.5. The public seam creates durable, content-free retrieval receipts and reauthorizes their manifest items before returning canonical fact values.

## Scenarios

| Scenario | Evidence | Result |
| --- | --- | --- |
| Create, replay, and read | `POST` returns a UUIDv7 receipt, policy identity, `Location`, and `private, no-store`; an exact idempotent replay and `GET` return the same authorized receipt | Pass |
| Authorization before ranking | Same-scope restricted, cross-subject, and cross-tenant marker facts do not enter an internal-only result; database inspection finds only the allowed revision in its manifest | Pass |
| Current reauthorization | A receipt created with internal and restricted access returns only the internal item when read with reduced sensitivity grants, and its response scope digest changes | Pass |
| Scope cloaking | A principal from another tenant receives the same redacted `404` used for an absent receipt, without hidden values | Pass |
| Content-free durability | Receipt and manifest storage contain identifiers, scores, and digests but none of the raw query or private marker values | Pass |
| Bitemporal selection | An explicit historical perspective returns Wellington while the current perspective returns the superseding Auckland revision | Pass |
| Deterministic pagination | Three policy-ordered results page as two plus one; a second independent receipt preserves ordered IDs, score components, values, and policy digest | Pass |
| Idempotency scope and concurrency | Cross-subject key reuse returns stable `422`; concurrent identical requests converge on one receipt with one replay | Pass |
| Exact identity and abstention | An exact key match ranks first with an exact-identity component; a missing query creates an empty `abstained` receipt | Pass |
| Projection integrity and recovery | Missing rows, corrupted projection digests, and corrupted vectors fail closed with `503`; rebuilding from canonical content restores retrieval | Pass |
| Expiry and deletion | Expired content disappears on receipt read; a deleted successor is not replaced by its superseded predecessor | Pass |
| Request bounds | A future recorded-time coordinate returns `422`, a 4,097-byte query returns `413`, and a missing idempotency key returns `400` | Pass |

The privacy fixtures use unique marker values. Unauthorized, projection-failure, and post-deletion responses are checked for those values and for hidden revision identifiers where applicable.

## Commands and gates

```text
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo test --locked -p palimpsest-server --test conformance_postgres18 -- --nocapture
npm exec --yes @redocly/cli@2.18.1 lint api/openapi.yaml
bash scripts/check-repo.sh
git diff --check
```

The focused PostgreSQL retrieval scenario and repository-wide Rust and contract gates passed locally. The conformance scenario creates and removes a fresh database per run and validates PostgreSQL and pgvector versions before starting the HTTP server. A second focused run used a synthetic `NOSUPERUSER NOBYPASSRLS` runtime role with separate migration authority; it passed, and its temporary role, database, and template extension were removed afterward.

## Boundaries

- The implemented policy is exact identity plus PostgreSQL lexical search. It does not yet include vector candidates, reciprocal-rank fusion, learned reranking, or an approximate-nearest-neighbor index.
- The evaluation proves scenario correctness for the fixed fixtures. It is not a relevance-quality corpus, load test, latency benchmark, capacity result, or production-readiness claim.
- Receipt and manifest records are immutable and retained indefinitely in v1. The service currently has no receipt cleanup or deletion workflow.
- Search projections are rebuildable derived data, but creation deliberately fails closed when a required projection is absent or stale.
- Static bearer credentials are a local composition adapter. Production OAuth/OIDC integration remains outside this issue.
