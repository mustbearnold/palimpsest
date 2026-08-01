# Exact-vector retrieval fusion evaluation — 2026-07-29

## Scope

Issue #20 extends the existing authorization-first lexical receipt contract with
an explicitly selected `retrieval-hybrid-v1` policy. The lexical policy remains
the default. The hybrid policy builds exact-identity, lexical, and exact-cosine
vector channels from the same authorized-effective fact revisions, then combines
their sequential ranks with equal-weight reciprocal-rank fusion (RRF) at
`k = 60`.

This report distinguishes the deterministic mechanics evidence from claims that
the fixture cannot support. Final pass/fail status must be recorded only after
the focused PostgreSQL 18 conformance run and the full repository gates finish.

## Primary-source constraints

- pgvector defines cosine distance with `<=>` and documents nearest-neighbor
  ordering with `ORDER BY embedding <=> query LIMIT n`: [pgvector 0.8.5
  querying](https://github.com/pgvector/pgvector/blob/v0.8.5/README.md#querying).
- pgvector performs exact nearest-neighbor search by default; HNSW and IVFFlat
  trade recall for speed and are opt-in indexes: [pgvector 0.8.5
  indexing](https://github.com/pgvector/pgvector/blob/v0.8.5/README.md#indexing).
- PostgreSQL can force a side-effect-free `WITH` query to be evaluated as a
  materialized relation. The retrieval statement uses this boundary before
  projection joins and ranking: [PostgreSQL 18 CTE
  materialization](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION).
- PostgreSQL does not guarantee row order without an explicit sort, and later
  sort expressions break ties. Every channel and the final manifest therefore
  include stable revision identifiers in their ordering: [PostgreSQL 18 row
  ordering](https://www.postgresql.org/docs/18/queries-order.html).
- pgvector-rust 0.4.2 added SQLx 0.9 support, matching this workspace's database
  client boundary: [pgvector-rust 0.4.2
  changelog](https://github.com/pgvector/pgvector-rust/blob/v0.4.2/CHANGELOG.md#042-2026-05-22).

## Locked mechanics

The hybrid query must materialize bitemporally effective revisions first, apply
trusted tenant, subject, principal, sensitivity, lifecycle, retention, and
request filters second, and only then join verified lexical and embedding
projections. Exact, lexical, and vector ranks are independent. Cosine distance
sorts ascending, and all ties use stable revision identifiers.

The policy permits no request-supplied vectors, models, weights, limits, or
ranking knobs. It pins channel limits, manifest limit, cosine distance,
quantization scales, equal channel weights, `k = 60`, 12-place fixed-point
rounding, exact precedence, and tie breaks. No HNSW, IVFFlat, half-vector, or
other approximate-nearest-neighbor index is allowed.

For an item in channel set `C`, the fixture checks:

```text
fused_score = sum(round_12(1 / (60 + rank(channel))) for channel in C)
```

## Deterministic 4D fixture

The normalized query vector is `[1, 0, 0, 0]`. A restricted exact/text/vector
trap has cosine distance zero and would consume rank one in every channel if
authorization occurred after candidate generation. The trap must be absent from
channel ranks, score explanations, receipt storage, and the public response.

| Final rank | Item | Exact rank / RRF | Lexical rank / RRF | Vector rank / RRF | Fused score |
| ---: | --- | --- | --- | --- | ---: |
| 1 | exact | 1 / `.016393442623` | 3 / `.015873015873` | 5 / `.015384615385` | `0.047651073881` |
| 2 | beta | — | 2 / `.016129032258` | 1 / `.016393442623` | `0.032522474881` |
| 3 | alpha | — | 1 / `.016393442623` | 4 / `.015625000000` | `0.032018442623` |
| 4 | gamma | — | 4 / `.015625000000` | 2 / `.016129032258` | `0.031754032258` |
| 5 | delta | — | — | 3 / `.015873015873` | `0.015873015873` |

The receipt explanation must expose applicable channel ranks, lexical score,
vector distance and similarity, per-channel RRF contributions, fused and final
scores, final rank, and redacted embedding profile/projection/input/vector
digests. It must never expose a raw vector.

## Required scenarios

| Scenario | Required evidence | Status |
| --- | --- | --- |
| Authorization before fusion | The restricted rank-one trap is absent from every channel, manifest row, receipt, and response under a `NOSUPERUSER NOBYPASSRLS` runtime role | Pass — full NOBYPASSRLS workspace gate |
| Deterministic exact fusion | Two independent receipts produce the five-item order, channel ranks, 12-place RRF values, final ranks, and stable digests above | Pass — exact receipt and manifest assertions |
| Strict projection coverage | Missing, failed, stale, corrupt, wrong-profile, wrong-dimension, and hash-mismatched authorized projections return one generic retryable `503` before receipt persistence | Pass — every case also proves zero reservation, receipt, and manifest rows |
| Provider boundary and replay | Provider output validation rejects cardinality, digest, finite-value, dimension, zero-norm, and normalization failures; a completed replay succeeds without a provider call during outage | Pass — provider contract and replay-call-count assertions |
| Rebuild recovery | A bounded document rebuild restores a failed hybrid request only when source, profile, projection, and input digests still match | Pass — missing, stale, failed, and corrupt recovery assertions |
| Exact-search catalog | PostgreSQL catalog inspection finds no HNSW, IVFFlat, or other ANN index on the embedding projection | Pass — catalog assertion |
| Public redaction | OpenAPI and serialized responses include optional digest lineage and additive score components but no raw query, vector, provider response, hidden revision ID, or private marker | Pass — response/error redaction assertions and valid OpenAPI |
| Lexical compatibility | Omitting `policy_id` preserves the lexical default and the existing lexical conformance scenarios | Pass — manifest-backed pre-0007 migration/replay fixture |

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

All commands above passed on 2026-07-29. The locked workspace test passed both
PostgreSQL integration binaries: the main conformance test (`1 passed`, one
intentional crash-child test ignored) and the pre-0007 manifest-backed lexical
receipt upgrade test (`1 passed`). The fixture preserves every legacy receipt
and manifest column, foreign-key target, cursor, digest, rank, and score, then
proves identical public replay and GET behavior. The full locked workspace test
passed again with the runtime
connection set to a temporary `NOSUPERUSER NOBYPASSRLS` role and a separate
migration-authority connection. The temporary role and template extension were
removed after the gate. Independent Standards and Spec reviews both returned
clean; their evidence is recorded with the direct `main` commit and its
push-triggered CI run.

## Strict limits and non-claims

- The committed 4D vectors prove ranking arithmetic, deterministic ordering,
  authorization isolation, projection validation, and redaction mechanics only.
- They do not prove semantic relevance quality, model quality, provider
  compatibility, latency, throughput, scale, capacity, cost, or production
  readiness.
- The main server adapter is deliberately unavailable. The conformance adapter
  is deterministic test infrastructure, not a production embedding provider.
- Exact scan is the v1 truth path. ANN remains a separately versioned future
  derived projection with its own recall, isolation, migration, and release
  evidence; it cannot silently replace this policy.
- Hybrid requests fail closed with a generic retryable `503` when provider or
  authorized projection coverage is not complete and valid. There is no lexical
  fallback and no partial receipt.
