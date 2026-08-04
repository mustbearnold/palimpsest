# ADR-0007: Deterministic temporal retrieval uses trusted fixed-point factors

Status: accepted

Date: 2026-07-29

## Context

ADR-0005 established authorization-first lexical retrieval and immutable, content-free receipts. ADR-0006 added opt-in exact-vector reciprocal-rank fusion without changing the lexical default or making derived projections canonical. Palimpsest now needs an opt-in policy that can rank already eligible revisions using recency, confidence, and importance while preserving the same bitemporal, authorization, durability, and replay boundaries.

Temporal scores become durable audit evidence and participate in ordering. They therefore cannot depend on a host floating-point environment, a C math library, PostgreSQL's transcendental implementation, or PostgreSQL's half-away-from-zero `numeric` rounding. Recency and importance must also come from attributable trusted metadata rather than caller-supplied ranking controls. Adding this behavior must not reinterpret `retrieval-lexical-v1`, `retrieval-hybrid-v1`, or any receipt already persisted under either policy.

Issue #21 owns deterministic temporal mechanics and temporal-correctness evidence. Issue #22 owns the frozen 128-scenario evaluation corpus and every broader relevance-quality claim.

## Decision

### Add one immutable policy without changing legacy policies

`retrieval-hybrid-temporal-v1` is a new explicitly selected policy. Omission of `policy_id` continues to select the lexical default, and `retrieval-hybrid-v1` retains its exact existing fusion, ordering, manifest, digest, and replay semantics. Existing receipts are never rescored.

The temporal policy retains ADR-0006's independent exact-identity, lexical, and exact cosine-vector channels and equal-weight reciprocal-rank fusion with `k = 60`. Each `1 / (60 + rank)` channel contribution is rounded half-even to 12 decimal places before the contributions are added exactly into `fused_score`.

### Keep eligibility and metadata assignment ahead of scoring

PostgreSQL remains authoritative for effective bitemporal selection, authorization-first eligibility, channel ranks, and durable `numeric(20,12)` storage. The service first selects the revision effective at both request coordinates, then applies tenant, subject, principal, sensitivity, lifecycle, retention, deletion, and request filters. Factors operate only on that eligible relation; no factor, bonus, or score can grant access or resurrect a revision.

The governance envelope stores immutable resolved temporal metadata plus its assignment lineage. A digest-bound internal metadata-policy registry assigns:

- `recency_profile_id`;
- `recency_anchor_at`; and
- importance in the inclusive range zero through one.

The HTTP retrieval request cannot set or override those values. Confidence is the canonical attributable confidence already stored on the effective fact revision. Unknown or inconsistent assignment-policy or recency-profile lineage fails closed.

Fact commands may name only a write policy whose retrieval-metadata assignment has been registered by migration authority. An unknown policy is a permanent, nonretryable `422` rather than a transient storage failure; the command can be retried only after trusted policy registration. This makes the formerly open write-policy string a governed public identity without exposing recency or importance as direct request controls.

Existing governance rows are attributed during migration to the stable, neutral assignment: `stable-v1`, importance `0.5`, and the revision's observed time as its anchor. This backfill preserves existing score behavior while making the metadata lineage explicit.

### Make Rust's checked integer scorer normative

Rust is the normative scoring implementation. Public scores use checked signed `i128` units at scale `10^-12`. Every named multiplication boundary rounds to nearest with ties to even. Overflow, malformed factor domains, or unresolved metadata fails the request before a receipt is persisted; values never wrap, saturate, or silently fall back to another policy.

Recency age is computed on the valid-time axis in exact elapsed microseconds:

```text
age_us = max(0, request.valid_at - governance.recency_anchor_at)
```

`stable-v1` returns exactly `1.000000000000` for every age. `active-case-30d-v1` is:

```text
max(0.125, 2 ^ (-age_us / 2_592_000_000_000))
```

The active profile reaches `0.5` after 30 elapsed days and its `0.125` floor at 90 elapsed days. Calendar months, local civil-day arithmetic, `recorded_at`, and receipt evaluation time do not determine age. Negative age is clamped to zero.

The immutable policy digest binds a frozen Q63 `exp2` algorithm, its constants, generation tool and version, constants digest, approximation bound, scale, factor domains, rounding rule, and operation order. The committed Q63 integers are production authority. PostgreSQL `round()`, `power()`, and host floating point may be independent test oracles but are not durable scoring authorities.

The scorer applies and rounds these boundaries in order:

1. Multiply `fused_score` by `recency_factor`; publish the difference from the fused score as `temporal_adjustment`.
2. Multiply that result by `confidence_factor`, equal to revision confidence; publish the difference as `confidence_adjustment`.
3. Multiply that result by `importance_factor = 0.5 + importance`; publish the difference as `importance_adjustment`. Neutral importance `0.5` therefore produces factor `1.0`.
4. Add the policy-owned exact-identity bonus: namespace plus key `0.016393442623`, key only `0.008196721311`, or none `0`.

The public additive explanation is exactly:

```text
final_score = fused_score
            + temporal_adjustment
            + confidence_adjustment
            + importance_adjustment
            + exact_identity_bonus
```

Receipts also publish `recency_factor`, `confidence_factor`, and `importance_factor`. Every decimal score and factor uses a canonical string with exactly 12 fractional digits. Component names remain open and additive in `/v1`; clients ignore names they do not understand.

### Freeze one complete result order and durable explanation

The same complete tuple governs row numbering, materialization, pagination, receipt manifests, and responses:

```text
exact_identity_rank ASC NULLS LAST,
final_score_units DESC,
exact_rank ASC NULLS LAST,
lexical_rank ASC NULLS LAST,
vector_rank ASC NULLS LAST,
case_id ASC,
fact_id ASC,
revision_id ASC
```

Temporal manifest fields are compatibility-nullable for legacy rows, but are all-or-none and mandatory under `retrieval-hybrid-temporal-v1`. Canonical recency profile and anchor, age, three factors, three adjustments, exact bonus, and final score enter the item and manifest digests. The policy and arithmetic identities enter the receipt's immutable policy digest.

Receipt replay returns the persisted historical ranks, factors, and scores. It still reauthorizes canonical content under current rules and may hide an item, but it does not rescore, replace, or resurrect one.

## Consequences

- Given the same bounded channel ranks, the normative integer algorithm is designed to preserve temporal scores and order across Rust targets, PostgreSQL hosts, process restarts, projection rebuilds, and 2027 dependency upgrades as long as the immutable policy identity and digest remain the same. Cross-target, backup/restore, or additional PostgreSQL-version support becomes a release claim only after its explicit compatibility matrix passes.
- Late-recorded evidence can change which revision was knowable at a recorded cutoff without making old valid-time evidence artificially recent.
- Stable and neutral backfill preserves the meaning of legacy governance rows; the new policy requires attributable metadata instead of guessing.
- Fixed-point and Q63 code adds reviewed constants, range proofs, generators, digest fixtures, and boundary tests to the release surface.
- The conformance gate must prove half-even boundaries, 30/90-day recency, nonneutral confidence and importance, late successors, future-valid exclusion, expiry/deletion, deterministic replay and rebuilds, digest integrity, and preservation of pre-ADR-0007 receipts.
- Passing those mechanics does not establish relevance quality, model quality, latency, throughput, cost, capacity, provider compatibility, or production readiness. Those claims require the issue #22 corpus and separate operational evidence.
