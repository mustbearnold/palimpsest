# Deterministic temporal retrieval evaluation — 2026-07-29

Status: implementation and local verification complete; independent reviews clean

## Scope

Issue #21 adds the explicitly selected `retrieval-hybrid-temporal-v1` policy to the authorization-first receipt contract. This evaluation will cover the deterministic temporal mechanics, trusted metadata assignment, bitemporal correctness, durable explanation, legacy compatibility, and failure behavior locked by ADR-0007.

No scenario is marked passed until its implementation evidence and applicable focused and full gates have completed. Issue #22, not this report, owns the frozen 128-scenario corpus and all relevance-quality claims.

## Evidence matrix

| Scenario | Required evidence | Status |
| --- | --- | --- |
| Legacy policy preservation | Omitted `policy_id`, `retrieval-lexical-v1`, `retrieval-hybrid-v1`, and pre-ADR-0007 durable receipts preserve their policy digests, manifests, score components, order, replay, and public representation | Pass — upgrade fixture preserves lexical and hybrid rows byte-for-byte |
| Authorization before temporal scoring | Effective bitemporal selection and trusted tenant, subject, principal, sensitivity, lifecycle, retention, deletion, and request filters run before factors, candidates, or ranking under a `NOSUPERUSER NOBYPASSRLS` runtime role | Pass — dedicated `SET ROLE` SQLx pool executes temporal HTTP POST/GET/pagination/replay, deletion/expiry no-resurrection, same-scope principal denial, and forbidden lexical candidates |
| Trusted metadata assignment | Requests cannot set recency profile, anchor, or importance; immutable assignment lineage resolves them, and unknown or inconsistent lineage fails closed before receipt persistence | Pass — direct knobs reject, registered-only write policy returns nonretryable `422`, and correctly rehashed policy/profile tampering rejects |
| Checked half-even scoring | Unit vectors prove signed midpoint parity, just-below/at/above boundaries, checked overflow, 12-place canonical strings, independently rounded RRF contributions, and the fixed multiplication order | Pass — nine domain vectors plus independent SQL manifest validation |
| Q63 recency algorithm | Committed constants reproduce the pinned generator digest and approximation bound; `stable-v1` is one, negative age clamps to zero, active age is one at zero, `0.5` at 30 elapsed days, and floors at `0.125` at 90 elapsed days | Pass — full-array digest, adjacent floor, maximum-age, and fixed SQL-vs-Rust 1µs/15d/30d/90d−1µs/90d vectors |
| Nonneutral confidence | Stable alpha root and late successor both use confidence `0.8`, publish `confidence_factor = 0.800000000000` and `confidence_adjustment = -0.006453291699`, and finish at `0.025813166797` | Pass |
| Nonneutral importance | The exact item receives importance `0.75` only through its trusted metadata policy, publishes `importance_factor = 1.250000000000` and `importance_adjustment = 0.001496608078`, and finishes at `0.015679761701` | Pass |
| Fixed temporal fixture | Active recent gamma uses the 30-day factor, active stale beta uses the 90-day floor, stable alpha does not decay, exact identity remains first, and all locked fused scores and additive components match exactly | Pass |
| Bitemporal contradiction | A receipt before late alpha evidence returns the root; a later recorded-time receipt returns the successor; both use valid-time age rather than recorded or evaluation time, and the ineffective revision never leaks | Pass |
| Future-valid exclusion | The future-valid delta revision never enters channel ranks, temporal scoring, the manifest, receipt, or response at the fixed historical valid-time coordinate | Pass |
| Expiry and deletion | Expired or deleted revisions are ineligible before scoring and are not replaced by superseded predecessors during receipt read or replay | Pass — new retrieval, durable GET, and idempotent replay all abstain without resurrection |
| Deterministic durability | Ten independent runs, exact replay, receipt GET, pagination, child-process restart, and full search-document/embedding rebuild preserve order, factor strings, additive components, item/manifest digests, and policy digest | Pass |
| Public contract and redaction | OpenAPI exposes additive factor and adjustment component names while responses, receipts, errors, and storage omit raw vectors, private values, hidden IDs, credentials, and model/tool payloads | Pass — Redocly and redaction assertions pass |
| Migration and rollback safety | A pre-ADR-0007 database upgrades with attributable stable/neutral governance; compatibility-null temporal fields remain unchanged for legacy rows; partial temporal manifests and unknown lineage fail closed | Pass — upgrade, partial-manifest, rehashed policy/profile tampering, and wrong-factor cases reject |
| Independent review | Standards review and Spec review independently evaluate the finished change and all actionable findings are resolved before merge | Pass — final Standards and Spec re-reviews are clean after all findings were repaired |

## Verified environment and gates

Local evidence was produced on `x86_64-unknown-linux-gnu` with Rust `1.97.1`, PostgreSQL `18.4`, and pgvector `0.8.5`. The administrative test connection was superuser-capable; the conformance scenario additionally created a `NOLOGIN NOSUPERUSER NOBYPASSRLS` role, applied it on every connection in a dedicated runtime pool, exercised temporal creation, durable read, pagination, idempotent replay, deletion/expiry read and replay without resurrection, same-scope principal denial, and forbidden-candidate exclusion through HTTP, then removed the role. The explicit principal predicate was also exercised while the administrative connection could bypass RLS.

All listed gates passed on the final pre-review tree:

```text
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo test --locked -p palimpsest-server --test conformance_postgres18 -- --nocapture
cargo test --locked -p palimpsest-server --test lexical_receipt_upgrade_postgres18 -- --nocapture
npm exec --yes @redocly/cli@2.18.1 lint api/openapi.yaml
bash scripts/check-repo.sh
python3 scripts/generate-q63-exp2.py
git diff --check
```

The generator reproduced constants digest `769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa` with a maximum bound of 64 Q63 units. Cross-architecture and additional PostgreSQL-version comparison and database backup/restore remain release-matrix work; no cross-host compatibility or backup/restore result is claimed.

## Locked fixed-fixture results

These are expected values, not recorded test results.

| Final rank | Item | Recency | Confidence | Importance | Exact bonus | Expected final score |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | exact | `0.125000000000` | `1.000000000000` | `1.250000000000` | `0.008196721311` | `0.015679761701` |
| 2 | alpha root or successor | `1.000000000000` | `0.800000000000` | `1.000000000000` | `0.000000000000` | `0.025813166797` |
| 3 | gamma | `0.500000000000` | `1.000000000000` | `1.000000000000` | `0.000000000000` | `0.015877016129` |
| 4 | beta | `0.125000000000` | `1.000000000000` | `1.000000000000` | `0.000000000000` | `0.004065309360` |

Exact identity precedence intentionally keeps the exact item first even when a nonexact item has a higher final score. The future-valid delta item is expected to be absent.

## Recorded gates

```text
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo test --locked -p palimpsest-server --test conformance_postgres18 -- --nocapture
cargo test --locked -p palimpsest-server --test lexical_receipt_upgrade_postgres18 -- --nocapture
npm exec --yes @redocly/cli@2.18.1 lint api/openapi.yaml
bash scripts/check-repo.sh
git diff --check
```

Independent Standards and Spec re-reviews inspected the repaired implementation, migration, public contract, conformance evidence, and bounded claims. Both returned clean with no remaining actionable findings.

## Strict limits and non-claims

- This matrix evaluates deterministic mechanics against fixed fixtures. Until its rows are updated with evidence, it does not claim that #21 passes.
- The issue #22 corpus is required before making retrieval-quality, contradiction-resolution quality, or ranking-superiority claims.
- The deterministic 4D embedding provider is conformance infrastructure, not a production provider or evidence of provider compatibility or model quality.
- Exact vector scan remains the reference behavior. This work makes no ANN recall, latency, throughput, scale, cost, capacity, or production-readiness claim.
- Process restart and derived-projection rebuild are verified; database backup/restore recovery and cross-host compatibility are not claimed.
- A first production deployment or security-sensitive release remains subject to founder approval and independent review under `AGENTS.md`.
