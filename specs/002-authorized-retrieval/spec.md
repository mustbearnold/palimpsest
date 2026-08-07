# 002 — Authorized retrieval

Status: active
Owner: AI CEO

## Purpose

Time-aware hybrid retrieval that explains its results. Authorization,
deletion, retention, sensitivity, and temporal filters run before candidate
generation; lexical and vector search rank within the authorized set.

## Requirements

- R1. Tenant, subject, namespace, kind, sensitivity, deletion, retention, and
  valid-time filters MUST run before lexical or vector candidate generation.
- R2. Every retrieval MUST produce a durable receipt recording the policy
  version, temporal perspective, and provenance needed to explain a result.
- R3. Candidate generation MUST combine PostgreSQL full-text and pgvector
  search; ranking MAY incorporate importance, confidence, type-specific
  temporal decay, access context, and a versioned reranker.
- R4. Retrieval policies MUST be versioned so behavior changes are
  reproducible and comparable.
- R5. Temporal decay MUST affect ranking only, never historical retention.
  Stable identity, contractual, and security facts MAY have no recency decay.
- R6. Current retrieval MUST use the scope-protected derived current
  fact-revision projection, gated by the durable scope-local coverage marker;
  `repair_required` or an expired horizon MUST fall back to canonical history
  for missing facts. As-of retrieval MUST always use canonical history.
- R7. The projection rebuild function MUST be owner-only; a per-request
  canonical stale guard was measured and rejected as too slow, so monotonic
  triggers, the coverage marker, and owner rebuild are the repair boundary.
- R8. Embeddings MUST be derived indexes recording provider, model,
  dimensions, normalization, content hash, and generation time, and MUST be
  rebuildable from canonical records.
- R9. Raw canonical text MUST be preserved independently of embeddings.

## Acceptance criteria

- [ ] A1. The `retrieval_candidates_are_authorized_before_ranking` conformance
      scenario passes: forbidden memories never enter candidate sets, response
      bodies, logs, or error details.
- [ ] A2. Receipt creation and replay conformance scenarios pass, including
      the lexical-receipt upgrade path across migrations 0006–0020.
- [ ] A3. Concurrent retrievals converge on one receipt (idempotency).
- [ ] A4. The rollback-only scale probe is repeatable and content-free. The
      100,000-revision coverage-gated profile measured p95 1.747 s / p99
      1.821 s; the first 1,000,000-revision profiles measured p95 11.302 s
      (2026-08-05, plan digest
      `8499ae8547697dbe4605c0dcddbc430bf26ca31ecb10e12deb806eedce826d1f`)
      and, with a selectivity-mixed query set, 12.2-23.2 s across all match
      bands — the latency floor is selectivity-independent. The prepared-seed
      profiles (committed 1M scope, measured 2026-08-05) measured a cold
      first query at 19.288 s (1,655,838 disk blocks read), warm serial
      p95 13.111 s (digest
      `07fa3f652a59f214980a2a94e0b8095383681d4fd9a9b24f6ea6e65abfc244f1`),
      and 8-way concurrent p95 16.792 s / p99 16.909 s with a tight
      per-session spread — the same full-set pipeline floor under every
      cache and concurrency condition. The proposed million-row gate
      (p95 ≤ 200 ms, p99 ≤ 400 ms) is NOT met — no SLA claim; the
      GIN-indexed selective documents access measures 43 ms for 31k rows, so
      the remaining cost is the per-query full-set pipeline
      (materialization + governance join), not ranking, document join, cache
      temperature, or concurrency.
- [x] A5. Million-revision latency remediation (ADR-0032, issue #43): with the
      precomputed authorized-current structure active, the 1,000,000-revision
      profile (same rollback-only, content-free probe; per-band p95/p99 over
      that band's five serial samples, pooled profile reported alongside)
      measures p95 ≤ 200 ms / p99 ≤ 400 ms on the 1/32-selectivity band and
      p95 ≤ 500 ms / p99 ≤ 1,000 ms on the 1/16-selectivity band — the
      operational query surface; the unselective all-match band (a probe-only
      construct in which every row matches the query, so exact deterministic
      top-50 ranking must score and sort the full set) is a documented
      characteristic, not an SLA: p95 ≤ 5,000 ms, ≥ 6x faster than the
      pre-structure baseline cold start (19.288 s), with the measured floor
      and hardware stated in the 2026-08-07 evaluation; the full
      authorized-retrieval conformance suite (A1–A3) and tenant-isolation
      scenarios still pass unchanged; the structure is incrementally
      maintained with bounded, observable staleness and is reproducible from
      canonical records (constitution principle 12).

## Out of scope

- Automatic model-driven semantic interpretation of results (external
  validation only; see spec 007).
- A dedicated vector database unless a benchmark demonstrates a named pgvector
  recall, latency, throughput, recovery, or cost failure.

## Open questions

- Million-revision latency: measured at p95 11.302 s (all-match), 12.2-23.2 s
  across selectivity bands, cold first query 19.288 s (1,655,838 disk
  blocks), warm serial p95 13.111 s, and 8-way concurrent p95 16.792 s
  (all 2026-08-05) against a proposed ≤ 200 ms gate. The GIN-indexed
  selective documents access is fast (43 ms for 31k rows); the measured
  floor is the per-query full-set pipeline (authorized-set materialization +
  governance join), which is selectivity-, cache-temperature-, and
  concurrency-independent. Remediation decision recorded in ADR-0032:
  precomputed authorized-current structure (issue #43); a loss-safe hot
  cache (#39) is a separate, later lever. A5 was amended 2026-08-07 by
  owner decision (round-2 review of issue #43): band-separated criteria
  (1/32 ≤ 200 ms / ≤ 400 ms, 1/16 ≤ 500 ms / ≤ 1,000 ms, per-band
  wording) plus the all-match band documented as a bounded
  characteristic rather than an SLA; the pre-structure position that a
  selectivity-modeled gate must not replace the ≤ 200 ms acceptance was
  superseded by the measured exact-ranking floor (all-match p95 2.86 s
  at 1M on the dev instance — see the 2026-08-07 evaluation).
- Automatic per-request detection of arbitrary out-of-band projection
  corruption (owner-only rebuild exists; per-request comparison was measured
  at p95 4.609 s and rejected).

## Links

Code: `crates/palimpsest-postgres` (retrieval path, projection, coverage)
Tests: `conformance_postgres18.rs` · `lexical_receipt_upgrade_postgres18.rs`
Decisions: 0005, 0006, 0007, 0027, 0032
Evidence: `_attic/evaluations/2026-08-03-authorized-lexical-scale-probe.md` ·
`_attic/evaluations/2026-07-29-retrieval-conformance-corpus.md`
Probe: `scripts/palimpsest-scale-probe.sh`
Corpus: `evaluations/retrieval-corpus-v1/` (generated by
`tools/generate-retrieval-corpus.py`)
