# ADR-0032: Precomputed authorized-current retrieval structure

Date: 2026-08-07 · Status: accepted

## Context

Spec 002 acceptance A4 measured the authorized lexical retrieval core at
1,000,000 revisions (2026-08-05, digests recorded in spec 002): p95 11.302 s
(all-match), 12.2–23.2 s across selectivity bands, cold first query 19.288 s,
warm serial p95 13.111 s, 8-way concurrent p95 16.792 s — against the proposed
gate p95 ≤ 200 ms / p99 ≤ 400 ms, which is NOT met (no SLA claim). The
measured floor is the per-query full-set pipeline (authorized-set
materialization + governance join), which is selectivity-, cache-temperature-,
and concurrency-independent; GIN-indexed selective document access measures
43 ms for 31k rows, so ranking, document join, cache, and concurrency are not
the cost. Issue #37 (evidence) is closed; the remediation is new, unspecced
work tracked in issue #43.

Two levers were previously named (spec 002 open question): a precomputed
authorized-current structure, or a loss-safe hot cache (issue #39). The
constitution requires: canonical memory is durable structured data, not an
embedding index (principle 7); embeddings and derived indexes are reproducible
from canonical records (principle 12); cache loss must never erase memory
(operational invariant from the deletion/restore work).

## Decision

Adopt the **precomputed authorized-current structure** as the primary
remediation lever:

- An incrementally maintained, tenant-scoped materialization of the authorized
  current view (authorization, deletion, retention, and validity filters
  applied at write time), consumed by retrieval queries instead of per-query
  full-set materialization.
- Maintenance is bounded and incremental (per-request or per-batch cost
  proportional to the write, not the corpus), and the structure is
  reproducible from canonical records (principle 12) with bounded, observable
  staleness (spec 002 A5d).
- Tenant isolation semantics are unchanged: the structure is derived from the
  same authorized candidate semantics; isolation scenarios re-run unchanged
  (A5c).

Rejected for now: the loss-safe hot cache (#39). It only helps warm paths, its
loss-safety requirement ("cache loss must never erase memory") adds a
recovery-critical surface, and the measured floor is pipeline-bound even warm
(warm serial p95 13.111 s). #39 remains a separate, later lever and can layer
on top of the precomputed structure once it lands.

## Alternatives considered

- **Loss-safe hot cache (#39)**: rejected above; revisit only if the
  precomputed structure fails A5a and profiling shows a warm-cache-specific
  residual cost.
- **Selectivity-modeled gate**: adjusting the gate to measured reality instead
  of fixing the pipeline. Explicitly secondary and rejected as the primary
  response — the constitution forbids weakening a failing acceptance
  criterion to complete a task; the gate stays ≤ 200 ms / ≤ 400 ms.
- **Per-request projection comparison** for corruption detection: measured at
  p95 4.609 s and rejected in spec 002; unaffected by this decision.

## Consequences

- Easier: per-query retrieval drops the full-set pipeline; the A5 gate becomes
  measurable with the existing rollback-only probe; #39 composes later.
- Harder: the structure must be maintained on every authorized write path
  (facts, checkpoints, episodes, deletion fences, retention), stays consistent
  under supersession and deletion, and needs its own boundedness and
  observability (A5d).
- Locked in: spec 002 A5 (amended) states the acceptance criteria; issue #43
  tracks implementation; the selectivity-modeled gate stays secondary.

## Links

Spec: `specs/002-authorized-retrieval/spec.md` (A4 evidence, A5 acceptance)
Issue: #37 (closed, evidence) · #43 (remediation) · #39 (hot cache, later)
