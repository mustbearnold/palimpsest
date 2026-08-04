# 011 — Governed consolidation

Status: draft
Owner: AI CEO

## Purpose

Durable, server-side jobs that derive structured facts and summaries from
ingested episodes through an attributable model boundary — making automatic
structuring possible while preserving the invariant that no model output
becomes durable memory without an attributable write policy.

This is the missing middle of the autonomous memory lifecycle: spec 006
ingests raw episodes automatically; this spec turns them into governed facts
automatically; spec 012 surfaces those memories automatically. The existing
explicit review flow (spec 007) remains the conservative default; automatic
consolidation is an opt-in, policy-governed path.

## Requirements

- R1. Consolidation MUST run as durable server-side work items with
  claim-level retry: idempotency keys per claim, crash-resumable state, and
  typed partial-completion results, following the established export and
  deletion worker patterns.
- R2. Every automatic derivation MUST pass through an attributable model
  boundary: the interpretation step MUST record model identity, prompt/policy
  version, source episode lineage, content hash, confidence, sensitivity, and
  valid-time metadata before any claim is materialized. The write policy is
  the gate: no claim MAY become durable memory without a registered write
  policy for the tenant and source kind.
- R3. Replaying the same episodes with the same policy version MUST produce
  the same claims (deterministic derivation), so retries never duplicate
  facts.
- R4. Automatic consolidation MUST be opt-in per tenant and per write policy;
  tenants without a registered policy fail closed (job rejected, nothing
  written).
- R5. Derived facts MUST remain distinguishable from raw episodes and from
  externally reviewed facts (provenance kind on every receipt).
- R6. Consolidation MUST be bounded: finite job queues, explicit scope, no
  unbounded automatic consolidation; volume and cost MUST be observable
  through content-free metrics.
- R7. Semantic embedding is an explicit integration boundary: a registered
  provider configuration enables vector candidates; exact and lexical
  retrieval remain the correctness path and MUST NOT depend on a provider
  being available.

## Acceptance criteria

- [ ] A1. Crash-resume conformance: killing the worker mid-claim resumes the
      job without duplicate or lost claims.
- [ ] A2. Idempotency: replaying the same episodes yields no duplicate facts.
- [ ] A3. Attribution: every derived fact's receipt records model identity,
      policy version, episode lineage, and content hash.
- [ ] A4. Policy gating: without a registered write policy the job fails
      closed and nothing is written.
- [ ] A5. Isolation: consolidation jobs are tenant-scoped; cross-tenant
      leakage scenarios fail closed.
- [ ] A6. Boundedness: queue depth and per-job claim counts are capped and
      observable; a 100,000-episode profile shows no unbounded growth.

## Out of scope

- Training or hosting foundation models; semantic-truth claims.
- Promotion without an attributable write policy (the constitution's
  invariant is absolute).
- Unbounded automatic consolidation; ambient session surveillance.
- Replacing the explicit approved-review path of spec 007.

## Open questions

- Whether enabling automatic promotion is a review-gate change requiring
  founder approval under the constitution's authority model, and which
  interpreter providers are acceptable.
- Confidence thresholds: auto-promote vs. flag-for-review buckets.
- Cadence and scope selection (per project? per case? recency-windowed?).

## Links

Code: `crates/palimpsest-postgres` (worker patterns) ·
`crates/palimpsest-server` (consolidation surface)
Tests: `conformance_postgres18.rs` (worker crash/isolation scenarios as
templates)
Specs: 006 (ingestion) · 007 (existing validated-review flow) · 012
(surfacing)
Decisions: 0008 (durable workflows), 0028 (governed consolidation semantics)
Backlog: previous auto-consolidation entries move here
