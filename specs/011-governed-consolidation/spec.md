# 011 — Governed consolidation

Status: active
Owner: AI CEO

## Purpose

Durable server-side jobs derive structured facts and summaries from raw
episodes. A model interprets the episodes. Every claim passes an attributable
boundary before it becomes durable memory. No model output becomes durable
memory without a registered write policy.

This is the missing middle of the autonomous memory lifecycle. Spec 006
ingests raw episodes automatically. This spec turns them into governed facts
automatically. Spec 012 surfaces those memories automatically. The explicit
review flow (spec 007) remains the conservative default. Automatic
consolidation is an opt-in, policy-governed path.

## Decisions (2026-08-07 finalization)

- Automatic promotion stays inside the attributable-policy invariant
  (constitution principle 13). Policy registration is the gate. No
  constitution change is required.
- Confidence thresholds are per-policy. A policy states the minimum
  confidence for automatic promotion. A claim below the threshold is skipped
  with the reason `low_confidence`. It never enters an ambient review queue.
- Scope selection is explicit per job. A job names one subject and a recency
  window. No ambient session surveillance exists.
- Interpreter providers are opt-in integrations. Version 1 ships a
  deterministic fixture provider for tests. No default provider exists. A job
  without a registered interpreter fails closed.
- Determinism is workflow-level. The job records the interpretation with its
  input digest. A retry replays the recorded claims. A retry never re-derives.

## Requirements

- R1. Consolidation MUST run as durable server-side work items with
  claim-level retry: idempotency keys per claim, crash-resumable state, and
  typed partial-completion results, following the established export and
  deletion worker patterns.
- R2. Every automatic derivation MUST pass through an attributable model
  boundary. The interpretation step MUST record model identity,
  prompt/policy version, source episode lineage, content hash, confidence,
  sensitivity, and valid-time metadata before any claim is written. The
  write policy is the gate: no claim MAY become durable memory without a
  registered write policy for the tenant and source kind.
- R3. Replaying the same episodes with the same policy version MUST produce
  the same claims. The job stores the interpretation with its input digest.
  A retry replays the recorded claims. A retry never re-derives.
- R4. Automatic consolidation MUST be opt-in per tenant and per write
  policy. Tenants without a registered policy fail closed: the job is
  rejected and nothing is written.
- R5. Derived facts MUST remain distinguishable from raw episodes and from
  externally reviewed facts. Every receipt carries provenance kind
  `derived`.
- R6. Consolidation MUST be bounded: finite job queues, explicit scope, no
  unbounded automatic consolidation. Volume and cost MUST be observable
  through content-free metrics.
- R7. Semantic embedding is an explicit integration boundary: a registered
  provider configuration enables vector candidates. Exact and lexical
  retrieval remain the correctness path and MUST NOT depend on a provider
  being available.

## Design (v1)

### Interpreter boundary

The application defines an interpreter port. A provider accepts a scope, a
policy snapshot, and a set of episodes. It returns claims. The built-in
`fixture-deterministic-v1` provider derives claims by a pure deterministic
rule. External providers are opt-in integrations. The port records the
provider identity and config digest on every claim.

### Policy registry

`memory.consolidation_policies` holds one row per
(tenant, source_kind, policy_id): interpreter config reference, write policy
id and version, auto-promote confidence minimum, enabled flag. RLS FORCE and
scope GUCs apply.

### Jobs and claims

`memory.consolidation_jobs` holds job state: `pending -> running ->
complete | failed`, with scope, policy snapshot, claim counts, and caps.
`memory.consolidation_claims` holds claim state: `pending -> leased -> done`,
with episode lineage, content hash, confidence, sensitivity, valid time, and
a deterministic idempotency key from the claim id. Workers claim and renew
leases exactly like the deletion worker. A job is never failed while any
claim is still leased with an unexpired lease: another worker pass owns
that claim, so the pass defers and leaves the job running until the pass
completes it (issue #47).

### API surface

Version 1 adds:

- POST /v1/tenants/{tenant_id}/consolidation-interpreter-configs
- POST /v1/tenants/{tenant_id}/consolidation-policies
- GET /v1/tenants/{tenant_id}/consolidation-policies/{source_kind}/{policy_id}
- POST /v1/tenants/{tenant_id}/subjects/{subject_id}/consolidations
  (requires Idempotency-Key; rejects when no policy exists)
- GET /v1/tenants/{tenant_id}/subjects/{subject_id}/consolidations/{job_id}

Claim writes reuse the governed fact path: `create_fact` with the policy
snapshot and the per-claim idempotency key. The write audit receipt records
provenance kind `derived`. A derived fact stays distinguishable by writer
identity and write policy on its revision rows.

### Metrics

`palimpsest_consolidation_jobs_*`, `palimpsest_consolidation_claims_*`, and
cost counters are content-free. The metrics surface version bumps with this
change.

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
- External interpreter providers in version 1 (opt-in integrations only).

## Links

Code: `crates/palimpsest-postgres` (worker patterns) ·
`crates/palimpsest-server` (consolidation surface)
Tests: `conformance_postgres18.rs` (worker crash/isolation scenarios as
templates)
Specs: 006 (ingestion) · 007 (existing validated-review flow) · 012
(surfacing)
Decisions: 0008 (durable workflows), 0028 (governed consolidation semantics)
Backlog: previous auto-consolidation entries move here
