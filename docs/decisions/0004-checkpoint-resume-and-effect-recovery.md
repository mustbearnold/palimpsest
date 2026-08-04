# ADR-0004: Checkpoints use a single head and explicit effect recovery

Status: accepted

Date: 2026-07-29

## Context

An interrupted agent must resume a tenant-, subject-, agent-, and thread-scoped checkpoint without silently repeating a successful external effect. PostgreSQL cannot atomically commit an arbitrary third-party side effect, so a completed receipt alone leaves an unsafe crash window between provider success and the checkpoint write. Palimpsest must provide honest recovery semantics without becoming a workflow engine or coupling canonical state to one agent framework.

## Decision

Expose one logical checkpoint-head resource through `GET` and conditional `PUT`. `If-None-Match: *` creates the first revision; a strong `If-Match` advances the current head. Every mutation also requires `Idempotency-Key`. Completed idempotent retries are resolved before the head precondition so a lost response can replay the exact committed outcome.

Each accepted save writes a complete, independently resumable JSON snapshot as an immutable revision. Revisions form one linear parent chain per exact tenant, subject, agent, and thread scope; the case identifier is fixed when that lineage is created. Public deltas, branching, history traversal, workflow scheduling, and provider execution are outside the first checkpoint interface.

External effects use an append-only `prepared` then `completed` lifecycle. Preparation creates a stable effect identifier before execution. The caller uses that identifier as the provider idempotency key or reconciliation reference. After interruption, a prepared effect is retried with the same identifier or reconciled; a completed effect is skipped. Palimpsest does not claim exactly-once behavior for an external provider that supports neither idempotency nor reconciliation.

One PostgreSQL transaction commits the checkpoint revision, head compare-and- swap, effect transition, trusted authorization metadata, redacted audit receipt, outbox intent, and durable idempotency response. The current representation contains the cumulative effect ledger needed to resume. Audit and telemetry may contain bounded identifiers, policy versions, outcomes, counts, timestamps, and digests, but never checkpoint state or effect receipt payloads.

Retention is policy-driven and explicit in the representation. The active head has a server-derived expiry. Expired or hidden checkpoints return the same redacted `404`; cleanup targets the complete composite scope and cannot cascade to another tenant, subject, agent, or thread.

## Consequences

- Callers learn only load and save while concurrency, revisions, recovery, retention, idempotency, audit, and outbox behavior remain behind one deep module interface.
- Full snapshots consume more storage than deltas but keep every accepted head independently resumable and framework-neutral.
- Preparing an effect adds one durable round trip before execution. This is the minimum honest cost of closing the blind-replay window without owning the external effect.
- Framework adapters translate their native checkpoint state into the opaque JSON snapshot and never define Palimpsest scope, authority, or durability.
- Black-box failure scenarios must terminate after provider success but before completion and after completion commit but before response. Both restart against the same PostgreSQL database and prove stable-ID recovery, exact replay, and a single externally applied effect.
