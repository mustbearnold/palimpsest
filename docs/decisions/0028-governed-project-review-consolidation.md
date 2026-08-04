# ADR-0028: Governed project-review consolidation

Status: accepted

Date: 2026-08-03

## Context

Project-separated recall and structural comparison make evidence from several repositories visible without mixing their candidate sets. An external model or human can then interpret that evidence, and `validate_project_review` checks the interpretation's citations and attribution. Validation alone is not a useful product workflow if a caller still has to hand-build an unrelated fact write, but promoting an unreviewed summary automatically would violate the canonical-memory and provenance invariants.

## Decision

Add `consolidate_project_review` to the dependency-free Python and TypeScript clients and to the local MCP adapter. The operation accepts a prior structural comparison, the external review, an explicit list of selected claim write plans, and a caller-chosen `consolidation_id`.

Before the first HTTP request, the client validates the review and the write plans. A plan must provide its claim ID, namespace, key, value, observed time, valid-time object, registered write policy, confidence, sensitivity, and retention policy. It must not provide episode IDs or an idempotency key:

- episode IDs are the sorted union of the source episode citations on the validated claim, so the caller cannot silently substitute unrelated lineage;
- `insufficient_evidence` claims cannot be written; and
- the client hashes the consolidation ID and claim ID into a deterministic per-claim idempotency key.

Each selected claim is sent as an ordinary governed `create_fact` request to the canonical HTTP service. The service remains authoritative for tenant and subject authorization, case scope, temporal validation, registered write policy, evidence existence, revision persistence, and writer attribution. The client returns the validated reviewer and policy metadata alongside the fact receipts. It reports the committed prefix and failed plan through a typed partial-consolidation error when a later request fails.

This is a client-coordinated sequence, not a new server-side transaction or atomic batch. A retry must reuse the same consolidation ID and identical write inputs. No model call is made, no semantic truth is established, and no automatic raw-session consolidation is performed. The caller remains responsible for obtaining explicit approval before invoking the writing operation.

## Consequences

The Python, TypeScript, and MCP surfaces now support a complete, attributable cross-project review path: isolate evidence, compare it, validate an external interpretation, and explicitly promote selected claims through the existing governed fact seam. Retry keys and derived episode lineage make partial outcomes recoverable without duplicate fact revisions.

The workflow remains provider-neutral and does not store model prompts or raw review transcripts. Server-side atomic consolidation jobs, automatic model selection, semantic truth evaluation, and raw-episode summarization remain future capabilities rather than hidden promises.
