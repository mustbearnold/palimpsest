# 003 — Subject lifecycle and deletion

Status: active
Owner: AI CEO

## Purpose

Effective, auditable removal of a subject's memory: a monotonic lifecycle
fence, bounded content leases for in-flight disclosure, durable deletion
operations, and content-free tombstones. Deletion removes derived indexes, not
just canonical rows.

## Requirements

- R1. The subject lifecycle fence MUST be monotonic: fenced subjects MUST NOT
  receive new content leases and MUST NEVER return to active.
- R2. Deletion MUST be a durable, independently authorized workflow that
  fences the subject, drains or revokes in-flight content leases, purges
  configured live targets (canonical records, derived indexes, projections,
  and the coverage marker), verifies absence, and records a minimal
  content-free tombstone.
- R3. Content leases MUST be bounded, subject-scoped grants held by
  content-producing responses, storing no response content.
- R4. Tombstones MUST be retention-governed and content-free: they MUST NOT
  contain raw subject or memory identifiers or deleted payload digests.
- R5. Export and deletion operations MUST be independently authorized and
  durable; request payloads MUST NOT grant a principal authority.
- R6. Deletion MUST be effective rather than cosmetic: derived indexes and
  projections MUST be accounted for in residual checks.

## Acceptance criteria

- [ ] A1. The `subject_lifecycle_postgres18.rs` suite passes: a pending
      subject is hidden from existing HTTP reads and writes; residual
      accounting includes the current projection and coverage marker.
- [ ] A2. Deletion worker scenarios pass: lease recovery after worker expiry,
      failed-operation repair and resume, retry exhaustion remains fenced,
      worker fails closed when the export store is unavailable.
- [ ] A3. Deletion authorization revocation fails closed.

## Out of scope

- Legal-rights determinations (export and deletion operations are not
  legal-rights determinations).
- Provider-specific artifact/object deletion, revocation, outage, and
  recovery evidence.

## Open questions

- Provider-native artifact deletion and recovery evidence (backlog).

## Links

Code: `crates/palimpsest-postgres` · `crates/palimpsest-server`
Tests: `subject_lifecycle_postgres18.rs` · `conformance_postgres18.rs`
Decisions: 0008, 0010
