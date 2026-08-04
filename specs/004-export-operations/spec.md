# 004 — Export operations

Status: active
Owner: AI CEO

## Purpose

Durable, independently authorized exports that freeze an immutable membership
manifest and materialize a versioned canonical-history package, inspectable
and portable by the end user.

## Requirements

- R1. An export operation MUST be durable and independently authorized,
  freezing an immutable membership manifest before materialization.
- R2. Export packages MUST contain canonical history with provenance, not
  derived summaries; the manifest MUST be versioned.
- R3. The default store MUST be a private filesystem path; an S3-compatible
  store MUST be selectable only with a complete configuration
  (endpoint, bucket, region, access key, secret) and MUST fail startup on
  partial configuration rather than silently falling back.
- R4. The S3 store MUST use SigV4 signing, conditional publication, retry
  comparison, and delete-already-absent semantics.
- R5. Export is NOT a legal-rights determination.

## Acceptance criteria

- [ ] A1. Export worker conformance scenarios pass: lease recovery fences
      stale completion; workers fail closed on store failure and on
      authorization revocation.
- [ ] A2. The S3-compatible adapter passes its contract tests against a local
      object-shaped fixture.
- [ ] A3. Export manifests are authorized per principal; cross-tenant export
      attempts fail closed.

## Out of scope

- Live provider durability, deletion, outage, and recovery evidence (the S3
  adapter is fixture-tested only).

## Open questions

- Live object-store evidence; export store durability guarantees.

## Links

Code: `crates/palimpsest-postgres` · `crates/palimpsest-server`
Tests: `conformance_postgres18.rs` (export scenarios)
Decisions: 0008, 0024
Evidence: `_attic/evaluations/2026-08-02-export-deletion-recovery.md`
