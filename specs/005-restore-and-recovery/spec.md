# 005 — Restore and recovery

Status: active
Owner: AI CEO

## Purpose

Fail-closed restore replay and guarded backup rehearsal so durability and
recovery claims are proven rather than assumed.

## Requirements

- R1. Restore MUST require a separately privileged database authority,
  a content-free fence ledger plus its SHA-256 digest, and an explicit
  `restore` subcommand (or environment-driven restore mode); normal serving
  credentials MUST NOT be sufficient.
- R2. `restore verify` MUST be read-only and MUST report only profile,
  schema, entry count, generation time, and ledger digest.
- R3. `restore apply` MUST verify the ledger, replay every matching scope's
  canonical and derived-data purge (including the current projection and its
  coverage marker), record an idempotent content-free receipt, and check the
  returned counts and ledger digest internally before exiting without binding
  HTTP.
- R4. Missing, stale, corrupt, or unmatched ledger evidence MUST fail closed.
- R5. The logical-backup rehearsal MUST use PostgreSQL custom-format dump,
  verify the archive, restore into an isolated empty database, and compare
  only content-free schema/extension/row-count probes; it MUST refuse to
  restore over a database that already has the `memory` schema and MUST NOT
  print either connection URL.

## Acceptance criteria

- [ ] A1. `restore_verify.rs` and the restore-fence conformance scenarios
      pass (replay hidden over HTTP, ledger verification, residual counts).
- [ ] A2. `scripts/palimpsest-logical-backup-rehearsal.sh` passes against an
      isolated empty restore database.
- [ ] A3. Restore mode is disabled by default and fails closed when enabled
      without a verified current ledger.

## Out of scope

- Base-backup/WAL/PITR recovery, backup expiry, and production RPO/RTO
  evidence; provider-managed backup orchestration.

## Open questions

- Provider-managed backup/PITR orchestration and independent backup
  disposition (backlog).

## Links

Code: `crates/palimpsest-server` (restore subcommand) ·
`scripts/palimpsest-logical-backup-rehearsal.sh`
Tests: `restore_verify.rs` · `conformance_postgres18.rs`
Decisions: 0011, 0017, 0023
Evidence: `_attic/evaluations/2026-08-02-restore-fence-replay.md` ·
`_attic/evaluations/2026-08-03-logical-backup-rehearsal.md`
