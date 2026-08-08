# 016 — Provider-managed backup and PITR

## Purpose

Provide base-backup and WAL capture against a real provider, with documented RPO/RTO evidence. Provide independent backup disposition and restore suppression: fenced and deleted scopes must not come back through a restore. This spec extends specs/005-restore-and-recovery.

## Decisions (2026-08-08 draft)

1. The first provider contract is S3-compatible storage, per the existing boundary in docs/decisions/0001.
2. Orchestration for v1 lives in operator scripts under `scripts/`, not in a server-side worker. The spec 011 worker pattern is a later option.
3. The logical-backup rehearsal (`scripts/palimpsest-logical-backup-rehearsal.sh`) remains the guarded baseline fixture.
4. A restore is not complete until restore suppression is proven for the restored scope set.

## Requirements

- R1. Base backup and WAL capture run against a real S3-compatible provider. The run records RPO and RTO evidence.
- R2. Backup disposition is independent of the primary store. A fenced or deleted scope stays fenced or deleted after any restore.
- R3. Backup expiry and retention follow a declared policy. Expired backups are removed by the orchestration.
- R4. Failure injection covers missing, corrupt, and stale backups. A failed restore is clean and explicit. It never silently returns partial or resurrected data.
- R5. The rehearsal script keeps passing. It stays the guarded fixture for the provider path.
- R6. Restore suppression uses the fence and deletion markers from the canonical store. The restore applies those markers before any scope becomes visible.

## Design (v1)

- Backup. `pg_basebackup` plus continuous WAL archiving to the provider bucket. Retention policy is declared in the orchestration script.
- Disposition. Backup metadata records the fence and deletion markers at backup time. Restore replays the canonical markers after data restore and before acceptance.
- Restore. Reuse the replay path from spec 005. Add the suppression check as a mandatory post-restore gate.
- Evidence. Each rehearsal run writes RPO/RTO numbers and provider round-trip times to the evaluation-report convention.

## Acceptance criteria

- A1. `verify_backup_base_wal` — a base backup plus WAL capture completes against the real provider. RPO and RTO evidence is recorded.
- A2. `verify_backup_restore_suppression` — a fenced scope and a deleted scope are restored from backup. Both stay fenced and deleted after restore.
- A3. `verify_backup_expiry` — expired backups are removed per the retention policy.
- A4. `verify_backup_failure_injection` — a missing backup, a corrupt backup, and a stale backup each produce a clean, explicit restore failure. No silent data loss.
- A5. `verify_backup_rehearsal_guard` — the logical-backup rehearsal script passes.

## Out of scope

- Server-side worker orchestration (later option, spec 011 pattern).
- Non-S3-compatible providers.
- Restore of scopes that were never fenced or deleted.

## Links

- Issue #38 · specs/005-restore-and-recovery · specs/010-operations · specs/011-governed-consolidation · docs/decisions/0001-postgres-temporal-source-of-truth.md · scripts/palimpsest-logical-backup-rehearsal.sh · _attic/evaluations/2026-08-03-logical-backup-rehearsal.md · specs/BACKLOG.md
