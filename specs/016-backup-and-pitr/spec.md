# 016 — Provider-managed backup and PITR

## Status

Draft. Spec review round 1 FAIL; fixes applied 2026-08-08 (fence-ledger source pinned, fixture contract named, retention mechanism named, restore sequence pinned). Round 2 review PASS (8/8 conditions fixed, no new blockers).

## Owner

Agent lane: scripts/, palimpsest-postgres, palimpsest-server.

## Purpose

Provide base-backup and WAL capture against a real provider, with documented RPO/RTO evidence. Provide independent backup disposition and restore suppression: fenced and deleted scopes MUST NOT come back through a restore. This spec extends specs/005-restore-and-recovery.

## Decisions (2026-08-08)

1. The first provider contract is S3-compatible storage, per the existing boundary in docs/decisions/0001 and the S3-compatible export package store precedent (docs/decisions/0024).
2. Orchestration for v1 lives in operator scripts under `scripts/`, not in a server-side worker. The spec 011 worker pattern is a later option.
3. The logical-backup rehearsal (`scripts/palimpsest-logical-backup-rehearsal.sh`) remains the guarded baseline fixture.
4. A restore is not complete until restore suppression is proven for the restored scope set.
5. Suppression markers come from the live independent fence ledger (docs/decisions/0011). They are recorded at backup time and re-verified at restore time.

## Requirements

- R1. Base backup and WAL capture MUST run against a real S3-compatible provider. The run MUST record RPO and RTO evidence.
- R2. Backup disposition MUST be independent of the primary store. A fenced or deleted scope MUST stay fenced or deleted after a restore. The suppression gate MUST apply the fence ledger state as of the restore, not as of the backup.
- R3. Backup expiry and retention MUST follow a declared policy. The policy is a named retention policy id per backup job. Expired backups MUST be removed by the orchestration.
- R4. Failure injection MUST cover missing, corrupt, and stale backups. A failed restore MUST be clean and explicit. It MUST NOT silently return partial or resurrected data.
- R5. The rehearsal script MUST keep passing. It stays the guarded fixture for the provider path.
- R6. Restore suppression MUST use the live fence ledger (docs/decisions/0011) as the source of truth. The restore MUST apply the ledger state before any scope becomes visible.

## Design (v1)

- Backup. `pg_basebackup` plus continuous WAL archiving to the provider bucket. The retention policy id is declared in the orchestration script.
- Fixture contract. A1 runs against a local S3-compatible fixture named by the `PALIMPSEST_S3_ENDPOINT` environment variable. The fixture follows the ADR-0024 local HTTP fixture precedent. CI runs without the fixture and skips A1.
- Disposition. The orchestration records the live fence ledger state at backup time. Restore re-reads the live ledger and applies fences after data restore and before acceptance.
- Restore sequence. Restore data -> verify the ledger -> apply fences -> purge -> run the suppression gate -> bind HTTP.
- Evidence. Each rehearsal run writes RPO/RTO numbers and provider round-trip times to the evaluation-report convention.

## Acceptance criteria

- A1. `verify_backup_base_wal` — Given a running S3-compatible fixture (`PALIMPSEST_S3_ENDPOINT` set), When a base backup plus WAL capture completes, Then RPO and RTO evidence is recorded. Without the fixture, the scenario skips.
- A2. `verify_backup_restore_suppression` — Given a fenced scope and a deleted scope, When a restore runs with a fence recorded before the backup and a fence recorded after the backup, Then both scopes stay fenced and deleted after the restore.
- A3. `verify_backup_expiry` — Given a declared retention policy id, When the policy expiry passes, Then the expired backup is removed.
- A4. `verify_backup_failure_injection` — Given a missing backup, a corrupt backup, and a stale backup, When a restore runs, Then each case produces a clean, explicit failure and no silent data loss.
- A5. `verify_backup_rehearsal_guard` — When the logical-backup rehearsal script runs, Then it passes.

## Out of scope

- Server-side worker orchestration (later option, spec 011 pattern).
- Non-S3-compatible providers.
- Restore of scopes that were never fenced or deleted.

## Open questions

- Which WAL archiving mode is used (archive_command versus archive modules) for the first provider run.

## Links

- Issue #38 · specs/005-restore-and-recovery · specs/010-operations · specs/011-governed-consolidation · docs/decisions/0001-postgres-temporal-source-of-truth.md · docs/decisions/0011-restore-fence-ledger-verification.md · docs/decisions/0024-s3-compatible-export-package-store.md · scripts/palimpsest-logical-backup-rehearsal.sh · _attic/evaluations/2026-08-03-logical-backup-rehearsal.md · specs/BACKLOG.md
