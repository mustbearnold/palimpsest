# ADR-0023: Guarded logical backup rehearsal

Status: accepted

Date: 2026-08-03

## Context

Palimpsest already verifies a database-copy restore fence replay, but operators still need a repeatable way to test that a PostgreSQL logical backup can be restored into an isolated database without confusing that portability check with PITR or a subject export.

## Decision

Add `scripts/palimpsest-logical-backup-rehearsal.sh`. It requires explicit `PALIMPSEST_BACKUP_SOURCE_URL` and `PALIMPSEST_BACKUP_RESTORE_URL` values, refuses to continue when the two connections identify the same database, and refuses to restore over a target containing the Palimpsest `memory` schema. It creates a private temporary custom-format `pg_dump`, validates the archive listing, restores with `pg_restore --exit-on-error`, then compares source and restore probes for server version, pgvector version, migration maximum, and selected canonical row counts. The temporary dump and probes are removed on exit, and the output contains only a digest, size, versions, and equality status; connection URLs and memory values are never printed.

The rehearsal is a logical dump/restore portability check. It does not create base backups, inspect or archive WAL, prove PITR, make backup-expiry or deletion disposition claims, or establish production RPO/RTO.

## Consequences

Self-hosters gain a guarded, reproducible backup smoke test that can be run against a separately provisioned restore database. The explicit empty-target requirement makes the operation reversible and prevents an operator mistake from overwriting a live Palimpsest database. A provider-specific backup/PITR adapter and production-shaped restore gate remain future work.
