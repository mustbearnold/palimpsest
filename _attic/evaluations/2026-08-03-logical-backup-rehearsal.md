# Logical backup rehearsal evaluation

Date: 2026-08-03

Profile: PostgreSQL 18.4 plus pgvector 0.8.5, Palimpsest schema version 16, custom-format `pg_dump` restored into a separate empty database

## Evidence

`scripts/palimpsest-logical-backup-rehearsal.sh` was run against the local Palimpsest database and a newly created isolated restore database. The script validated the custom archive listing, restored it with `pg_restore --exit-on- error`, and compared content-free probes for server version, pgvector version, maximum SQLx migration, episode rows, fact-revision rows, and checkpoint rows.

The result was:

```json
{"backup_profile":"postgresql-logical-custom-v1","dump_sha256":"23d8ac29ce3a2e6a5b61f1d0ee4243cbf036d039d6437df0670052a05d2dff34","dump_size_bytes":560961,"schema_version":16,"vector_version":"0.8.5","probe_equal":true}
```

The temporary archive, probes, and isolated restore database were removed after the run. The same-database guard and the existing-memory-schema target guard are checked before any dump or restore operation.

## Boundary

This is logical dump/restore portability evidence for the stated local profile. It is not proof of PostgreSQL base-backup or WAL-archive recovery, PITR, backup-expiry or deletion disposition, object-store/cache recovery, or a production RPO/RTO.
