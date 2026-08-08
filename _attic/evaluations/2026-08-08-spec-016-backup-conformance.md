# spec 016 backup conformance — local run evidence

Date: 2026-08-08
Host: CachyOS (user machine)
PostgreSQL: 18.4 (scratch cluster, initdb + archive_mode)
S3 fixture: in-repo example binary (`backup_s3_fixture`), in-memory
Runner: `scripts/test_palimpsest_backup_conformance.sh`
Result: **PASS** — `A1:pass A2:pass A3:pass A4:pass A5:pass` (exit 0)

## Scenarios

| ID | Scenario | Evidence |
|----|----------|----------|
| A1 | Base backup + WAL capture vs S3 fixture | create JSON: status complete, wal_from/wal_to derived from LSN after pg_basebackup end-switch, base_sha256 64-hex, base_size_bytes > 0, rpo_estimate_ms >= 0, fence_entry_count 1 (fence before backup) |
| A2 | Restore suppression: fence before + after backup | Restored cluster: recovery through wal_to via restore_command fetch-wal, promote, timeline switch (00000002.history archived), replay purged subject_one (lifecycle probe `deleted:1`), subject_two vacuous (fenced after backup; no row in the copy), residual_rows 0, source corpus untouched |
| A3 | Backup expiry | Second create on same cluster (idle WAL) succeeded; expire with retention 1s removed both backups (fixture DELETE + index rewrite); fetch after expiry blocked `base-not-indexed` |
| A4 | Failure injection | wiped store → `base-not-indexed`; deleted base object → `base-missing`; corrupted base bytes → `base-corrupt`; max_age 1s → `backup-stale`; unknown WAL segment → `wal-missing` |
| A5 | Logical rehearsal guard | Rehearsal script (schema + data + identity probe) on a separate restore DB: equal probes, exit 0 |

## Notable fixes during the run

1. **WAL end-switch semantics**: `pg_basebackup` finalizes with a WAL switch; the second `pg_switch_wal()` returns an empty never-archived segment. `wal_to` is now derived as the segment before `pg_current_wal_lsn()` after the base backup (`wal_previous` helper), matching the end-switch.
2. **`archive_mode` restart**: `ALTER SYSTEM SET archive_mode` needs a server restart, not a reload. The operator script now bails with a clear message if `archive_mode` is not `on`; the conformance runner restarts its scratch cluster.
3. **Fence ledger SHA plumbing**: `restore verify`/`apply` expect the digest *value*; the script passed the file *path*. Both call sites fixed.
4. **Restored cluster must not inherit archiving**: `postgresql.auto.conf` from the source cluster (archive_mode=on) overrides the restore's `archive_mode=off`; the restore removes `postgresql.auto.conf` before start.
5. **Replay of scopes fenced after the backup** (spec A2): the ledger digest is an HMAC over tenant+subject IDs. A scope fenced after the backup has no lifecycle row in the restored copy — the replay now (a) resolves ledger scopes against data-bearing subjects (`0025_replay_resolve_data_scopes.sql`) and (b) treats an absent scope as vacuously satisfied instead of aborting. `0024_restore_purge_missing_scope.sql` makes `restore_purge_scope` insert the `deleted` lifecycle row when the copy lacks it.
6. **sqlx migrate embed staleness**: the `migrate!` macro embeds `migrations/` at palimpsest-postgres compile time; cargo does not fingerprint the directory. The runner touches the crate's lib.rs before building so migration changes always land in the binary.

## Artifacts

- `scripts/palimpsest-backup.sh` — operator create/expire/restore
- `scripts/test_palimpsest_backup_conformance.sh` — the runner (A1-A5)
- `scripts/palimpsest-logical-backup-rehearsal.sh` — A5 guard (existing)
- `crates/palimpsest-server/examples/backup_s3_fixture.rs` — S3-compatible fixture with request logging + `__wipe`
- `migrations/0024_restore_purge_missing_scope.sql`, `migrations/0025_replay_resolve_data_scopes.sql`
- `.github/workflows/ci.yml` — `Run backup and PITR conformance` step
