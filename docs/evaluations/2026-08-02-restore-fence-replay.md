# Restore-fence replay evaluation

Date: 2026-08-02
Baseline implementation commit: `d6fa1429bff56c5ff298accc36bac8845fb3f7fd`
Profile: PostgreSQL 18 plus pgvector 0.8.5, forced RLS, privileged restore authority

## Outcome

The PostgreSQL-backed development slice now has an executable restore-fence
replay path. Restore mode verifies the independent content-free ledger in Rust,
connects through `PALIMPSEST_RESTORE_DATABASE_URL`, invokes the migration-owned
replay function, and exits without starting HTTP. The serving database URL and
normal serving migrations are not used by this path.

For every ledger entry, the replay matches the opaque HMAC-backed scope digest
to a subject lifecycle, purges canonical memory plus retrieval, embedding,
lexical, checkpoint-effect, outbox, idempotency, audit, export, and lease rows,
transitions the subject to `deleted`, checks for residual rows, and records a
content-free idempotent receipt keyed by the ledger digest. A repeated replay
returns the recorded result without re-running the purge.

The conformance fixture also creates a PostgreSQL template copy after the
public HTTP restore corpus is populated. That pre-deletion copy serves the
private fixture before replay, then the real restore-mode binary applies the
independent ledger and the same public endpoint returns a redacted `404`.
This is a database-copy rehearsal, not a claim about PostgreSQL PITR or a
backup provider.

## Evidence

- The Rust conformance fixture inserts a private episode, rejects an incorrect
  independently supplied digest, successfully purges the matched scope,
  verifies the deleted lifecycle and zero episode rows through both the
  privileged and scoped runtime connections, and verifies that a normal HTTP
  client receives a redacted `404` without the payload or episode identifier.
  It repeats the replay to prove idempotency. Mismatched-digest and
  unmatched-scope ledgers are rejected before purge, with the original
  episode still present after each rejected attempt.
- The same fixture now seeds the restore scope through the public HTTP write
  paths for episodes, fact revisions and evidence, lexical retrieval receipts,
  and resumable checkpoints. It requires those canonical, projection, receipt,
  and checkpoint rows to exist before replay, compares every durable scoped row
  count after each rejected ledger, and requires every replay-purge residual
  count to be zero.
- The conformance test also launches the real `palimpsest-server` restore-mode
  binary. A wrong expected digest exits before database mutation; a verified
  ledger exits successfully, and a second process invocation proves the
  content-free replay receipt is idempotent.
- The pre-deletion copy is exercised through a worker-free public HTTP router
  before and after restore-mode, so the negative result is not inferred only
  from privileged row counts.
- Application tests cover missing, malformed, unsupported, stale, future,
  unordered, duplicate, noncanonical, and digest-mismatched ledgers without
  echoing ledger content in errors.
- Local gates passed: `bash scripts/check-repo.sh`, `cargo fmt --all --
  --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`,
  `cargo test --locked --workspace`, the PostgreSQL conformance test, and
  `npm exec --yes @redocly/cli@2.18.1 lint api/openapi.yaml`.
- Push-triggered GitHub CI run `30738238361` and Repository quality run
  `30738238389` both passed for this commit. The exact remote `main` SHA is
  `d6fa1429bff56c5ff298accc36bac8845fb3f7fd`.

## Boundary

This is deterministic replay and database-copy recovery evidence, not a
production recovery claim. Backup/PITR and object-storage adapters,
backup-expiry disposition, cache-loss evidence, fault injection after every
external effect, and broad negative HTTP conformance across the full
export/deletion corpus remain unproven.
