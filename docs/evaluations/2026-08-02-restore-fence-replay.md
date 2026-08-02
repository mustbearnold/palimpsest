# Restore-fence replay evaluation

Date: 2026-08-02
Commit: `d6fa1429bff56c5ff298accc36bac8845fb3f7fd`
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

## Evidence

- The Rust conformance fixture inserts a private episode, rejects an incorrect
  independently supplied digest, successfully purges the matched scope,
  verifies the deleted lifecycle and zero episode rows through both the
  privileged and scoped runtime connections, and verifies that a normal HTTP
  client receives a redacted `404` without the payload or episode identifier.
  It repeats the replay to prove idempotency.
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

This is deterministic replay and purge evidence, not a production recovery
claim. Backup/PITR and object-storage adapters, backup-expiry disposition,
cache-loss evidence, fault injection after every external effect, a full
black-box pre-deletion recovery fixture, and broad negative HTTP conformance
across the full export/deletion corpus remain unproven.
