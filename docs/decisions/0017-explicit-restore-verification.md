# ADR-0017: Explicit restore-ledger verification

Status: accepted

Date: 2026-08-03

## Context

Palimpsest already has a provider-neutral deletion-fence ledger and a privileged replay path, but operators previously had to enter the environment-driven restore mode to exercise the verifier. That makes a non-mutating preflight harder to automate and blurs the distinction between checking recovery evidence and applying it to an isolated database.

## Decision

Expose two explicit restore operations on `palimpsest-server`:

- `restore verify` reads `PALIMPSEST_RESTORE_FENCE_LEDGER_PATH`, verifies it against `PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256` and the current time, and returns content-free JSON without opening a database connection; and
- `restore apply` runs the existing privileged replay against `PALIMPSEST_RESTORE_DATABASE_URL`, with the same ledger verification before any database mutation.

Verification failures return a nonzero exit status and a fixed error code. Successful verification reports only the ledger profile, schema version, generation timestamp, entry count, and document digest. It never reports scope digests, ledger paths, file contents, database URLs, or query errors. The environment-driven `PALIMPSEST_RESTORE_MODE=1` path remains compatible for existing automation; it is equivalent to the apply operation and still exits without binding HTTP.

## Consequences

Deployment and restore automation can gate a recovered database on an explicit read-only evidence check before invoking the mutating replay. The command boundary remains provider-neutral: it does not create backups, inspect WAL, make a backup-disposition claim, or replace the independent restore-fence ledger and negative conformance requirements.
