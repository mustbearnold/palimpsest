# ADR-0011: Verify an independent restore fence ledger

Status: accepted

Date: 2026-08-02

## Context

PostgreSQL point-in-time recovery can restore rows from before a subject
deletion. The deletion tombstone in the serving database is therefore not
enough evidence for a restore: a pre-deletion database copy may not contain
that tombstone at all. A restore path needs an independent, content-free
deletion-fence ledger and must fail closed when that evidence is missing,
stale, corrupt, or unverifiable.

Palimpsest does not currently own a PostgreSQL backup, WAL, object-storage, or
restore provider. It must not turn an operator assertion or a transaction-local
row into a claim that backup deletion safety has been proven.

## Decision

The application exposes a small, provider-neutral restore primitive named
`palimpsest-deletion-fence-ledger-v1`. Its deterministic JSON document contains
only:

- the versioned profile and schema number;
- a generation timestamp;
- sorted unique opaque versioned scope digests, deletion state versions,
  deletion watermarks, and fence expiry timestamps; and
- a SHA-256 digest over the unsigned canonical document.

Verification requires both the ledger bytes and an independently supplied
expected digest. It rejects missing input, unknown profile or schema, malformed
scope or timestamp metadata, future watermarks, expired fences, duplicate or
unordered entries, digest mismatches, and non-canonical JSON. Errors are
content-free. A valid ledger is necessary evidence for a restore path; it does
not by itself claim that backups, later tombstones, purge reruns, derived-index
rebuilds, or negative conformance have completed.

Restore automation must keep a recovered database out of the serving role until
it has verified this ledger, applied the ledger's fences, rerun canonical and
derived purges, and passed the negative conformance suite. The server now has a
privileged replay runner for this bounded step: with
`PALIMPSEST_RESTORE_MODE=1`, it reads the independent ledger, verifies it in
application code, connects through the separately supplied
`PALIMPSEST_RESTORE_DATABASE_URL`, invokes the migration-owned replay function,
checks the returned scope counts and ledger digest, and exits without binding
HTTP.
The replay matches opaque digests against the database's HMAC-backed scope
function, purges canonical and derived/export rows, transitions matched
subjects to `deleted`, checks for residual rows, and records an idempotent
content-free receipt. It does not run normal serving migrations or use the
serving database URL.

The replay runner is not a backup/PITR adapter and does not prove backup
disposition, a complete restore rehearsal, or the negative HTTP conformance
gate. Those remain separate v1 readiness work; backup disposition remains
`not_configured`.

The deletion authority produces the opaque scope digests from its HMAC-backed
scope key. This verifier checks their versioned shape and the ledger's
independently supplied document digest; it cannot prove HMAC origin without
scope identifiers and key material. A trusted ledger exporter and restore
control plane therefore remain responsible for authenticating the source of
the entries.

## Consequences

- Restore tooling has one deterministic, testable format instead of inventing
  ad hoc JSON or trusting a database dump.
- Missing or stale independent evidence has an explicit fail-closed result.
- The verifier cannot be mistaken for a backup provider or a completed restore
  rehearsal; those remain separate v1 readiness work.
- A fresh ledger is required while any recorded fence is still within its
  retention window. Expiry evidence and backup policy belong to the future
  restore adapter, not to this content-free format.
