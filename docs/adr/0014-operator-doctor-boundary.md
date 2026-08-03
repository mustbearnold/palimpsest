# ADR-0014: Read-only operator doctor boundary

Status: accepted

Date: 2026-08-03

## Context

Self-hosted operators need a machine-readable way to distinguish an
unavailable service prerequisite from an application failure before starting
HTTP traffic. Readiness is intentionally an HTTP process probe, while startup
currently owns the local development migration path. A diagnostic command must
not silently start the server, apply migrations, or echo connection credentials.

## Decision

Add `palimpsest-server doctor` as a read-only operator seam. It connects using
`PALIMPSEST_DATABASE_URL`, prints content-free JSON, and exits successfully only
when all of these checks pass:

- PostgreSQL is version 18 or newer;
- the `vector` extension is exactly pgvector 0.8.5;
- the complete checked-in SQLx migration range is applied with no failed rows;
- the required lifecycle tables exist; and
- the connected role can log in but is neither a superuser nor a role that
  bypasses row-level security.

The command reports fixed check names and failure codes, never includes the
database URL or query errors, and returns a nonzero status for unavailable or
incompatible prerequisites. `--help` is content-free usage text. Unknown
subcommands fail instead of falling through to HTTP server startup.

This is a diagnostic boundary, not a migration authority or a backup/restore
provider. Future `migrate plan/apply/status` and backup/restore commands must
remain separate explicit operations with their own privilege and recovery
contracts.

## Consequences

Deployment automation can gate process startup on a stable JSON result without
scraping logs or touching memory data. The runtime role check makes the
least-privilege expectation executable. The existing local launcher remains
convenient, while operators get an explicit read-only check that cannot mutate
the database.
