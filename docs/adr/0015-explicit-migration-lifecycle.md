# ADR-0015: Explicit operator-owned migration lifecycle

Status: accepted

Date: 2026-08-03

## Context

The service used to apply SQLx migrations while starting the HTTP process.
That couples schema mutation to application availability, requires the runtime
identity to own migration privileges, and makes an incompatible deployment
harder to diagnose or roll back safely. The operations baseline requires
separate migrate status, migrate plan, and migrate apply roles.

## Decision

Expose these explicit commands on palimpsest-server:

- migrate status reports the current database, migration ledger presence,
  applied and failed versions, unknown versions, checksum mismatches, pending
  versions, transaction mode, and lock availability;
- migrate plan reports the same content-free plan without applying SQL; and
- migrate apply takes the stable Palimpsest PostgreSQL advisory lock and runs
  the embedded forward SQLx migrations, then reports the resulting status.

The command prefers PALIMPSEST_MIGRATION_DATABASE_URL and falls back to
PALIMPSEST_DATABASE_URL for local development. It never prints a connection
URL, migration SQL, query error, or memory content. HTTP startup no longer runs
migrations. The local launcher performs migrate apply before starting HTTP;
deployment automation should do the same with a privileged migrator identity
before handing the database to the restricted runtime identity.

Migrations remain forward-only and checksum-validated by SQLx. This decision
does not add down-migrations, online schema-change orchestration, backup/PITR,
or rollback claims for destructive data changes.

## Consequences

Schema changes become an attributable, observable deployment step. A service
can be restarted without unexpectedly mutating durable storage, and a runtime
role can be denied migration privileges. The explicit command remains usable in
the one-command local development path because the launcher invokes it first.
