# Local verification

Founder directive 2026-08-08: the development feedback loop is local. GitHub
Actions runs after each push, but no agent blocks on it.

## The gate

`scripts/dev-check.sh` runs the same checks that CI runs. It uses no VM and
no cold cache. The live-database guard runs before every tier.

| Tier | Command | Checks |
| --- | --- | --- |
| fast | `bash scripts/dev-check.sh` | repository contract, MCP adapter tests, Python and TypeScript client tests |
| rust | `bash scripts/dev-check.sh --rust` | `cargo fmt --all --check` and clippy with warnings denied |
| postgres | `bash scripts/dev-check.sh --postgres` | `migrate apply` and the full workspace test suite |
| backup | `bash scripts/dev-check.sh --backup` | backup and PITR conformance (spec 016) |
| all | `bash scripts/dev-check.sh --all` | every tier above |

## Prerequisites

- Fast and rust tiers need the pinned Rust toolchain, `python3`, and `node`.
  They need no database.
- The postgres tier needs a local PostgreSQL 18.4 plus pgvector 0.8.5 cluster
  on 127.0.0.1:55432. See the [quickstart](quickstart.md). The gate creates
  the scratch database `palimpsest_local_ci_db` and the role
  `palimpsest_local_runtime` when they do not exist. Override the
  `PALIMPSEST_MIGRATION_DATABASE_URL`, `PALIMPSEST_TEST_DATABASE_URL`, and
  `PALIMPSEST_LOCAL_SUPERUSER_URL` environment variables to point at another
  cluster.
- The backup tier needs local PostgreSQL tools (`initdb`, `pg_ctl`,
  `pg_basebackup`) on `PATH`. It builds its own scratch clusters on ports
  55434 and 55435 and an in-repo S3 fixture on port 19000. It never touches
  the live database.

## Before you push

Run `bash scripts/dev-check.sh --all`. Push when it passes. Do not wait for
GitHub Actions.
