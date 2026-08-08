#!/usr/bin/env bash
# Local development gate (founder directive 2026-08-08).
#
# This gate is the development feedback loop for this repository. GitHub
# Actions still runs after each push, but no developer blocks on it. Every
# tier here runs the same checks that CI runs, with no VM provisioning and no
# cold caches.
#
# Tiers:
#   bash scripts/dev-check.sh              fast tier (seconds, no database)
#   bash scripts/dev-check.sh --rust       add cargo fmt and clippy
#   bash scripts/dev-check.sh --postgres   add migrations and workspace tests
#   bash scripts/dev-check.sh --backup     add backup and PITR conformance
#   bash scripts/dev-check.sh --all        every tier
#
# The --postgres tier defaults to the local development cluster on
# 127.0.0.1:55432 with a scratch test database. Override the PALIMPSEST_*
# environment variables to point at another cluster. The live-database guard
# runs before any tier.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
cd "$project_root"

mode_rust=false
mode_postgres=false
mode_backup=false

for arg in "$@"; do
    case "$arg" in
    --rust) mode_rust=true ;;
    --postgres) mode_postgres=true ;;
    --backup) mode_backup=true ;;
    --all)
        mode_rust=true
        mode_postgres=true
        mode_backup=true
        ;;
    *)
        echo "unknown option: $arg" >&2
        echo "usage: scripts/dev-check.sh [--rust] [--postgres] [--backup] [--all]" >&2
        exit 2
        ;;
    esac
done

step() {
    printf '\n=== %s ===\n' "$1"
}

"$script_dir/guard-palimpsest-db-env.sh"

step "repository contract"
bash "$script_dir/check-repo.sh"

step "MCP adapter tests"
python3 tools/test_palimpsest_mcp.py

step "Python client tests"
python3 -m unittest discover -s clients/python/tests -p 'test_*.py'

step "TypeScript client tests"
node --test clients/typescript/test/*.test.mjs

if [[ "$mode_rust" == true ]]; then
    step "cargo fmt --all --check"
    cargo fmt --all --check

    step "cargo clippy --locked --workspace --all-targets -- -D warnings"
    cargo clippy --locked --workspace --all-targets -- -D warnings
fi

if [[ "$mode_postgres" == true ]]; then
    local_superuser_url="${PALIMPSEST_LOCAL_SUPERUSER_URL:-postgresql://$(id -un)@127.0.0.1:55432/postgres}"
    migration_url="${PALIMPSEST_MIGRATION_DATABASE_URL:-postgresql://$(id -un)@127.0.0.1:55432/palimpsest_local_ci_db}"
    runtime_url="${PALIMPSEST_TEST_DATABASE_URL:-postgresql://palimpsest_local_runtime@127.0.0.1:55432/palimpsest_local_ci_db}"
    export PALIMPSEST_MIGRATION_DATABASE_URL="$migration_url"
    export PALIMPSEST_TEST_DATABASE_URL="$runtime_url"
    "$script_dir/guard-palimpsest-db-env.sh"

    step "local test database bootstrap (idempotent)"
    psql "$local_superuser_url" --tuples-only --no-align --quiet --command \
        "SELECT 'CREATE DATABASE palimpsest_local_ci_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'palimpsest_local_ci_db')" |
        psql "$local_superuser_url" --quiet >/dev/null
    psql "$local_superuser_url" --quiet --command \
        "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'palimpsest_local_runtime') THEN CREATE ROLE palimpsest_local_runtime LOGIN CREATEDB NOSUPERUSER NOINHERIT NOBYPASSRLS; END IF; END \$\$;" >/dev/null
    psql "${local_superuser_url%/*}/template1" --quiet --command \
        "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public" >/dev/null

    step "migrate apply (local test database)"
    cargo run --locked --quiet --package palimpsest-server -- migrate apply

    step "cargo test --locked --workspace (PostgreSQL suites)"
    cargo test --locked --workspace
fi

if [[ "$mode_backup" == true ]]; then
    step "backup and PITR conformance"
    bash "$script_dir/test_palimpsest_backup_conformance.sh"
fi

printf '\ndev-check: all requested tiers passed\n'
