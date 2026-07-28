#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
cd "$project_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker with Compose v2 is required" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker Compose 2.20.0 or newer is required" >&2
  exit 1
fi

minimum_compose_version="2.20.0"
compose_version="$(docker compose version --short)"
compose_version="${compose_version#v}"
oldest_compose_version="$(printf '%s\n' "$minimum_compose_version" "$compose_version" | LC_ALL=C sort -V | head -n 1)"
if [[ "$oldest_compose_version" != "$minimum_compose_version" ]]; then
  echo "docker Compose ${minimum_compose_version} or newer is required; found ${compose_version}" >&2
  exit 1
fi

postgres_port="${PALIMPSEST_POSTGRES_PORT:-5432}"
export PALIMPSEST_POSTGRES_PORT="$postgres_port"

docker compose up --detach --wait postgres

if ! runtime_probe="$(
  docker compose exec --no-TTY postgres \
    psql 'postgresql://palimpsest_runtime:palimpsest-runtime-local-only@127.0.0.1:5432/palimpsest' \
      --tuples-only \
      --no-align \
      --command "SELECT current_user, extversion, rolsuper, rolbypassrls FROM pg_extension CROSS JOIN pg_roles WHERE extname = 'vector' AND rolname = current_user"
)"; then
  echo "the existing local volume predates the non-superuser runtime profile" >&2
  echo "preserve or back up needed data before changing the volume" >&2
  echo "if its data is disposable, recreate it explicitly with: docker compose down --volumes" >&2
  exit 1
fi

if [[ "$runtime_probe" != "palimpsest_runtime|0.8.5|f|f" ]]; then
  echo "the local database runtime role or pgvector version is incompatible" >&2
  exit 1
fi

export PALIMPSEST_DATABASE_URL="${PALIMPSEST_DATABASE_URL:-postgresql://palimpsest_runtime:palimpsest-runtime-local-only@127.0.0.1:${postgres_port}/palimpsest}"
export PALIMPSEST_BEARER_TOKEN="${PALIMPSEST_BEARER_TOKEN:-palimpsest-local-development-token}"
export PALIMPSEST_PRINCIPAL_ID="${PALIMPSEST_PRINCIPAL_ID:-local-development-principal}"
export PALIMPSEST_TENANT_ID="${PALIMPSEST_TENANT_ID:-019be000-0000-7000-8000-000000000010}"
export PALIMPSEST_SUBJECT_ID="${PALIMPSEST_SUBJECT_ID:-019be000-0000-7000-8000-000000000020}"
export PALIMPSEST_BIND="${PALIMPSEST_BIND:-127.0.0.1:8080}"

echo "PostgreSQL is healthy; starting Palimpsest at http://${PALIMPSEST_BIND}"
exec cargo run --locked --package palimpsest-server
