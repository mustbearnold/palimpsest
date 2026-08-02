#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
cd "$project_root"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
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
else
  if ! command -v initdb >/dev/null 2>&1 || ! command -v pg_ctl >/dev/null 2>&1 || ! command -v psql >/dev/null 2>&1; then
    echo "Docker Compose or local PostgreSQL tools (initdb, pg_ctl, psql) are required" >&2
    exit 1
  fi

  user_home="$(getent passwd "$(id -u)" | cut -d: -f6)"
  local_runtime_root="${PALIMPSEST_LOCAL_RUNTIME_DIR:-${user_home}/.local/state/palimpsest}"
  local_data_dir="${local_runtime_root}/postgres"
  local_socket_dir="${local_runtime_root}/socket"
  local_log_path="${local_runtime_root}/postgres.log"
  postgres_port="${PALIMPSEST_POSTGRES_PORT:-55432}"
  mkdir -p "$local_data_dir" "$local_socket_dir"
  chmod 700 "$local_data_dir" "$local_socket_dir"

  if [[ ! -f "$local_data_dir/PG_VERSION" ]]; then
    initdb --no-locale --encoding=UTF8 --auth=trust --username="$(id -un)" --pgdata="$local_data_dir"
  fi
  if ! pg_ctl --pgdata="$local_data_dir" status >/dev/null 2>&1; then
    pg_ctl --pgdata="$local_data_dir" --log="$local_log_path" \
      --options="-h 127.0.0.1 -p ${postgres_port} -k ${local_socket_dir}" start
  fi

  if [[ "$(psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres --tuples-only --no-align --command="SELECT 1 FROM pg_roles WHERE rolname = 'palimpsest_runtime'")" != "1" ]]; then
    psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres \
      --command="CREATE ROLE palimpsest_runtime LOGIN PASSWORD 'palimpsest-runtime-local-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS"
  fi
  if [[ "$(psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres --tuples-only --no-align --command="SELECT 1 FROM pg_database WHERE datname = 'palimpsest'")" != "1" ]]; then
    psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres \
      --command="CREATE DATABASE palimpsest OWNER palimpsest_runtime"
    psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres \
      --command="REVOKE ALL ON DATABASE palimpsest FROM PUBLIC"
    psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=postgres \
      --command="GRANT CONNECT, TEMPORARY ON DATABASE palimpsest TO palimpsest_runtime"
  fi
  psql --host=127.0.0.1 --port="$postgres_port" --username="$(id -un)" --dbname=palimpsest \
    --command="CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"

  runtime_probe="$(PGPASSWORD='palimpsest-runtime-local-only' psql \
    --host=127.0.0.1 --port="$postgres_port" --username=palimpsest_runtime --dbname=palimpsest \
    --tuples-only --no-align \
    --command "SELECT current_user, extversion, rolsuper, rolbypassrls FROM pg_extension CROSS JOIN pg_roles WHERE extname = 'vector' AND rolname = current_user")"
  if [[ "$runtime_probe" != "palimpsest_runtime|0.8.5|f|f" ]]; then
    echo "the local database runtime role or pgvector version is incompatible" >&2
    echo "install pgvector 0.8.5 or use Docker Compose with the pinned image" >&2
    exit 1
  fi
fi

export PALIMPSEST_DATABASE_URL="${PALIMPSEST_DATABASE_URL:-postgresql://palimpsest_runtime:palimpsest-runtime-local-only@127.0.0.1:${postgres_port}/palimpsest}"
export PALIMPSEST_BEARER_TOKEN="${PALIMPSEST_BEARER_TOKEN:-palimpsest-local-development-token}"
export PALIMPSEST_PRINCIPAL_ID="${PALIMPSEST_PRINCIPAL_ID:-local-development-principal}"
export PALIMPSEST_TENANT_ID="${PALIMPSEST_TENANT_ID:-019be000-0000-7000-8000-000000000010}"
export PALIMPSEST_SUBJECT_ID="${PALIMPSEST_SUBJECT_ID:-019be000-0000-7000-8000-000000000020}"
export PALIMPSEST_ALLOWED_SENSITIVITIES="${PALIMPSEST_ALLOWED_SENSITIVITIES:-internal}"
export PALIMPSEST_BIND="${PALIMPSEST_BIND:-127.0.0.1:8080}"

echo "PostgreSQL is healthy; starting Palimpsest at http://${PALIMPSEST_BIND}"
exec cargo run --locked --package palimpsest-server
