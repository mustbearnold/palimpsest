#!/usr/bin/env bash
set -euo pipefail

source_url="${PALIMPSEST_BACKUP_SOURCE_URL:-}"
restore_url="${PALIMPSEST_BACKUP_RESTORE_URL:-}"

if [[ -z "$source_url" || -z "$restore_url" ]]; then
  echo "PALIMPSEST_BACKUP_SOURCE_URL and PALIMPSEST_BACKUP_RESTORE_URL are required" >&2
  exit 2
fi

for command_name in pg_dump pg_restore psql sha256sum; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required backup tool is unavailable" >&2
    exit 2
  fi
done

source_identity="$(psql "$source_url" --tuples-only --no-align --quiet --command \
  "SELECT current_database() || '|' || coalesce(inet_server_addr()::text, '') || '|' || inet_server_port()::text")"
restore_identity="$(psql "$restore_url" --tuples-only --no-align --quiet --command \
  "SELECT current_database() || '|' || coalesce(inet_server_addr()::text, '') || '|' || inet_server_port()::text")"
if [[ "$source_identity" == "$restore_identity" ]]; then
  echo "source and restore connections must identify different databases" >&2
  exit 2
fi

if [[ "$(psql "$restore_url" --tuples-only --no-align --quiet --command \
  "SELECT count(*) FROM pg_namespace WHERE nspname = 'memory'")" != "0" ]]; then
  echo "restore database already contains the Palimpsest memory schema" >&2
  exit 2
fi

umask 077
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/palimpsest-logical-backup.XXXXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT
dump_path="$temporary_root/palimpsest.dump"
source_probe="$temporary_root/source.probe"
restore_probe="$temporary_root/restore.probe"

probe_sql="
SELECT current_setting('server_version_num');
SELECT coalesce((SELECT extversion FROM pg_extension WHERE extname = 'vector'), 'missing');
SELECT coalesce((SELECT max(version)::text FROM _sqlx_migrations), '0');
SELECT (SELECT count(*) FROM memory.episodes);
SELECT (SELECT count(*) FROM memory.fact_revisions);
SELECT (SELECT count(*) FROM memory.checkpoint_revisions);
"

psql "$source_url" --tuples-only --no-align --quiet --command "$probe_sql" >"$source_probe"
pg_dump --format=custom --no-owner --no-privileges --file="$dump_path" "$source_url"
pg_restore --list "$dump_path" >/dev/null
pg_restore --exit-on-error --no-owner --no-privileges --dbname="$restore_url" "$dump_path"
psql "$restore_url" --tuples-only --no-align --quiet --command "$probe_sql" >"$restore_probe"

if ! cmp --silent "$source_probe" "$restore_probe"; then
  echo "logical backup rehearsal probe mismatch" >&2
  exit 1
fi

dump_size="$(stat --format='%s' "$dump_path")"
dump_sha256="$(sha256sum "$dump_path" | awk '{print $1}')"
schema_version="$(sed -n '3p' "$restore_probe")"
vector_version="$(sed -n '2p' "$restore_probe")"
printf '{"backup_profile":"postgresql-logical-custom-v1","dump_sha256":"%s","dump_size_bytes":%s,"schema_version":%s,"vector_version":"%s","probe_equal":true}\n' \
  "$dump_sha256" "$dump_size" "$schema_version" "$vector_version"
