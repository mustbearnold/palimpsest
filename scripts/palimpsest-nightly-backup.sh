#!/usr/bin/env bash
# Nightly logical backup of the LIVE Palimpsest database (hardening, 2026-08-09).
# Custom-format dump + sha256 + archive sanity check + 14-day retention.
#
# This is a deliberate live operation, so the test-env guard does not apply
# here. The script verifies the target identity before dumping.
#
# Environment:
#   PALIMPSEST_NIGHTLY_BACKUP_URL              live database URL (default below)
#   PALIMPSEST_NIGHTLY_BACKUP_DIR              backup directory (default ~/backups/palimpsest)
#   PALIMPSEST_NIGHTLY_BACKUP_RETENTION_DAYS   retention window (default 14)

set -euo pipefail

live_url="${PALIMPSEST_NIGHTLY_BACKUP_URL:-postgresql://mustbearn@127.0.0.1:55432/palimpsest}"
backup_dir="${PALIMPSEST_NIGHTLY_BACKUP_DIR:-$HOME/backups/palimpsest}"
retention_days="${PALIMPSEST_NIGHTLY_BACKUP_RETENTION_DAYS:-14}"

for command_name in psql pg_dump pg_restore sha256sum; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "required backup tool is unavailable: $command_name" >&2
        exit 2
    fi
done

identity="$(psql "$live_url" --tuples-only --no-align --quiet --command \
    "SELECT current_database() || '|' || inet_server_port() || '|' || (SELECT count(*) FROM pg_namespace WHERE nspname = 'memory')")"
expected="palimpsest|55432|1"
if [[ "$identity" != "$expected" ]]; then
    echo "REFUSED: target is not the live database (got '$identity')" >&2
    exit 2
fi

mkdir -p "$backup_dir"
stamp="$(date +%Y%m%d-%H%M%S)"
dump_path="$backup_dir/live-$stamp.dump"

pg_dump "$live_url" --format=custom --no-owner > "$dump_path"
sha256sum "$dump_path" > "$dump_path.sha256"
pg_restore --list "$dump_path" > /dev/null

echo "backup ok: $dump_path ($(wc -c < "$dump_path") bytes, sha256 $(awk '{print $1}' "$dump_path.sha256"))"
find "$backup_dir" -maxdepth 1 -name 'live-*.dump' -mtime "+$retention_days" -delete
find "$backup_dir" -maxdepth 1 -name 'live-*.dump.sha256' -mtime "+$retention_days" -delete
