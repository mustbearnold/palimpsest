#!/usr/bin/env bash
# Physical backup and point-in-time recovery orchestration (spec 016).
#
# Subcommands:
#   create <backup_id>   base backup + WAL archiving + fence ledger record
#   expire               remove backups past their declared retention window
#   restore <backup_id>  fetch base + WALs, replay, apply the live fence ledger
#
# Environment:
#   PALIMPSEST_BACKUP_SOURCE_URL          source cluster connection (replication-capable for create)
#   PALIMPSEST_BACKUP_ARCHIVE_SQL_URL     superuser connection for archive configuration (defaults to SOURCE_URL)
#   PALIMPSEST_BACKUP_BINARY              palimpsest-server binary path (default target/debug/palimpsest-server)
#   PALIMPSEST_BACKUP_RETENTION_POLICY_ID named retention policy id for this backup job
#   PALIMPSEST_BACKUP_RETENTION_SECONDS   retention window enforced by expire
#   PALIMPSEST_BACKUP_RESTORE_URL         restored application database URL (restore)
#   PALIMPSEST_BACKUP_RESTORE_DATA_DIR    restored cluster data directory (restore)
#   PALIMPSEST_BACKUP_RESTORE_PORT        restored cluster port (default 55433)
#   PALIMPSEST_BACKUP_RESTORE_MAX_AGE_SECONDS  staleness bound for fetch-base (optional)
#   PALIMPSEST_RESTORE_FENCE_LEDGER_PATH  fence ledger file (export-ledger writes, apply reads)
#   PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256 fence ledger digest file (export-ledger writes)
#   PALIMPSEST_RESTORE_EXPORT_DATABASE_URL live database for the ledger export (defaults to SOURCE_URL)
#   PALIMPSEST_RESTORE_FENCE_EXPIRY_HOURS  fence expiry window in hours (default 24)
#   PALIMPSEST_BACKUP_S3_ENDPOINT|BUCKET|REGION|ACCESS_KEY_ID|SECRET_ACCESS_KEY  S3-compatible provider
#
# The script emits one JSON document per completed step and one summary
# document. A failing step exits nonzero with a message on stderr.

set -euo pipefail

operation="${1:-}"

binary="${PALIMPSEST_BACKUP_BINARY:-target/debug/palimpsest-server}"
source_url="${PALIMPSEST_BACKUP_SOURCE_URL:-}"
archive_sql_url="${PALIMPSEST_BACKUP_ARCHIVE_SQL_URL:-$source_url}"
export_database_url="${PALIMPSEST_RESTORE_EXPORT_DATABASE_URL:-$source_url}"
retention_policy_id="${PALIMPSEST_BACKUP_RETENTION_POLICY_ID:-}"
s3_endpoint="${PALIMPSEST_BACKUP_S3_ENDPOINT:-}"
s3_bucket="${PALIMPSEST_BACKUP_S3_BUCKET:-}"

backup_profile="postgresql-physical-base-wal-s3-v1"

for command_name in psql sha256sum; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "required backup tool is unavailable: $command_name" >&2
        exit 2
    fi
done

if [[ ! -x "$binary" ]]; then
    echo "palimpsest-server binary is unavailable: $binary" >&2
    exit 2
fi

require_s3_config() {
    if [[ -z "$s3_endpoint" || -z "$s3_bucket" ]]; then
        echo "PALIMPSEST_BACKUP_S3_ENDPOINT and PALIMPSEST_BACKUP_S3_BUCKET are required" >&2
        exit 2
    fi
}

reject_archive_quoting() {
    local value="$1"
    if [[ "$value" =~ [\ \'] ]]; then
        echo "backup environment values must not contain spaces or single quotes" >&2
        exit 2
    fi
}

wal_number() {
    local name="$1"
    printf '%d' "0x${name: -16}"
}

wal_previous() {
    local name="$1"
    local number
    number="$(wal_number "$name")"
    printf '%s%016x' "${name:0:8}" "$((number - 1))"
}

wal_to_lsn_start() {
    local name="$1"
    printf '%d' "$(($(wal_number "$name") << 24))"
}

lsn_value() {
    local lsn="$1"
    local high low
    high="$(printf '%d' "0x${lsn%/*}")"
    low="$(printf '%d' "0x${lsn#*/}")"
    printf '%d' "$(((high << 32) | low))"
}

write_json() {
    printf '%s\n' "$1"
}

run_backup_create() {
    local backup_id="${1:-}"
    if [[ -z "$backup_id" ]]; then
        echo "backup create requires <backup_id>" >&2
        exit 2
    fi
    if [[ -z "$source_url" || -z "$retention_policy_id" ]]; then
        echo "PALIMPSEST_BACKUP_SOURCE_URL and PALIMPSEST_BACKUP_RETENTION_POLICY_ID are required" >&2
        exit 2
    fi
    if ! command -v pg_basebackup >/dev/null 2>&1; then
        echo "required backup tool is unavailable: pg_basebackup" >&2
        exit 2
    fi
    require_s3_config
    if [[ -z "${PALIMPSEST_BACKUP_S3_REGION:-}" || -z "${PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID:-}" || -z "${PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY:-}" ]]; then
        echo "PALIMPSEST_BACKUP_S3_REGION, PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID, and PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY are required" >&2
        exit 2
    fi

    umask 077
    backup_tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/palimpsest-backup.XXXXXXXX")"
    trap 'rm -rf -- "${backup_tmp_root:-}" "${restore_tmp_root:-}"' EXIT
    mkdir -p "$backup_tmp_root/base"
    local base_dir="$backup_tmp_root/base"
    local base_path="$base_dir/base.tar.gz"

    local fence_path="${PALIMPSEST_RESTORE_FENCE_LEDGER_PATH:-$backup_tmp_root/fence-ledger.json}"
    local fence_sha_path="${PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256:-$backup_tmp_root/fence-ledger.sha256}"

    local backup_start_epoch_ms
    backup_start_epoch_ms="$(date +%s%3N)"

    PALIMPSEST_RESTORE_EXPORT_DATABASE_URL="$export_database_url" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$fence_path" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$fence_sha_path" \
        "$binary" restore export-ledger >/dev/null

    local ledger_sha256
    ledger_sha256="$(tr -d '\n' <"$fence_sha_path")"
    local fence_entry_count
    fence_entry_count="$(psql "$export_database_url" --tuples-only --no-align --quiet --command \
        "SELECT count(*) FROM memory.subject_lifecycles WHERE lifecycle_state <> 'active'")"

    local archive_mode
    archive_mode="$(psql "$archive_sql_url" --tuples-only --no-align --quiet --command "SHOW archive_mode")"
    if [[ "$archive_mode" != "on" ]]; then
        echo "archive_mode is not enabled on the source cluster" >&2
        echo "enable archive_mode and restart the cluster, then re-run backup create" >&2
        exit 1
    fi
    reject_archive_quoting "$s3_endpoint"
    reject_archive_quoting "$s3_bucket"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_REGION}"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID}"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY}"
    reject_archive_quoting "$(dirname "$binary")"
    local archive_command="PALIMPSEST_BACKUP_S3_ENDPOINT=$s3_endpoint PALIMPSEST_BACKUP_S3_BUCKET=$s3_bucket PALIMPSEST_BACKUP_S3_REGION=${PALIMPSEST_BACKUP_S3_REGION} PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID=${PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID} PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY=${PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY} $(readlink -f "$binary") backup archive-wal %f %p"
    local current_archive_command
    current_archive_command="$(psql "$archive_sql_url" --tuples-only --no-align --quiet --command "SHOW archive_command")"
    if [[ "$current_archive_command" != "$archive_command" ]]; then
        psql "$archive_sql_url" --tuples-only --no-align --quiet --command \
            "ALTER SYSTEM SET archive_command = '$archive_command'"
        psql "$archive_sql_url" --tuples-only --no-align --quiet --command \
            "SELECT pg_reload_conf()" >/dev/null
    fi

    local wal_from
    wal_from="$(psql "$source_url" --tuples-only --no-align --quiet --command \
        "SELECT pg_walfile_name(pg_switch_wal())")"

    # A fast checkpoint avoids a wait for the next spread checkpoint. On an
    # idle cluster that wait can reach checkpoint_timeout.
    pg_basebackup --format=tar --compress=gzip --checkpoint=fast --pgdata="$backup_tmp_root/base" \
        --dbname="$source_url" --label="palimpsest-backup-$backup_id"

    local wal_to
    # pg_basebackup finalizes with a WAL switch, so the segment containing the
    # current LSN is one past the last segment that holds backup content. The
    # end switch closes the last content segment and archives it.
    wal_to="$(psql "$source_url" --tuples-only --no-align --quiet --command \
        "SELECT pg_walfile_name(pg_current_wal_lsn())")"
    wal_to="$(wal_previous "$wal_to")"

    local wal_archived_epoch_ms=""
    local probe_path="$backup_tmp_root/wal-probe"
    for _attempt in $(seq 1 60); do
        if PALIMPSEST_BACKUP_S3_ENDPOINT="$s3_endpoint" \
            PALIMPSEST_BACKUP_S3_BUCKET="$s3_bucket" \
            "$binary" backup fetch-wal "$wal_to" "$probe_path" >/dev/null 2>&1; then
            wal_archived_epoch_ms="$(date +%s%3N)"
            break
        fi
        sleep 1
    done
    if [[ -z "$wal_archived_epoch_ms" ]]; then
        echo "WAL segment $wal_to was not archived within 60 seconds" >&2
        exit 1
    fi

    local backup_end_epoch_ms
    backup_end_epoch_ms="$(date +%s%3N)"

    local base_sha256 base_size_bytes
    base_sha256="$(sha256sum "$base_path" | awk '{print $1}')"
    base_size_bytes="$(stat --format='%s' "$base_path")"

    PALIMPSEST_BACKUP_S3_ENDPOINT="$s3_endpoint" \
        PALIMPSEST_BACKUP_S3_BUCKET="$s3_bucket" \
        "$binary" backup push-base "$backup_id" "$base_path" "$retention_policy_id" \
        "$wal_from" "$wal_to" >/dev/null

    write_json "{\"operation\":\"create\",\"status\":\"complete\",\"backup_profile\":\"$backup_profile\",\"backup_id\":\"$backup_id\",\"retention_policy_id\":\"$retention_policy_id\",\"wal_from\":\"$wal_from\",\"wal_to\":\"$wal_to\",\"base_sha256\":\"$base_sha256\",\"base_size_bytes\":$base_size_bytes,\"backup_start_epoch_ms\":$backup_start_epoch_ms,\"backup_end_epoch_ms\":$backup_end_epoch_ms,\"wal_archived_epoch_ms\":$wal_archived_epoch_ms,\"rpo_estimate_ms\":$((backup_end_epoch_ms - wal_archived_epoch_ms)),\"fence_ledger_sha256\":\"$ledger_sha256\",\"fence_entry_count\":$fence_entry_count,\"provider\":\"s3-compatible\"}"
}

run_backup_expire() {
    local retention_seconds="${PALIMPSEST_BACKUP_RETENTION_SECONDS:-}"
    if [[ -z "$retention_policy_id" || -z "$retention_seconds" ]]; then
        echo "PALIMPSEST_BACKUP_RETENTION_POLICY_ID and PALIMPSEST_BACKUP_RETENTION_SECONDS are required" >&2
        exit 2
    fi
    require_s3_config
    local output
    output="$(PALIMPSEST_BACKUP_S3_ENDPOINT="$s3_endpoint" \
        PALIMPSEST_BACKUP_S3_BUCKET="$s3_bucket" \
        "$binary" backup expire "$retention_policy_id" "$retention_seconds")"
    write_json "$output"
}

run_backup_restore() {
    local backup_id="${1:-}"
    if [[ -z "$backup_id" ]]; then
        echo "backup restore requires <backup_id>" >&2
        exit 2
    fi
    local restore_url="${PALIMPSEST_BACKUP_RESTORE_URL:-}"
    local restore_data_dir="${PALIMPSEST_BACKUP_RESTORE_DATA_DIR:-}"
    local restore_port="${PALIMPSEST_BACKUP_RESTORE_PORT:-55433}"
    if [[ -z "$restore_url" || -z "$restore_data_dir" ]]; then
        echo "PALIMPSEST_BACKUP_RESTORE_URL and PALIMPSEST_BACKUP_RESTORE_DATA_DIR are required" >&2
        exit 2
    fi
    if ! command -v pg_ctl >/dev/null 2>&1; then
        echo "required backup tool is unavailable: pg_ctl" >&2
        exit 2
    fi
    require_s3_config

    umask 077
    restore_tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/palimpsest-restore.XXXXXXXX")"
    trap 'rm -rf -- "${backup_tmp_root:-}" "${restore_tmp_root:-}"' EXIT
    local base_path="$restore_tmp_root/base.tar.gz"

    local fence_path="${PALIMPSEST_RESTORE_FENCE_LEDGER_PATH:-$restore_tmp_root/fence-ledger.json}"
    local fence_sha_path="${PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256:-$restore_tmp_root/fence-ledger.sha256}"
    local restore_start_epoch_ms
    restore_start_epoch_ms="$(date +%s%3N)"

    PALIMPSEST_RESTORE_EXPORT_DATABASE_URL="$export_database_url" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$fence_path" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$fence_sha_path" \
        "$binary" restore export-ledger >/dev/null

    local ledger_sha256
    ledger_sha256="$(tr -d '\n' <"$fence_sha_path")"

    PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$fence_path" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$ledger_sha256" \
        "$binary" restore verify >/dev/null

    local fetch_base_args="fetch-base $backup_id $base_path"
    if [[ -n "${PALIMPSEST_BACKUP_RESTORE_MAX_AGE_SECONDS:-}" ]]; then
        fetch_base_args="$fetch_base_args ${PALIMPSEST_BACKUP_RESTORE_MAX_AGE_SECONDS}"
    fi
    local fetch_base_output
    # shellcheck disable=SC2086
    fetch_base_output="$(PALIMPSEST_BACKUP_S3_ENDPOINT="$s3_endpoint" \
        PALIMPSEST_BACKUP_S3_BUCKET="$s3_bucket" \
        "$binary" backup $fetch_base_args)"
    local base_sha256 wal_to
    base_sha256="$(printf '%s' "$fetch_base_output" | grep -o '"base_sha256": *"[^"]*"' | head -1 | sed 's/.*"\([0-9a-f]*\)".*/\1/')"
    wal_to="$(printf '%s' "$fetch_base_output" | grep -o '"wal_to": *"[^"]*"' | head -1 | sed 's/.*"\([0-9a-f]*\)".*/\1/')"
    if [[ -z "$base_sha256" || -z "$wal_to" ]]; then
        echo "backup fetch-base returned no verification metadata" >&2
        exit 1
    fi

    mkdir -p "$restore_data_dir"
    tar -xzf "$base_path" -C "$restore_data_dir"

    reject_archive_quoting "$s3_endpoint"
    reject_archive_quoting "$s3_bucket"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_REGION}"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID}"
    reject_archive_quoting "${PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY}"
    reject_archive_quoting "$(dirname "$binary")"
    local restore_command="PALIMPSEST_BACKUP_S3_ENDPOINT=$s3_endpoint PALIMPSEST_BACKUP_S3_BUCKET=$s3_bucket PALIMPSEST_BACKUP_S3_REGION=${PALIMPSEST_BACKUP_S3_REGION} PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID=${PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID} PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY=${PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY} $(readlink -f "$binary") backup fetch-wal %f %p"

    cat >"$restore_data_dir/postgresql.conf" <<EOF
port = $restore_port
listen_addresses = '127.0.0.1'
unix_socket_directories = '$restore_tmp_root'
archive_mode = off
restore_command = '$restore_command'
log_min_messages = warning
EOF
    cat >"$restore_data_dir/pg_hba.conf" <<EOF
local all all trust
host all all 127.0.0.1/32 trust
EOF
    # The base backup carries the source cluster's postgresql.auto.conf,
    # which overrides this data dir and would re-enable archiving. The
    # restored cluster must be a clean PITR target.
    rm -f "$restore_data_dir/postgresql.auto.conf"
    touch "$restore_data_dir/recovery.signal"

    local restore_log="${PALIMPSEST_BACKUP_RESTORE_LOG:-$restore_tmp_root/restore.log}"
    pg_ctl --pgdata="$restore_data_dir" --log="$restore_log" --options="-c listen_addresses=127.0.0.1" -w start >/dev/null

    local recovered_wal=""
    local recovered_lsn=""
    local recovery_state=""
    for _attempt in $(seq 1 60); do
        recovery_state="$(psql "$restore_url" --tuples-only --no-align --quiet --command \
            "SELECT pg_is_in_recovery()" 2>&1 || true)"
        if [[ "$recovery_state" == "f" ]]; then
            break
        fi
        pg_ctl --pgdata="$restore_data_dir" promote >/dev/null 2>&1 || true
        sleep 1
    done
    if [[ "$recovery_state" != "f" ]]; then
        echo "restored cluster did not finish recovery: $recovery_state" >&2
        tail -20 "$restore_log" >&2 || true
        exit 1
    fi
    recovered_lsn="$(psql "$restore_url" --tuples-only --no-align --quiet --command \
        "SELECT pg_walfile_name(pg_current_wal_lsn()) || '|' || pg_current_wal_lsn()" \
        2>&1 || true)"
    if [[ -n "$recovered_lsn" ]]; then
        recovered_wal="${recovered_lsn%%|*}"
        recovered_lsn="${recovered_lsn##*|}"
    fi
    if [[ -z "$recovered_wal" ]]; then
        echo "restored cluster did not report a recovery position" >&2
        tail -20 "$restore_log" >&2 || true
        exit 1
    fi

    local recovered_lsn_value expected_lsn_value
    recovered_lsn_value="$(lsn_value "$recovered_lsn")"
    expected_lsn_value="$(wal_to_lsn_start "$wal_to")"
    if ((recovered_lsn_value < expected_lsn_value)); then
        echo "restored cluster recovered only through $recovered_wal, expected to reach $wal_to" >&2
        echo "the WAL archive is incomplete" >&2
        tail -20 "$restore_log" >&2 || true
        exit 1
    fi

    local apply_output
    apply_output="$(PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$fence_path" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$ledger_sha256" \
        PALIMPSEST_RESTORE_DATABASE_URL="$restore_url" \
        "$binary" restore apply)"
    write_json "$apply_output"

    local restore_end_epoch_ms
    restore_end_epoch_ms="$(date +%s%3N)"
    local lifecycle_probe
    lifecycle_probe="$(psql "$restore_url" --tuples-only --no-align --quiet --command \
        "SELECT lifecycle_state || ':' || count(*) FROM memory.subject_lifecycles GROUP BY lifecycle_state ORDER BY lifecycle_state")"

    write_json "{\"operation\":\"restore\",\"status\":\"complete\",\"backup_profile\":\"$backup_profile\",\"backup_id\":\"$backup_id\",\"base_sha256\":\"$base_sha256\",\"restore_start_epoch_ms\":$restore_start_epoch_ms,\"restore_end_epoch_ms\":$restore_end_epoch_ms,\"rto_estimate_ms\":$((restore_end_epoch_ms - restore_start_epoch_ms)),\"recovered_wal_to\":\"$recovered_wal\",\"fence_ledger_sha256\":\"$ledger_sha256\",\"lifecycle_probe\":\"$lifecycle_probe\",\"provider\":\"s3-compatible\"}"
}

case "$operation" in
create)
    run_backup_create "${2:-}"
    ;;
expire)
    run_backup_expire
    ;;
restore)
    run_backup_restore "${2:-}"
    ;;
*)
    echo "Usage: palimpsest-backup.sh <create|expire|restore>" >&2
    exit 2
    ;;
esac
