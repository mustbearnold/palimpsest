#!/usr/bin/env bash
# Backup and PITR conformance runner (spec 016, scenarios A1-A5).
#
# Scenarios:
#   A1 verify_backup_base_wal        base backup + WAL capture vs an S3 fixture
#   A2 verify_backup_restore_suppression  fences recorded before and after the
#                                    backup both hold after the restore
#   A3 verify_backup_expiry          a declared retention policy removes backups
#   A4 verify_backup_failure_injection  missing, corrupt, and stale backups fail
#                                    cleanly
#   A5 verify_backup_rehearsal_guard the logical backup rehearsal still passes
#
# Environment:
#   PALIMPSEST_BACKUP_BINARY               server binary (default target/debug/palimpsest-server)
#   PALIMPSEST_S3_ENDPOINT                 external S3-compatible fixture (optional)
#   PALIMPSEST_BACKUP_CONFORMANCE_SUPERUSER_URL  running cluster (optional)
#   PALIMPSEST_BACKUP_CONFORMANCE_SCRATCH_PORT   scratch cluster port (default 55434)
#   PALIMPSEST_BACKUP_RESTORE_PORT         restored cluster port (default 55435)
#   PALIMPSEST_BACKUP_CONFORMANCE_FIXTURE_PORT   fixture port (default 19000)
#
# Without PALIMPSEST_S3_ENDPOINT the runner builds and starts the in-repo
# fixture. Without a superuser URL the runner creates a scratch cluster.

set -euo pipefail

binary="${PALIMPSEST_BACKUP_BINARY:-target/debug/palimpsest-server}"
superuser_url="${PALIMPSEST_BACKUP_CONFORMANCE_SUPERUSER_URL:-}"
s3_endpoint="${PALIMPSEST_S3_ENDPOINT:-}"
scratch_port="${PALIMPSEST_BACKUP_CONFORMANCE_SCRATCH_PORT:-55434}"
restore_port="${PALIMPSEST_BACKUP_RESTORE_PORT:-55435}"
fixture_port="${PALIMPSEST_BACKUP_CONFORMANCE_FIXTURE_PORT:-19000}"
s3_bucket="palimpsest-backup-conformance"
s3_region="us-east-1"
s3_access_key="conformance-access-key"
s3_secret_key="conformance-secret-key"

# Refuse a test/gate environment that points at the live database.
"$(dirname -- "${BASH_SOURCE[0]}")/guard-palimpsest-db-env.sh"

tenant_id="10000000-0000-4000-8000-000000000001"
subject_one="10000000-0000-4000-8000-000000000011"
subject_two="10000000-0000-4000-8000-000000000012"
case_id="10000000-0000-4000-8000-000000000021"
episode_one="10000000-0000-4000-8000-000000000031"
episode_two="10000000-0000-4000-8000-000000000032"
payload='{"probe":"backup-conformance-a2"}'

backup_one="10000000-0000-4000-8000-000000000041"
backup_two="10000000-0000-4000-8000-000000000042"
backup_three="10000000-0000-4000-8000-000000000043"

results="A1:skip A2:skip A3:skip A4:skip A5:skip"

cleanup() {
    if [[ -n "${fixture_pid:-}" ]]; then
        kill "$fixture_pid" 2>/dev/null || true
    fi
    if [[ -n "${scratch_cluster_dir:-}" && -f "$scratch_cluster_dir/postmaster.pid" ]]; then
        pg_ctl --pgdata="$scratch_cluster_dir" -m immediate stop >/dev/null 2>&1 || true
    fi
    if [[ -n "${restore_cluster_dir:-}" && -f "$restore_cluster_dir/postmaster.pid" ]]; then
        pg_ctl --pgdata="$restore_cluster_dir" -m immediate stop >/dev/null 2>&1 || true
    fi
    if [[ -n "${workdir:-}" ]]; then
        rm -rf -- "$workdir"
    fi
}
trap cleanup EXIT

fail() {
    echo "backup conformance FAILED: $1" >&2
    exit 1
}

for command_name in psql sha256sum curl pg_basebackup pg_ctl tar; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        fail "required backup tool is unavailable: $command_name"
    fi
done
if [[ ! -x "$binary" ]]; then
    fail "palimpsest-server binary is unavailable: $binary"
fi

workdir="$(mktemp -d "${TMPDIR:-/tmp}/palimpsest-backup-conformance.XXXXXXXX")"
restore_cluster_dir="$workdir/restore-cluster"
restore_log="$workdir/restore.log"
sql_source="$workdir/source.sql"

if [[ -z "$superuser_url" ]]; then
    if ! command -v initdb >/dev/null 2>&1; then
        fail "required backup tool is unavailable: initdb"
    fi
    scratch_cluster_dir="$workdir/scratch-cluster"
    initdb --pgdata="$scratch_cluster_dir" --auth=trust --username=postgres >/dev/null
    pg_ctl --pgdata="$scratch_cluster_dir" --log="$workdir/scratch.log" \
        --options="-p $scratch_port -c listen_addresses=127.0.0.1 -c unix_socket_directories=$workdir" \
        -w start >/dev/null
    superuser_url="postgres://postgres@127.0.0.1:$scratch_port/postgres"
    psql "$superuser_url" --tuples-only --no-align --quiet --command \
        "ALTER SYSTEM SET archive_mode = 'on'" >/dev/null
    pg_ctl --pgdata="$scratch_cluster_dir" --log="$workdir/scratch.log" \
        --options="-p $scratch_port -c listen_addresses=127.0.0.1 -c unix_socket_directories=$workdir" \
        -w restart >/dev/null
fi

source_db="palimpsest_backup_src_$$"
restore_db="palimpsest_backup_rehearsal_$$"
superuser_host_port="${superuser_url%/*}"
superuser_host_port="${superuser_host_port#*@}"
psql "$superuser_url" --tuples-only --no-align --quiet --command "CREATE DATABASE $source_db" >/dev/null
psql "$superuser_url" --tuples-only --no-align --quiet --command "CREATE DATABASE $restore_db" >/dev/null
source_url="${superuser_url%/*}/$source_db"

PALIMPSEST_DATABASE_URL="$source_url" "$binary" migrate apply >/dev/null

if [[ -z "$s3_endpoint" ]]; then
    if ! command -v cargo >/dev/null 2>&1; then
        fail "required backup tool is unavailable: cargo"
    fi
    # The sqlx migrate! macro embeds migrations/ at palimpsest-postgres
    # compile time, but cargo does not track the migrations directory in its
    # fingerprint. Force the crate to recompile so the binary always carries
    # the current migration set.
    touch crates/palimpsest-postgres/src/lib.rs
    cargo build --quiet --bins --examples
    fixture_binary="$(dirname "$binary")/examples/backup_s3_fixture"
    "$fixture_binary" "$fixture_port" >"$workdir/fixture.log" 2>&1 &
    fixture_pid="$!"
    for _attempt in $(seq 1 30); do
        if curl --silent --output /dev/null "http://127.0.0.1:$fixture_port/__probe"; then
            break
        fi
        sleep 1
    done
    if ! kill -0 "$fixture_pid" 2>/dev/null; then
        fail "S3 fixture did not start"
    fi
    s3_endpoint="http://127.0.0.1:$fixture_port"
fi

export PALIMPSEST_BACKUP_S3_ENDPOINT="$s3_endpoint"
export PALIMPSEST_BACKUP_S3_BUCKET="$s3_bucket"
export PALIMPSEST_BACKUP_S3_REGION="$s3_region"
export PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID="$s3_access_key"
export PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY="$s3_secret_key"
export PALIMPSEST_BACKUP_SOURCE_URL="$source_url"
export PALIMPSEST_BACKUP_BINARY="$binary"
export PALIMPSEST_RESTORE_EXPORT_DATABASE_URL="$source_url"

wipe_fixture() {
    curl --silent --output /dev/null -X POST "$s3_endpoint/__wipe" || true
}
wipe_fixture

seed_episode() {
    local subject_id="$1"
    local episode_id="$2"
    psql "$source_url" --tuples-only --no-align --quiet --command "
        INSERT INTO memory.episodes (
            tenant_id, subject_id, case_id, episode_id, kind, observed_at,
            writer_principal_id, source_type, sensitivity, retention_policy_id,
            schema_version, payload, payload_sha256
        )
        VALUES (
            '$tenant_id', '$subject_id', '$case_id', '$episode_id', 'observation', clock_timestamp(),
            'backup-conformance', 'backup-fixture', 'internal', 'standard',
            1, '$payload'::jsonb,
            encode(public.digest(convert_to('$payload', 'UTF8'), 'sha256'), 'hex')
        )" >/dev/null
}

fence_subject() {
    local subject_id="$1"
    psql "$source_url" --tuples-only --no-align --quiet --command "
        INSERT INTO memory.subject_lifecycles (tenant_id, subject_id, lifecycle_state, state_version)
        VALUES ('$tenant_id', '$subject_id', 'deleted', 1)" >/dev/null
}

episode_count() {
    local subject_id="$1"
    local database_url="$2"
    psql "$database_url" --tuples-only --no-align --quiet --command \
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = '$tenant_id' AND subject_id = '$subject_id'"
}

lifecycle_state() {
    local subject_id="$1"
    local database_url="$2"
    psql "$database_url" --tuples-only --no-align --quiet --command \
        "SELECT lifecycle_state FROM memory.subject_lifecycles WHERE tenant_id = '$tenant_id' AND subject_id = '$subject_id'"
}

echo "backup conformance: A5 logical backup rehearsal guard"

if PALIMPSEST_BACKUP_SOURCE_URL="$source_url" \
    PALIMPSEST_BACKUP_RESTORE_URL="postgres://postgres@$superuser_host_port/$restore_db" \
    scripts/palimpsest-logical-backup-rehearsal.sh >"$workdir/a5.out" 2>&1; then
    if grep -q '"probe_equal":true' "$workdir/a5.out"; then
        results="A1:skip A2:skip A3:skip A4:skip A5:pass"
    else
        fail "A5 rehearsal probe mismatch"
    fi
else
    fail "A5 rehearsal failed: $(tail -3 "$workdir/a5.out")"
fi

echo "backup conformance: A1 base backup and WAL capture"

seed_episode "$subject_one" "$episode_one"
fence_subject "$subject_one"

create_output="$(
    PALIMPSEST_BACKUP_RETENTION_POLICY_ID="pitr-conformance-v1" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$workdir/fence-ledger.json" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$workdir/fence-ledger.sha256" \
        scripts/palimpsest-backup.sh create "$backup_one"
)"
printf '%s\n' "$create_output" >"$workdir/a1-create.json"

a1_base_sha256="$(printf '%s' "$create_output" | grep -o '"base_sha256": *"[^"]*"' | head -1 | sed 's/.*"\([0-9a-f]*\)".*/\1/' || true)"
a1_size="$(printf '%s' "$create_output" | grep -o '"base_size_bytes":[0-9]*' | head -1 | sed 's/.*://' || true)"
a1_rpo="$(printf '%s' "$create_output" | grep -o '"rpo_estimate_ms":[0-9]*' | head -1 | sed 's/.*://' || true)"
a1_entries="$(printf '%s' "$create_output" | grep -o '"fence_entry_count":[0-9]*' | head -1 | sed 's/.*://' || true)"
[[ "$a1_base_sha256" =~ ^[0-9a-f]{64}$ ]] || fail "A1 base_sha256 is not a 64-hex digest"
(( a1_size > 0 )) || fail "A1 base_size_bytes is not positive"
(( a1_rpo >= 0 )) || fail "A1 rpo_estimate_ms is negative"
(( a1_entries == 1 )) || fail "A1 fence_entry_count is not 1"
results="A1:pass A2:skip A3:skip A4:skip A5:pass"

echo "backup conformance: A2 restore suppression with fences before and after the backup"

seed_episode "$subject_two" "$episode_two"
fence_subject "$subject_two"
[[ "$(episode_count "$subject_one" "$source_url")" == "1" ]] || fail "A2 source corpus missing subject one"
[[ "$(episode_count "$subject_two" "$source_url")" == "1" ]] || fail "A2 source corpus missing subject two"

restore_url="postgres://postgres@${superuser_host_port%:*}:$restore_port/$source_db"
restore_output="$(
    PALIMPSEST_BACKUP_RESTORE_URL="$restore_url" \
        PALIMPSEST_BACKUP_RESTORE_DATA_DIR="$restore_cluster_dir" \
        PALIMPSEST_BACKUP_RESTORE_PORT="$restore_port" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_PATH="$workdir/fence-ledger.json" \
        PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256="$workdir/fence-ledger.sha256" \
        scripts/palimpsest-backup.sh restore "$backup_one" 2>&1
)" || fail "A2 restore failed: $restore_output"
printf '%s\n' "$restore_output" >"$workdir/a2-restore.out"

a2_status="$(printf '%s' "$restore_output" | grep '"status":"complete"' | head -1 || true)"
[[ -n "$a2_status" ]] || fail "A2 restore did not complete"
a2_probe="$(printf '%s' "$restore_output" | grep -o '"lifecycle_probe":"[^"]*"' | head -1 | sed 's/.*"lifecycle_probe":"\([^"]*\)".*/\1/' || true)"
[[ "$a2_probe" == *"deleted:1"* ]] || fail "A2 restored lifecycle probe is not deleted:1: $a2_probe"

[[ "$(episode_count "$subject_one" "$restore_url")" == "0" ]] || fail "A2 subject one survived the restore"
[[ "$(episode_count "$subject_two" "$restore_url")" == "0" ]] || fail "A2 subject two survived the restore"
[[ "$(lifecycle_state "$subject_one" "$restore_url")" == "deleted" ]] || fail "A2 subject one is not fenced after restore"
# subject two was fenced after the backup: the restored copy predates the
# fence, so the scope is vacuous and must be absent, not resurrected.
[[ -z "$(lifecycle_state "$subject_two" "$restore_url")" ]] || fail "A2 subject two was resurrected by the restore"
[[ "$(episode_count "$subject_one" "$source_url")" == "1" ]] || fail "A2 restore touched the source corpus"
results="A1:pass A2:pass A3:skip A4:skip A5:pass"

pg_ctl --pgdata="$restore_cluster_dir" -m immediate stop >/dev/null 2>&1 || true
rm -rf -- "$restore_cluster_dir"

echo "backup conformance: A3 backup expiry"

expiry_output="$(
    PALIMPSEST_BACKUP_RETENTION_POLICY_ID="pitr-expiry-v1" \
        scripts/palimpsest-backup.sh create "$backup_two" 2>&1
)" || fail "A3 create failed: $expiry_output"
sleep 2
expire_output="$(
    PALIMPSEST_BACKUP_RETENTION_POLICY_ID="pitr-expiry-v1" \
        PALIMPSEST_BACKUP_RETENTION_SECONDS="1" \
        scripts/palimpsest-backup.sh expire 2>&1
)" || fail "A3 expire failed: $expire_output"
[[ "$expire_output" == *"$backup_two"* ]] || fail "A3 expired backup was not removed: $expire_output"

fetch_after_expiry="$(
    "$binary" backup fetch-base "$backup_two" "$workdir/expired-base.tar.gz" 2>&1 || true
)"
[[ "$fetch_after_expiry" == *'"code": "base-not-indexed"'* ]] || fail "A3 expired backup is still fetchable: $fetch_after_expiry"
results="A1:pass A2:pass A3:pass A4:skip A5:pass"

echo "backup conformance: A4 failure injection"

wipe_fixture
missing_output="$("$binary" backup fetch-base "$backup_one" "$workdir/missing-base.tar.gz" 2>&1 || true)"
[[ "$missing_output" == *'"code": "base-not-indexed"'* ]] || fail "A4 wiped store did not fail cleanly: $missing_output"

create_output="$(
    PALIMPSEST_BACKUP_RETENTION_POLICY_ID="pitr-failure-v1" \
        scripts/palimpsest-backup.sh create "$backup_three" 2>&1
)" || fail "A4 create failed: $create_output"
curl --silent --output /dev/null -X DELETE \
    "$s3_endpoint/$s3_bucket/base/$backup_three.tar.gz"
missing_base_output="$("$binary" backup fetch-base "$backup_three" "$workdir/missing-base.tar.gz" 2>&1 || true)"
[[ "$missing_base_output" == *'"code": "base-missing"'* ]] || fail "A4 missing base did not fail cleanly: $missing_base_output"
printf 'corrupt garbage bytes' | curl --silent --output /dev/null -X PUT \
    --data-binary @- "$s3_endpoint/$s3_bucket/base/$backup_three.tar.gz"
corrupt_output="$("$binary" backup fetch-base "$backup_three" "$workdir/corrupt-base.tar.gz" 2>&1 || true)"
[[ "$corrupt_output" == *'"code": "base-corrupt"'* ]] || fail "A4 corrupt base did not fail cleanly: $corrupt_output"

sleep 2
stale_output="$("$binary" backup fetch-base "$backup_three" "$workdir/stale-base.tar.gz" 1 2>&1 || true)"
[[ "$stale_output" == *'"code": "backup-stale"'* ]] || fail "A4 stale base did not fail cleanly: $stale_output"

wal_missing_output="$("$binary" backup fetch-wal "0000000100000000000000ff" "$workdir/missing-wal" 2>&1 || true)"
[[ "$wal_missing_output" == *'"code": "wal-missing"'* ]] || fail "A4 missing WAL did not fail cleanly: $wal_missing_output"
results="A1:pass A2:pass A3:pass A4:pass A5:pass"

echo "backup conformance: PASS $results"
