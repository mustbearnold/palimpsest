#!/usr/bin/env bash
# Refuse dangerous database environments (incident class 2026-08-08).
#
# A URL variable pointing at the LIVE database with a non-runtime role is
# refused. The live cluster is 127.0.0.1:55432, database palimpsest, role
# palimpsest_runtime. This guard prevents the class of failure where a test
# or gate session exported a URL with a local-dev role against the live DB
# and a "recreate the stale base DB" step dropped the live database.
#
# Usage: source this file from gate/conformance/dev scripts, or run it as a
# preflight:  scripts/guard-palimpsest-db-env.sh   (exit 0 = safe, 1 = refuse)
# Override the live cluster for non-standard deployments:
#   PALIMPSEST_GUARD_LIVE_URL='postgresql://palimpsest_runtime@127.0.0.1:55432/palimpsest'

set -uo pipefail

live_url="${PALIMPSEST_GUARD_LIVE_URL:-postgresql://palimpsest_runtime@127.0.0.1:55432/palimpsest}"

parse_url() {
    # parse_url URL -> role host port db (global output variables)
    local url="$1"
    url="${url%%\?*}"
    url="${url%%#*}"
    local authority path
    local without_scheme="${url#*://}"
    authority="${without_scheme%%/*}"
    path="${without_scheme#*/}"
    _guard_db="${path%%/*}"
    local creds hostport
    if [[ "$authority" == *"@"* ]]; then
        creds="${authority%%@*}"
        hostport="${authority#*@}"
    else
        creds=""
        hostport="$authority"
    fi
    if [[ -n "$creds" ]]; then
        _guard_role="${creds%%:*}"
    else
        _guard_role=""
    fi
    if [[ "$hostport" == *"["* ]]; then
        # IPv6 literal [::1]:port or [::1]
        _guard_host="${hostport%%]*}"
        _guard_host="${_guard_host#[}"
        _guard_port=""
        if [[ "$hostport" == *"]]:"* ]]; then
            _guard_port="${hostport##*]:}"
        fi
    else
        _guard_host="${hostport%%:*}"
        if [[ "$hostport" == *":"* ]]; then
            _guard_port="${hostport##*:}"
        else
            _guard_port=""
        fi
    fi
    [[ -z "$_guard_port" ]] && _guard_port="5432"
}

guard_live_hosts="127.0.0.1 localhost ::1"

guard_url_vars=(
    PALIMPSEST_DATABASE_URL
    PALIMPSEST_MIGRATION_DATABASE_URL
    PALIMPSEST_TEST_DATABASE_URL
    PALIMPSEST_RESTORE_DATABASE_URL
    PALIMPSEST_BACKUP_CONFORMANCE_SUPERUSER_URL
    PALIMPSEST_BACKUP_SOURCE_URL
    PALIMPSEST_BACKUP_ARCHIVE_SQL_URL
    PALIMPSEST_RESTORE_EXPORT_DATABASE_URL
    PALIMPSEST_BACKUP_RESTORE_URL
)

guard_live_role=""
guard_live_host=""
guard_live_port=""
guard_live_db=""
parse_url "$live_url"
guard_live_role="$_guard_role"
guard_live_host="$_guard_host"
guard_live_port="$_guard_port"
guard_live_db="$_guard_db"

guard_violations=()
guard_warnings=()

for var in "${guard_url_vars[@]}"; do
    url="${!var:-}"
    [[ -z "$url" ]] && continue
    parse_url "$url"
    if [[ "$_guard_db" == "$guard_live_db" && "$_guard_host" == "$guard_live_host" && "$_guard_port" == "$guard_live_port" ]]; then
        if [[ "$_guard_role" != "$guard_live_role" ]]; then
            guard_violations+=("$var points at the live database ($guard_live_host:$guard_live_port/$guard_live_db) with role '$_guard_role' (live role is '$guard_live_role')")
        fi
    elif [[ "$_guard_db" == "$guard_live_db" ]]; then
        guard_warnings+=("$var names database '$guard_live_db' on a non-live cluster ($_guard_host:$_guard_port) - rename the scratch database")
    fi
done

if [[ "${#guard_violations[@]}" -gt 0 ]]; then
    echo "REFUSED: unsafe database environment (incident class 2026-08-08)" >&2
    for v in "${guard_violations[@]}"; do
        echo "  - $v" >&2
    done
    echo "Remediation: unset the offending variables, or point them at a scratch" >&2
    echo "database on a scratch cluster with a scratch role." >&2
    exit 1
fi

if [[ "${#guard_warnings[@]}" -gt 0 ]]; then
    for w in "${guard_warnings[@]}"; do
        echo "WARNING: $w" >&2
    done
fi

exit 0
