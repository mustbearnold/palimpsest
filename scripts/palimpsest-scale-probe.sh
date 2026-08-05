#!/usr/bin/env bash
set -euo pipefail
shopt -s extglob

if [[ -z "${PALIMPSEST_SCALE_DATABASE_URL:-}" ]]; then
    echo "PALIMPSEST_SCALE_DATABASE_URL is required" >&2
    exit 2
fi

scale_revisions="${PALIMPSEST_SCALE_REVISIONS:-100000}"
scale_queries="${PALIMPSEST_SCALE_QUERIES:-20}"

if [[ "$scale_revisions" != +([0-9]) || "$scale_queries" != +([0-9]) ]]; then
    echo "PALIMPSEST_SCALE_REVISIONS and PALIMPSEST_SCALE_QUERIES must be decimal integers" >&2
    exit 2
fi
if ((scale_revisions < 1000 || scale_revisions > 1000000)); then
    echo "PALIMPSEST_SCALE_REVISIONS must be between 1000 and 1000000" >&2
    exit 2
fi
if ((scale_queries < 5 || scale_queries > 100)); then
    echo "PALIMPSEST_SCALE_QUERIES must be between 5 and 100" >&2
    exit 2
fi

for required_tool in psql sha256sum awk jq; do
    if ! command -v "$required_tool" >/dev/null 2>&1; then
        echo "required tool is unavailable: $required_tool" >&2
        exit 2
    fi
done

probe_dir="$(mktemp -d)"
metrics_file="$probe_dir/metrics.txt"
plan_file="$probe_dir/plan.txt"
plan_selective_file="$probe_dir/plan_selective.txt"
error_file="$probe_dir/error.txt"
cleanup() {
    rm -rf -- "$probe_dir"
}
trap cleanup EXIT
chmod 700 "$probe_dir"

if ! psql \
    --no-psqlrc \
    --dbname="$PALIMPSEST_SCALE_DATABASE_URL" \
    --quiet \
    --tuples-only \
    --no-align \
    --set=ON_ERROR_STOP=1 \
    --set="scale_revisions=$scale_revisions" \
    --set="scale_queries=$scale_queries" \
    --set="plan_file=$plan_file" \
    --set="plan_selective_file=$plan_selective_file" \
    >"$metrics_file" \
    2>"$error_file" <<'SQL'; then
BEGIN;
SET LOCAL statement_timeout = '15min';
SET LOCAL lock_timeout = '15s';
SET LOCAL synchronous_commit = off;

\set tenant_id '019bca00-0000-7000-8000-000000000001'
\set subject_id '019bca00-0000-7000-8000-000000000002'
\set case_id '019bca00-0000-7000-8000-000000000003'
\set probe_query 'scale probe'
\set selective_query 'scale probe grp0'

SELECT set_config('palimpsest.tenant_id', :'tenant_id', true) AS ignored \gset
SELECT set_config('palimpsest.subject_id', :'subject_id', true) AS ignored \gset
SELECT set_config('palimpsest.principal_id', 'palimpsest-scale-probe', true) AS ignored \gset
SELECT set_config('palimpsest.allowed_sensitivities', '["internal"]', true) AS ignored \gset
SELECT set_config('palimpsest.scale_queries', :'scale_queries', true) AS ignored \gset
SELECT set_config('palimpsest.retrieval_perspective', 'current', true) AS ignored \gset

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM memory.subject_lifecycles
        WHERE tenant_id = current_setting('palimpsest.tenant_id')::uuid
          AND subject_id = current_setting('palimpsest.subject_id')::uuid
    ) OR EXISTS (
        SELECT 1
        FROM memory.facts
        WHERE tenant_id = current_setting('palimpsest.tenant_id')::uuid
          AND subject_id = current_setting('palimpsest.subject_id')::uuid
    ) THEN
        RAISE EXCEPTION 'reserved scale probe scope is already occupied';
    END IF;
END
$guard$;

INSERT INTO memory.subject_lifecycles (
    tenant_id, subject_id, lifecycle_state, state_version
)
VALUES (:'tenant_id'::uuid, :'subject_id'::uuid, 'active', 0);

INSERT INTO memory.episodes (
    tenant_id, subject_id, case_id, episode_id, kind, observed_at,
    writer_principal_id, source_type, source_uri, external_id, sensitivity,
    retention_policy_id, schema_version, payload, payload_sha256
)
VALUES (
    :'tenant_id'::uuid,
    :'subject_id'::uuid,
    :'case_id'::uuid,
    md5('palimpsest-scale-episode')::uuid,
    'scale_probe',
    '2026-08-03T00:00:00Z'::timestamptz,
    'palimpsest-scale-probe',
    'scale.probe',
    NULL,
    'palimpsest-scale-probe',
    'internal',
    'standard',
    1,
    '{"kind":"rollback-only-scale-probe"}'::jsonb,
    encode(sha256(convert_to('palimpsest-scale-episode', 'UTF8')), 'hex')
);

INSERT INTO memory.facts (
    tenant_id, subject_id, case_id, fact_id, namespace, fact_key, schema_version
)
SELECT
    :'tenant_id'::uuid,
    :'subject_id'::uuid,
    :'case_id'::uuid,
    md5('palimpsest-scale-fact-' || series)::uuid,
    'scale.probe',
    'revision-' || series,
    1
FROM generate_series(1, :scale_revisions::bigint) AS generated(series);

INSERT INTO memory.fact_revisions (
    tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
    supersedes_revision_id, observed_at, valid_during, value, confidence,
    writer_principal_id, write_policy_id, write_policy_version, sensitivity,
    retention_policy_id, schema_version, content_sha256
)
SELECT
    :'tenant_id'::uuid,
    :'subject_id'::uuid,
    :'case_id'::uuid,
    md5('palimpsest-scale-fact-' || series)::uuid,
    md5('palimpsest-scale-revision-' || series)::uuid,
    1,
    NULL,
    '2026-08-03T00:00:00Z'::timestamptz,
    tstzrange('2026-01-01T00:00:00Z'::timestamptz, NULL, '[)'),
    jsonb_build_object('content', 'palimpsest scale probe revision ' || series || ' grp' || (series % 32)),
    1.0,
    'palimpsest-scale-probe',
    'direct-evidence',
    '1',
    'internal',
    'standard',
    1,
    encode(sha256(convert_to('palimpsest-scale-content-' || series, 'UTF8')), 'hex')
FROM generate_series(1, :scale_revisions::bigint) AS generated(series);

INSERT INTO memory.fact_revision_evidence (
    tenant_id, subject_id, case_id, fact_id, revision_id, episode_id, evidence_role
)
SELECT
    :'tenant_id'::uuid,
    :'subject_id'::uuid,
    :'case_id'::uuid,
    md5('palimpsest-scale-fact-' || series)::uuid,
    md5('palimpsest-scale-revision-' || series)::uuid,
    md5('palimpsest-scale-episode')::uuid,
    'scale_probe'
FROM generate_series(1, :scale_revisions::bigint) AS generated(series);

ANALYZE memory.facts;
ANALYZE memory.fact_revisions;
ANALYZE memory.fact_revision_current;
ANALYZE memory.fact_revision_governance;
ANALYZE memory.fact_revision_search_documents;

CREATE TEMP TABLE scale_probe_latencies (band text NOT NULL, elapsed_ms double precision NOT NULL);
DO $measure$
DECLARE
    started_at timestamptz;
    matched_rows bigint;
    iteration integer;
    band_query text;
BEGIN
    FOR iteration IN 1..current_setting('palimpsest.scale_queries')::integer LOOP
        started_at := clock_timestamp();
        band_query := CASE (iteration - 1) % 4
            WHEN 0 THEN 'scale probe'
            WHEN 1 THEN 'scale probe grp0 OR grp4 OR grp8 OR grp12 OR grp16 OR grp20 OR grp24 OR grp28'
            WHEN 2 THEN 'scale probe grp0 OR grp16'
            ELSE 'scale probe grp0'
        END;
        WITH current_coverage AS MATERIALIZED (
            SELECT COALESCE(
                (
                    SELECT coverage_state = 'complete'
                        AND (
                            coverage_valid_until IS NULL
                            OR coverage_valid_until > '2026-08-03T00:00:00Z'::timestamptz
                        )
                    FROM memory.fact_revision_current_coverage
                    WHERE tenant_id = current_setting('palimpsest.tenant_id')::uuid
                      AND subject_id = current_setting('palimpsest.subject_id')::uuid
                ),
                false
            ) AS projection_complete
        ), current_projection AS MATERIALIZED (
            SELECT projection.tenant_id,
                projection.subject_id,
                projection.case_id,
                projection.fact_id,
                projection.revision_id,
                projection.namespace,
                projection.fact_key,
                projection.sensitivity,
                projection.content_sha256
            FROM memory.fact_revision_current AS projection
            WHERE projection.tenant_id = current_setting('palimpsest.tenant_id')::uuid
              AND projection.subject_id = current_setting('palimpsest.subject_id')::uuid
              AND projection.recorded_at <= clock_timestamp()
              AND projection.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
        ), missing_facts AS MATERIALIZED (
            SELECT fact.tenant_id,
                fact.subject_id,
                fact.case_id,
                fact.fact_id,
                fact.namespace,
                fact.fact_key
            FROM memory.facts AS fact
            WHERE fact.tenant_id = current_setting('palimpsest.tenant_id')::uuid
              AND fact.subject_id = current_setting('palimpsest.subject_id')::uuid
              AND NOT (SELECT projection_complete FROM current_coverage)
              AND NOT EXISTS (
                  SELECT 1
                  FROM current_projection AS current_row
                  WHERE current_row.tenant_id = fact.tenant_id
                    AND current_row.subject_id = fact.subject_id
                    AND current_row.case_id = fact.case_id
                    AND current_row.fact_id = fact.fact_id
              )
        ), fallback AS MATERIALIZED (
            SELECT revision.tenant_id,
                revision.subject_id,
                revision.case_id,
                revision.fact_id,
                revision.revision_id,
                missing.namespace,
                missing.fact_key,
                revision.sensitivity,
                revision.content_sha256
            FROM missing_facts AS missing
            CROSS JOIN LATERAL (
                SELECT revision.tenant_id,
                    revision.subject_id,
                    revision.case_id,
                    revision.fact_id,
                    revision.revision_id,
                    revision.sensitivity,
                    revision.content_sha256
                FROM memory.fact_revisions AS revision
                WHERE revision.tenant_id = missing.tenant_id
                  AND revision.subject_id = missing.subject_id
                  AND revision.case_id = missing.case_id
                  AND revision.fact_id = missing.fact_id
                  AND revision.recorded_at <= clock_timestamp()
                  AND revision.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
                ORDER BY revision.revision_no DESC, revision.revision_id
                LIMIT 1
            ) AS revision
        ), effective AS MATERIALIZED (
            SELECT * FROM current_projection
            UNION ALL
            SELECT * FROM fallback
        ), authorized AS MATERIALIZED (
            SELECT effective.*
            FROM effective
            JOIN memory.fact_revision_governance AS governance
              ON governance.tenant_id = effective.tenant_id
             AND governance.subject_id = effective.subject_id
             AND governance.case_id = effective.case_id
             AND governance.fact_id = effective.fact_id
             AND governance.revision_id = effective.revision_id
            WHERE governance.lifecycle_state = 'active'
              AND (
                  governance.retention_expires_at IS NULL
                  OR governance.retention_expires_at > clock_timestamp()
              )
              AND effective.sensitivity = ANY (ARRAY['internal']::text[])
        ), ranked AS (
            SELECT authorized.fact_id, authorized.revision_id,
                ts_rank_cd(
                    document.search_vector,
                    websearch_to_tsquery('pg_catalog.simple', band_query)
                ) AS lexical_score
            FROM authorized
            JOIN memory.fact_revision_search_documents AS document
              ON document.tenant_id = authorized.tenant_id
             AND document.subject_id = authorized.subject_id
             AND document.case_id = authorized.case_id
             AND document.fact_id = authorized.fact_id
             AND document.revision_id = authorized.revision_id
             AND document.projection_schema_version = 1
             AND document.source_content_sha256 = authorized.content_sha256
            WHERE document.search_vector
                @@ websearch_to_tsquery('pg_catalog.simple', band_query)
            ORDER BY lexical_score DESC, authorized.fact_id, authorized.revision_id
            LIMIT 50
        )
        SELECT count(*) INTO matched_rows FROM ranked;
        INSERT INTO scale_probe_latencies (band, elapsed_ms)
        VALUES (
            CASE (iteration - 1) % 4
                WHEN 0 THEN 'all'
                WHEN 1 THEN 'quarter'
                WHEN 2 THEN 'sixteenth'
                ELSE 'thirtysecond'
            END,
            extract(epoch FROM clock_timestamp() - started_at) * 1000.0
        );
    END LOOP;
END
$measure$;

SELECT
    (SELECT count(*) FROM memory.fact_revisions WHERE tenant_id = :'tenant_id'::uuid AND subject_id = :'subject_id'::uuid)
    || '|' || :scale_queries
    || '|' || (SELECT count(*) FROM scale_probe_latencies WHERE band = 'all')
    || '|' || round((SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ms) FROM scale_probe_latencies WHERE band = 'all')::numeric, 3)
    || '|' || round((SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms) FROM scale_probe_latencies WHERE band = 'all')::numeric, 3)
    || '|' || round((SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ms) FROM scale_probe_latencies WHERE band = 'all')::numeric, 3)
    || '|' || round((SELECT avg(elapsed_ms) FROM scale_probe_latencies WHERE band = 'all')::numeric, 3)
    || '|' || round((SELECT max(elapsed_ms) FROM scale_probe_latencies WHERE band = 'all')::numeric, 3)
    || '|' || (SELECT count(*) FROM memory.fact_revision_search_documents WHERE tenant_id = :'tenant_id'::uuid AND subject_id = :'subject_id'::uuid);
SELECT band,
    count(*) AS measured_queries,
    round((percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ms))::numeric, 3) AS p50_ms,
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms))::numeric, 3) AS p95_ms,
    round((percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ms))::numeric, 3) AS p99_ms,
    round((avg(elapsed_ms))::numeric, 3) AS mean_ms,
    round((max(elapsed_ms))::numeric, 3) AS max_ms
FROM scale_probe_latencies
GROUP BY band
ORDER BY band;

\o :plan_file
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH current_coverage AS MATERIALIZED (
    SELECT COALESCE(
        (
            SELECT coverage_state = 'complete'
                AND (
                    coverage_valid_until IS NULL
                    OR coverage_valid_until > '2026-08-03T00:00:00Z'::timestamptz
                )
            FROM memory.fact_revision_current_coverage
            WHERE tenant_id = :'tenant_id'::uuid
              AND subject_id = :'subject_id'::uuid
        ),
        false
    ) AS projection_complete
), current_projection AS MATERIALIZED (
    SELECT projection.tenant_id,
        projection.subject_id,
        projection.case_id,
        projection.fact_id,
        projection.revision_id,
        projection.namespace,
        projection.fact_key,
        projection.sensitivity,
        projection.content_sha256
    FROM memory.fact_revision_current AS projection
    WHERE projection.tenant_id = :'tenant_id'::uuid
      AND projection.subject_id = :'subject_id'::uuid
      AND projection.recorded_at <= clock_timestamp()
      AND projection.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
), missing_facts AS MATERIALIZED (
    SELECT fact.tenant_id,
        fact.subject_id,
        fact.case_id,
        fact.fact_id,
        fact.namespace,
        fact.fact_key
    FROM memory.facts AS fact
    WHERE fact.tenant_id = :'tenant_id'::uuid
      AND fact.subject_id = :'subject_id'::uuid
      AND NOT (SELECT projection_complete FROM current_coverage)
      AND NOT EXISTS (
          SELECT 1
          FROM current_projection AS current_row
          WHERE current_row.tenant_id = fact.tenant_id
            AND current_row.subject_id = fact.subject_id
            AND current_row.case_id = fact.case_id
            AND current_row.fact_id = fact.fact_id
      )
), fallback AS MATERIALIZED (
    SELECT revision.tenant_id,
        revision.subject_id,
        revision.case_id,
        revision.fact_id,
        revision.revision_id,
        missing.namespace,
        missing.fact_key,
        revision.sensitivity,
        revision.content_sha256
    FROM missing_facts AS missing
    CROSS JOIN LATERAL (
        SELECT revision.tenant_id,
            revision.subject_id,
            revision.case_id,
            revision.fact_id,
            revision.revision_id,
            revision.sensitivity,
            revision.content_sha256
        FROM memory.fact_revisions AS revision
        WHERE revision.tenant_id = missing.tenant_id
          AND revision.subject_id = missing.subject_id
          AND revision.case_id = missing.case_id
          AND revision.fact_id = missing.fact_id
          AND revision.recorded_at <= clock_timestamp()
          AND revision.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
        ORDER BY revision.revision_no DESC, revision.revision_id
        LIMIT 1
    ) AS revision
), effective AS MATERIALIZED (
    SELECT * FROM current_projection
    UNION ALL
    SELECT * FROM fallback
), authorized AS MATERIALIZED (
    SELECT effective.*
    FROM effective
    JOIN memory.fact_revision_governance AS governance
      ON governance.tenant_id = effective.tenant_id
     AND governance.subject_id = effective.subject_id
     AND governance.case_id = effective.case_id
     AND governance.fact_id = effective.fact_id
     AND governance.revision_id = effective.revision_id
    WHERE governance.lifecycle_state = 'active'
      AND (governance.retention_expires_at IS NULL OR governance.retention_expires_at > clock_timestamp())
      AND effective.sensitivity = ANY (ARRAY['internal']::text[])
), ranked AS (
    SELECT authorized.fact_id, authorized.revision_id,
        ts_rank_cd(document.search_vector, websearch_to_tsquery('pg_catalog.simple', 'scale probe')) AS lexical_score
    FROM authorized
    JOIN memory.fact_revision_search_documents AS document
      ON document.tenant_id = authorized.tenant_id
     AND document.subject_id = authorized.subject_id
     AND document.case_id = authorized.case_id
     AND document.fact_id = authorized.fact_id
     AND document.revision_id = authorized.revision_id
     AND document.projection_schema_version = 1
     AND document.source_content_sha256 = authorized.content_sha256
    WHERE document.search_vector @@ websearch_to_tsquery('pg_catalog.simple', :'probe_query')
    ORDER BY lexical_score DESC, authorized.fact_id, authorized.revision_id
    LIMIT 50
)
SELECT count(*) FROM ranked;
\o

\o :plan_selective_file
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT count(*)
FROM memory.fact_revision_search_documents AS document
WHERE document.tenant_id = :'tenant_id'::uuid
  AND document.subject_id = :'subject_id'::uuid
  AND document.search_vector @@ websearch_to_tsquery('pg_catalog.simple', :'selective_query');
\o

ROLLBACK;
SQL
    echo "scale probe failed; no synthetic data was retained" >&2
    sed -E 's#(postgres(ql)?://)[^[:space:]]+#\1<redacted>#g' "$error_file" | tail -20 >&2
    exit 1
fi

psql_output="$(<"$metrics_file")"
plan_sha256="$(sha256sum "$plan_file" | awk '{print $1}')"
selective_plan_sha256="$(sha256sum "$plan_selective_file" | awk '{print $1}')"
plan_summary="$(jq -c '
    .[0] as $root
    | {
        planning_time_ms: ($root["Planning Time"] // 0),
        execution_time_ms: ($root["Execution Time"] // 0),
        top_nodes: (
            [$root.Plan | .. | objects | select(has("Node Type"))]
            | sort_by(-(."Actual Total Time" // 0))
            | .[:12]
            | map({
                node_type: .["Node Type"],
                relation: (.["Relation Name"] // null),
                actual_total_time_ms: (."Actual Total Time" // 0),
                actual_rows: (."Actual Rows" // 0),
                actual_loops: (."Actual Loops" // 0),
                shared_hit_blocks: (."Shared Hit Blocks" // 0),
                shared_read_blocks: (."Shared Read Blocks" // 0),
                temp_read_blocks: (."Temp Read Blocks" // 0),
                temp_written_blocks: (."Temp Written Blocks" // 0)
            })
        )
    }
' "$plan_file")"
selective_plan_summary="$(jq -c '
    .[0] as $root
    | {
        planning_time_ms: ($root["Planning Time"] // 0),
        execution_time_ms: ($root["Execution Time"] // 0),
        top_nodes: (
            [$root.Plan | .. | objects | select(has("Node Type"))]
            | sort_by(-(."Actual Total Time" // 0))
            | .[:12]
            | map({
                node_type: .["Node Type"],
                relation: (.["Relation Name"] // null),
                actual_total_time_ms: (."Actual Total Time" // 0),
                actual_rows: (."Actual Rows" // 0),
                actual_loops: (."Actual Loops" // 0),
                shared_hit_blocks: (."Shared Hit Blocks" // 0),
                shared_read_blocks: (."Shared Read Blocks" // 0),
                temp_read_blocks: (."Temp Read Blocks" // 0),
                temp_written_blocks: (."Temp Written Blocks" // 0)
            })
        )
    }
' "$plan_selective_file")"
IFS='|' read -r revision_count requested_queries measured_queries p50_ms p95_ms p99_ms mean_ms max_ms projection_count <<<"$(head -1 <<<"$psql_output")"

bands_json="$(
    tail -n +2 <<<"$psql_output" | while IFS='|' read -r band band_count b_p50 b_p95 b_p99 b_mean b_max; do
        printf '{"band":"%s","measured_queries":%s,"p50_ms":%s,"p95_ms":%s,"p99_ms":%s,"mean_ms":%s,"max_ms":%s},' \
            "$band" "$band_count" "$b_p50" "$b_p95" "$b_p99" "$b_mean" "$b_max"
    done | sed 's/,$//' | sed 's/^/[/; s/$/]/'
)"
total_measured="$(tail -n +2 <<<"$psql_output" | cut -d'|' -f2 | awk '{s += $1} END {print s}')"

if [[ -z "${projection_count:-}" || -z "$bands_json" || "$bands_json" == "[]" || "$total_measured" != "$requested_queries" ]]; then
    echo "scale probe returned an incomplete measurement" >&2
    exit 1
fi

printf '{"profile":"authorized-lexical-retrieval-scale-v1","revision_count":%s,"projection_count":%s,"query_count":%s,"p50_ms":%s,"p95_ms":%s,"p99_ms":%s,"mean_ms":%s,"max_ms":%s,"bands":%s,"plan_sha256":"%s","plan_summary":%s,"selective_plan_sha256":"%s","selective_plan_summary":%s,"transaction_rolled_back":true}\n' \
    "$revision_count" "$projection_count" "$measured_queries" "$p50_ms" "$p95_ms" "$p99_ms" "$mean_ms" "$max_ms" "$bands_json" "$plan_sha256" "$plan_summary" "$selective_plan_sha256" "$selective_plan_summary"
