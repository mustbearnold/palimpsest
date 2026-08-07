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
SET LOCAL work_mem = '256MB';
SET LOCAL max_parallel_workers_per_gather = 8;
SET LOCAL max_parallel_workers = 16;
SET LOCAL max_parallel_maintenance_workers = 4;
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

-- ANALYZE after the facts batch so the fact_revisions FK check plans
-- against real statistics. A maintained DB always has stats; on a cold
-- unanalyzed table the RI check can pick a non-PK index and crawl.
ANALYZE memory.facts;

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
ANALYZE memory.authorized_current_projection;

-- The maintenance trigger functions SET LOCAL work_mem / enable_nestloop
-- for their own execution and those settings leak into this transaction;
-- re-assert the measurement settings so the reported conditions match the
-- claim.
SET LOCAL work_mem = '256MB';
SET LOCAL max_parallel_workers_per_gather = 8;
SET LOCAL max_parallel_workers = 16;

\set evaluated_at `date -u +%Y-%m-%dT%H:%M:%SZ`
SELECT set_config('palimpsest.evaluated_at', :'evaluated_at', true) AS ignored \gset

DO $coverage_guard$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM memory.authorized_current_projection_coverage
        WHERE tenant_id = current_setting('palimpsest.tenant_id')::uuid
          AND subject_id = current_setting('palimpsest.subject_id')::uuid
          AND coverage_state = 'complete'
          AND projection_schema_version_min = 1
          AND projection_schema_sha256 = (
              SELECT projection_sha256
              FROM memory.search_projection_schemas
              WHERE projection_schema_version = 1
          )
          AND (
              coverage_valid_until IS NULL
              OR coverage_valid_until > current_setting('palimpsest.evaluated_at')::timestamptz
          )
    ) THEN
        RAISE EXCEPTION 'authorized-current structure is not complete for the scale probe scope';
    END IF;
END
$coverage_guard$;

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
        WITH scored AS (
            SELECT projection.fact_id,
                projection.revision_id,
                CASE
                    WHEN lower(projection.namespace || ':' || projection.fact_key)
                        = lower(btrim(band_query)) THEN 1::smallint
                    WHEN lower(projection.fact_key) = lower(btrim(band_query)) THEN 2::smallint
                    ELSE NULL::smallint
                END AS exact_identity_rank,
                projection.search_vector
                    @@ websearch_to_tsquery('pg_catalog.simple', band_query)
                    AS lexical_match,
                ts_rank_cd(
                    projection.search_vector,
                    websearch_to_tsquery('pg_catalog.simple', band_query)
                ) AS lexical_score
            FROM memory.authorized_current_projection AS projection
            WHERE projection.tenant_id = current_setting('palimpsest.tenant_id')::uuid
              AND projection.subject_id = current_setting('palimpsest.subject_id')::uuid
              AND projection.recorded_at <= current_setting('palimpsest.evaluated_at')::timestamptz
              AND projection.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
              AND projection.lifecycle_state = 'active'
              AND (
                  projection.retention_expires_at IS NULL
                  OR projection.retention_expires_at > current_setting('palimpsest.evaluated_at')::timestamptz
              )
              AND projection.sensitivity = ANY (ARRAY['internal']::text[])
              AND projection.projection_ready
              AND projection.projection_schema_version = 1
              AND projection.search_vector
                  @@ websearch_to_tsquery('pg_catalog.simple', band_query)
        ), ranked AS (
            SELECT scored.fact_id, scored.revision_id,
                CASE WHEN scored.lexical_match THEN
                    row_number() OVER (
                        PARTITION BY scored.lexical_match
                        ORDER BY scored.lexical_score DESC,
                            scored.fact_id, scored.revision_id
                    )
                END AS lexical_rank,
                scored.lexical_score
            FROM scored
            WHERE scored.exact_identity_rank IS NOT NULL OR scored.lexical_match
        ), limited AS MATERIALIZED (
            SELECT fact_id, revision_id, lexical_rank, lexical_score
            FROM ranked
            ORDER BY exact_identity_rank ASC NULLS LAST,
                lexical_rank ASC NULLS LAST, fact_id, revision_id
            LIMIT 50
        )
        SELECT count(*) INTO matched_rows FROM limited;
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
WITH scored AS (
    SELECT projection.fact_id,
        projection.revision_id,
        ts_rank_cd(
            projection.search_vector,
            websearch_to_tsquery('pg_catalog.simple', 'scale probe')
        ) AS lexical_score
    FROM memory.authorized_current_projection AS projection
    WHERE projection.tenant_id = :'tenant_id'::uuid
      AND projection.subject_id = :'subject_id'::uuid
      AND projection.recorded_at <= current_setting('palimpsest.evaluated_at')::timestamptz
      AND projection.valid_during @> '2026-08-03T00:00:00Z'::timestamptz
      AND projection.lifecycle_state = 'active'
      AND (
          projection.retention_expires_at IS NULL
          OR projection.retention_expires_at > current_setting('palimpsest.evaluated_at')::timestamptz
      )
      AND projection.sensitivity = ANY (ARRAY['internal']::text[])
      AND projection.projection_ready
      AND projection.projection_schema_version = 1
      AND projection.search_vector
          @@ websearch_to_tsquery('pg_catalog.simple', 'scale probe')
), ranked AS (
    SELECT scored.fact_id, scored.revision_id, scored.lexical_score
    FROM scored
    ORDER BY scored.lexical_score DESC, scored.fact_id, scored.revision_id
    LIMIT 50
)
SELECT count(*) FROM ranked;
\o

\o :plan_selective_file
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT count(*)
FROM memory.authorized_current_projection AS projection
WHERE projection.tenant_id = :'tenant_id'::uuid
  AND projection.subject_id = :'subject_id'::uuid
  AND projection.projection_ready
  AND projection.projection_schema_version = 1
  AND projection.search_vector @@ websearch_to_tsquery('pg_catalog.simple', :'selective_query');
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
