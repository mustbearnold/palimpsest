use std::{env, fs, path::Path, str::FromStr, sync::Arc};

use anyhow::{Context, Result, ensure};
use palimpsest_application::MemoryService;
use palimpsest_domain::{
    CreateRetrieval, PrincipalId, PrincipalScope, RetrievalFilters, RetrievalId,
    RetrievalPerspective, RetrievalQuery, Sensitivity, SubjectId, TenantId,
};
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgConnectOptions};
use uuid::Uuid;

const PRE_VECTOR_MIGRATIONS: [&str; 6] = [
    include_str!("../../../migrations/0001_episodes.sql"),
    include_str!("../../../migrations/0002_facts.sql"),
    include_str!("../../../migrations/0003_idempotency.sql"),
    include_str!("../../../migrations/0004_governed_writes.sql"),
    include_str!("../../../migrations/0005_checkpoints.sql"),
    include_str!("../../../migrations/0006_authorized_lexical_retrieval.sql"),
];
const VECTOR_MIGRATION: &str = include_str!("../../../migrations/0007_exact_vector_retrieval.sql");
const CURRENT_MIGRATION_FILES: [&str; 9] = [
    "0008_deterministic_temporal_retrieval.sql",
    "0009_subject_lifecycle_fence.sql",
    "0010_deletion_operations.sql",
    "0011_canonical_history_exports.sql",
    "0012_deletion_rls_worker_paths.sql",
    "0013_deletion_terminal_outcomes.sql",
    "0014_bounded_projection_leases.sql",
    "0015_restore_fence_replay.sql",
    "0016_release_deletion_operation_lease.sql",
];

const TENANT_ID: &str = "019be100-0000-7000-8000-000000000010";
const SUBJECT_ID: &str = "019be100-0000-7000-8000-000000000020";
const RETRIEVAL_ID: &str = "019be100-0000-7000-8000-000000000030";
const CASE_ID: &str = "019be100-0000-7000-8000-000000000040";
const EPISODE_ID: &str = "019be100-0000-7000-8000-000000000050";
const FACT_ID: &str = "019be100-0000-7000-8000-000000000060";
const REVISION_ID: &str = "019be100-0000-7000-8000-000000000070";
const CURSOR_TOKEN: &str = "019be100-0000-7000-8000-000000000080";
const HYBRID_RETRIEVAL_ID: &str = "019be100-0000-7000-8000-000000000090";
const HYBRID_CURSOR_TOKEN: &str = "019be100-0000-7000-8000-0000000000a0";
const PRINCIPAL_ID: &str = "legacy-principal";
const IDEMPOTENCY_KEY: &str = "legacy-lexical-receipt-upgrade";
const HYBRID_IDEMPOTENCY_KEY: &str = "legacy-hybrid-receipt-upgrade";
const QUERY: &str = "legacy upgrade replay";

// These literals were captured from the stable 0006 createRetrieval contract.
// Keeping them independent of current hashing code makes drift break the test.
const REQUEST_FINGERPRINT: &str =
    "a7fb34245f9a507221689151d6d303a14cdcbe9c9b6bb87025dad5f1a34ed8aa";
const AUTHORIZATION_SCOPE_SHA256: &str =
    "54c775abae3b6f267a87fb688e09409f7d1ae25defe6bb37009e87042a0c0c4c";
const QUERY_SHA256: &str = "66fbc0dd66afcdc22017ced43e481513552f7de03d69ff15f0498e95bbafa513";
const EPISODE_PAYLOAD_SHA256: &str =
    "c287f16e0476f785c014d406aacbb82a1f76f87fdce205e63e3f0095e0f9ddae";
const CONTENT_SHA256: &str = "55b55db382bd4274e53aa78aaaa843d0788bad85b49867e336fbbe876c5817ef";
const PROJECTION_SHA256: &str = "1f3693911606f0075d3e5ca6a412fc68bd553a1ea0f05f67acb2176479fbf782";
const ITEM_SHA256: &str = "18fb83dd16500c7d9dfef1d84c82dce34e597d7b2468b1d842914972b6404e0b";
const MANIFEST_SHA256: &str = "fe77e700b2d5bd06965ab5076e7de342deb274311b7a71b3d80b1e26a88f89ca";
const HYBRID_ITEM_SHA256: &str = "b90d7af16d17b51df42d31b7ec38d93672c2643036eeac45fc7b9f99356e155a";
const HYBRID_MANIFEST_SHA256: &str =
    "96a7e172a8ed2f2022757ec09ce34000169f17ace87ade66d4f9109b1c00dcd7";

#[tokio::test]
async fn pre_vector_lexical_receipt_survives_and_replays_after_migration() -> Result<()> {
    let database_url = env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;
    let migration_database_url =
        env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_admin_pool = PgPool::connect(&migration_database_url)
        .await
        .with_context(|| {
            format!("connect to migration-authority PostgreSQL through {migration_database_url}")
        })?;
    verify_database_versions(&admin_pool).await?;

    let database_name = format!("palimpsest_upgrade_{}", Uuid::now_v7().simple());
    // The identifier is generated exclusively from a UUID's lowercase hex form.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;

    let runtime_options = PgConnectOptions::from_str(&database_url)?.database(&database_name);
    let runtime_pool = PgPool::connect_with(runtime_options).await?;
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let migration_pool = PgPool::connect_with(migration_options).await?;

    let result = async {
        apply_migrations(&runtime_pool, &PRE_VECTOR_MIGRATIONS).await?;
        insert_pre_vector_canonical_fact(&migration_pool).await?;
        insert_pre_vector_lexical_receipt(&migration_pool).await?;
        let legacy = load_legacy_receipt_evidence(&migration_pool).await?;

        apply_migrations(&runtime_pool, &[VECTOR_MIGRATION]).await?;
        register_pre_temporal_hybrid_policy(&migration_pool).await?;
        insert_pre_temporal_hybrid_receipt(&migration_pool).await?;
        let legacy_hybrid =
            load_receipt_evidence(&migration_pool, Uuid::parse_str(HYBRID_RETRIEVAL_ID)?).await?;

        apply_current_migrations(&migration_pool).await?;
        grant_runtime_content_lease_functions(&migration_pool, &runtime_pool).await?;
        verify_preserved_database_contract(&migration_pool, &legacy).await?;
        verify_preserved_hybrid_contract(&migration_pool, &legacy_hybrid).await?;
        verify_temporal_schema_contract(&migration_pool, &legacy, &legacy_hybrid).await?;
        verify_public_replay(&runtime_pool, &legacy).await
    }
    .await;

    migration_pool.close().await;
    runtime_pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&migration_admin_pool)
    .await?;
    migration_admin_pool.close().await;
    result
}

async fn verify_database_versions(pool: &PgPool) -> Result<()> {
    let version_num: i32 = sqlx::query("SELECT current_setting('server_version_num')::integer")
        .fetch_one(pool)
        .await?
        .try_get(0)?;
    let vector_version: String =
        sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_one(pool)
            .await?
            .try_get(0)?;
    ensure!(version_num >= 180_000, "PostgreSQL 18+ is required");
    ensure!(vector_version == "0.8.5", "pgvector 0.8.5 is required");
    Ok(())
}

async fn apply_migrations(pool: &PgPool, migrations: &[&'static str]) -> Result<()> {
    for migration in migrations {
        sqlx::raw_sql(*migration).execute(pool).await?;
    }
    Ok(())
}

async fn apply_current_migrations(pool: &PgPool) -> Result<()> {
    for file_name in CURRENT_MIGRATION_FILES {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations")
            .join(file_name);
        let migration = fs::read_to_string(&path)
            .with_context(|| format!("read current migration {}", path.display()))?;
        // The only dynamic input is repository-owned migration text from fixed names above.
        sqlx::raw_sql(AssertSqlSafe(migration))
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn grant_runtime_content_lease_functions(
    migration_pool: &PgPool,
    runtime_pool: &PgPool,
) -> Result<()> {
    let quoted_runtime_role: String = sqlx::query_scalar("SELECT quote_ident(current_user::text)")
        .fetch_one(runtime_pool)
        .await?;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON \
         memory.subject_lifecycles, memory.subject_content_leases \
         TO {quoted_runtime_role}; \
         GRANT SELECT, INSERT, UPDATE, DELETE ON \
         memory.export_operations, memory.export_manifest_items \
         TO {quoted_runtime_role}; \
         GRANT DELETE ON memory.subject_content_leases TO {quoted_runtime_role}; \
         GRANT EXECUTE ON FUNCTION \
         memory.acquire_subject_content_lease(uuid, uuid, uuid, text), \
         memory.release_subject_content_lease(uuid, uuid, uuid, text), \
         memory.claim_next_export_operation(uuid, integer), \
         memory.claim_next_expired_export_operation(uuid, integer) \
         TO {quoted_runtime_role}"
    )))
    .execute(migration_pool)
    .await?;
    Ok(())
}

async fn insert_pre_vector_canonical_fact(pool: &PgPool) -> Result<()> {
    sqlx::query("ALTER TABLE memory.fact_revisions DISABLE TRIGGER fact_revisions_prepare_insert")
        .execute(pool)
        .await?;
    let mut transaction = pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO memory.episodes (
            tenant_id, subject_id, case_id, episode_id, kind,
            observed_at, recorded_at, writer_principal_id,
            source_type, source_uri, external_id, sensitivity,
            retention_policy_id, schema_version, payload, payload_sha256
        )
        VALUES (
            $1, $2, $3, $4, 'observation',
            '2026-07-28 23:58:00+00', '2026-07-28 23:58:30+00', $5,
            'migration-fixture', NULL, 'legacy-upgrade-evidence', 'internal',
            'standard', 1, '{"source":"legacy fixture"}'::jsonb, $6
        )
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(EPISODE_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(EPISODE_PAYLOAD_SHA256)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.facts (
            tenant_id, subject_id, case_id, fact_id,
            namespace, fact_key, created_at, schema_version
        )
        VALUES (
            $1, $2, $3, $4,
            'legacy.profile', 'upgrade_result', '2026-07-28 23:59:00+00', 1
        )
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            revision_no, supersedes_revision_id, observed_at, recorded_at,
            valid_during, value, confidence, writer_principal_id,
            write_policy_id, write_policy_version, sensitivity,
            retention_policy_id, schema_version, content_sha256
        )
        VALUES (
            $1, $2, $3, $4, $5,
            1, NULL, '2026-07-28 23:59:00+00', '2026-07-28 23:59:30+00',
            tstzrange('2026-01-01 00:00:00+00', NULL, '[)'),
            '{"answer":"legacy upgrade replay"}'::jsonb, 0.9000, $6,
            'fixture-policy', '1', 'internal',
            'standard', 1, $7
        )
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(CONTENT_SHA256)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.fact_revision_evidence (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            episode_id, evidence_role
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .bind(Uuid::parse_str(EPISODE_ID)?)
    .execute(&mut *transaction)
    .await?;

    transaction.commit().await?;
    sqlx::query("ALTER TABLE memory.fact_revisions ENABLE TRIGGER fact_revisions_prepare_insert")
        .execute(pool)
        .await?;

    let generated_projection: String = sqlx::query(
        r#"
        SELECT projection_sha256
        FROM memory.fact_revision_search_documents
        WHERE tenant_id = $1 AND subject_id = $2
          AND case_id = $3 AND fact_id = $4 AND revision_id = $5
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .fetch_one(pool)
    .await?
    .try_get("projection_sha256")?;
    ensure!(generated_projection == PROJECTION_SHA256);
    Ok(())
}

async fn insert_pre_vector_lexical_receipt(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_idempotency_reservations (
            tenant_id, subject_id, principal_id, idempotency_key,
            request_fingerprint, retrieval_id, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, '2026-07-29 00:00:00+00')
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(IDEMPOTENCY_KEY)
    .bind(REQUEST_FINGERPRINT)
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_receipts (
            tenant_id, subject_id, retrieval_id, principal_id,
            idempotency_key, request_fingerprint, query_sha256,
            perspective, valid_at, recorded_at, evaluated_at,
            policy_id, policy_version, policy_sha256,
            projection_schema_version, projection_schema_sha256,
            authorization_scope_sha256, authorization_policy_version,
            page_size, outcome, abstention_reason, stage_timings_ms,
            manifest_sha256, created_at, schema_version
        )
        SELECT
            $1, $2, $3, $4,
            $5, $6, $7,
            'current', fixture_time, fixture_time, fixture_time,
            policy.policy_id, policy.policy_version, policy.policy_sha256,
            projection.projection_schema_version, projection.projection_sha256,
            $8, 'principal-scope-v1',
            10, 'results', NULL,
            '{"candidate_generation": 0.25}'::jsonb,
            $9, fixture_time, 1
        FROM memory.lexical_retrieval_policies AS policy
        CROSS JOIN memory.search_projection_schemas AS projection
        CROSS JOIN LATERAL (
            SELECT '2026-07-29 00:00:00+00'::timestamptz AS fixture_time
        ) AS fixture
        WHERE policy.policy_id = 'retrieval-lexical-v1'
          AND policy.policy_version = '1'
          AND projection.projection_schema_version = 1
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(IDEMPOTENCY_KEY)
    .bind(REQUEST_FINGERPRINT)
    .bind(QUERY_SHA256)
    .bind(AUTHORIZATION_SCOPE_SHA256)
    .bind(MANIFEST_SHA256)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_manifest_items (
            tenant_id, subject_id, retrieval_id, principal_id,
            ordinal, cursor_token, case_id, fact_id, revision_id,
            exact_identity_rank, lexical_rank, lexical_score,
            final_rank, final_score, source_content_sha256,
            projection_sha256, item_sha256, schema_version
        )
        VALUES (
            $1, $2, $3, $4,
            1, $5, $6, $7, $8,
            NULL, 1, 0.750000000000,
            1, 0.750000000000, $9,
            $10, $11, 1
        )
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(Uuid::parse_str(CURSOR_TOKEN)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .bind(CONTENT_SHA256)
    .bind(PROJECTION_SHA256)
    .bind(ITEM_SHA256)
    .execute(pool)
    .await?;
    Ok(())
}

async fn register_pre_temporal_hybrid_policy(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        WITH fixture AS (
            SELECT jsonb_build_object(
                'provider', 'palimpsest-upgrade-fixture',
                'model', 'deterministic-fixture',
                'model_revision', 'fixture-4d-2026-07-29',
                'dimensions', 4,
                'normalization', jsonb_build_object(
                    'kind', 'unit_l2', 'tolerance', '0.000001'
                ),
                'distance_metric', 'cosine',
                'scalar_type', 'float32',
                'task_modes', jsonb_build_object(
                    'query', 'query', 'document', 'document'
                ),
                'serialization', 'utf8',
                'provider_contract_schema_version', 1,
                'schema_version', 1
            ) AS document
        )
        INSERT INTO memory.embedding_profiles (
            profile_id, profile_version, provider, model, model_revision,
            dimensions, normalization, normalization_tolerance,
            distance_metric, scalar_type, input_serialization,
            query_task_mode, document_task_mode,
            provider_contract_schema_version,
            profile_document, profile_sha256, schema_version
        )
        SELECT
            'embedding-upgrade-4d-v1', '1',
            'palimpsest-upgrade-fixture', 'deterministic-fixture',
            'fixture-4d-2026-07-29', 4, 'unit_l2', 0.000001,
            'cosine', 'float32', 'utf8', 'query', 'document', 1,
            document, encode(sha256(convert_to(document::text, 'UTF8')), 'hex'), 1
        FROM fixture;

        WITH fixture AS (
            SELECT
                embedding.profile_id AS embedding_profile_id,
                embedding.profile_version AS embedding_profile_version,
                embedding.profile_sha256 AS embedding_profile_sha256,
                source.projection_schema_version,
                source.projection_sha256,
                jsonb_build_object(
                    'memory_kind', 'fact_revision',
                    'projection_schema_version', 1,
                    'serialization', 'fact-projection-v1',
                    'input_schema_version', 1,
                    'schema_version', 1,
                    'fields', jsonb_build_array('namespace', 'key', 'value'),
                    'embedding_profile', jsonb_build_object(
                        'id', embedding.profile_id,
                        'version', embedding.profile_version,
                        'digest', embedding.profile_sha256
                    ),
                    'source_projection', jsonb_build_object(
                        'schema_version', source.projection_schema_version,
                        'digest', source.projection_sha256
                    )
                ) AS document
            FROM memory.embedding_profiles AS embedding
            CROSS JOIN memory.search_projection_schemas AS source
            WHERE embedding.profile_id = 'embedding-upgrade-4d-v1'
              AND embedding.profile_version = '1'
              AND source.projection_schema_version = 1
        )
        INSERT INTO memory.embedding_projection_profiles (
            projection_profile_id, projection_profile_version,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            source_projection_schema_version,
            source_projection_schema_sha256,
            input_serialization, input_schema_version,
            projection_document, projection_profile_sha256, schema_version
        )
        SELECT
            'fact-embedding-upgrade-v1', '1',
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            projection_schema_version, projection_sha256,
            'fact-projection-v1', 1, document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'), 1
        FROM fixture;

        WITH plan AS (
            SELECT
                embedding.profile_id,
                embedding.profile_version,
                embedding.profile_sha256,
                projection.projection_profile_id,
                projection.projection_profile_version,
                projection.projection_profile_sha256,
                jsonb_build_object(
                    'candidate_limit', 50,
                    'candidate_limits', jsonb_build_object(
                        'exact', 50, 'lexical', 50, 'vector', 50
                    ),
                    'manifest_limit', 50,
                    'default_page_size', 10,
                    'maximum_page_size', 50,
                    'fts_configuration', 'pg_catalog.simple',
                    'fts_rank', 'ts_rank_cd',
                    'fts_rank_normalization', 32,
                    'exact_identity_precedence', true,
                    'distance_metric', 'cosine',
                    'fusion', jsonb_build_object(
                        'method', 'reciprocal-rank', 'k', 60,
                        'weights', jsonb_build_object(
                            'exact', 1, 'lexical', 1, 'vector', 1
                        )
                    ),
                    'score_scale', 12,
                    'rounding', 'half-away-from-zero',
                    'embedding_profile', jsonb_build_object(
                        'id', embedding.profile_id,
                        'version', embedding.profile_version,
                        'digest', embedding.profile_sha256
                    ),
                    'projection_profile', jsonb_build_object(
                        'id', projection.projection_profile_id,
                        'version', projection.projection_profile_version,
                        'digest', projection.projection_profile_sha256
                    ),
                    'fallback', 'none',
                    'channel_tie_breaks', jsonb_build_object(
                        'exact', jsonb_build_array(
                            'exact_identity_rank_asc', 'case_id_asc',
                            'fact_id_asc', 'revision_id_asc'
                        ),
                        'lexical', jsonb_build_array(
                            'lexical_score_desc', 'case_id_asc',
                            'fact_id_asc', 'revision_id_asc'
                        ),
                        'vector', jsonb_build_array(
                            'vector_distance_asc', 'case_id_asc',
                            'fact_id_asc', 'revision_id_asc'
                        )
                    ),
                    'tie_break', jsonb_build_array(
                        'fused_score_desc',
                        'exact_identity_rank_asc_nulls_last',
                        'exact_rank_asc_nulls_last',
                        'lexical_rank_asc_nulls_last',
                        'vector_rank_asc_nulls_last',
                        'case_id_asc', 'fact_id_asc', 'revision_id_asc'
                    )
                ) AS document
            FROM memory.embedding_profiles AS embedding
            JOIN memory.embedding_projection_profiles AS projection
              ON projection.embedding_profile_id = embedding.profile_id
             AND projection.embedding_profile_version = embedding.profile_version
             AND projection.embedding_profile_sha256 = embedding.profile_sha256
            WHERE embedding.profile_id = 'embedding-upgrade-4d-v1'
              AND projection.projection_profile_id = 'fact-embedding-upgrade-v1'
        )
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        )
        SELECT
            'retrieval-hybrid-v1', '1', document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1, 'hybrid', profile_id, profile_version, profile_sha256,
            projection_profile_id, projection_profile_version,
            projection_profile_sha256
        FROM plan;
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_pre_temporal_hybrid_receipt(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_idempotency_reservations (
            tenant_id, subject_id, principal_id, idempotency_key,
            request_fingerprint, retrieval_id, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, '2026-07-29 00:01:00+00')
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(HYBRID_IDEMPOTENCY_KEY)
    .bind(REQUEST_FINGERPRINT)
    .bind(Uuid::parse_str(HYBRID_RETRIEVAL_ID)?)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_receipts (
            tenant_id, subject_id, retrieval_id, principal_id,
            idempotency_key, request_fingerprint, query_sha256,
            perspective, valid_at, recorded_at, evaluated_at,
            policy_id, policy_version, policy_sha256,
            projection_schema_version, projection_schema_sha256,
            authorization_scope_sha256, authorization_policy_version,
            page_size, outcome, abstention_reason, stage_timings_ms,
            manifest_sha256, created_at, schema_version,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            query_input_sha256, query_vector_sha256
        )
        SELECT
            $1, $2, $3, $4, $5, $6, $7,
            'current', fixture_time, fixture_time, fixture_time,
            policy.policy_id, policy.policy_version, policy.policy_sha256,
            projection.projection_schema_version, projection.projection_sha256,
            $8, 'principal-scope-v1', 10, 'results', NULL,
            '{"candidate_generation":0.50}'::jsonb,
            $9, fixture_time, 1,
            policy.embedding_profile_id, policy.embedding_profile_version,
            policy.embedding_profile_sha256,
            policy.embedding_projection_profile_id,
            policy.embedding_projection_profile_version,
            policy.embedding_projection_profile_sha256,
            $7, $10
        FROM memory.retrieval_policies AS policy
        CROSS JOIN memory.search_projection_schemas AS projection
        CROSS JOIN LATERAL (
            SELECT '2026-07-29 00:01:00+00'::timestamptz AS fixture_time
        ) AS fixture
        WHERE policy.policy_id = 'retrieval-hybrid-v1'
          AND policy.policy_version = '1'
          AND projection.projection_schema_version = 1
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(HYBRID_RETRIEVAL_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(HYBRID_IDEMPOTENCY_KEY)
    .bind(REQUEST_FINGERPRINT)
    .bind(QUERY_SHA256)
    .bind(AUTHORIZATION_SCOPE_SHA256)
    .bind(HYBRID_MANIFEST_SHA256)
    .bind("1111111111111111111111111111111111111111111111111111111111111111")
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO memory.retrieval_manifest_items (
            tenant_id, subject_id, retrieval_id, principal_id,
            ordinal, cursor_token, case_id, fact_id, revision_id,
            exact_identity_rank, exact_rank, lexical_rank, lexical_score,
            vector_rank, vector_distance, vector_similarity,
            exact_rrf_contribution, lexical_rrf_contribution,
            vector_rrf_contribution, fused_score,
            final_rank, final_score, source_content_sha256,
            projection_sha256, item_sha256,
            embedding_profile_sha256,
            embedding_projection_profile_sha256,
            embedding_input_sha256, embedding_vector_sha256,
            schema_version
        )
        SELECT
            $1, $2, $3, $4, 1, $5, $6, $7, $8,
            1, 1, 1, 0.750000000000,
            1, 0.100000000000, 0.900000000000,
            0.016393442623, 0.016393442623, 0.016393442623,
            0.049180327869, 1, 0.049180327869,
            $9, $10, $11,
            policy.embedding_profile_sha256,
            policy.embedding_projection_profile_sha256,
            $10, $12, 1
        FROM memory.retrieval_policies AS policy
        WHERE policy.policy_id = 'retrieval-hybrid-v1'
          AND policy.policy_version = '1'
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(HYBRID_RETRIEVAL_ID)?)
    .bind(PRINCIPAL_ID)
    .bind(Uuid::parse_str(HYBRID_CURSOR_TOKEN)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .bind(CONTENT_SHA256)
    .bind(PROJECTION_SHA256)
    .bind(HYBRID_ITEM_SHA256)
    .bind("2222222222222222222222222222222222222222222222222222222222222222")
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug)]
struct LegacyReceiptEvidence {
    policy_sha256: String,
    receipt: serde_json::Value,
    manifest: serde_json::Value,
}

async fn load_legacy_receipt_evidence(pool: &PgPool) -> Result<LegacyReceiptEvidence> {
    load_receipt_evidence(pool, Uuid::parse_str(RETRIEVAL_ID)?).await
}

async fn load_receipt_evidence(pool: &PgPool, retrieval_id: Uuid) -> Result<LegacyReceiptEvidence> {
    let row = sqlx::query(
        r#"
        SELECT receipt.policy_sha256,
            to_jsonb(receipt) AS receipt, to_jsonb(manifest) AS manifest
        FROM memory.retrieval_receipts AS receipt
        JOIN memory.retrieval_manifest_items AS manifest
          ON manifest.tenant_id = receipt.tenant_id
         AND manifest.subject_id = receipt.subject_id
         AND manifest.retrieval_id = receipt.retrieval_id
        WHERE receipt.tenant_id = $1
          AND receipt.subject_id = $2
          AND receipt.retrieval_id = $3
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(retrieval_id)
    .fetch_one(pool)
    .await?;
    Ok(LegacyReceiptEvidence {
        policy_sha256: row.try_get("policy_sha256")?,
        receipt: row.try_get("receipt")?,
        manifest: row.try_get("manifest")?,
    })
}

async fn verify_preserved_database_contract(
    pool: &PgPool,
    legacy: &LegacyReceiptEvidence,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            to_jsonb(receipt) - ARRAY[
                'embedding_profile_id',
                'embedding_profile_version',
                'embedding_profile_sha256',
                'embedding_projection_profile_id',
                'embedding_projection_profile_version',
                'embedding_projection_profile_sha256',
                'query_input_sha256',
                'query_vector_sha256'
            ]::text[] AS legacy_receipt,
            to_jsonb(manifest) - ARRAY[
                'exact_rank',
                'vector_rank',
                'vector_distance',
                'vector_similarity',
                'exact_rrf_contribution',
                'lexical_rrf_contribution',
                'vector_rrf_contribution',
                'fused_score',
                'embedding_profile_sha256',
                'embedding_projection_profile_sha256',
                'embedding_input_sha256',
                'embedding_vector_sha256',
                'recency_profile_id',
                'recency_profile_version',
                'recency_profile_sha256',
                'recency_anchor_at',
                'recency_age_us',
                'recency_factor',
                'confidence_factor',
                'importance_factor',
                'temporal_adjustment',
                'confidence_adjustment',
                'importance_adjustment',
                'exact_identity_bonus'
            ]::text[] AS legacy_manifest,
            receipt.embedding_profile_id,
            receipt.embedding_projection_profile_id,
            receipt.query_input_sha256,
            receipt.query_vector_sha256,
            policy.retrieval_mode,
            reservation.retrieval_id IS NOT NULL AS reservation_fk_target_present,
            projection.projection_schema_version IS NOT NULL
                AS projection_fk_target_present,
            revision.revision_id IS NOT NULL AS revision_fk_target_present,
            (
                SELECT count(*) = 3
                FROM pg_catalog.pg_constraint AS constraint_row
                WHERE constraint_row.conrelid = 'memory.retrieval_receipts'::regclass
                  AND constraint_row.contype = 'f'
                  AND (
                      pg_get_constraintdef(constraint_row.oid)
                          LIKE '%memory.retrieval_idempotency_reservations%'
                      OR pg_get_constraintdef(constraint_row.oid)
                          LIKE '%memory.retrieval_policies%'
                      OR pg_get_constraintdef(constraint_row.oid)
                          LIKE '%memory.search_projection_schemas%'
                  )
            ) AS legacy_receipt_fks_present,
            (
                SELECT count(*) = 2
                FROM pg_catalog.pg_constraint AS constraint_row
                WHERE constraint_row.conrelid
                    = 'memory.retrieval_manifest_items'::regclass
                  AND constraint_row.contype = 'f'
                  AND (
                      pg_get_constraintdef(constraint_row.oid)
                          LIKE '%memory.retrieval_receipts%'
                      OR pg_get_constraintdef(constraint_row.oid)
                          LIKE '%memory.fact_revisions%'
                  )
            ) AS legacy_manifest_fks_present
        FROM memory.retrieval_receipts AS receipt
        JOIN memory.retrieval_manifest_items AS manifest
          ON manifest.tenant_id = receipt.tenant_id
         AND manifest.subject_id = receipt.subject_id
         AND manifest.retrieval_id = receipt.retrieval_id
         AND manifest.principal_id = receipt.principal_id
        JOIN memory.retrieval_policies AS policy
          ON policy.policy_id = receipt.policy_id
         AND policy.policy_version = receipt.policy_version
         AND policy.policy_sha256 = receipt.policy_sha256
        JOIN memory.retrieval_idempotency_reservations AS reservation
          ON reservation.tenant_id = receipt.tenant_id
         AND reservation.subject_id = receipt.subject_id
         AND reservation.retrieval_id = receipt.retrieval_id
         AND reservation.principal_id = receipt.principal_id
        JOIN memory.search_projection_schemas AS projection
          ON projection.projection_schema_version
             = receipt.projection_schema_version
         AND projection.projection_sha256 = receipt.projection_schema_sha256
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = manifest.tenant_id
         AND revision.subject_id = manifest.subject_id
         AND revision.case_id = manifest.case_id
         AND revision.fact_id = manifest.fact_id
         AND revision.revision_id = manifest.revision_id
        WHERE receipt.tenant_id = $1
          AND receipt.subject_id = $2
          AND receipt.retrieval_id = $3
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
    .fetch_one(pool)
    .await?;

    ensure!(row.try_get::<serde_json::Value, _>("legacy_receipt")? == legacy.receipt);
    ensure!(row.try_get::<serde_json::Value, _>("legacy_manifest")? == legacy.manifest);
    ensure!(
        row.try_get::<Option<String>, _>("embedding_profile_id")?
            .is_none()
    );
    ensure!(
        row.try_get::<Option<String>, _>("embedding_projection_profile_id")?
            .is_none()
    );
    ensure!(
        row.try_get::<Option<String>, _>("query_input_sha256")?
            .is_none()
    );
    ensure!(
        row.try_get::<Option<String>, _>("query_vector_sha256")?
            .is_none()
    );
    ensure!(row.try_get::<String, _>("retrieval_mode")? == "lexical");
    ensure!(row.try_get::<bool, _>("reservation_fk_target_present")?);
    ensure!(row.try_get::<bool, _>("projection_fk_target_present")?);
    ensure!(row.try_get::<bool, _>("revision_fk_target_present")?);
    ensure!(row.try_get::<bool, _>("legacy_receipt_fks_present")?);
    ensure!(row.try_get::<bool, _>("legacy_manifest_fks_present")?);
    Ok(())
}

async fn verify_preserved_hybrid_contract(
    pool: &PgPool,
    legacy: &LegacyReceiptEvidence,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            to_jsonb(receipt) AS receipt,
            to_jsonb(manifest) - ARRAY[
                'recency_profile_id',
                'recency_profile_version',
                'recency_profile_sha256',
                'recency_anchor_at',
                'recency_age_us',
                'recency_factor',
                'confidence_factor',
                'importance_factor',
                'temporal_adjustment',
                'confidence_adjustment',
                'importance_adjustment',
                'exact_identity_bonus'
            ]::text[] AS legacy_manifest,
            receipt.policy_sha256,
            receipt.manifest_sha256,
            manifest.item_sha256,
            policy.policy_sha256 AS registered_policy_sha256
        FROM memory.retrieval_receipts AS receipt
        JOIN memory.retrieval_manifest_items AS manifest
          ON manifest.tenant_id = receipt.tenant_id
         AND manifest.subject_id = receipt.subject_id
         AND manifest.retrieval_id = receipt.retrieval_id
         AND manifest.principal_id = receipt.principal_id
        JOIN memory.retrieval_policies AS policy
          ON policy.policy_id = receipt.policy_id
         AND policy.policy_version = receipt.policy_version
        WHERE receipt.tenant_id = $1
          AND receipt.subject_id = $2
          AND receipt.retrieval_id = $3
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(HYBRID_RETRIEVAL_ID)?)
    .fetch_one(pool)
    .await?;

    ensure!(
        row.try_get::<serde_json::Value, _>("receipt")? == legacy.receipt,
        "0008 changed the durable legacy hybrid receipt"
    );
    ensure!(
        row.try_get::<serde_json::Value, _>("legacy_manifest")? == legacy.manifest,
        "0008 changed fields covered by the legacy hybrid item digest"
    );
    ensure!(
        row.try_get::<String, _>("policy_sha256")? == legacy.policy_sha256,
        "0008 changed the legacy hybrid receipt policy digest"
    );
    ensure!(
        row.try_get::<String, _>("registered_policy_sha256")? == legacy.policy_sha256,
        "0008 mutated the registered legacy hybrid policy"
    );
    ensure!(
        row.try_get::<String, _>("manifest_sha256")? == HYBRID_MANIFEST_SHA256,
        "0008 changed the legacy hybrid manifest digest"
    );
    ensure!(
        row.try_get::<String, _>("item_sha256")? == HYBRID_ITEM_SHA256,
        "0008 changed the legacy hybrid item digest"
    );
    Ok(())
}

async fn verify_temporal_schema_contract(
    pool: &PgPool,
    legacy_lexical: &LegacyReceiptEvidence,
    legacy_hybrid: &LegacyReceiptEvidence,
) -> Result<()> {
    let registries = sqlx::query(
        r#"
        SELECT
            (
                SELECT count(*) = 2
                    AND bool_and(
                        profile_sha256 = encode(
                            sha256(convert_to(profile_document::text, 'UTF8')),
                            'hex'
                        )
                    )
                FROM memory.recency_profiles
                WHERE (profile_id, profile_version) IN (
                    ('stable-v1', '1'),
                    ('active-case-30d-v1', '1')
                )
            ) AS recency_profiles_registered,
            (
                SELECT count(*) > 0
                    AND bool_and(
                        policy_sha256 = encode(
                            sha256(convert_to(policy_document::text, 'UTF8')),
                            'hex'
                        )
                    )
                FROM memory.fact_retrieval_metadata_policies
            ) AS metadata_policies_registered,
            (
                SELECT count(*) = 12 AND bool_and(is_nullable = 'YES')
                FROM information_schema.columns
                WHERE table_schema = 'memory'
                  AND table_name = 'retrieval_manifest_items'
                  AND column_name = ANY (ARRAY[
                      'recency_profile_id',
                      'recency_profile_version',
                      'recency_profile_sha256',
                      'recency_anchor_at',
                      'recency_age_us',
                      'recency_factor',
                      'confidence_factor',
                      'importance_factor',
                      'temporal_adjustment',
                      'confidence_adjustment',
                      'importance_adjustment',
                      'exact_identity_bonus'
                  ])
            ) AS legacy_temporal_columns_nullable
        "#,
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        registries.try_get::<bool, _>("recency_profiles_registered")?,
        "0008 did not register both immutable recency profiles with valid digests"
    );
    ensure!(
        registries.try_get::<bool, _>("metadata_policies_registered")?,
        "0008 did not register an attributable metadata-assignment policy"
    );
    ensure!(
        registries.try_get::<bool, _>("legacy_temporal_columns_nullable")?,
        "0008 temporal manifest fields must remain nullable for legacy rows"
    );

    let governance = sqlx::query(
        r#"
        SELECT
            governance.recency_profile_id,
            governance.recency_profile_version,
            governance.recency_profile_sha256,
            governance.importance::text AS importance,
            governance.recency_anchor_at = revision.observed_at
                AS anchor_uses_observed_time,
            governance.metadata_policy_id,
            governance.metadata_policy_version,
            governance.metadata_policy_sha256,
            profile.profile_sha256 = encode(
                sha256(convert_to(profile.profile_document::text, 'UTF8')),
                'hex'
            ) AS recency_profile_digest_valid,
            metadata.policy_sha256 = encode(
                sha256(convert_to(metadata.policy_document::text, 'UTF8')),
                'hex'
            ) AS metadata_policy_digest_valid
        FROM memory.fact_revision_governance AS governance
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = governance.tenant_id
         AND revision.subject_id = governance.subject_id
         AND revision.case_id = governance.case_id
         AND revision.fact_id = governance.fact_id
         AND revision.revision_id = governance.revision_id
        JOIN memory.recency_profiles AS profile
          ON profile.profile_id = governance.recency_profile_id
         AND profile.profile_version = governance.recency_profile_version
         AND profile.profile_sha256 = governance.recency_profile_sha256
        JOIN memory.fact_retrieval_metadata_policies AS metadata
          ON metadata.policy_id = governance.metadata_policy_id
         AND metadata.policy_version = governance.metadata_policy_version
         AND metadata.policy_sha256 = governance.metadata_policy_sha256
        WHERE governance.tenant_id = $1
          AND governance.subject_id = $2
          AND governance.case_id = $3
          AND governance.fact_id = $4
          AND governance.revision_id = $5
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(CASE_ID)?)
    .bind(Uuid::parse_str(FACT_ID)?)
    .bind(Uuid::parse_str(REVISION_ID)?)
    .fetch_one(pool)
    .await?;
    ensure!(
        governance.try_get::<String, _>("recency_profile_id")? == "stable-v1",
        "legacy governance must retain the neutral stable profile"
    );
    ensure!(
        governance.try_get::<String, _>("recency_profile_version")? == "1",
        "legacy governance is missing recency-profile version"
    );
    ensure!(
        governance
            .try_get::<String, _>("recency_profile_sha256")?
            .len()
            == 64,
        "legacy governance is missing recency-profile digest"
    );
    ensure!(
        governance.try_get::<String, _>("importance")? == "0.5000",
        "legacy governance must retain neutral importance"
    );
    ensure!(
        governance.try_get::<bool, _>("anchor_uses_observed_time")?,
        "legacy governance must backfill the stable observed-time anchor"
    );
    ensure!(
        !governance
            .try_get::<String, _>("metadata_policy_id")?
            .is_empty(),
        "legacy governance is missing metadata-policy identity"
    );
    ensure!(
        !governance
            .try_get::<String, _>("metadata_policy_version")?
            .is_empty(),
        "legacy governance is missing metadata-policy version"
    );
    ensure!(
        governance
            .try_get::<String, _>("metadata_policy_sha256")?
            .len()
            == 64,
        "legacy governance is missing metadata-policy digest"
    );
    ensure!(
        governance.try_get::<bool, _>("recency_profile_digest_valid")?,
        "legacy governance points at an invalid recency profile"
    );
    ensure!(
        governance.try_get::<bool, _>("metadata_policy_digest_valid")?,
        "legacy governance points at an invalid metadata-assignment policy"
    );

    let policies = sqlx::query(
        r#"
        SELECT
            lexical.policy_sha256 AS lexical_policy_sha256,
            hybrid.policy_sha256 AS hybrid_policy_sha256,
            temporal.policy_sha256 AS temporal_policy_sha256,
            temporal.policy_sha256 = encode(
                sha256(convert_to(temporal.policy_document::text, 'UTF8')),
                'hex'
            ) AS temporal_policy_digest_valid,
            temporal.retrieval_mode,
            temporal.embedding_profile_id = hybrid.embedding_profile_id
                AND temporal.embedding_profile_version
                    = hybrid.embedding_profile_version
                AND temporal.embedding_profile_sha256
                    = hybrid.embedding_profile_sha256
                AND temporal.embedding_projection_profile_id
                    = hybrid.embedding_projection_profile_id
                AND temporal.embedding_projection_profile_version
                    = hybrid.embedding_projection_profile_version
                AND temporal.embedding_projection_profile_sha256
                    = hybrid.embedding_projection_profile_sha256
                AS temporal_embedding_lineage_preserved
        FROM memory.retrieval_policies AS lexical
        CROSS JOIN memory.retrieval_policies AS hybrid
        CROSS JOIN memory.retrieval_policies AS temporal
        WHERE lexical.policy_id = 'retrieval-lexical-v1'
          AND lexical.policy_version = '1'
          AND hybrid.policy_id = 'retrieval-hybrid-v1'
          AND hybrid.policy_version = '1'
          AND temporal.policy_id = 'retrieval-hybrid-temporal-v1'
          AND temporal.policy_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    ensure!(
        policies.try_get::<String, _>("lexical_policy_sha256")? == legacy_lexical.policy_sha256,
        "0008 mutated retrieval-lexical-v1"
    );
    ensure!(
        policies.try_get::<String, _>("hybrid_policy_sha256")? == legacy_hybrid.policy_sha256,
        "0008 mutated retrieval-hybrid-v1"
    );
    ensure!(
        policies.try_get::<String, _>("temporal_policy_sha256")? != legacy_hybrid.policy_sha256,
        "the temporal policy must have its own immutable digest"
    );
    ensure!(
        policies.try_get::<bool, _>("temporal_policy_digest_valid")?,
        "retrieval-hybrid-temporal-v1 has an invalid digest"
    );
    ensure!(
        policies.try_get::<String, _>("retrieval_mode")? == "hybrid",
        "retrieval-hybrid-temporal-v1 must remain a hybrid policy"
    );
    ensure!(
        policies.try_get::<bool, _>("temporal_embedding_lineage_preserved")?,
        "the temporal policy must preserve the registered hybrid embedding lineage"
    );

    let legacy_manifests = sqlx::query(
        r#"
        SELECT count(*) = 2 AND bool_and(
            recency_profile_id IS NULL
            AND recency_profile_version IS NULL
            AND recency_profile_sha256 IS NULL
            AND recency_anchor_at IS NULL
            AND recency_age_us IS NULL
            AND recency_factor IS NULL
            AND confidence_factor IS NULL
            AND importance_factor IS NULL
            AND temporal_adjustment IS NULL
            AND confidence_adjustment IS NULL
            AND importance_adjustment IS NULL
            AND exact_identity_bonus IS NULL
        ) AS legacy_temporal_fields_are_null
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1
          AND subject_id = $2
          AND retrieval_id IN ($3, $4)
        "#,
    )
    .bind(Uuid::parse_str(TENANT_ID)?)
    .bind(Uuid::parse_str(SUBJECT_ID)?)
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
    .bind(Uuid::parse_str(HYBRID_RETRIEVAL_ID)?)
    .fetch_one(pool)
    .await?;
    ensure!(
        legacy_manifests.try_get::<bool, _>("legacy_temporal_fields_are_null")?,
        "0008 must not invent temporal lineage or scores for legacy manifests"
    );

    verify_temporal_registry_immutability(pool).await
}

async fn verify_temporal_registry_immutability(pool: &PgPool) -> Result<()> {
    let recency_error = sqlx::query(
        r#"
        UPDATE memory.recency_profiles
        SET profile_version = profile_version
        WHERE profile_id = 'stable-v1' AND profile_version = '1'
        "#,
    )
    .execute(pool)
    .await
    .expect_err("recency profile registry accepted an update");
    let recency_sqlstate = recency_error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    ensure!(
        recency_sqlstate.as_deref() == Some("55000"),
        "recency profile mutation failed for the wrong reason: {recency_error}"
    );

    let metadata_error = sqlx::query(
        r#"
        UPDATE memory.fact_retrieval_metadata_policies
        SET policy_version = policy_version
        WHERE (policy_id, policy_version) = (
            SELECT policy_id, policy_version
            FROM memory.fact_retrieval_metadata_policies
            ORDER BY policy_id, policy_version
            LIMIT 1
        )
        "#,
    )
    .execute(pool)
    .await
    .expect_err("metadata-assignment policy registry accepted an update");
    let metadata_sqlstate = metadata_error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    ensure!(
        metadata_sqlstate.as_deref() == Some("55000"),
        "metadata-policy mutation failed for the wrong reason: {metadata_error}"
    );
    Ok(())
}

async fn verify_public_replay(pool: &PgPool, legacy: &LegacyReceiptEvidence) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str(TENANT_ID)?);
    let subject_id = SubjectId(Uuid::parse_str(SUBJECT_ID)?);
    let principal = PrincipalScope {
        principal_id: PrincipalId(PRINCIPAL_ID.to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
        operation_grants: vec![],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository,
    );
    let content_lease = service
        .acquire_subject_content_lease(&principal, tenant_id, subject_id)
        .await?;

    let replay = service
        .create_retrieval(
            &content_lease,
            &principal,
            IDEMPOTENCY_KEY.to_owned(),
            CreateRetrieval {
                tenant_id,
                subject_id,
                query: RetrievalQuery::try_from(QUERY.to_owned())?,
                perspective: RetrievalPerspective::Current,
                page_size: 10,
                policy_id: None,
                filters: RetrievalFilters::default(),
            },
        )
        .await?;

    ensure!(replay.replayed, "pre-0007 receipt was not replayed");
    ensure!(replay.receipt.retrieval_id == RetrievalId(Uuid::parse_str(RETRIEVAL_ID)?));
    ensure!(replay.receipt.status == "results");
    ensure!(replay.receipt.policy.id.as_str() == "retrieval-lexical-v1");
    ensure!(replay.receipt.policy.digest == legacy.policy_sha256);
    ensure!(replay.receipt.query_embedding.is_none());
    ensure!(replay.receipt.items.len() == 1);
    ensure!(replay.receipt.next_cursor.is_none());
    let item = &replay.receipt.items[0];
    ensure!(item.fact_id.0 == Uuid::parse_str(FACT_ID)?);
    ensure!(item.revision_id.0 == Uuid::parse_str(REVISION_ID)?);
    ensure!(item.namespace.as_str() == "legacy.profile");
    ensure!(item.key.as_str() == "upgrade_result");
    ensure!(item.value == serde_json::json!({"answer": "legacy upgrade replay"}));
    ensure!(item.evidence_episode_ids.len() == 1);
    ensure!(item.evidence_episode_ids[0].0 == Uuid::parse_str(EPISODE_ID)?);
    ensure!(item.embedding.is_none());
    ensure!(
        item.scores
            .iter()
            .any(|score| { score.component == "lexical_rank" && score.value == "1" })
    );
    ensure!(
        item.scores
            .iter()
            .any(|score| { score.component == "lexical_score" && score.value == "0.750000000000" })
    );
    ensure!(
        item.scores
            .iter()
            .any(|score| { score.component == "final_score" && score.value == "0.750000000000" })
    );

    let fetched = service
        .get_retrieval(
            &content_lease,
            &principal,
            tenant_id,
            subject_id,
            RetrievalId(Uuid::parse_str(RETRIEVAL_ID)?),
            None,
        )
        .await?;
    ensure!(
        fetched == replay.receipt,
        "GET changed the migrated receipt"
    );
    let content_lease_release = content_lease.into_release();
    service
        .release_subject_content_lease(&content_lease_release)
        .await?;
    Ok(())
}
