use std::{env, str::FromStr, sync::Arc};

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

const TENANT_ID: &str = "019be100-0000-7000-8000-000000000010";
const SUBJECT_ID: &str = "019be100-0000-7000-8000-000000000020";
const RETRIEVAL_ID: &str = "019be100-0000-7000-8000-000000000030";
const CASE_ID: &str = "019be100-0000-7000-8000-000000000040";
const EPISODE_ID: &str = "019be100-0000-7000-8000-000000000050";
const FACT_ID: &str = "019be100-0000-7000-8000-000000000060";
const REVISION_ID: &str = "019be100-0000-7000-8000-000000000070";
const CURSOR_TOKEN: &str = "019be100-0000-7000-8000-000000000080";
const PRINCIPAL_ID: &str = "legacy-principal";
const IDEMPOTENCY_KEY: &str = "legacy-lexical-receipt-upgrade";
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
        verify_preserved_database_contract(&migration_pool, &legacy).await?;
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

#[derive(Debug)]
struct LegacyReceiptEvidence {
    policy_sha256: String,
    receipt: serde_json::Value,
    manifest: serde_json::Value,
}

async fn load_legacy_receipt_evidence(pool: &PgPool) -> Result<LegacyReceiptEvidence> {
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
    .bind(Uuid::parse_str(RETRIEVAL_ID)?)
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
                'embedding_vector_sha256'
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

async fn verify_public_replay(pool: &PgPool, legacy: &LegacyReceiptEvidence) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str(TENANT_ID)?);
    let subject_id = SubjectId(Uuid::parse_str(SUBJECT_ID)?);
    let principal = PrincipalScope {
        principal_id: PrincipalId(PRINCIPAL_ID.to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository,
    );

    let replay = service
        .create_retrieval(
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
    Ok(())
}
