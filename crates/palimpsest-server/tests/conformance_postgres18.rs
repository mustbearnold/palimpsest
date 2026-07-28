use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{Path, Request},
    middleware::{self, Next},
    response::Response,
    routing::post,
};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    env,
    process::Stdio,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use palimpsest_conformance::{
    RetrievalIsolationFixture, RetrievalLifecycleFixture, Target, checkpoint_scopes_fail_closed,
    concurrent_retrievals_converge_on_one_receipt, creates_an_attributable_fact_revision,
    creates_and_replays_a_lexical_retrieval_receipt, creates_retrieval_lifecycle_fixture,
    cross_scope_reads_fail_closed, expires_only_the_targeted_checkpoint,
    reconstructs_both_temporal_axes, records_and_reads_an_immutable_episode,
    rejects_cross_subject_idempotency_reuse, rejects_cross_subject_retrieval_idempotency_reuse,
    rejects_invalid_domain_and_timestamp_inputs,
    retrieval_candidates_are_authorized_before_ranking,
    retrieval_fails_closed_when_projection_is_corrupt,
    retrieval_fails_closed_when_projection_is_missing,
    retrieval_paginates_and_rejects_invalid_replays,
    retrieval_receipt_does_not_resurrect_deleted_history, retrieval_receipt_hides_expired_content,
    retrieval_recovers_after_projection_rebuild, retrieval_succeeds_after_projection_rebuild,
    retrieves_the_effective_bitemporal_revision, saves_and_reads_a_resumable_checkpoint,
    supersedes_the_fact_head,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use sqlx::{AssertSqlSafe, ConnectOptions, PgPool, Row, postgres::PgConnectOptions};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
};
use uuid::Uuid;

static PROVIDER_APPLICATIONS: AtomicUsize = AtomicUsize::new(0);
static PROVIDER_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static PROVIDER_EFFECTS: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[tokio::test]
async fn serves_the_bitemporal_lifecycle_over_http_and_postgres() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;

    let version_num: i32 = sqlx::query("SELECT current_setting('server_version_num')::integer")
        .fetch_one(&admin_pool)
        .await?
        .try_get(0)?;
    let vector_version: String =
        sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_one(&admin_pool)
            .await?
            .try_get(0)?;
    ensure!(version_num >= 180_000, "PostgreSQL 18+ is required");
    ensure!(vector_version == "0.8.5", "pgvector 0.8.5 is required");

    let database_name = format!("palimpsest_test_{}", Uuid::now_v7().simple());
    // The identifier is generated exclusively from a UUID's lowercase hex form.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;
    let options = PgConnectOptions::from_str(&database_url)?.database(&database_name);
    let test_database_url = options.to_url_lossy().to_string();
    let pool = PgPool::connect_with(options).await?;
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let migration_pool = PgPool::connect_with(migration_options).await?;
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000010")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000020")?;
    let target = Target {
        base_url: String::new(),
        bearer_token: "principal-a-test-token".to_owned(),
        tenant_id,
        subject_id,
        principal_a_secondary_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000021")?,
        principal_a_internal_bearer_token: "principal-a-internal-test-token".to_owned(),
        principal_b_bearer_token: "principal-b-test-token".to_owned(),
        principal_b_tenant_id: Uuid::parse_str("019be000-0000-7000-8000-000000000110")?,
        principal_b_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000120")?,
        principal_c_bearer_token: "principal-c-test-token".to_owned(),
        principal_c_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000220")?,
    };
    let result = async {
        palimpsest_postgres::migrate(&pool).await?;
        verify_lexical_retrieval_policy(&migration_pool).await?;
        sqlx::query(
            r#"
            INSERT INTO memory.fact_retention_policies (
                retention_policy_id, retention_interval, policy_origin, schema_version
            )
            VALUES ('retrieval-test-1s-v1', interval '1 second', 'migration', 1)
            "#,
        )
        .execute(&migration_pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO memory.checkpoint_retention_policies (
                retention_policy_id, retention_interval
            )
            VALUES ('checkpoint-test-1s-v1', interval '1 second')
            "#,
        )
        .execute(&pool)
        .await?;
        let authenticator = Arc::new(StaticAuthenticator::new([
            (
                target.bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                },
            ),
            (
                target.principal_a_internal_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                    ],
                    allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                },
            ),
            (
                target.principal_b_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(target.principal_b_tenant_id),
                    subject_ids: vec![SubjectId(target.principal_b_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                },
            ),
            (
                target.principal_c_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(target.principal_c_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                },
            ),
        ]));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_pool = pool.clone();
        let server_authenticator = authenticator.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(server_pool, server_authenticator),
            )
            .await
        });
        let scenario_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        let scenario = async {
            records_and_reads_an_immutable_episode(&scenario_target).await?;
            creates_an_attributable_fact_revision(&scenario_target).await?;
            creates_and_replays_a_lexical_retrieval_receipt(&scenario_target).await?;
            supersedes_the_fact_head(&scenario_target).await?;
            reconstructs_both_temporal_axes(&scenario_target).await?;
            retrieves_the_effective_bitemporal_revision(&scenario_target).await?;
            cross_scope_reads_fail_closed(&scenario_target).await?;
            rejects_cross_subject_idempotency_reuse(&scenario_target).await?;
            rejects_invalid_domain_and_timestamp_inputs(&scenario_target).await?;
            verify_governed_write_records(&pool, &scenario_target).await?;
            let retrieval_isolation =
                retrieval_candidates_are_authorized_before_ranking(&scenario_target).await?;
            concurrent_retrievals_converge_on_one_receipt(&scenario_target).await?;
            rejects_cross_subject_retrieval_idempotency_reuse(&scenario_target).await?;
            verify_retrieval_manifest_is_authorized(&pool, &target, &retrieval_isolation).await?;
            delete_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_fails_closed_when_projection_is_missing(&scenario_target).await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_recovers_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            corrupt_retrieval_projection_digest(
                &pool,
                &target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            retrieval_fails_closed_when_projection_is_corrupt(
                &scenario_target,
                "retrieval-projection-digest-retry",
            )
            .await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_succeeds_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
                "retrieval-projection-digest-retry",
            )
            .await?;
            corrupt_retrieval_search_vector(
                &pool,
                &target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            retrieval_fails_closed_when_projection_is_corrupt(
                &scenario_target,
                "retrieval-projection-vector-retry",
            )
            .await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_succeeds_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
                "retrieval-projection-vector-retry",
            )
            .await?;
            retrieval_paginates_and_rejects_invalid_replays(&scenario_target).await?;
            retrieval_receipt_hides_expired_content(&scenario_target).await?;
            let lifecycle = creates_retrieval_lifecycle_fixture(&scenario_target).await?;
            delete_retrieval_revision(&pool, &target, &lifecycle).await?;
            retrieval_receipt_does_not_resurrect_deleted_history(&scenario_target, &lifecycle)
                .await?;
            saves_and_reads_a_resumable_checkpoint(&scenario_target).await?;
            checkpoint_scopes_fail_closed(&scenario_target).await?;
            expires_only_the_targeted_checkpoint(&scenario_target).await?;
            verify_checkpoint_governance(&pool, &scenario_target).await
        }
        .await;
        server.abort();
        let _ = server.await;
        scenario?;
        recovers_a_committed_effect_after_response_loss(&pool, &target, &test_database_url).await
    }
    .await;

    migration_pool.close().await;
    pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&admin_pool)
    .await?;
    result
}

async fn verify_lexical_retrieval_policy(pool: &PgPool) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT policy_document, policy_sha256,
            encode(
                sha256(convert_to(policy_document::text, 'UTF8')),
                'hex'
            ) AS calculated_sha256
        FROM memory.lexical_retrieval_policies
        WHERE policy_id = 'retrieval-lexical-v1' AND policy_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let policy_document: Value = row.try_get("policy_document")?;
    let expected = json!({
        "candidate_limit": 50,
        "default_page_size": 10,
        "exact_identity_precedence": true,
        "fts_configuration": "pg_catalog.simple",
        "fts_rank": "ts_rank_cd",
        "fts_rank_normalization": 32,
        "maximum_page_size": 50,
        "score_scale": 12,
        "tie_break": [
            "exact_identity_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "fact_id_asc",
            "revision_id_asc"
        ]
    });
    ensure!(
        policy_document == expected,
        "retrieval-lexical-v1 did not pin the complete lexical-only ranking policy"
    );
    let stored_sha256: String = row.try_get("policy_sha256")?;
    let calculated_sha256: String = row.try_get("calculated_sha256")?;
    ensure!(
        stored_sha256 == calculated_sha256,
        "retrieval-lexical-v1 digest does not hash its canonical policy document"
    );
    Ok(())
}

async fn verify_retrieval_manifest_is_authorized(
    pool: &PgPool,
    target: &Target,
    fixture: &RetrievalIsolationFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let revision_ids = sqlx::query(
        r#"
        SELECT revision_id
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| row.try_get::<Uuid, _>("revision_id"))
    .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        revision_ids == vec![fixture.allowed_revision_id],
        "the internal-only receipt manifest contains unauthorized candidates"
    );
    ensure!(
        fixture
            .forbidden_revision_ids
            .iter()
            .all(|revision_id| !revision_ids.contains(revision_id)),
        "a forbidden revision entered the durable retrieval manifest"
    );
    let receipt_record: String = sqlx::query(
        r#"
        SELECT to_jsonb(receipt)::text AS record
        FROM memory.retrieval_receipts AS receipt
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    let manifest_record: String = sqlx::query(
        r#"
        SELECT COALESCE(jsonb_agg(to_jsonb(item)), '[]'::jsonb)::text AS record
        FROM memory.retrieval_manifest_items AS item
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    let idempotency_record: String = sqlx::query(
        r#"
        SELECT to_jsonb(reservation)::text AS record
        FROM memory.retrieval_idempotency_reservations AS reservation
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    for private_text in [
        "cobalt-otter-731",
        "internal-visible-value",
        "restricted-hidden-value",
        "cross-subject-hidden-value",
        "cross-tenant-hidden-value",
    ] {
        ensure!(
            !receipt_record.contains(private_text)
                && !manifest_record.contains(private_text)
                && !idempotency_record.contains(private_text),
            "durable retrieval metadata stored raw private text"
        );
    }
    transaction.commit().await?;
    Ok(())
}

async fn delete_retrieval_revision(
    pool: &PgPool,
    target: &Target,
    fixture: &RetrievalLifecycleFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let manifest_revision_ids = sqlx::query(
        r#"
        SELECT revision_id
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| row.try_get::<Uuid, _>("revision_id"))
    .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(manifest_revision_ids == vec![fixture.deleted_revision_id]);
    ensure!(!manifest_revision_ids.contains(&fixture.superseded_revision_id));
    let pending = sqlx::query(
        r#"
        UPDATE memory.fact_revision_governance
        SET lifecycle_state = 'deletion_pending'
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.deleted_revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(pending.rows_affected() == 1);
    let deleted = sqlx::query(
        r#"
        UPDATE memory.fact_revision_governance
        SET lifecycle_state = 'deleted'
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
          AND lifecycle_state = 'deletion_pending'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.deleted_revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn delete_retrieval_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let deleted = sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_search_documents
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn corrupt_retrieval_projection_digest(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let updated = sqlx::query(
        r#"
        UPDATE memory.fact_revision_search_documents
        SET projection_sha256 = repeat('0', 64)
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(updated.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn corrupt_retrieval_search_vector(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let updated = sqlx::query(
        r#"
        UPDATE memory.fact_revision_search_documents
        SET search_vector = to_tsvector('pg_catalog.simple', 'corrupted projection')
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(updated.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn rebuild_retrieval_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let rebuilt = sqlx::query(
        r#"
        INSERT INTO memory.fact_revision_search_documents (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            projection_schema_version, projection_schema_sha256,
            source_content_sha256, projection_sha256, search_vector
        )
        SELECT revision.tenant_id, revision.subject_id, revision.case_id,
            revision.fact_id, revision.revision_id,
            projection.projection_schema_version, projection.projection_sha256,
            revision.content_sha256,
            memory.fact_projection_sha256_v1(
                fact.namespace, fact.fact_key, revision.value
            ),
            memory.fact_search_vector_v1(
                fact.namespace, fact.fact_key, revision.value
            )
        FROM memory.fact_revisions AS revision
        JOIN memory.facts AS fact
          ON fact.tenant_id = revision.tenant_id
         AND fact.subject_id = revision.subject_id
         AND fact.case_id = revision.case_id
         AND fact.fact_id = revision.fact_id
        CROSS JOIN memory.search_projection_schemas AS projection
        WHERE revision.tenant_id = $1
          AND revision.subject_id = $2
          AND revision.revision_id = $3
          AND projection.projection_schema_version = 1
        ON CONFLICT (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ) DO UPDATE SET
            projection_schema_sha256 = EXCLUDED.projection_schema_sha256,
            source_content_sha256 = EXCLUDED.source_content_sha256,
            projection_sha256 = EXCLUDED.projection_sha256,
            search_vector = EXCLUDED.search_vector
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(rebuilt.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn crash_after_selected_commit(request: Request, next: Next) -> Response {
    let should_crash = request
        .headers()
        .get("idempotency-key")
        .is_some_and(|value| value == "checkpoint-run-321-complete");
    let response = next.run(request).await;
    if should_crash {
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("idempotency-replayed").is_none());
        assert!(response.headers().get(header::ETAG).is_some());
        std::process::exit(86);
    }
    response
}

#[tokio::test]
#[ignore = "spawned by the crash-recovery conformance scenario"]
async fn crash_after_checkpoint_commit_child() -> Result<()> {
    if env::var("PALIMPSEST_CRASH_CHILD").as_deref() != Ok("1") {
        return Ok(());
    }
    let pool = PgPool::connect(&env::var("PALIMPSEST_TEST_CHILD_DATABASE_URL")?).await?;
    let tenant_id = Uuid::parse_str(&env::var("PALIMPSEST_TEST_CHILD_TENANT_ID")?)?;
    let subject_id = Uuid::parse_str(&env::var("PALIMPSEST_TEST_CHILD_SUBJECT_ID")?)?;
    let bearer_token = env::var("PALIMPSEST_TEST_CHILD_BEARER_TOKEN")?;
    let authenticator = Arc::new(StaticAuthenticator::new([(
        bearer_token,
        PrincipalScope {
            principal_id: PrincipalId("principal-a".to_owned()),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![SubjectId(subject_id)],
            allowed_sensitivities: vec![
                Sensitivity::try_from("internal".to_owned())?,
                Sensitivity::try_from("restricted".to_owned())?,
            ],
        },
    )]));
    let listener = TcpListener::bind(&env::var("PALIMPSEST_TEST_CHILD_BIND")?).await?;
    let router = palimpsest_server::app(pool, authenticator)
        .layer(middleware::from_fn(crash_after_selected_commit));
    axum::serve(listener, router).await?;
    Ok(())
}

async fn apply_mock_provider_effect(Path(effect_id): Path<Uuid>) -> StatusCode {
    PROVIDER_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    if PROVIDER_EFFECTS
        .lock()
        .expect("provider effect lock poisoned")
        .insert(effect_id)
    {
        PROVIDER_APPLICATIONS.fetch_add(1, Ordering::SeqCst);
    }
    StatusCode::OK
}

async fn recovers_a_committed_effect_after_response_loss(
    pool: &PgPool,
    target: &Target,
    database_url: &str,
) -> Result<()> {
    PROVIDER_APPLICATIONS.store(0, Ordering::SeqCst);
    PROVIDER_ATTEMPTS.store(0, Ordering::SeqCst);
    PROVIDER_EFFECTS
        .lock()
        .expect("provider effect lock poisoned")
        .clear();

    let provider_listener = TcpListener::bind("127.0.0.1:0").await?;
    let provider_address = provider_listener.local_addr()?;
    let provider_server = tokio::spawn(async move {
        axum::serve(
            provider_listener,
            Router::new().route("/effects/{effect_id}", post(apply_mock_provider_effect)),
        )
        .await
    });

    let scenario = async {
        let client = Client::new();
        let crash_address = reserve_local_address().await?;
        let mut crash_server = spawn_crash_server(database_url, target, crash_address)?;
        wait_for_listener(crash_address).await?;
        let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000321")?;
        let agent_id = Uuid::parse_str("019be000-0000-7000-8000-000000000322")?;
        let thread_id = Uuid::parse_str("019be000-0000-7000-8000-000000000323")?;
        let checkpoint_path = format!(
            "/v1/tenants/{}/subjects/{}/agents/{agent_id}/threads/{thread_id}/checkpoint",
            target.tenant_id, target.subject_id
        );
        let fault_url = format!("http://{crash_address}{checkpoint_path}");
        let provenance = json!({
            "source_type": "conformance.crash-recovery",
            "source_uri": null,
            "external_id": "checkpoint-run-321"
        });
        let create_body = json!({
            "case_id": case_id,
            "parent_revision_id": null,
            "state": {"step": "created"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let create_response = client
            .put(&fault_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", "checkpoint-run-321-create")
            .header(header::IF_NONE_MATCH, "*")
            .json(&create_body)
            .send()
            .await?;
        ensure!(create_response.status() == StatusCode::CREATED);
        let create_etag = create_response
            .headers()
            .get(header::ETAG)
            .context("crash scenario create omitted ETag")?
            .to_str()?
            .to_owned();
        let created: Value = create_response.json().await?;
        let created_revision_id = created["checkpoint_revision_id"]
            .as_str()
            .context("crash scenario create omitted revision ID")?;

        let prepare_body = json!({
            "case_id": case_id,
            "parent_revision_id": created_revision_id,
            "state": {"step": "effect-prepared"},
            "state_schema_version": 1,
            "effect_transitions": [{
                "type": "prepare",
                "effect_key": "apply-case-321",
                "kind": "test-provider.apply",
                "recovery_mode": "idempotency_key"
            }],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let prepare_response = client
            .put(&fault_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", "checkpoint-run-321-prepare")
            .header(header::IF_MATCH, create_etag)
            .json(&prepare_body)
            .send()
            .await?;
        ensure!(prepare_response.status() == StatusCode::OK);
        let prepare_etag = prepare_response
            .headers()
            .get(header::ETAG)
            .context("crash scenario prepare omitted ETag")?
            .to_str()?
            .to_owned();
        let prepared: Value = prepare_response.json().await?;
        let prepared_revision_id = prepared["checkpoint_revision_id"]
            .as_str()
            .context("crash scenario prepare omitted revision ID")?
            .to_owned();
        let effect_id = prepared["effects"][0]["effect_id"]
            .as_str()
            .context("crash scenario prepare omitted effect ID")?
            .to_owned();

        let provider_response = client
            .post(format!("http://{provider_address}/effects/{effect_id}"))
            .send()
            .await?;
        ensure!(provider_response.status() == StatusCode::OK);
        ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 1);
        ensure!(PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1);

        crash_server.kill().await?;
        let _ = crash_server.wait().await?;
        let recovery_address = reserve_local_address().await?;
        let mut recovery_server = spawn_production_server(database_url, target, recovery_address)?;
        wait_for_listener(recovery_address).await?;
        let recovery_url = format!("http://{recovery_address}{checkpoint_path}");
        let recovered_prepared: Value = client
            .get(&recovery_url)
            .bearer_auth(&target.bearer_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(recovered_prepared["checkpoint_revision_id"] == prepared_revision_id);
        ensure!(recovered_prepared["effects"][0]["effect_id"] == effect_id);
        ensure!(recovered_prepared["effects"][0]["status"] == "prepared");

        let provider_retry = client
            .post(format!("http://{provider_address}/effects/{effect_id}"))
            .send()
            .await?;
        ensure!(provider_retry.status() == StatusCode::OK);
        ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 2);
        ensure!(
            PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1,
            "recovery retried the provider with the stable effect ID but applied it twice"
        );
        recovery_server.kill().await?;
        let _ = recovery_server.wait().await?;

        let completion_crash_address = reserve_local_address().await?;
        let mut crash_server = spawn_crash_server(database_url, target, completion_crash_address)?;
        wait_for_listener(completion_crash_address).await?;
        let completion_url = format!("http://{completion_crash_address}{checkpoint_path}");

        let complete_body = json!({
            "case_id": case_id,
            "parent_revision_id": prepared_revision_id,
            "state": {"step": "effect-completed", "private_marker": "never-audit-this"},
            "state_schema_version": 1,
            "effect_transitions": [{
                "type": "complete",
                "effect_id": effect_id,
                "receipt": {
                    "observed_at": "2026-07-29T02:00:00Z",
                    "external_reference": "mock-provider-321",
                    "outcome_sha256": "b".repeat(64)
                }
            }],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let completion_task = tokio::spawn({
            let client = client.clone();
            let completion_url = completion_url.clone();
            let token = target.bearer_token.clone();
            let prepare_etag = prepare_etag.clone();
            let complete_body = complete_body.clone();
            async move {
                client
                    .put(completion_url)
                    .bearer_auth(token)
                    .header("Idempotency-Key", "checkpoint-run-321-complete")
                    .header(header::IF_MATCH, prepare_etag)
                    .json(&complete_body)
                    .send()
                    .await
            }
        });

        let crash_status = tokio::time::timeout(Duration::from_secs(5), crash_server.wait())
            .await
            .context("checkpoint crash child did not terminate after commit")??;
        ensure!(
            crash_status.code() == Some(86),
            "checkpoint crash child exited with {crash_status}"
        );
        let lost_response = tokio::time::timeout(Duration::from_secs(5), completion_task)
            .await
            .context("terminated checkpoint response did not close the client connection")??;
        ensure!(
            lost_response.is_err(),
            "fault injection unexpectedly delivered the committed response"
        );
        let committed_etag =
            checkpoint_idempotency_etag(pool, target, "checkpoint-run-321-complete").await?;

        let restart_address = reserve_local_address().await?;
        let mut restart_server = spawn_production_server(database_url, target, restart_address)?;
        wait_for_listener(restart_address).await?;
        let restarted_url = format!("http://{restart_address}{checkpoint_path}");
        let verification = async {
            let replay_response = client
                .put(&restarted_url)
                .bearer_auth(&target.bearer_token)
                .header("Idempotency-Key", "checkpoint-run-321-complete")
                .header(header::IF_MATCH, &prepare_etag)
                .json(&complete_body)
                .send()
                .await?;
            ensure!(replay_response.status() == StatusCode::OK);
            ensure!(
                replay_response
                    .headers()
                    .get("idempotency-replayed")
                    .is_some_and(|value| value == "true")
            );
            let replay_etag = replay_response
                .headers()
                .get(header::ETAG)
                .context("completion replay omitted ETag")?
                .to_str()?
                .to_owned();
            ensure!(
                committed_etag == replay_etag,
                "completion replay did not preserve the withheld response ETag"
            );
            let replayed: Value = replay_response.json().await?;
            ensure!(replayed["revision_number"] == 3);
            ensure!(replayed["effects"][0]["status"] == "completed");
            ensure!(
                replayed["effects"][0]["receipt"]
                    == complete_body["effect_transitions"][0]["receipt"]
            );

            let current_response = client
                .get(&restarted_url)
                .bearer_auth(&target.bearer_token)
                .send()
                .await?
                .error_for_status()?;
            ensure!(
                current_response.headers().get(header::ETAG)
                    == Some(&header::HeaderValue::from_str(&replay_etag)?),
                "current checkpoint ETag differs from the replayed completion"
            );
            let current: Value = current_response.json().await?;
            ensure!(current == replayed);
            ensure!(
                PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1,
                "completed replay caused the external effect to be applied twice"
            );
            ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 2);
            verify_crash_recovery_records(pool, target, agent_id, thread_id).await
        }
        .await;
        let _ = restart_server.kill().await;
        let _ = restart_server.wait().await;
        verification
    }
    .await;

    provider_server.abort();
    let _ = provider_server.await;
    scenario
}

async fn reserve_local_address() -> Result<std::net::SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn spawn_crash_server(
    database_url: &str,
    target: &Target,
    address: std::net::SocketAddr,
) -> Result<tokio::process::Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("crash_after_checkpoint_commit_child")
        .arg("--test-threads=1")
        .env("PALIMPSEST_CRASH_CHILD", "1")
        .env("PALIMPSEST_TEST_CHILD_DATABASE_URL", database_url)
        .env(
            "PALIMPSEST_TEST_CHILD_TENANT_ID",
            target.tenant_id.to_string(),
        )
        .env(
            "PALIMPSEST_TEST_CHILD_SUBJECT_ID",
            target.subject_id.to_string(),
        )
        .env("PALIMPSEST_TEST_CHILD_BEARER_TOKEN", &target.bearer_token)
        .env("PALIMPSEST_TEST_CHILD_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn().context("spawn checkpoint crash child")
}

fn spawn_production_server(
    database_url: &str,
    target: &Target,
    address: std::net::SocketAddr,
) -> Result<tokio::process::Child> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"));
    command
        .env("PALIMPSEST_DATABASE_URL", database_url)
        .env("PALIMPSEST_BEARER_TOKEN", &target.bearer_token)
        .env("PALIMPSEST_PRINCIPAL_ID", "principal-a")
        .env("PALIMPSEST_TENANT_ID", target.tenant_id.to_string())
        .env("PALIMPSEST_SUBJECT_ID", target.subject_id.to_string())
        .env("PALIMPSEST_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
        .spawn()
        .context("restart production checkpoint server")
}

async fn wait_for_listener(address: std::net::SocketAddr) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    drop(stream);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .with_context(|| format!("server did not listen on {address}"))?;
    Ok(())
}

async fn checkpoint_idempotency_etag(
    pool: &PgPool,
    target: &Target,
    idempotency_key: &str,
) -> Result<String> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let etag = sqlx::query_scalar(
        r#"
        SELECT response_etag
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'principal-a'
          AND operation_id = 'saveCheckpoint'
          AND idempotency_key = $3
          AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(idempotency_key)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(etag)
}

async fn verify_crash_recovery_records(
    pool: &PgPool,
    target: &Target,
    agent_id: Uuid,
    thread_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let counts = sqlx::query(
        r#"
        SELECT
            (SELECT count(*) FROM memory.checkpoint_revisions
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS revision_count,
            (SELECT count(*) FROM memory.checkpoint_effect_intents
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS prepared_count,
            (SELECT count(*) FROM memory.checkpoint_effect_receipts
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS completed_count,
            (SELECT count(*) FROM memory.write_audit_receipts
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND authorization_context::text NOT LIKE '%never-audit-this%'
               AND authorization_context::text NOT LIKE '%mock-provider-321%')
                AS audit_count,
            (SELECT count(*) FROM memory.outbox_intents
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND payload::text NOT LIKE '%never-audit-this%'
               AND payload::text NOT LIKE '%mock-provider-321%')
                AS outbox_count
            ,
            (SELECT count(*) FROM memory.idempotency_receipts
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND state = 'completed')
                AS idempotency_count
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(agent_id)
    .bind(thread_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(counts.try_get::<i64, _>("revision_count")? == 3);
    ensure!(counts.try_get::<i64, _>("prepared_count")? == 1);
    ensure!(counts.try_get::<i64, _>("completed_count")? == 1);
    ensure!(counts.try_get::<i64, _>("audit_count")? == 3);
    ensure!(counts.try_get::<i64, _>("outbox_count")? == 3);
    ensure!(counts.try_get::<i64, _>("idempotency_count")? == 3);
    transaction.commit().await?;
    Ok(())
}

async fn verify_governed_write_records(pool: &PgPool, target: &Target) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;

    let audit = sqlx::query(
        r#"
        SELECT
            count(*) FILTER (WHERE operation_id = 'appendEpisode') AS episode_count,
            count(*) FILTER (WHERE operation_id = 'createFact') AS create_fact_count,
            count(*) FILTER (WHERE operation_id = 'supersedeFact') AS supersede_fact_count,
            count(*) AS total_count
        FROM memory.write_audit_receipts
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(audit.try_get::<i64, _>("episode_count")? == 3);
    ensure!(audit.try_get::<i64, _>("create_fact_count")? == 1);
    ensure!(audit.try_get::<i64, _>("supersede_fact_count")? == 1);
    ensure!(audit.try_get::<i64, _>("total_count")? == 5);

    let outbox = sqlx::query(
        r#"
        SELECT
            count(*) FILTER (WHERE event_type = 'memory.episode.appended.v1') AS episode_count,
            count(*) FILTER (WHERE event_type = 'memory.fact.created.v1') AS create_fact_count,
            count(*) FILTER (WHERE event_type = 'memory.fact.superseded.v1') AS supersede_fact_count,
            count(*) AS total_count
        FROM memory.outbox_intents
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(outbox.try_get::<i64, _>("episode_count")? == 3);
    ensure!(outbox.try_get::<i64, _>("create_fact_count")? == 1);
    ensure!(outbox.try_get::<i64, _>("supersede_fact_count")? == 1);
    ensure!(outbox.try_get::<i64, _>("total_count")? == 5);

    let paired_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.write_audit_receipts AS audit
        JOIN memory.outbox_intents AS outbox
          ON outbox.tenant_id = audit.tenant_id
         AND outbox.subject_id = audit.subject_id
         AND outbox.case_id = audit.case_id
         AND outbox.resource_episode_id IS NOT DISTINCT FROM audit.resource_episode_id
         AND outbox.resource_fact_id IS NOT DISTINCT FROM audit.resource_fact_id
         AND outbox.resource_revision_id IS NOT DISTINCT FROM audit.resource_revision_id
        WHERE audit.tenant_id = $1 AND audit.subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        paired_count == 5,
        "every durable mutation needs one audit/outbox pair"
    );

    let receipt_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1 AND principal_id = 'principal-a' AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        receipt_count == 5,
        "idempotent replays must not create durable write records"
    );

    let published = sqlx::query(
        r#"
        UPDATE memory.outbox_intents
        SET published_at = clock_timestamp()
        WHERE tenant_id = $1
          AND subject_id = $2
          AND intent_id = (
            SELECT intent_id
            FROM memory.outbox_intents
            WHERE tenant_id = $1
              AND subject_id = $2
              AND published_at IS NULL
            ORDER BY created_at, intent_id
            LIMIT 1
        )
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        published.rows_affected() == 1,
        "scoped outbox publisher could not mark one intent published"
    );
    transaction.commit().await?;
    Ok(())
}

async fn verify_checkpoint_governance(pool: &PgPool, target: &Target) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;

    let checkpoint_audits: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.write_audit_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND operation_id = 'saveCheckpoint'
          AND resource_checkpoint_id IS NOT NULL
          AND authorization_context::text NOT LIKE '%provider-call%'
          AND authorization_context::text NOT LIKE '%provider-result-301%'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        checkpoint_audits == 5,
        "checkpoint retries or failures duplicated audit records, or audit content leaked state"
    );

    let checkpoint_outbox: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.outbox_intents
        WHERE tenant_id = $1
          AND subject_id = $2
          AND event_type = 'memory.checkpoint.saved.v1'
          AND resource_checkpoint_id IS NOT NULL
          AND payload::text NOT LIKE '%provider-call%'
          AND payload::text NOT LIKE '%provider-result-301%'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        checkpoint_outbox == 5,
        "checkpoint retries or failures duplicated outbox records, or outbox content leaked state"
    );

    let checkpoint_receipts: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'principal-a'
          AND operation_id = 'saveCheckpoint'
          AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(checkpoint_receipts == 5);

    let prepared_effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.checkpoint_effect_intents WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    let completed_effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.checkpoint_effect_receipts WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(prepared_effects == 1);
    ensure!(completed_effects == 1);
    transaction.commit().await?;
    Ok(())
}
