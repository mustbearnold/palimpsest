//! temporal — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Result, ensure};
use serde_json::json;
use std::{str::FromStr, sync::Arc};

use palimpsest_application::EmbeddingProvider;
use palimpsest_conformance::{
    RetrievalIsolationFixture, Target, TemporalLifecycleFixture, TemporalLifecycleReplayFixture,
    TemporalReplayFixture, TemporalRetrievalFixture,
    creates_temporal_receipt_through_nonbypass_runtime,
    replays_temporal_receipt_through_nonbypass_runtime,
    temporal_policy_does_not_resurrect_ineligible_successors,
};
use palimpsest_domain::{RecencyProfile, SubjectId, TenantId, temporal_factor_q63};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::EmbeddingProjectionCoordinator;
use sqlx::{
    AssertSqlSafe, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::net::TcpListener;
use uuid::Uuid;

use super::deletion::exercise_export_and_deletion_http;
use super::deletion_ops::{delete_retrieval_projection, rebuild_retrieval_projection};
use super::fixtures::{DeterministicEmbeddingProvider, EmbeddingFixtureMode};
use super::projection_helpers::set_retrieval_test_scope;
use super::projections::verify_embedding_projection_rows_for_revisions;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TemporalReceiptDigests {
    manifest_sha256: String,
    ordered_item_sha256: Vec<(Uuid, String)>,
}

pub(crate) async fn temporal_receipt_digests(
    pool: &PgPool,
    target: &Target,
    retrieval_id: Uuid,
) -> Result<TemporalReceiptDigests> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let manifest_sha256: String = sqlx::query(
        r#"
        SELECT manifest_sha256
        FROM memory.retrieval_receipts
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
          AND policy_id = 'retrieval-hybrid-temporal-v1'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("manifest_sha256")?;
    let ordered_item_sha256 = sqlx::query(
        r#"
        SELECT revision_id, item_sha256
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get::<Uuid, _>("revision_id")?,
            row.try_get::<String, _>("item_sha256")?,
        ))
    })
    .collect::<Result<Vec<_>>>()?;
    ensure!(manifest_sha256.len() == 64);
    ensure!(ordered_item_sha256.len() == 4);
    ensure!(
        ordered_item_sha256
            .iter()
            .all(|(_, item_sha256)| item_sha256.len() == 64)
    );
    transaction.commit().await?;
    Ok(TemporalReceiptDigests {
        manifest_sha256,
        ordered_item_sha256,
    })
}

pub(crate) async fn verify_temporal_persistence_rejects_tampering(
    migration_pool: &PgPool,
    target: &Target,
    retrieval_id: Uuid,
) -> Result<()> {
    for (name, profile_id, field, malformed_value) in [
        (
            "active half-life",
            "active-case-30d-v1",
            "half_life_us",
            json!(2592000000001_u64),
        ),
        (
            "active floor",
            "active-case-30d-v1",
            "floor_q63_units",
            json!(1152921504606846977_u64),
        ),
        (
            "active Q63 scale",
            "active-case-30d-v1",
            "q63_scale_units",
            json!(9223372036854775809_u64),
        ),
        (
            "active algorithm",
            "active-case-30d-v1",
            "q63_algorithm",
            json!("tampered-exp2"),
        ),
        (
            "stable factor",
            "stable-v1",
            "factor_units",
            json!(999999999999_u64),
        ),
    ] {
        let mut transaction = migration_pool.begin().await?;
        let malformed_profile = sqlx::query(
            r#"
            WITH source AS (
                SELECT profile.*,
                    jsonb_set(
                        profile_document,
                        ARRAY[$2]::text[],
                        $3::jsonb,
                        false
                    ) AS malformed_document
                FROM memory.recency_profiles AS profile
                WHERE profile_id = $1 AND profile_version = '1'
            )
            INSERT INTO memory.recency_profiles (
                profile_id, profile_version, profile_document,
                profile_sha256, schema_version
            )
            SELECT profile_id, profile_version, malformed_document,
                encode(sha256(convert_to(malformed_document::text, 'UTF8')), 'hex'),
                schema_version
            FROM source
            "#,
        )
        .bind(profile_id)
        .bind(field)
        .bind(malformed_value)
        .execute(&mut *transaction)
        .await;
        transaction.rollback().await?;
        ensure!(
            malformed_profile
                .as_ref()
                .err()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.constraint())
                == Some("recency_profile_registration_consistent"),
            "a correctly rehashed recency profile with tampered {name} reached another constraint"
        );
    }

    let factors = sqlx::query(
        r#"
        SELECT
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 1
            )::text AS one_microsecond,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 1296000000000
            )::text AS fifteen_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 2592000000000
            )::text AS thirty_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 7775999999999
            )::text AS just_before_ninety_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 7776000000000
            )::text AS ninety_days
        "#,
    )
    .fetch_one(migration_pool)
    .await?;
    for (column, age_us, exact_units) in [
        ("one_microsecond", 1_i128, "1000000000000"),
        ("fifteen_days", 1_296_000_000_000_i128, "707106781187"),
        ("thirty_days", 2_592_000_000_000_i128, "500000000000"),
        (
            "just_before_ninety_days",
            7_775_999_999_999_i128,
            "125000000000",
        ),
        ("ninety_days", 7_776_000_000_000_i128, "125000000000"),
    ] {
        let sql_units = factors.try_get::<String, _>(column)?;
        let rust_units = temporal_factor_q63(RecencyProfile::ActiveCase30dV1, age_us, 0)
            .and_then(|factor| factor.to_score_units())
            .map_err(|error| anyhow::anyhow!("Rust recency vector {column} failed: {error:?}"))?
            .raw_units()
            .to_string();
        ensure!(
            sql_units == exact_units,
            "SQL recency vector {column} drifted"
        );
        ensure!(
            sql_units == rust_units,
            "SQL and Rust recency vectors disagree at {column}"
        );
    }

    let mut policy_transaction = migration_pool.begin().await?;
    let malformed_policy = sqlx::query(
        r#"
        WITH source AS (
            SELECT policy.*,
                jsonb_set(
                    policy_document,
                    '{arithmetic,operation_order}',
                    '["exact-identity-bonus","importance-half-even"]'::jsonb
                ) AS malformed_document
            FROM memory.retrieval_policies AS policy
            WHERE policy_id = 'retrieval-hybrid-temporal-v1'
              AND policy_version = '1'
        )
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            scoring_mode
        )
        SELECT policy_id, '2', malformed_document,
            encode(sha256(convert_to(malformed_document::text, 'UTF8')), 'hex'),
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            scoring_mode
        FROM source
        "#,
    )
    .execute(&mut *policy_transaction)
    .await;
    policy_transaction.rollback().await?;
    ensure!(
        malformed_policy
            .as_ref()
            .err()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|error| error.constraint())
            == Some("retrieval_policy_registration_consistent"),
        "a correctly rehashed policy with a malformed operation order was registered"
    );

    for (name, patch) in [
        (
            "partial lineage",
            json!({
                "ordinal": 98,
                "final_rank": 98,
                "cursor_token": Uuid::now_v7(),
                "confidence_factor": null,
                "item_sha256": "1".repeat(64)
            }),
        ),
        (
            "plausible wrong recency factor",
            json!({
                "ordinal": 99,
                "final_rank": 99,
                "cursor_token": Uuid::now_v7(),
                "recency_factor": "0.500000000001",
                "item_sha256": "2".repeat(64)
            }),
        ),
    ] {
        let mut transaction = migration_pool.begin().await?;
        set_retrieval_test_scope(&mut transaction, target).await?;
        let insert = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_manifest_items
            SELECT (jsonb_populate_record(item, $4::jsonb)).*
            FROM memory.retrieval_manifest_items AS item
            WHERE item.tenant_id = $1
              AND item.subject_id = $2
              AND item.retrieval_id = $3
              AND item.recency_profile_id = 'active-case-30d-v1'
              AND item.recency_age_us = 2592000000000
            LIMIT 1
            "#,
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .bind(retrieval_id)
        .bind(patch)
        .execute(&mut *transaction)
        .await;
        transaction.rollback().await?;
        ensure!(insert.is_err(), "temporal manifest accepted {name}");
    }
    Ok(())
}

pub(crate) async fn rebuild_temporal_fixture_projections(
    pool: &PgPool,
    target: &Target,
    fixture: &TemporalRetrievalFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    let revision_ids = [
        fixture.exact_revision_id,
        fixture.alpha_root_revision_id,
        fixture.alpha_successor_revision_id,
        fixture.beta_revision_id,
        fixture.gamma_revision_id,
        fixture.delta_revision_id,
    ];
    for revision_id in revision_ids {
        delete_retrieval_projection(pool, target, revision_id).await?;
    }
    for revision_id in revision_ids {
        rebuild_retrieval_projection(pool, target, revision_id).await?;
    }
    let report = coordinator
        .rebuild_pending(
            TenantId(target.tenant_id),
            SubjectId(target.subject_id),
            revision_ids.len(),
        )
        .await?;
    ensure!(report.attempted == revision_ids.len());
    ensure!(report.ready == revision_ids.len() && report.failed == 0);
    verify_embedding_projection_rows_for_revisions(pool, target, &revision_ids).await
}

pub(crate) struct NonbypassTemporalRuntime<'a> {
    pub(crate) migration_pool: &'a PgPool,
    pub(crate) database_url: &'a str,
    pub(crate) authenticator: Arc<StaticAuthenticator>,
    pub(crate) provider: Arc<DeterministicEmbeddingProvider>,
    pub(crate) provider_port: Arc<dyn EmbeddingProvider>,
    pub(crate) target: &'a Target,
    pub(crate) temporal_fixture: &'a TemporalRetrievalFixture,
    pub(crate) temporal_replay: &'a TemporalReplayFixture,
    pub(crate) isolation_fixture: &'a RetrievalIsolationFixture,
    pub(crate) lifecycle_fixture: &'a TemporalLifecycleFixture,
    pub(crate) lifecycle_replay: &'a TemporalLifecycleReplayFixture,
}

pub(crate) async fn verify_nonbypass_temporal_runtime(
    runtime: NonbypassTemporalRuntime<'_>,
) -> Result<()> {
    let NonbypassTemporalRuntime {
        migration_pool,
        database_url,
        authenticator,
        provider,
        provider_port,
        target,
        temporal_fixture,
        temporal_replay,
        isolation_fixture,
        lifecycle_fixture,
        lifecycle_replay,
    } = runtime;
    let login_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(database_url)?)
        .await?;
    let login_role = sqlx::query(
        "SELECT session_user::text AS role_name, quote_ident(session_user) AS quoted_role_name",
    )
    .fetch_one(&login_pool)
    .await?;
    let login_role_name: String = login_role.try_get("role_name")?;
    let quoted_login_role_name: String = login_role.try_get("quoted_role_name")?;
    login_pool.close().await;

    let role_name = format!("palimpsest_test_runtime_{}", Uuid::now_v7().simple());
    sqlx::query(AssertSqlSafe(format!(
        "CREATE ROLE \"{role_name}\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
    )))
    .execute(migration_pool)
    .await?;

    let verification = async {
        sqlx::query(AssertSqlSafe(format!(
            "GRANT \"{role_name}\" TO {quoted_login_role_name}"
        )))
        .execute(migration_pool)
        .await?;
        sqlx::raw_sql(AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA memory TO \"{role_name}\"; \
             GRANT SELECT ON \
                 memory.retrieval_policies, \
                 memory.recency_profiles, \
                 memory.embedding_profiles, \
                 memory.embedding_projection_profiles, \
                 memory.embedding_projection_lease_policies, \
                 memory.fact_retrieval_metadata_policies, \
                 memory.fact_retention_policies, \
                 memory.search_projection_schemas, \
                 memory.subject_lifecycles, \
                 memory.subject_content_leases, \
                 memory.facts, \
                 memory.fact_revisions, \
                 memory.fact_revision_evidence, \
                 memory.fact_revision_current, \
                 memory.fact_revision_current_coverage, \
                 memory.checkpoints, \
                 memory.checkpoint_revisions, \
                 memory.fact_revision_governance, \
                 memory.fact_revision_search_documents, \
                 memory.fact_revision_embedding_projections, \
                 memory.retrieval_idempotency_reservations, \
                 memory.retrieval_receipts, \
                 memory.retrieval_manifest_items, \
                 memory.retrieval_ready_fact_revision_embeddings, \
                 memory.authorized_retrieval_manifest, \
                 memory.episodes, \
                 memory.idempotency_receipts, \
                 memory.write_audit_receipts, \
                 memory.outbox_intents, \
                 memory.export_operations, \
                 memory.export_manifest_items \
             TO \"{role_name}\"; \
             GRANT INSERT ON \
                 memory.subject_lifecycles, \
                 memory.subject_content_leases, \
                 memory.retrieval_idempotency_reservations, \
                 memory.retrieval_receipts, \
                 memory.retrieval_manifest_items, \
                 memory.episodes, \
                 memory.idempotency_receipts, \
                 memory.write_audit_receipts, \
                 memory.outbox_intents \
             TO \"{role_name}\"; \
             GRANT UPDATE ON memory.idempotency_receipts TO \"{role_name}\"; \
             GRANT INSERT, UPDATE, DELETE ON \
                 memory.export_operations, memory.export_manifest_items \
             TO \"{role_name}\"; \
             GRANT DELETE ON memory.subject_content_leases TO \"{role_name}\"; \
             GRANT EXECUTE ON FUNCTION \
                 memory.round_half_even_integer_v1(numeric, numeric), \
                 memory.temporal_recency_factor_units_v1(text, text, numeric), \
                 memory.acquire_subject_content_lease(uuid, uuid, uuid, text), \
                 memory.release_subject_content_lease(uuid, uuid, uuid, text), \
                 memory.claim_next_export_operation(uuid, integer), \
                 memory.claim_next_expired_export_operation(uuid, integer), \
                 memory.deletion_workflow_allows(uuid, uuid), \
                 memory.create_deletion_operation(uuid, uuid, uuid, text, text, character, text[], integer), \
                 memory.poll_deletion_operation(uuid, uuid, uuid), \
                 memory.claim_next_deletion_operation(uuid, integer), \
                 memory.renew_deletion_operation_lease(uuid, uuid, uuid, uuid, integer), \
                 memory.release_deletion_operation_lease(uuid, uuid, uuid, uuid), \
                 memory.claim_next_deletion_target(uuid, uuid, uuid, uuid, uuid, integer), \
                 memory.renew_deletion_target_lease(uuid, uuid, uuid, uuid, character, uuid, integer), \
                 memory.fail_deletion_target(uuid, uuid, uuid, uuid, text, character, uuid, text, integer), \
                 memory.purge_deletion_target(uuid, uuid, text), \
                 memory.complete_deletion_target(uuid, uuid, uuid, uuid, text, character, uuid, character), \
                 memory.advance_deletion_operation(uuid, uuid, uuid, uuid, integer) \
             TO \"{role_name}\""
        )))
        .execute(migration_pool)
        .await?;

        let role_statement = format!("SET ROLE \"{role_name}\"");
        let runtime_pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _metadata| {
                let role_statement = role_statement.clone();
                Box::pin(async move {
                    sqlx::query(AssertSqlSafe(role_statement))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(PgConnectOptions::from_str(database_url)?)
            .await?;
        let role = sqlx::query(
            "SELECT current_user AS role_name, session_user AS login_role_name, \
                    rolsuper, rolbypassrls \
             FROM pg_roles WHERE rolname = current_user",
        )
        .fetch_one(&runtime_pool)
        .await?;
        ensure!(role.try_get::<String, _>("role_name")? == role_name);
        ensure!(role.try_get::<String, _>("login_role_name")? == login_role_name);
        ensure!(!role.try_get::<bool, _>("rolsuper")?);
        ensure!(!role.try_get::<bool, _>("rolbypassrls")?);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app_with_embedding_provider(
                    runtime_pool.clone(),
                    runtime_pool.clone(),
                    authenticator,
                    provider_port,
                ),
            )
            .await
        });
        let runtime_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        let mut nonbypass_export_target = runtime_target.clone();
        nonbypass_export_target.principal_a_secondary_subject_id =
            Uuid::parse_str("019be000-0000-7000-8000-000000000022")?;
        let runtime_scenario = async {
            let runtime_replay = creates_temporal_receipt_through_nonbypass_runtime(
                &runtime_target,
                temporal_fixture,
                temporal_replay,
                isolation_fixture,
            )
            .await?;
            temporal_policy_does_not_resurrect_ineligible_successors(
                &runtime_target,
                lifecycle_fixture,
                lifecycle_replay,
            )
            .await?;

            provider.set_mode(EmbeddingFixtureMode::Unavailable);
            let calls_before_replay = provider.calls();
            let replay_result = replays_temporal_receipt_through_nonbypass_runtime(
                &runtime_target,
                &runtime_replay,
            )
            .await;
            let calls_after_replay = provider.calls();
            provider.set_mode(EmbeddingFixtureMode::Valid);
            replay_result?;
            ensure!(
                calls_after_replay == calls_before_replay,
                "non-bypass durable replay called the unavailable embedding provider"
            );
            exercise_export_and_deletion_http(
                &runtime_target,
                &nonbypass_export_target,
                "principal-a-export-delete-test-token",
                migration_pool,
            )
            .await?;
            let residual: i64 = sqlx::query_scalar(
                r#"
                SELECT
                    (SELECT count(*) FROM memory.episodes
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.facts
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.fact_revisions
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.fact_revision_current
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.fact_revision_current_coverage
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.checkpoints
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.export_operations
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.export_manifest_items
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.deletion_tombstone_seeds
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.deletion_audit_seeds
                     WHERE tenant_id = $1 AND subject_id = $2)
                "#,
            )
            .bind(target.tenant_id)
            .bind(nonbypass_export_target.principal_a_secondary_subject_id)
            .fetch_one(migration_pool)
            .await?;
            ensure!(residual == 0, "non-bypass deletion left residual rows: {residual}");
            Ok::<(), anyhow::Error>(())
        }
        .await;
        server.abort();
        let _ = server.await;
        runtime_scenario
    }
    .await;

    let cleanup = async {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP OWNED BY \"{role_name}\"; \
             REVOKE \"{role_name}\" FROM {quoted_login_role_name}; \
             DROP ROLE \"{role_name}\""
        )))
        .execute(migration_pool)
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    verification?;
    cleanup?;
    Ok(())
}
