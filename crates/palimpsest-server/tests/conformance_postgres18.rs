use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use std::{str::FromStr, sync::Arc};

use palimpsest_conformance::{
    Target, checkpoint_scopes_fail_closed, concurrent_retrievals_converge_on_one_receipt,
    creates_an_attributable_fact_revision, creates_and_replays_a_lexical_retrieval_receipt,
    creates_retrieval_lifecycle_fixture, cross_scope_reads_fail_closed,
    expires_only_the_targeted_checkpoint, hybrid_retrieval_rejects_caller_ranking_internals,
    hybrid_retrieval_requires_an_available_provider, reconstructs_both_temporal_axes,
    records_and_reads_an_immutable_episode, rejects_cross_subject_idempotency_reuse,
    rejects_cross_subject_retrieval_idempotency_reuse, rejects_invalid_domain_and_timestamp_inputs,
    rejects_unregistered_write_policies, retrieval_candidates_are_authorized_before_ranking,
    retrieval_fails_closed_when_projection_is_corrupt,
    retrieval_fails_closed_when_projection_is_missing,
    retrieval_paginates_and_rejects_invalid_replays,
    retrieval_receipt_does_not_resurrect_deleted_history, retrieval_receipt_hides_expired_content,
    retrieval_recovers_after_projection_rebuild, retrieval_succeeds_after_projection_rebuild,
    retrieves_the_effective_bitemporal_revision, saves_and_reads_a_resumable_checkpoint,
    supersedes_the_fact_head,
};
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use sqlx::{AssertSqlSafe, ConnectOptions, PgPool, Row, postgres::PgConnectOptions};
use tokio::net::TcpListener;
use uuid::Uuid;

#[path = "conformance_postgres18/consolidation.rs"]
mod consolidation;
#[path = "conformance_postgres18/corpus.rs"]
mod corpus;
#[path = "conformance_postgres18/crash.rs"]
mod crash;
#[path = "conformance_postgres18/deletion.rs"]
mod deletion;
#[path = "conformance_postgres18/deletion_ops.rs"]
mod deletion_ops;
#[path = "conformance_postgres18/fixtures.rs"]
mod fixtures;
#[path = "conformance_postgres18/harness.rs"]
mod harness;
#[path = "conformance_postgres18/hybrid_setup.rs"]
mod hybrid_setup;
#[path = "conformance_postgres18/projection_helpers.rs"]
mod projection_helpers;
#[path = "conformance_postgres18/projections.rs"]
mod projections;
#[path = "conformance_postgres18/property_idempotency.rs"]
mod property_idempotency;
#[path = "conformance_postgres18/restore.rs"]
mod restore;

#[path = "conformance_postgres18/sleep_budget.rs"]
mod sleep_budget;
#[path = "conformance_postgres18/surface.rs"]
mod surface;
#[path = "conformance_postgres18/temporal.rs"]
mod temporal;
#[path = "conformance_postgres18/vault.rs"]
mod vault;
#[path = "conformance_postgres18/write_back.rs"]
mod write_back;
use consolidation::{
    consolidation_crash_resume_yields_no_duplicates_or_loss,
    consolidation_fails_closed_without_registered_policy,
    consolidation_job_not_failed_while_claims_in_flight, consolidation_jobs_are_isolated_by_scope,
    consolidation_jobs_enforce_bounded_queues,
    consolidation_worker_materializes_attributable_facts,
};
use crash::{verify_checkpoint_governance, verify_governed_write_records};
use deletion::{
    deletion_failed_operation_can_be_repaired_and_resumed,
    deletion_retry_backoff_rewinds_instead_of_waiting,
    deletion_target_lease_recovers_after_worker_expiry,
    deletion_target_retry_exhaustion_remains_fenced,
    deletion_worker_fails_closed_when_export_store_is_unavailable,
    exercise_export_and_deletion_http, export_worker_fails_closed_on_authorization_revocation,
    export_worker_fails_closed_on_store_failure,
    export_worker_lease_recovery_fences_stale_completion,
};
use deletion_ops::{
    corrupt_retrieval_projection_digest, corrupt_retrieval_search_vector,
    delete_retrieval_projection, delete_retrieval_revision, rebuild_retrieval_projection,
    recovers_a_committed_effect_after_response_loss,
};
use hybrid_setup::{
    install_deterministic_hybrid_fixture, runs_hybrid_retrieval_conformance,
    verify_lexical_retrieval_policy,
};
use projection_helpers::{
    rebuilds_the_current_fact_revision_projection, set_retrieval_test_scope,
    verify_retrieval_manifest_is_authorized,
};
use projections::{
    verify_hybrid_retrieval_policy_and_profiles, verify_no_retrieval_artifacts_for_idempotency_key,
};
use restore::{
    build_restore_fence_ledger, exercise_restore_fence_replay, populate_restore_corpus_over_http,
    rehearse_predeletion_restore_copy, seed_restore_fence_fixture,
    verify_restore_replay_is_hidden_over_http,
};
use surface::{
    verify_surface_caps_and_explained_bundle, verify_surface_default_empty,
    verify_surface_filters_before_ranking, verify_surface_idempotent_replay,
    verify_surface_respects_fence_and_purge, verify_surface_revoked_principal_empty,
    verify_surface_tenant_isolation,
};
use vault::{
    vault_export_kind_leaves_canonical_packages_unchanged, vault_pages_rebuild_byte_for_byte,
    vault_sync_rejects_direct_sync_back,
};

#[tokio::test]
async fn serves_the_bitemporal_lifecycle_over_http_and_postgres() -> Result<()> {
    sleep_budget::reset();
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_admin_pool = PgPool::connect(&migration_database_url)
        .await
        .with_context(|| {
            format!("connect to migration-authority PostgreSQL through {migration_database_url}")
        })?;

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
    let mut pool = PgPool::connect_with(options).await?;
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let mut migration_pool = PgPool::connect_with(migration_options).await?;
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
        principal_d_same_scope_bearer_token: "principal-d-same-scope-test-token".to_owned(),
    };
    let result = async {
        let probe_listener = TcpListener::bind("127.0.0.1:0").await?;
        let probe_address = probe_listener.local_addr()?;
        let probe_pool = pool.clone();
        let probe_server = tokio::spawn(async move {
            axum::serve(probe_listener, palimpsest_server::probe_router(probe_pool)).await
        });
        let unready = Client::new()
            .get(format!("http://{probe_address}/readyz"))
            .send()
            .await?;
        ensure!(unready.status() == StatusCode::SERVICE_UNAVAILABLE);
        ensure!(
            unready.headers().get(header::CACHE_CONTROL)
                == Some(&header::HeaderValue::from_static("no-store"))
        );
        ensure!(unready.content_length().is_none_or(|length| length == 0));
        probe_server.abort();
        let _ = probe_server.await;

        palimpsest_postgres::migrate(&pool).await?;
        let restore_fixture = seed_restore_fence_fixture(&migration_pool).await?;
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
        .await
        .context("create deletion operation")?;
        sqlx::query(
            r#"
            INSERT INTO memory.checkpoint_retention_policies (
                retention_policy_id, retention_interval
            )
            VALUES ('checkpoint-test-1s-v1', interval '1 second')
            "#,
        )
        .execute(&pool)
        .await
        .context("poll leased deletion target")?;
        let authenticator = Arc::new(StaticAuthenticator::new([
            (
                target.bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                        SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000022")?),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![],
                },
            ),
            (
                "principal-a-export-delete-test-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                        SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000022")?),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![
                        OperationGrant::CanonicalHistoryExport,
                        OperationGrant::SubjectDelete,
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
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_b_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(target.principal_b_tenant_id),
                    subject_ids: vec![SubjectId(target.principal_b_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_c_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(target.principal_c_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_d_same_scope_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-d".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(subject_id)],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![],
                },
            ),
            (
                "restore-conformance-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("restore-conformance".to_owned()),
                    tenant_id: TenantId(restore_fixture.tenant_id),
                    subject_ids: vec![SubjectId(restore_fixture.subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                    operation_grants: vec![],
                },
            ),
            (
                "restore-corpus-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(restore_fixture.tenant_id),
                    subject_ids: vec![SubjectId(restore_fixture.subject_id)],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![],
                },
            ),
            (
                "restore-corpus-principal-c-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(restore_fixture.tenant_id),
                    subject_ids: vec![SubjectId(Uuid::parse_str(
                        "019be000-0000-7000-8000-000000000317",
                    )?)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                    operation_grants: vec![],
                },
            ),
        ]));
        deletion_target_lease_recovers_after_worker_expiry(&pool, &migration_pool).await?;
        deletion_retry_backoff_rewinds_instead_of_waiting(&pool, &migration_pool).await?;
        deletion_failed_operation_can_be_repaired_and_resumed(&pool, &migration_pool).await?;
        deletion_target_retry_exhaustion_remains_fenced(&pool, &migration_pool).await?;
        export_worker_lease_recovery_fences_stale_completion(&pool, &migration_pool).await?;
        export_worker_fails_closed_on_store_failure(&pool, &migration_pool).await?;
        export_worker_fails_closed_on_authorization_revocation(&pool, &migration_pool).await?;
        vault_pages_rebuild_byte_for_byte(&pool, &migration_pool).await?;
        vault_export_kind_leaves_canonical_packages_unchanged(&pool, &migration_pool).await?;
        vault_sync_rejects_direct_sync_back(&pool, &migration_pool).await?;
        write_back::attributed_write_back_is_governed_and_fail_closed(&pool, &migration_pool)
            .await?;
        write_back::filed_answers_record_agent_writer_and_derived_provenance(
            &pool,
            &migration_pool,
        )
        .await?;
        deletion_worker_fails_closed_when_export_store_is_unavailable(&pool, &migration_pool)
            .await?;
        let restore_listener = TcpListener::bind("127.0.0.1:0").await?;
        let restore_address = restore_listener.local_addr()?;
        let restore_pool = pool.clone();
        let restore_authenticator = authenticator.clone();
        let restore_server = tokio::spawn(async move {
            axum::serve(
                restore_listener,
                palimpsest_server::app_without_workers(
                    restore_pool.clone(),
                    restore_pool,
                    restore_authenticator,
                ),
            )
            .await
        });
        let restore_target = Target {
            base_url: format!("http://{restore_address}"),
            ..target.clone()
        };
        let client = Client::new();
        let health = client
            .get(format!("{}/healthz", restore_target.base_url))
            .send()
            .await?;
        ensure!(health.status() == StatusCode::OK);
        ensure!(
            health.headers().get(header::CACHE_CONTROL)
                == Some(&header::HeaderValue::from_static("no-store"))
        );
        ensure!(health.content_length().is_none_or(|length| length == 0));
        let readiness = client
            .get(format!("{}/readyz", restore_target.base_url))
            .send()
            .await?;
        ensure!(readiness.status() == StatusCode::OK);
        ensure!(
            readiness.headers().get(header::CACHE_CONTROL)
                == Some(&header::HeaderValue::from_static("no-store"))
        );
        ensure!(readiness.content_length().is_none_or(|length| length == 0));
        populate_restore_corpus_over_http(&restore_target, &restore_fixture).await?;
        restore_server.abort();
        let _ = restore_server.await;

        let (snapshot_ledger, snapshot_ledger_bytes) =
            build_restore_fence_ledger(&migration_pool, &restore_fixture).await?;
        pool.close().await;
        migration_pool.close().await;
        let snapshot_database_name =
            format!("palimpsest_restore_snapshot_{}", Uuid::now_v7().simple());
        sqlx::query(AssertSqlSafe(format!(
            "CREATE DATABASE \"{snapshot_database_name}\" TEMPLATE \"{database_name}\""
        )))
        .execute(&admin_pool)
        .await
        .context("create pre-deletion restore snapshot")?;
        let snapshot_result = async {
            pool = PgPool::connect_with(
                PgConnectOptions::from_str(&database_url)?.database(&database_name),
            )
            .await?;
            migration_pool = PgPool::connect_with(
                PgConnectOptions::from_str(&migration_database_url)?.database(&database_name),
            )
            .await?;
            let snapshot_database_url = PgConnectOptions::from_str(&database_url)?
                .database(&snapshot_database_name)
                .to_url_lossy()
                .to_string();
            rehearse_predeletion_restore_copy(
                &snapshot_database_url,
                &snapshot_ledger_bytes,
                &snapshot_ledger.ledger_sha256,
                &restore_target,
                &restore_fixture,
                authenticator.clone(),
            )
            .await
        }
        .await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE \"{snapshot_database_name}\" WITH (FORCE)"
        )))
        .execute(&migration_admin_pool)
        .await
        .context("drop pre-deletion restore snapshot")?;
        snapshot_result?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_pool = pool.clone();
        let server_authenticator = authenticator.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(server_pool.clone(), server_pool, server_authenticator),
            )
            .await
        });
        let scenario_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        exercise_restore_fence_replay(&pool, &migration_pool, &restore_fixture, &test_database_url)
            .await?;
        verify_restore_replay_is_hidden_over_http(&scenario_target, &restore_fixture).await?;
        let scenario = async {
            records_and_reads_an_immutable_episode(&scenario_target).await?;
            creates_an_attributable_fact_revision(&scenario_target).await?;
            rebuilds_the_current_fact_revision_projection(&pool, &migration_pool, &scenario_target)
                .await?;
            creates_and_replays_a_lexical_retrieval_receipt(&scenario_target).await?;
            supersedes_the_fact_head(&scenario_target).await?;
            reconstructs_both_temporal_axes(&scenario_target).await?;
            retrieves_the_effective_bitemporal_revision(&scenario_target).await?;
            cross_scope_reads_fail_closed(&scenario_target).await?;
            rejects_cross_subject_idempotency_reuse(&scenario_target).await?;
            rejects_invalid_domain_and_timestamp_inputs(&scenario_target).await?;
            consolidation_worker_materializes_attributable_facts(&pool, &scenario_target).await?;
            consolidation_fails_closed_without_registered_policy(&pool, &scenario_target).await?;
            consolidation_jobs_are_isolated_by_scope(&pool, &scenario_target).await?;
            consolidation_jobs_enforce_bounded_queues(&pool, &scenario_target).await?;
            consolidation_job_not_failed_while_claims_in_flight(&pool, &scenario_target).await?;
            consolidation_crash_resume_yields_no_duplicates_or_loss(
                &test_database_url,
                &scenario_target,
            )
            .await?;
            verify_governed_write_records(
                &pool,
                &scenario_target,
                Uuid::parse_str("019be000-0000-7000-8000-000000000001")?,
            )
            .await?;
            verify_surface_tenant_isolation(&scenario_target).await?;
            verify_surface_caps_and_explained_bundle(&scenario_target).await?;
            verify_surface_default_empty(&scenario_target).await?;
            verify_surface_respects_fence_and_purge(&pool, &migration_pool, &scenario_target)
                .await?;
            verify_surface_revoked_principal_empty(&pool, &scenario_target).await?;
            verify_surface_filters_before_ranking(&scenario_target).await?;
            verify_surface_idempotent_replay(&scenario_target).await?;
            rejects_unregistered_write_policies(&scenario_target).await?;
            let retrieval_isolation =
                retrieval_candidates_are_authorized_before_ranking(&scenario_target).await?;
            concurrent_retrievals_converge_on_one_receipt(&scenario_target).await?;
            rejects_cross_subject_retrieval_idempotency_reuse(&scenario_target).await?;
            // ADR-0032 (issue #43): the precomputed authorized-current
            // structure is maintained at write time and its durable coverage
            // marker is complete, so the retrieval scenarios above (A1–A3 and
            // tenant isolation) exercised the fast path (A5b/A5c).
            let mut structure_scope = pool.begin().await?;
            set_retrieval_test_scope(&mut structure_scope, &scenario_target).await?;
            let structure_state: String = sqlx::query(
                r#"
                SELECT coverage_state
                FROM memory.authorized_current_projection_coverage
                WHERE tenant_id = $1 AND subject_id = $2
                "#,
            )
            .bind(target.tenant_id)
            .bind(target.subject_id)
            .fetch_one(&mut *structure_scope)
            .await?
            .try_get(0)?;
            ensure!(
                structure_state == "complete",
                "authorized-current structure is not complete: {structure_state}"
            );
            let structure_rows: i64 = sqlx::query(
                r#"
                SELECT count(*)::bigint
                FROM memory.authorized_current_projection
                WHERE tenant_id = $1 AND subject_id = $2
                "#,
            )
            .bind(target.tenant_id)
            .bind(target.subject_id)
            .fetch_one(&mut *structure_scope)
            .await?
            .try_get(0)?;
            ensure!(
                structure_rows > 0,
                "authorized-current structure is empty for the conformance scope"
            );
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
            retrieval_receipt_hides_expired_content(&scenario_target, &migration_pool).await?;
            let lifecycle = creates_retrieval_lifecycle_fixture(&scenario_target).await?;
            delete_retrieval_revision(&pool, &target, &lifecycle).await?;
            retrieval_receipt_does_not_resurrect_deleted_history(&scenario_target, &lifecycle)
                .await?;
            saves_and_reads_a_resumable_checkpoint(&scenario_target).await?;
            checkpoint_scopes_fail_closed(&scenario_target).await?;
            expires_only_the_targeted_checkpoint(&scenario_target, &migration_pool).await?;
            verify_checkpoint_governance(&pool, &scenario_target).await?;
            install_deterministic_hybrid_fixture(&migration_pool).await?;
            verify_hybrid_retrieval_policy_and_profiles(&migration_pool).await?;
            hybrid_retrieval_rejects_caller_ranking_internals(&scenario_target).await?;
            hybrid_retrieval_requires_an_available_provider(&scenario_target).await?;
            verify_no_retrieval_artifacts_for_idempotency_key(
                &pool,
                &target,
                "hybrid-provider-unavailable-default",
            )
            .await?;
            Ok::<_, anyhow::Error>(retrieval_isolation)
        }
        .await;
        let retrieval_isolation = scenario?;
        server.abort();
        let _ = server.await;
        runs_hybrid_retrieval_conformance(
            &pool,
            &migration_pool,
            authenticator.clone(),
            &target,
            &test_database_url,
            &retrieval_isolation,
        )
        .await?;
        let export_listener = TcpListener::bind("127.0.0.1:0").await?;
        let export_address = export_listener.local_addr()?;
        let export_pool = pool.clone();
        let export_authenticator = authenticator.clone();
        let export_server = tokio::spawn(async move {
            axum::serve(
                export_listener,
                palimpsest_server::app(export_pool.clone(), export_pool, export_authenticator),
            )
            .await
        });
        let export_target = Target {
            base_url: format!("http://{export_address}"),
            ..target.clone()
        };
        let export_result = exercise_export_and_deletion_http(
            &export_target,
            &target,
            "principal-a-export-delete-test-token",
            &migration_pool,
        )
        .await;
        export_server.abort();
        let _ = export_server.await;
        export_result?;
        recovers_a_committed_effect_after_response_loss(&pool, &target, &test_database_url).await
    }
    .await;

    // Spec 018 AC6: the deliberate timing sleeps must stay inside ten
    // seconds. Conditional-wait poll sleeps are bounded by their own poll
    // deadlines; the loose bound below catches an unbounded poll regression.
    ensure!(
        sleep_budget::total() <= std::time::Duration::from_secs(10),
        "the deliberate sleep budget exceeded ten seconds: {} ms",
        sleep_budget::total().as_millis()
    );
    ensure!(
        sleep_budget::poll_total() <= std::time::Duration::from_secs(70),
        "the conditional-wait poll sleeps exceeded their bound: {} ms",
        sleep_budget::poll_total().as_millis()
    );

    migration_pool.close().await;
    pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&migration_admin_pool)
    .await?;
    migration_admin_pool.close().await;
    result
}
