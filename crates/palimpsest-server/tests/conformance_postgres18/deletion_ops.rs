//! deletion_ops — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

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
    env,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use palimpsest_conformance::{RetrievalLifecycleFixture, Target, TemporalLifecycleFixture};
use palimpsest_domain::{PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use sqlx::{PgPool, Row};
use tokio::net::TcpListener;
use uuid::Uuid;

use super::crash::{
    checkpoint_idempotency_etag, reserve_local_address, spawn_crash_server,
    spawn_production_server, verify_crash_recovery_records, wait_for_listener,
};
use super::fixtures::{PROVIDER_APPLICATIONS, PROVIDER_ATTEMPTS, PROVIDER_EFFECTS};
use super::projection_helpers::set_retrieval_test_scope;

pub(crate) async fn delete_retrieval_revision(
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
    transition_revision_to_deleted(&mut transaction, target, fixture.deleted_revision_id).await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn delete_temporal_lifecycle_successor(
    pool: &PgPool,
    target: &Target,
    fixture: &TemporalLifecycleFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    transition_revision_to_deleted(
        &mut transaction,
        target,
        fixture.deleted_successor_revision_id,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn transition_revision_to_deleted(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
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
    .bind(revision_id)
    .execute(&mut **transaction)
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
    .bind(revision_id)
    .execute(&mut **transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    Ok(())
}

pub(crate) async fn delete_retrieval_projection(
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

pub(crate) async fn corrupt_retrieval_projection_digest(
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
    sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
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

pub(crate) async fn corrupt_retrieval_search_vector(
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
    sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
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

pub(crate) async fn rebuild_retrieval_projection(
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

pub(crate) async fn crash_after_selected_commit(request: Request, next: Next) -> Response {
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
pub(crate) async fn crash_after_checkpoint_commit_child() -> Result<()> {
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
            operation_grants: vec![],
        },
    )]));
    let listener = TcpListener::bind(&env::var("PALIMPSEST_TEST_CHILD_BIND")?).await?;
    let router = palimpsest_server::app(pool.clone(), pool, authenticator)
        .layer(middleware::from_fn(crash_after_selected_commit));
    axum::serve(listener, router).await?;
    Ok(())
}

pub(crate) async fn apply_mock_provider_effect(Path(effect_id): Path<Uuid>) -> StatusCode {
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

pub(crate) async fn recovers_a_committed_effect_after_response_loss(
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
