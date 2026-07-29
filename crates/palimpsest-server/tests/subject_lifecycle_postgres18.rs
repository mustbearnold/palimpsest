use anyhow::{Context, Result, ensure};
use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use palimpsest_application::{
    EmbeddingProvider, EmbeddingProviderError, EmbeddingRequest, EmbeddingResponse,
    RepositoryError, SubjectLifecycleRepository,
};
use palimpsest_conformance::{
    Target, creates_an_attributable_fact_revision, creates_and_replays_a_lexical_retrieval_receipt,
    saves_and_reads_a_resumable_checkpoint,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::{EmbeddingProjectionCoordinator, PostgresMemoryRepository};
use reqwest::{Client, StatusCode};
use serde_json::json;
use sqlx::{
    AssertSqlSafe, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone, Default)]
struct ResponseHold {
    arrived: Arc<Notify>,
    release: Arc<Notify>,
}

#[derive(Debug)]
struct PanicEmbeddingProvider;

#[async_trait::async_trait]
impl EmbeddingProvider for PanicEmbeddingProvider {
    async fn embed(
        &self,
        _request: EmbeddingRequest,
    ) -> std::result::Result<EmbeddingResponse, EmbeddingProviderError> {
        panic!("fenced projection reached the embedding provider")
    }
}

async fn hold_selected_response(
    State(hold): State<ResponseHold>,
    request: Request,
    next: Next,
) -> Response {
    let should_hold = request.headers().contains_key("x-hold-response");
    let response = next.run(request).await;
    if should_hold {
        hold.arrived.notify_one();
        hold.release.notified().await;
    }
    response
}

#[tokio::test]
async fn pending_subject_is_hidden_from_existing_http_reads_and_writes() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;
    let migration_admin_pool = PgPool::connect(&migration_database_url)
        .await
        .with_context(|| {
            format!("connect to migration authority through {migration_database_url}")
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

    let database_name = format!("palimpsest_lifecycle_{}", Uuid::now_v7().simple());
    let runtime_role = format!("palimpsest_lifecycle_{}", Uuid::now_v7().simple());
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;
    let login_identity = sqlx::query(
        "SELECT session_user::text AS role_name, quote_ident(session_user) AS quoted_role_name",
    )
    .fetch_one(&admin_pool)
    .await?;
    let login_role: String = login_identity.try_get("role_name")?;
    let quoted_login_role: String = login_identity.try_get("quoted_role_name")?;
    sqlx::query(AssertSqlSafe(format!(
        "CREATE ROLE \"{runtime_role}\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
    )))
    .execute(&migration_admin_pool)
    .await?;
    sqlx::query(AssertSqlSafe(format!(
        "GRANT \"{runtime_role}\" TO {quoted_login_role}"
    )))
    .execute(&migration_admin_pool)
    .await?;

    let options = PgConnectOptions::from_str(&database_url)?.database(&database_name);
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let migration_pool = PgPool::connect_with(migration_options).await?;
    let result = async {
        palimpsest_postgres::migrate(&migration_pool).await?;
        sqlx::raw_sql(AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA memory TO \"{runtime_role}\"; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA memory TO \"{runtime_role}\"; \
             GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA memory TO \"{runtime_role}\""
        )))
        .execute(&migration_pool)
        .await?;

        let role_statement = format!("SET ROLE \"{runtime_role}\"");
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
            .connect_with(options)
            .await?;
        let runtime_identity = sqlx::query(
            "SELECT current_user::text AS role_name, rolsuper, rolbypassrls \
             FROM pg_roles WHERE rolname = current_user",
        )
        .fetch_one(&runtime_pool)
        .await?;
        ensure!(runtime_identity.try_get::<String, _>("role_name")? == runtime_role);
        ensure!(!runtime_identity.try_get::<bool, _>("rolsuper")?);
        ensure!(!runtime_identity.try_get::<bool, _>("rolbypassrls")?);

        let tenant_id = Uuid::parse_str("019be100-0000-7000-8000-000000000010")?;
        let subject_id = Uuid::parse_str("019be100-0000-7000-8000-000000000020")?;
        let bearer_token = "lifecycle-principal-token";
        let principal = PrincipalScope {
            principal_id: PrincipalId("principal-a".to_owned()),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![
                SubjectId(subject_id),
                SubjectId(Uuid::parse_str(
                    "019be100-0000-7000-8000-000000000021",
                )?),
            ],
            allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
            operation_grants: vec![],
        };
        let authenticator = Arc::new(StaticAuthenticator::new([
            (bearer_token.to_owned(), principal.clone()),
            (
                "unused-internal-token".to_owned(),
                principal.clone(),
            ),
            (
                "unused-principal-b-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(Uuid::parse_str(
                        "019be100-0000-7000-8000-000000000110",
                    )?),
                    subject_ids: vec![SubjectId(Uuid::parse_str(
                        "019be100-0000-7000-8000-000000000120",
                    )?)],
                    allowed_sensitivities: vec![Sensitivity::try_from(
                        "restricted".to_owned(),
                    )?],
                    operation_grants: vec![],
                },
            ),
            (
                "unused-principal-c-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(Uuid::parse_str(
                        "019be100-0000-7000-8000-000000000220",
                    )?)],
                    allowed_sensitivities: vec![Sensitivity::try_from(
                        "restricted".to_owned(),
                    )?],
                    operation_grants: vec![],
                },
            ),
            (
                "unused-principal-d-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-d".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from(
                        "internal".to_owned(),
                    )?],
                    operation_grants: vec![],
                },
            ),
        ]));
        let lifecycle_repository = PostgresMemoryRepository::new(runtime_pool.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let projection_pool = runtime_pool.clone();
        let response_hold = ResponseHold::default();
        let server_hold = response_hold.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(runtime_pool.clone(), authenticator).layer(
                    middleware::from_fn_with_state(server_hold, hold_selected_response),
                ),
            )
            .await
        });

        let client = Client::new();
        let conformance_target = Target {
            base_url: format!("http://{address}"),
            bearer_token: bearer_token.to_owned(),
            tenant_id,
            subject_id,
            principal_a_secondary_subject_id: Uuid::parse_str(
                "019be100-0000-7000-8000-000000000021",
            )?,
            principal_a_internal_bearer_token: "unused-internal-token".to_owned(),
            principal_b_bearer_token: "unused-principal-b-token".to_owned(),
            principal_b_tenant_id: Uuid::parse_str(
                "019be100-0000-7000-8000-000000000110",
            )?,
            principal_b_subject_id: Uuid::parse_str(
                "019be100-0000-7000-8000-000000000120",
            )?,
            principal_c_bearer_token: "unused-principal-c-token".to_owned(),
            principal_c_subject_id: Uuid::parse_str(
                "019be100-0000-7000-8000-000000000220",
            )?,
            principal_d_same_scope_bearer_token: "unused-principal-d-token".to_owned(),
        };
        let collection_url = format!(
            "http://{address}/v1/tenants/{tenant_id}/subjects/{subject_id}/episodes"
        );
        let episode = json!({
            "case_id": "019be100-0000-7000-8000-000000000001",
            "kind": "message",
            "observed_at": "2026-07-29T08:00:00Z",
            "provenance": {
                "source_type": "subject-lifecycle-conformance",
                "source_uri": null,
                "external_id": "before-fence"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"canary": "must-not-cross-subject-fence"}
        });
        let create = client
            .post(&collection_url)
            .bearer_auth(bearer_token)
            .header("Idempotency-Key", "lifecycle-before-fence")
            .json(&episode)
            .send()
            .await?;
        ensure!(create.status() == StatusCode::CREATED);
        let location = create
            .headers()
            .get(reqwest::header::LOCATION)
            .context("episode create omitted Location")?
            .to_str()?
            .to_owned();
        let _ = create.bytes().await?;

        let before_fence = client
            .get(format!("http://{address}{location}"))
            .bearer_auth(bearer_token)
            .send()
            .await?;
        ensure!(before_fence.status() == StatusCode::OK);
        let _ = before_fence.bytes().await?;

        creates_an_attributable_fact_revision(&conformance_target).await?;
        saves_and_reads_a_resumable_checkpoint(&conformance_target).await?;
        creates_and_replays_a_lexical_retrieval_receipt(&conformance_target).await?;

        let fact_row = sqlx::query(
            r#"
            SELECT fact.fact_id, revision.revision_id
            FROM memory.facts AS fact
            JOIN memory.fact_revisions AS revision
              ON revision.tenant_id = fact.tenant_id
             AND revision.subject_id = fact.subject_id
             AND revision.case_id = fact.case_id
             AND revision.fact_id = fact.fact_id
            WHERE fact.tenant_id = $1
              AND fact.subject_id = $2
              AND fact.namespace = 'case.profile'
              AND fact.fact_key = 'shipping_address'
            ORDER BY revision.revision_no DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?;
        let fact_id: Uuid = fact_row.try_get("fact_id")?;
        let fact_revision_id: Uuid = fact_row.try_get("revision_id")?;
        let retrieval_id: Uuid = sqlx::query_scalar(
            r#"
            SELECT retrieval_id
            FROM memory.retrieval_receipts
            WHERE tenant_id = $1 AND subject_id = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?;

        wait_for_lease_count(&migration_pool, tenant_id, subject_id, 0).await?;

        let lease = lifecycle_repository
            .acquire_content_lease(&principal, TenantId(tenant_id), SubjectId(subject_id))
            .await?;
        ensure!(lease.lease_id.0.get_version_num() == 7);
        let active_lease_count: i64 = sqlx::query(
            "SELECT count(*) FROM memory.subject_content_leases \
             WHERE tenant_id = $1 AND subject_id = $2",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?
        .try_get(0)?;
        ensure!(active_lease_count == 1);
        lifecycle_repository.release_content_lease(lease).await?;

        let held_client = client.clone();
        let held_url = format!("http://{address}{location}");
        let held_response = tokio::spawn(async move {
            held_client
                .get(held_url)
                .bearer_auth(bearer_token)
                .header("x-hold-response", "1")
                .send()
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            response_hold.arrived.notified(),
        )
        .await
        .context("held response did not reach lifecycle middleware")?;
        let held_lease_count: i64 = sqlx::query(
            "SELECT count(*) FROM memory.subject_content_leases \
             WHERE tenant_id = $1 AND subject_id = $2",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?
        .try_get(0)?;
        ensure!(
            held_lease_count == 1,
            "HTTP response body did not retain exactly one content lease"
        );

        let state_version: i64 = sqlx::query_scalar(
            "SELECT memory.transition_subject_to_deletion_pending($1, $2)",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?;
        ensure!(state_version == 1);
        let retry_state_version: i64 = sqlx::query_scalar(
            "SELECT memory.transition_subject_to_deletion_pending($1, $2)",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(&migration_pool)
        .await?;
        ensure!(retry_state_version == state_version);
        let reverse_transition = sqlx::query(
            r#"
            UPDATE memory.subject_lifecycles
            SET lifecycle_state = 'active', state_version = state_version + 1
            WHERE tenant_id = $1 AND subject_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(subject_id)
        .execute(&migration_pool)
        .await;
        ensure!(
            reverse_transition.is_err(),
            "database lifecycle fence returned to active"
        );

        let fenced_lease = lifecycle_repository
            .acquire_content_lease(&principal, TenantId(tenant_id), SubjectId(subject_id))
            .await;
        ensure!(matches!(
            fenced_lease,
            Err(RepositoryError::SubjectUnavailable)
        ));

        let fact_url = format!(
            "http://{address}/v1/tenants/{tenant_id}/subjects/{subject_id}/facts/{fact_id}"
        );
        let checkpoint_url = format!(
            "http://{address}/v1/tenants/{tenant_id}/subjects/{subject_id}/agents/019be000-0000-7000-8000-000000000302/threads/019be000-0000-7000-8000-000000000303/checkpoint"
        );
        let retrievals_url = format!(
            "http://{address}/v1/tenants/{tenant_id}/subjects/{subject_id}/retrievals"
        );
        let retrieval_url = format!("{retrievals_url}/{retrieval_id}");
        let retrieval = json!({
            "query": "Wellington",
            "perspective": {"kind": "current"},
            "page_size": 10,
            "filters": {"namespaces": ["case.profile"]}
        });

        assert_fenced_response(
            client
                .get(format!("http://{address}{location}"))
                .bearer_auth(bearer_token)
                .send()
                .await?,
        )
        .await?;
        for idempotency_key in ["lifecycle-before-fence", "lifecycle-after-fence"] {
            assert_fenced_response(
                client
                    .post(&collection_url)
                    .bearer_auth(bearer_token)
                    .header("Idempotency-Key", idempotency_key)
                    .json(&episode)
                    .send()
                    .await?,
            )
            .await?;
        }
        assert_fenced_response(
            client.get(&fact_url).bearer_auth(bearer_token).send().await?,
        )
        .await?;
        assert_fenced_response(
            client
                .get(format!("{fact_url}/as-of"))
                .bearer_auth(bearer_token)
                .query(&[
                    ("valid_at", "2026-01-10T00:00:00Z"),
                    ("recorded_at", "2999-01-01T00:00:00Z"),
                ])
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .post(format!(
                    "http://{address}/v1/tenants/{tenant_id}/subjects/{subject_id}/facts"
                ))
                .bearer_auth(bearer_token)
                .header("Idempotency-Key", "fact-shipping-address-create")
                .json(&json!({}))
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .put(&fact_url)
                .bearer_auth(bearer_token)
                .header("Idempotency-Key", "fact-after-fence")
                .header(reqwest::header::IF_MATCH, format!("\"{fact_revision_id}\""))
                .json(&json!({}))
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .get(&checkpoint_url)
                .bearer_auth(bearer_token)
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .put(&checkpoint_url)
                .bearer_auth(bearer_token)
                .header("Idempotency-Key", "checkpoint-run-301-create")
                .header(reqwest::header::IF_NONE_MATCH, "*")
                .json(&json!({}))
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .get(&retrieval_url)
                .bearer_auth(bearer_token)
                .send()
                .await?,
        )
        .await?;
        assert_fenced_response(
            client
                .post(&retrievals_url)
                .bearer_auth(bearer_token)
                .header("Idempotency-Key", "retrieval-shipping-address")
                .json(&retrieval)
                .send()
                .await?,
        )
        .await?;

        let projection_provider: Arc<dyn EmbeddingProvider> = Arc::new(PanicEmbeddingProvider);
        let fenced_projection = EmbeddingProjectionCoordinator::new(
            projection_pool.clone(),
            projection_provider,
        )
        .rebuild_pending(TenantId(tenant_id), SubjectId(subject_id), 1)
        .await;
        ensure!(matches!(
            fenced_projection,
            Err(RepositoryError::SubjectUnavailable)
        ));

        response_hold.release.notify_waiters();
        let held_response = held_response.await??;
        ensure!(held_response.status() == StatusCode::OK);
        ensure!(
            held_response
                .text()
                .await?
                .contains("must-not-cross-subject-fence"),
            "the response admitted before draining lost its authorized content"
        );

        wait_for_lease_count(&migration_pool, tenant_id, subject_id, 0).await?;

        assert_transition_waits_for_inflight_scope(
            &projection_pool,
            &migration_pool,
            &lifecycle_repository,
            &principal,
            tenant_id,
            Uuid::parse_str("019be100-0000-7000-8000-000000000021")?,
        )
        .await?;

        let mut transaction = runtime_pool_for_role(&database_url, &database_name, &runtime_role)
            .await?
            .begin()
            .await?;
        sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
            .bind(subject_id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "SELECT set_config('palimpsest.allowed_sensitivities', '[\"internal\"]', true)",
        )
        .execute(&mut *transaction)
        .await?;
        let visible_rows: i64 = sqlx::query_scalar(
            r#"
            SELECT
                (SELECT count(*) FROM memory.episodes)
              + (SELECT count(*) FROM memory.idempotency_receipts)
              + (SELECT count(*) FROM memory.facts)
              + (SELECT count(*) FROM memory.fact_revisions)
              + (SELECT count(*) FROM memory.fact_revision_evidence)
              + (SELECT count(*) FROM memory.write_audit_receipts)
              + (SELECT count(*) FROM memory.outbox_intents)
              + (SELECT count(*) FROM memory.checkpoints)
              + (SELECT count(*) FROM memory.checkpoint_revisions)
              + (SELECT count(*) FROM memory.checkpoint_effect_intents)
              + (SELECT count(*) FROM memory.checkpoint_effect_receipts)
              + (SELECT count(*) FROM memory.fact_revision_governance)
              + (SELECT count(*) FROM memory.fact_revision_search_documents)
              + (SELECT count(*) FROM memory.fact_revision_embedding_projections)
              + (SELECT count(*) FROM memory.retrieval_receipts)
              + (SELECT count(*) FROM memory.retrieval_idempotency_reservations)
              + (SELECT count(*) FROM memory.retrieval_manifest_items)
            "#,
        )
            .fetch_one(&mut *transaction)
            .await?;
        ensure!(
            visible_rows == 0,
            "pending subject content remained visible through restricted forced RLS"
        );
        transaction.rollback().await?;

        server.abort();
        let _ = server.await;
        Ok::<_, anyhow::Error>(())
    }
    .await;

    migration_pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&migration_admin_pool)
    .await?;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "REVOKE \"{runtime_role}\" FROM {quoted_login_role}; DROP ROLE \"{runtime_role}\""
    )))
    .execute(&migration_admin_pool)
    .await?;
    migration_admin_pool.close().await;
    admin_pool.close().await;
    ensure!(!login_role.is_empty());
    result
}

async fn assert_fenced_response(response: reqwest::Response) -> Result<()> {
    ensure!(
        response.status() == StatusCode::NOT_FOUND,
        "fenced content path returned {}, expected 404",
        response.status()
    );
    ensure!(
        !response
            .text()
            .await?
            .contains("must-not-cross-subject-fence"),
        "fenced response disclosed the subject canary"
    );
    Ok(())
}

async fn wait_for_lease_count(
    pool: &PgPool,
    tenant_id: Uuid,
    subject_id: Uuid,
    expected: i64,
) -> Result<()> {
    let mut actual = -1_i64;
    for _ in 0..100 {
        actual = sqlx::query_scalar(
            "SELECT count(*) FROM memory.subject_content_leases \
             WHERE tenant_id = $1 AND subject_id = $2",
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(pool)
        .await?;
        if actual == expected {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    anyhow::bail!("content lease count was {actual}, expected {expected}")
}

async fn assert_transition_waits_for_inflight_scope(
    runtime_pool: &PgPool,
    migration_pool: &PgPool,
    lifecycle_repository: &PostgresMemoryRepository,
    principal: &PrincipalScope,
    tenant_id: Uuid,
    subject_id: Uuid,
) -> Result<()> {
    let lease = lifecycle_repository
        .acquire_content_lease(principal, TenantId(tenant_id), SubjectId(subject_id))
        .await?;
    lifecycle_repository.release_content_lease(lease).await?;

    let mut inflight_scope = runtime_pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *inflight_scope)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.to_string())
        .execute(&mut *inflight_scope)
        .await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared(\
            hashtextextended($1::text || ':' || $2::text, 0)\
        )",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .execute(&mut *inflight_scope)
    .await?;

    let transition_pool = migration_pool.clone();
    let transition = tokio::spawn(async move {
        sqlx::query_scalar::<_, i64>("SELECT memory.transition_subject_to_deletion_pending($1, $2)")
            .bind(tenant_id)
            .bind(subject_id)
            .fetch_one(&transition_pool)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ensure!(
        !transition.is_finished(),
        "deletion fence bypassed an in-flight shared subject scope"
    );
    inflight_scope.commit().await?;
    let transition_result = tokio::time::timeout(std::time::Duration::from_secs(5), transition)
        .await
        .context("deletion fence did not resume after the in-flight scope committed")??;
    let state_version = transition_result?;
    ensure!(state_version == 1);

    let fenced_lease = lifecycle_repository
        .acquire_content_lease(principal, TenantId(tenant_id), SubjectId(subject_id))
        .await;
    ensure!(matches!(
        fenced_lease,
        Err(RepositoryError::SubjectUnavailable)
    ));
    Ok(())
}

async fn runtime_pool_for_role(
    database_url: &str,
    database_name: &str,
    runtime_role: &str,
) -> Result<PgPool> {
    let role_statement = format!("SET ROLE \"{runtime_role}\"");
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let role_statement = role_statement.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(role_statement))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(PgConnectOptions::from_str(database_url)?.database(database_name))
        .await?)
}
