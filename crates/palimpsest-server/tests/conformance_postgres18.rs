use anyhow::{Context, Result, ensure};
use std::{str::FromStr, sync::Arc};

use palimpsest_conformance::{
    Target, creates_an_attributable_fact_revision, cross_scope_reads_fail_closed,
    reconstructs_both_temporal_axes, records_and_reads_an_immutable_episode,
    supersedes_the_fact_head,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgConnectOptions};
use tokio::net::TcpListener;
use uuid::Uuid;

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
    let pool = PgPool::connect_with(options).await?;
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000010")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000020")?;
    let target = Target {
        base_url: String::new(),
        bearer_token: "principal-a-test-token".to_owned(),
        tenant_id,
        subject_id,
        principal_b_bearer_token: "principal-b-test-token".to_owned(),
        principal_b_tenant_id: Uuid::parse_str("019be000-0000-7000-8000-000000000110")?,
        principal_b_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000120")?,
        principal_c_bearer_token: "principal-c-test-token".to_owned(),
        principal_c_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000220")?,
    };
    let result = async {
        palimpsest_postgres::migrate(&pool).await?;
        let authenticator = Arc::new(StaticAuthenticator::new([
            (
                target.bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(subject_id)],
                },
            ),
            (
                target.principal_b_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(target.principal_b_tenant_id),
                    subject_ids: vec![SubjectId(target.principal_b_subject_id)],
                },
            ),
            (
                target.principal_c_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(target.principal_c_subject_id)],
                },
            ),
        ]));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_pool = pool.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, palimpsest_server::app(server_pool, authenticator)).await
        });
        let scenario_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        let scenario = async {
            records_and_reads_an_immutable_episode(&scenario_target).await?;
            creates_an_attributable_fact_revision(&scenario_target).await?;
            supersedes_the_fact_head(&scenario_target).await?;
            reconstructs_both_temporal_axes(&scenario_target).await?;
            cross_scope_reads_fail_closed(&scenario_target).await
        }
        .await;
        server.abort();
        scenario
    }
    .await;

    pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&admin_pool)
    .await?;
    result
}
