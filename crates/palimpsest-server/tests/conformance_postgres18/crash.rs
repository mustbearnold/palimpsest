//! crash — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use std::{env, process::Stdio, time::Duration};

use palimpsest_conformance::Target;
use sqlx::{PgPool, Row};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
};
use uuid::Uuid;

pub(crate) async fn reserve_local_address() -> Result<std::net::SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

pub(crate) fn spawn_crash_server(
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

pub(crate) fn spawn_production_server(
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
        .env("PALIMPSEST_ALLOWED_SENSITIVITIES", "internal,restricted")
        .env("PALIMPSEST_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
        .spawn()
        .context("restart production checkpoint server")
}

pub(crate) async fn wait_for_listener(address: std::net::SocketAddr) -> Result<()> {
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

pub(crate) async fn checkpoint_idempotency_etag(
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

pub(crate) async fn verify_crash_recovery_records(
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

pub(crate) async fn verify_governed_write_records(pool: &PgPool, target: &Target) -> Result<()> {
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

pub(crate) async fn verify_checkpoint_governance(pool: &PgPool, target: &Target) -> Result<()> {
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
