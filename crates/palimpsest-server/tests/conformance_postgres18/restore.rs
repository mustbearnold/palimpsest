//! restore — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode};
use std::{collections::BTreeMap, env, fs, process::Stdio, sync::Arc};
use time::{Duration as TimeDuration, OffsetDateTime};

use palimpsest_application::{RestoreFenceEntry, RestoreFenceLedger};
use palimpsest_conformance::{
    Target, creates_an_attributable_fact_revision, creates_and_replays_a_lexical_retrieval_receipt,
    records_and_reads_an_immutable_episode, saves_and_reads_a_resumable_checkpoint,
};
use palimpsest_http::Authenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use tokio::{net::TcpListener, process::Command};
use uuid::Uuid;

use super::fixtures::RestoreFixture;

pub(crate) async fn seed_restore_fence_fixture(migration_pool: &PgPool) -> Result<RestoreFixture> {
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000310")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000311")?;
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000312")?;
    let episode_id = Uuid::parse_str("019be000-0000-7000-8000-000000000313")?;
    let payload = r#"{"restore":"private"}"#;

    sqlx::query(
        r#"
        INSERT INTO memory.subject_lifecycles (
            tenant_id, subject_id, lifecycle_state, state_version
        )
        VALUES ($1, $2, 'active', 0)
        "#,
    )
    .bind(tenant_id)
    .bind(subject_id)
    .execute(migration_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO memory.episodes (
            tenant_id, subject_id, case_id, episode_id, kind, observed_at,
            writer_principal_id, source_type, sensitivity, retention_policy_id,
            schema_version, payload, payload_sha256
        )
        VALUES (
            $1, $2, $3, $4, 'observation', clock_timestamp(),
            'restore-conformance', 'restore-fixture', 'internal', 'standard',
            1, $5::jsonb,
            encode(public.digest(convert_to($5, 'UTF8'), 'sha256'), 'hex')
        )
        "#,
    )
    .bind(tenant_id)
    .bind(subject_id)
    .bind(case_id)
    .bind(episode_id)
    .bind(payload)
    .execute(migration_pool)
    .await?;

    Ok(RestoreFixture {
        tenant_id,
        subject_id,
        episode_id,
    })
}

pub(crate) async fn build_restore_fence_ledger(
    migration_pool: &PgPool,
    fixture: &RestoreFixture,
) -> Result<(RestoreFenceLedger, Vec<u8>)> {
    let scope_digest: String = sqlx::query_scalar("SELECT memory.deletion_scope_digest($1, $2)")
        .bind(fixture.tenant_id)
        .bind(fixture.subject_id)
        .fetch_one(migration_pool)
        .await?;
    let now = OffsetDateTime::now_utc();
    let ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            scope_digest,
            1,
            now - TimeDuration::minutes(1),
            now + TimeDuration::hours(1),
        )?],
    )?;
    let bytes = ledger.to_bytes()?;
    Ok((ledger, bytes))
}

pub(crate) async fn rehearse_predeletion_restore_copy(
    database_url: &str,
    ledger_bytes: &[u8],
    expected_ledger_sha256: &str,
    target_template: &Target,
    fixture: &RestoreFixture,
    authenticator: Arc<dyn Authenticator>,
) -> Result<()> {
    let pool = PgPool::connect(database_url)
        .await
        .context("connect to pre-deletion restore copy")?;

    let first_listener = TcpListener::bind("127.0.0.1:0").await?;
    let first_address = first_listener.local_addr()?;
    let first_pool = pool.clone();
    let first_authenticator = authenticator.clone();
    let first_server = tokio::spawn(async move {
        axum::serve(
            first_listener,
            palimpsest_server::app_without_workers(
                first_pool.clone(),
                first_pool,
                first_authenticator,
            ),
        )
        .await
    });
    let copy_target = Target {
        base_url: format!("http://{first_address}"),
        ..target_template.clone()
    };
    let visible_result = verify_restore_corpus_is_visible_over_http(&copy_target, fixture).await;
    first_server.abort();
    let _ = first_server.await;
    visible_result?;

    let restore_status =
        run_restore_mode_process(database_url, ledger_bytes, expected_ledger_sha256).await?;
    ensure!(
        restore_status.success(),
        "restore replay failed against the pre-deletion copy"
    );

    let second_listener = TcpListener::bind("127.0.0.1:0").await?;
    let second_address = second_listener.local_addr()?;
    let second_pool = pool.clone();
    let second_authenticator = authenticator;
    let second_server = tokio::spawn(async move {
        axum::serve(
            second_listener,
            palimpsest_server::app_without_workers(
                second_pool.clone(),
                second_pool,
                second_authenticator,
            ),
        )
        .await
    });
    let hidden_target = Target {
        base_url: format!("http://{second_address}"),
        ..copy_target
    };
    let hidden_result = verify_restore_replay_is_hidden_over_http(&hidden_target, fixture).await;
    second_server.abort();
    let _ = second_server.await;
    pool.close().await;
    hidden_result
}

pub(crate) async fn verify_restore_corpus_is_visible_over_http(
    target: &Target,
    fixture: &RestoreFixture,
) -> Result<()> {
    let response = Client::new()
        .get(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes/{}",
            target.base_url, fixture.tenant_id, fixture.subject_id, fixture.episode_id
        ))
        .bearer_auth("restore-conformance-token")
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status == StatusCode::OK,
        "pre-deletion restore copy did not serve the private episode: {status}"
    );
    let episode_id = fixture.episode_id.to_string();
    for required in ["restore", "private", episode_id.as_str()] {
        ensure!(
            body.contains(required),
            "pre-deletion restore copy omitted {required}"
        );
    }
    Ok(())
}

pub(crate) async fn exercise_restore_fence_replay(
    pool: &PgPool,
    migration_pool: &PgPool,
    fixture: &RestoreFixture,
    database_url: &str,
) -> Result<()> {
    let tenant_id = fixture.tenant_id;
    let subject_id = fixture.subject_id;
    let populated_counts = restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?;
    let mut populated_durable_counts = populated_counts.clone();
    populated_durable_counts.remove("subject_content_leases");
    for table_name in [
        "episodes",
        "facts",
        "fact_revision_evidence",
        "fact_revision_governance",
        "fact_revision_search_documents",
        "fact_revision_current",
        "fact_revisions",
        "checkpoints",
        "checkpoint_revisions",
        "retrieval_idempotency_reservations",
        "retrieval_manifest_items",
        "retrieval_receipts",
    ] {
        ensure!(
            populated_counts
                .get(table_name)
                .copied()
                .unwrap_or_default()
                > 0,
            "restore corpus did not populate {table_name}"
        );
    }

    let scope_digest: String = sqlx::query_scalar("SELECT memory.deletion_scope_digest($1, $2)")
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(migration_pool)
        .await?;
    let now = OffsetDateTime::now_utc();
    let ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            scope_digest.clone(),
            1,
            now - TimeDuration::minutes(1),
            now + TimeDuration::hours(1),
        )?],
    )?;
    let ledger_bytes = ledger.to_bytes()?;
    let repository = PostgresMemoryRepository::new(migration_pool.clone());
    assert!(
        repository
            .replay_restore_fence_ledger(&ledger_bytes, &"0".repeat(64))
            .await
            .is_err(),
        "restore replay must reject a mismatched independent digest"
    );
    let episode_count_after_digest_mismatch: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(
        episode_count_after_digest_mismatch,
        populated_counts["episodes"]
    );
    assert_eq!(
        durable_restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?,
        populated_durable_counts
    );

    // A ledger produced under a rotated scope key fails closed without
    // touching the restored store: the digests cannot be re-derived.
    let rotated_ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            format!("v2:{}", "0".repeat(64)),
            1,
            now - TimeDuration::minutes(1),
            now + TimeDuration::hours(1),
        )?],
    )?;
    let rotated_bytes = rotated_ledger.to_bytes()?;
    assert!(
        repository
            .replay_restore_fence_ledger(&rotated_bytes, &rotated_ledger.ledger_sha256)
            .await
            .is_err(),
        "restore replay must reject a ledger from a rotated scope key"
    );

    let wrong_startup_status =
        run_restore_mode_process(database_url, &ledger_bytes, &"0".repeat(64)).await?;
    ensure!(
        !wrong_startup_status.success(),
        "restore mode accepted a mismatched independent digest"
    );
    assert_eq!(
        durable_restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?,
        populated_durable_counts
    );

    let startup_status =
        run_restore_mode_process(database_url, &ledger_bytes, &ledger.ledger_sha256).await?;
    ensure!(
        startup_status.success(),
        "restore mode failed to replay a verified ledger"
    );
    let idempotent_startup_status =
        run_restore_mode_process(database_url, &ledger_bytes, &ledger.ledger_sha256).await?;
    ensure!(
        idempotent_startup_status.success(),
        "restore mode failed to replay an already recorded ledger"
    );

    let report = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay fixture failed: {error}"))?;
    assert_eq!(report.scopes_found, 1);
    assert_eq!(report.scopes_purged, 1);
    assert_eq!(report.residual_rows, 0);
    assert_eq!(report.ledger_sha256, ledger.ledger_sha256);
    let residual_counts = restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?;
    assert!(
        residual_counts.values().all(|count| *count == 0),
        "restore replay left scoped rows: {residual_counts:?}"
    );

    let state: String = sqlx::query_scalar(
        "SELECT lifecycle_state FROM memory.subject_lifecycles WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(state, "deleted");
    let episode_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(episode_count, 0);

    let replayed = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay idempotency failed: {error}"))?;
    assert_eq!(replayed, report);

    let mut runtime_transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *runtime_transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.to_string())
        .execute(&mut *runtime_transaction)
        .await?;
    let runtime_episode_count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.episodes")
        .fetch_one(&mut *runtime_transaction)
        .await?;
    assert_eq!(runtime_episode_count, 0);
    // A ledger entry whose scope is absent from the restored store is
    // vacuously satisfied: the fence was recorded after the backup, so there
    // is nothing to suppress. The replay purges the matched scope and ignores
    // the absent one.
    let unmatched_ledger = RestoreFenceLedger::build(
        now,
        vec![
            RestoreFenceEntry::new(
                scope_digest,
                1,
                now - TimeDuration::minutes(1),
                now + TimeDuration::hours(1),
            )?,
            RestoreFenceEntry::new(
                format!("v1:{}", "0".repeat(64)),
                1,
                now - TimeDuration::minutes(1),
                now + TimeDuration::hours(1),
            )?,
        ],
    )?;
    let unmatched_bytes = unmatched_ledger.to_bytes()?;
    let vacuous_report = repository
        .replay_restore_fence_ledger(&unmatched_bytes, &unmatched_ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay absent scope failed: {error}"))?;
    assert_eq!(vacuous_report.scopes_found, 1);
    assert_eq!(vacuous_report.scopes_purged, 1);
    assert_eq!(vacuous_report.residual_rows, 0);
    let episode_count_after_unmatched_scope: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(episode_count_after_unmatched_scope, 0);
    let durable_counts_after_unmatched_scope =
        durable_restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?;
    assert!(
        durable_counts_after_unmatched_scope
            .values()
            .all(|count| *count == 0),
        "restore replay left durable scope rows: {durable_counts_after_unmatched_scope:?}"
    );

    runtime_transaction.rollback().await?;
    Ok(())
}

pub(crate) async fn populate_restore_corpus_over_http(
    server_target: &Target,
    fixture: &RestoreFixture,
) -> Result<()> {
    let principal_a_secondary_subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000314")?;
    let target = Target {
        base_url: server_target.base_url.clone(),
        bearer_token: "restore-corpus-token".to_owned(),
        tenant_id: fixture.tenant_id,
        subject_id: fixture.subject_id,
        principal_a_secondary_subject_id,
        principal_a_internal_bearer_token: "restore-corpus-token".to_owned(),
        principal_b_bearer_token: "unused-principal-b-token".to_owned(),
        principal_b_tenant_id: Uuid::parse_str("019be000-0000-7000-8000-000000000315")?,
        principal_b_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000316")?,
        principal_c_bearer_token: "restore-corpus-principal-c-token".to_owned(),
        principal_c_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000317")?,
        principal_d_same_scope_bearer_token: "unused-principal-d-token".to_owned(),
    };
    records_and_reads_an_immutable_episode(&target).await?;
    creates_an_attributable_fact_revision(&target).await?;
    creates_and_replays_a_lexical_retrieval_receipt(&target).await?;
    saves_and_reads_a_resumable_checkpoint(&target).await?;
    Ok(())
}

pub(crate) async fn restore_scope_row_counts(
    migration_pool: &PgPool,
    tenant_id: Uuid,
    subject_id: Uuid,
) -> Result<BTreeMap<String, i64>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT table_name, row_count
        FROM (
            SELECT 'episodes'::text AS table_name, count(*)::bigint AS row_count
            FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'facts', count(*)::bigint
            FROM memory.facts WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revisions', count(*)::bigint
            FROM memory.fact_revisions WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_current', count(*)::bigint
            FROM memory.fact_revision_current WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_current_coverage', count(*)::bigint
            FROM memory.fact_revision_current_coverage
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_evidence', count(*)::bigint
            FROM memory.fact_revision_evidence WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_governance', count(*)::bigint
            FROM memory.fact_revision_governance WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_search_documents', count(*)::bigint
            FROM memory.fact_revision_search_documents
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'fact_revision_embedding_projections', count(*)::bigint
            FROM memory.fact_revision_embedding_projections
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'checkpoints', count(*)::bigint
            FROM memory.checkpoints WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'checkpoint_revisions', count(*)::bigint
            FROM memory.checkpoint_revisions WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'checkpoint_effect_intents', count(*)::bigint
            FROM memory.checkpoint_effect_intents
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'checkpoint_effect_receipts', count(*)::bigint
            FROM memory.checkpoint_effect_receipts
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'outbox_intents', count(*)::bigint
            FROM memory.outbox_intents WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'idempotency_receipts', count(*)::bigint
            FROM memory.idempotency_receipts WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'write_audit_receipts', count(*)::bigint
            FROM memory.write_audit_receipts WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'retrieval_receipts', count(*)::bigint
            FROM memory.retrieval_receipts WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'retrieval_manifest_items', count(*)::bigint
            FROM memory.retrieval_manifest_items WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'retrieval_idempotency_reservations', count(*)::bigint
            FROM memory.retrieval_idempotency_reservations
            WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'export_manifest_items', count(*)::bigint
            FROM memory.export_manifest_items WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'export_operations', count(*)::bigint
            FROM memory.export_operations WHERE tenant_id = $1 AND subject_id = $2
            UNION ALL
            SELECT 'subject_content_leases', count(*)::bigint
            FROM memory.subject_content_leases WHERE tenant_id = $1 AND subject_id = $2
        ) AS counts
        ORDER BY table_name
        "#,
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_all(migration_pool)
    .await?;
    Ok(rows.into_iter().collect())
}

pub(crate) async fn durable_restore_scope_row_counts(
    migration_pool: &PgPool,
    tenant_id: Uuid,
    subject_id: Uuid,
) -> Result<BTreeMap<String, i64>> {
    let mut counts = restore_scope_row_counts(migration_pool, tenant_id, subject_id).await?;
    counts.remove("subject_content_leases");
    Ok(counts)
}

pub(crate) async fn run_restore_mode_process(
    database_url: &str,
    ledger_bytes: &[u8],
    expected_ledger_sha256: &str,
) -> Result<std::process::ExitStatus> {
    let ledger_path = format!("/tmp/palimpsest-restore-fence-{}.json", Uuid::now_v7());
    fs::write(&ledger_path, ledger_bytes).context("write restore fence ledger fixture")?;
    let result = async {
        let mut child = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
            .env("PALIMPSEST_RESTORE_MODE", "1")
            .env("PALIMPSEST_RESTORE_DATABASE_URL", database_url)
            .env("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH", &ledger_path)
            .env(
                "PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256",
                expected_ledger_sha256,
            )
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn restore mode process")?;
        child.wait().await.context("wait for restore mode process")
    }
    .await;
    let _ = fs::remove_file(&ledger_path);
    result
}

pub(crate) async fn verify_restore_replay_is_hidden_over_http(
    target: &Target,
    fixture: &RestoreFixture,
) -> Result<()> {
    let response = Client::new()
        .get(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes/{}",
            target.base_url, fixture.tenant_id, fixture.subject_id, fixture.episode_id
        ))
        .bearer_auth("restore-conformance-token")
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status == StatusCode::NOT_FOUND,
        "replayed private episode returned {status}"
    );
    let episode_id = fixture.episode_id.to_string();
    for forbidden in ["restore", "private", episode_id.as_str()] {
        ensure!(
            !body.contains(forbidden),
            "restore replay response disclosed {forbidden}"
        );
    }
    Ok(())
}
