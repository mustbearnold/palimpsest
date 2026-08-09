//! Embedded mode conformance (spec 014, issue #40) — PostgreSQL 18 substrate.
//!
//! The seven `verify_embedded_*` scenarios mirror the server facade
//! (`palimpsest-server/tests/conformance_postgres18.rs`) but drive the
//! `palimpsest-embedded` crate: `open` applies the canonical migrations and
//! opens no listener (A3), and the explicit loopback server serves
//! `palimpsest_embedded::router` instead of the server's app.

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use std::sync::Arc;
use time::{Duration as TimeDuration, OffsetDateTime};

use palimpsest_application::{RestoreFenceEntry, RestoreFenceLedger};
use palimpsest_conformance::{
    RetrievalLifecycleFixture, Target, concurrent_retrievals_converge_on_one_receipt,
    creates_an_attributable_fact_revision, creates_and_replays_a_lexical_retrieval_receipt,
    creates_retrieval_lifecycle_fixture, cross_scope_reads_fail_closed,
    reconstructs_both_temporal_axes, records_and_reads_an_immutable_episode,
    rejects_cross_subject_idempotency_reuse, rejects_cross_subject_retrieval_idempotency_reuse,
    rejects_invalid_domain_and_timestamp_inputs, rejects_unregistered_write_policies,
    retrieval_candidates_are_authorized_before_ranking,
    retrieval_fails_closed_when_projection_is_corrupt,
    retrieval_fails_closed_when_projection_is_missing,
    retrieval_paginates_and_rejects_invalid_replays,
    retrieval_receipt_does_not_resurrect_deleted_history, retrieval_receipt_hides_expired_content,
    retrieval_recovers_after_projection_rebuild, retrieval_succeeds_after_projection_rebuild,
    retrieves_the_effective_bitemporal_revision, supersedes_the_fact_head,
};
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_embedded::EmbeddedMemory;
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgConnectOptions};
use uuid::Uuid;

/// The restore fence corpus fixture (spec 014 A2).
struct RestoreFixture {
    tenant_id: Uuid,
    subject_id: Uuid,
    episode_id: Uuid,
}

/// The canonical episode recorded by the restore fence corpus.
fn restore_episode_payload() -> &'static str {
    r#"{"restore":"private"}"#
}

#[tokio::test]
async fn embedded_mode_conforms_to_spec_014() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://palimpsest_local_runtime:palimpsest-local-runtime-password@127.0.0.1:55432/postgres"
            .to_owned()
    });
    let migration_database_url = std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://mustbearn@127.0.0.1:55432/postgres".to_owned());
    let admin_pool = PgPool::connect(&database_url)
        .await
        .context("connect admin test pool")?;
    let migration_admin_pool = PgPool::connect(&migration_database_url)
        .await
        .context("connect admin migration pool")?;

    // PostgreSQL 18 + pgvector 0.8.5 (canonical substrate, spec 002 R1).
    let server_version: String = sqlx::query_scalar("SELECT current_setting('server_version_num')")
        .fetch_one(&admin_pool)
        .await?;
    ensure!(
        server_version.starts_with("18"),
        "expected PostgreSQL 18, found {server_version}"
    );
    let pgvector_version: String =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_one(&admin_pool)
            .await?;
    ensure!(
        pgvector_version.starts_with("0.8.5"),
        "expected pgvector 0.8.5, found {pgvector_version}"
    );

    let database_name = format!("palimpsest_test_{}", Uuid::now_v7().simple());
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;
    let pool = PgPool::connect_with(
        database_url
            .parse::<PgConnectOptions>()
            .context("parse test database url")?
            .database(&database_name),
    )
    .await
    .context("connect runtime pool")?;
    let migration_pool = PgPool::connect_with(
        migration_database_url
            .parse::<PgConnectOptions>()
            .context("parse migration database url")?
            .database(&database_name),
    )
    .await
    .context("connect migration pool")?;

    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000010")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000020")?;
    let principal_a_secondary_subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000021")?;
    let principal_c_subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000220")?;
    let principal_b_tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000110")?;
    let principal_b_subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000120")?;
    let principal_d_same_scope_subject_id =
        Uuid::parse_str("019be000-0000-7000-8000-000000000022")?;
    let principal_d_same_scope_bearer_token = "principal-d-same-scope-token".to_owned();
    let target = Target {
        base_url: "http://127.0.0.1:0".to_owned(),
        bearer_token: "principal-a-test-token".to_owned(),
        tenant_id,
        subject_id,
        principal_a_secondary_subject_id,
        principal_a_internal_bearer_token: "principal-a-internal-token".to_owned(),
        principal_b_bearer_token: "principal-b-token".to_owned(),
        principal_b_tenant_id,
        principal_b_subject_id,
        principal_c_bearer_token: "principal-c-token".to_owned(),
        principal_c_subject_id,
        principal_d_same_scope_bearer_token,
    };
    let authenticator = Arc::new(StaticAuthenticator::new([
        (
            "principal-a-test-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-a".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![
                    SubjectId(subject_id),
                    SubjectId(principal_a_secondary_subject_id),
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
                principal_id: PrincipalId("principal-a-export-delete".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![
                    SubjectId(subject_id),
                    SubjectId(principal_a_secondary_subject_id),
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
            "principal-a-internal-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-a".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![
                    SubjectId(subject_id),
                    SubjectId(principal_a_secondary_subject_id),
                ],
                allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                operation_grants: vec![],
            },
        ),
        (
            "principal-b-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-b".to_owned()),
                tenant_id: TenantId(principal_b_tenant_id),
                subject_ids: vec![SubjectId(principal_b_subject_id)],
                allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                operation_grants: vec![],
            },
        ),
        (
            "principal-c-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-c".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![SubjectId(principal_c_subject_id)],
                allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                operation_grants: vec![],
            },
        ),
        (
            "principal-d-same-scope-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-d-same-scope".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![
                    SubjectId(subject_id),
                    SubjectId(principal_d_same_scope_subject_id),
                ],
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
                tenant_id: TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000310")?),
                subject_ids: vec![SubjectId(Uuid::parse_str(
                    "019be000-0000-7000-8000-000000000311",
                )?)],
                allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                operation_grants: vec![],
            },
        ),
        (
            "restore-corpus-token".to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("restore-corpus".to_owned()),
                tenant_id: TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000310")?),
                subject_ids: vec![
                    SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000311")?),
                    SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000314")?),
                ],
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
                principal_id: PrincipalId("restore-corpus-principal-c".to_owned()),
                tenant_id: TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000310")?),
                subject_ids: vec![SubjectId(Uuid::parse_str(
                    "019be000-0000-7000-8000-000000000317",
                )?)],
                allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                operation_grants: vec![],
            },
        ),
    ]));

    // Spec 014 A6: the canonical migrations apply on the embedded substrate.
    // Spec 014 A3: `open` opens NO listener by default.
    let embedded =
        palimpsest_embedded::open(pool.clone(), pool.clone(), authenticator.clone()).await?;
    verify_embedded_no_listener_default(&embedded).await?;

    // Fixture seeding (mirror the server facade): lexical policy verification,
    // the restore fence corpus, and the 1-second retention policies.
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

    // Spec 014 A3: the explicit loopback opt-in binds a loopback-only
    // address and serves the canonical router.
    let server = embedded.serve_loopback().await?;
    ensure!(
        server.address.ip().is_loopback(),
        "embedded loopback server must bind a loopback address, found {}",
        server.address
    );
    let scenario_target = Target {
        base_url: format!("http://{}", server.address),
        ..target.clone()
    };

    let scenario = async {
        verify_embedded_retrieval_conformance(&scenario_target, &migration_pool).await?;
        // A4 must run before A2: A2 replays the restore fence ledger, which
        // purges the restore corpus that A4 reads back over the surface.
        verify_embedded_contract_parity(&scenario_target, &restore_fixture).await?;
        verify_embedded_lifecycle_fence_and_restore(
            &pool,
            &migration_pool,
            &scenario_target,
            &restore_fixture,
        )
        .await?;
        verify_embedded_tenant_isolation(&migration_pool, &scenario_target).await?;
        verify_embedded_index_reproducible(&pool, &scenario_target).await?;
        verify_embedded_surface_policy(&scenario_target).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    server.stop().await;
    scenario?;

    pool.close().await;
    migration_pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&migration_admin_pool)
    .await?;
    admin_pool.close().await;
    migration_admin_pool.close().await;
    Ok(())
}

/// A1. Spec 002 A1–A3 canonical suite passes unchanged against the embedded
/// substrate: episodes, fact revisions, and retrieval receipts over the
/// embedded loopback surface.
async fn verify_embedded_retrieval_conformance(
    target: &Target,
    migration_pool: &PgPool,
) -> Result<()> {
    records_and_reads_an_immutable_episode(target).await?;
    creates_an_attributable_fact_revision(target).await?;
    creates_and_replays_a_lexical_retrieval_receipt(target).await?;
    supersedes_the_fact_head(target).await?;
    reconstructs_both_temporal_axes(target).await?;
    retrieves_the_effective_bitemporal_revision(target).await?;
    rejects_cross_subject_idempotency_reuse(target).await?;
    rejects_invalid_domain_and_timestamp_inputs(target).await?;
    concurrent_retrievals_converge_on_one_receipt(target).await?;
    rejects_cross_subject_retrieval_idempotency_reuse(target).await?;
    retrieval_paginates_and_rejects_invalid_replays(target).await?;
    retrieval_receipt_hides_expired_content(target, migration_pool).await?;
    rejects_unregistered_write_policies(target).await?;
    Ok(())
}

/// A2. Lifecycle fence and restore: a deleted revision stays hidden from
/// retrieval receipts, and a verified restore fence ledger purges the scoped
/// corpus, tombstones the subject lifecycle, and replays idempotently.
/// Unverified ledgers fail closed and leave the corpus untouched.
async fn verify_embedded_lifecycle_fence_and_restore(
    pool: &PgPool,
    migration_pool: &PgPool,
    target: &Target,
    fixture: &RestoreFixture,
) -> Result<()> {
    let lifecycle = creates_retrieval_lifecycle_fixture(target).await?;
    delete_retrieval_revision(pool, target, &lifecycle).await?;
    retrieval_receipt_does_not_resurrect_deleted_history(target, &lifecycle).await?;

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
    let ledger_bytes = ledger.to_bytes()?;
    let repository = PostgresMemoryRepository::new(migration_pool.clone());

    ensure!(
        restore_episode_count(migration_pool, fixture).await? == 1,
        "restore corpus is missing before fence replay"
    );

    // A mismatched independent digest fails closed without touching rows.
    assert!(
        repository
            .replay_restore_fence_ledger(&ledger_bytes, &"0".repeat(64))
            .await
            .is_err(),
        "restore replay must reject a mismatched independent digest"
    );
    // An absent scope (fenced after the backup) is vacuously satisfied: the
    // restored copy predates the fence, so there is nothing to suppress. The
    // replay must succeed without touching rows (spec 016 A2).
    let unmatched_ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            format!("v1:{}", "0".repeat(64)),
            1,
            now - TimeDuration::minutes(1),
            now + TimeDuration::hours(1),
        )?],
    )?;
    let unmatched_bytes = unmatched_ledger.to_bytes()?;
    let unmatched_report = repository
        .replay_restore_fence_ledger(&unmatched_bytes, &unmatched_ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay absent scope failed: {error}"))?;
    assert_eq!(
        unmatched_report.scopes_purged, 0,
        "restore replay must treat an absent scope as vacuous"
    );
    ensure!(
        restore_episode_count(migration_pool, fixture).await? == 1,
        "vacuous restore replay mutated the corpus"
    );
    // A ledger produced by a rotated scope key cannot be re-derived in the
    // restored copy. The replay must fail closed without touching rows.
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
    ensure!(
        restore_episode_count(migration_pool, fixture).await? == 1,
        "failed restore replay mutated the corpus"
    );

    // The verified ledger purges the scope and tombstones the subject.
    let report = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay fixture failed: {error}"))?;
    assert_eq!(report.scopes_found, 1);
    assert_eq!(report.scopes_purged, 1);
    assert_eq!(report.residual_rows, 0);
    assert_eq!(report.ledger_sha256, ledger.ledger_sha256);
    ensure!(
        restore_episode_count(migration_pool, fixture).await? == 0,
        "restore replay left scoped episodes"
    );
    let state: String = sqlx::query_scalar(
        "SELECT lifecycle_state FROM memory.subject_lifecycles WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(
        state, "deleted",
        "restore replay must tombstone the lifecycle"
    );

    // Replay is idempotent and reports the same outcome.
    let replayed = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay idempotency failed: {error}"))?;
    assert_eq!(replayed.scopes_found, report.scopes_found);
    assert_eq!(replayed.scopes_purged, report.scopes_purged);
    assert_eq!(replayed.residual_rows, report.residual_rows);
    Ok(())
}

async fn restore_episode_count(pool: &PgPool, fixture: &RestoreFixture) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.subject_id)
    .fetch_one(pool)
    .await?)
}

/// A3. No listener by default: `open`/`connect` never bind a socket.
async fn verify_embedded_no_listener_default(embedded: &EmbeddedMemory) -> Result<()> {
    ensure!(
        embedded.loopback_addr().is_none(),
        "embedded connect must not open a listener by default (spec 014 A3)"
    );
    Ok(())
}

/// A4. Contract parity: the embedded loopback serves the canonical HTTP
/// surface — the governed episode route returns the canonical JSON envelope
/// with no framework-specific leakage.
async fn verify_embedded_contract_parity(target: &Target, fixture: &RestoreFixture) -> Result<()> {
    let response = Client::new()
        .get(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes/{}",
            target.base_url, fixture.tenant_id, fixture.subject_id, fixture.episode_id
        ))
        .bearer_auth("restore-conformance-token")
        .send()
        .await?;
    let status = response.status();
    ensure!(
        status == StatusCode::OK,
        "embedded surface did not serve the canonical episode: {status}"
    );
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    ensure!(
        content_type.starts_with("application/json"),
        "canonical envelope must be JSON, found {content_type}"
    );
    let body = response.text().await?;
    let envelope: Value =
        serde_json::from_str(&body).context("canonical envelope is not parseable JSON")?;
    ensure!(
        envelope.get("payload").is_some(),
        "canonical envelope omitted the payload field"
    );
    for required in [
        fixture.episode_id.to_string().as_str(),
        "restore",
        "private",
    ] {
        ensure!(
            body.contains(required),
            "canonical record omitted {required}"
        );
    }
    Ok(())
}

/// A5. Tenant isolation: canonical content tables are RLS-guarded, and the
/// canonical cross-tenant read fails closed over the embedded surface.
async fn verify_embedded_tenant_isolation(migration_pool: &PgPool, target: &Target) -> Result<()> {
    let unguarded: Vec<String> = sqlx::query(
        r#"
        SELECT relname
        FROM pg_class
        WHERE relnamespace = 'memory'::regnamespace
          AND NOT relrowsecurity
        ORDER BY relname
        "#,
    )
    .fetch_all(migration_pool)
    .await?
    .into_iter()
    .map(|row| row.try_get(0))
    .collect::<Result<Vec<_>, _>>()?;
    for table in ["episodes", "facts", "fact_revisions", "retrieval_receipts"] {
        ensure!(
            !unguarded.contains(&table.to_owned()),
            "content table {table} is not RLS-guarded"
        );
    }
    cross_scope_reads_fail_closed(target).await?;
    Ok(())
}

/// A6. Index reproducibility: derived projections rebuild from canonical
/// records — missing and corrupt projections fail closed, and the rebuild
/// endpoints restore identical receipts over the embedded surface.
async fn verify_embedded_index_reproducible(pool: &PgPool, target: &Target) -> Result<()> {
    let isolation = retrieval_candidates_are_authorized_before_ranking(target).await?;
    let allowed_revision_id = isolation.allowed_revision_id;

    delete_retrieval_projection(pool, target, allowed_revision_id).await?;
    retrieval_fails_closed_when_projection_is_missing(target).await?;
    rebuild_retrieval_projection(pool, target, allowed_revision_id).await?;
    retrieval_recovers_after_projection_rebuild(target, allowed_revision_id).await?;

    corrupt_retrieval_projection_digest(pool, target, allowed_revision_id).await?;
    retrieval_fails_closed_when_projection_is_corrupt(target, "retrieval-projection-digest-retry")
        .await?;
    rebuild_retrieval_projection(pool, target, allowed_revision_id).await?;
    retrieval_succeeds_after_projection_rebuild(
        target,
        allowed_revision_id,
        "retrieval-projection-digest-retry",
    )
    .await?;

    corrupt_retrieval_search_vector(pool, target, allowed_revision_id).await?;
    retrieval_fails_closed_when_projection_is_corrupt(target, "retrieval-projection-vector-retry")
        .await?;
    rebuild_retrieval_projection(pool, target, allowed_revision_id).await?;
    retrieval_succeeds_after_projection_rebuild(
        target,
        allowed_revision_id,
        "retrieval-projection-vector-retry",
    )
    .await?;

    ensure!(
        !isolation.forbidden_revision_ids.is_empty(),
        "isolation fixture carries no forbidden revisions"
    );
    Ok(())
}

/// A7. Surface policy: a registered surface policy returns the same bounded,
/// explained bundle over the embedded surface as the HTTP seam (spec 012).
async fn verify_embedded_surface_policy(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000702")?;
    let host_id = "hermes-desktop-caps";
    register_surface_policy(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        json!({
            "max_items": 2,
            "max_context_tokens": 256,
            "max_result_tokens": 4096,
        }),
    )
    .await?;
    for (index, (key, message)) in [
        ("cap_mercury", "Mercury quasar flyby confirmed."),
        ("cap_venus", "Venus quasar sample collected."),
        ("cap_mars", "Mars quasar baseline approved."),
    ]
    .iter()
    .enumerate()
    {
        append_surface_fact(
            &client,
            target,
            case_id,
            "case.profile",
            key,
            message,
            "internal",
            &format!("2026-01-10T09:{:02}:00Z", index),
            "2026-01-10T00:00:00Z",
            None,
            &format!("surface-a2-{key}"),
        )
        .await?;
    }
    let (status, bundle) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a2",
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {status}, expected 201"
    );
    let items = bundle["items"].as_array().context("bundle items missing")?;
    ensure!(
        items.len() == 2,
        "max_items cap violated: {} items surfaced",
        items.len()
    );
    ensure!(
        bundle["truncated"].as_bool() == Some(true),
        "surface did not report truncation"
    );
    for item in items {
        let lexical_score = item["lexical_score"]
            .as_f64()
            .context("item lexical_score missing")?;
        ensure!(
            lexical_score > 0.0,
            "item has no ranking explanation: {lexical_score}"
        );
        let item_sha256 = item["item_sha256"]
            .as_str()
            .context("item receipt missing")?;
        ensure!(
            item_sha256.len() == 64,
            "item receipt is not a sha256 digest"
        );
    }
    Ok(())
}

const SURFACE_PRINCIPAL_ID: &str = "principal-a-surface";
const RETENTION_POLICY_ID: &str = "standard";

fn surface_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/subjects/{}/surfaces",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    )
}

fn surface_policies_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/surface-policies",
        target.base_url.trim_end_matches('/'),
        target.tenant_id
    )
}

async fn register_surface_policy(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    body: Value,
) -> Result<Value> {
    let mut policy_body = body;
    let policy_object = policy_body
        .as_object_mut()
        .context("surface policy body must be a JSON object")?;
    policy_object.insert("host_id".to_owned(), json!(host_id));
    policy_object.insert("principal_id".to_owned(), json!(principal_id));
    let response = client
        .post(surface_policies_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&policy_body)
        .send()
        .await
        .context("register surface policy request failed")?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "register surface policy returned {}, expected 201",
        response.status()
    );
    response.json().await.context("policy view missing")
}

async fn post_surface(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    context_terms: &[&str],
    idempotency_key: &str,
) -> Result<(StatusCode, Value)> {
    let (status, body) = post_surface_unchecked(
        client,
        target,
        host_id,
        principal_id,
        context_terms,
        idempotency_key,
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {status}, expected 201: {body}"
    );
    Ok((status, body))
}

async fn post_surface_unchecked(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    context_terms: &[&str],
    idempotency_key: &str,
) -> Result<(StatusCode, Value)> {
    let response = client
        .post(surface_url(target))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "host_id": host_id,
            "principal_id": principal_id,
            "context_terms": context_terms,
        }))
        .send()
        .await
        .context("surface request failed")?;
    let status = response.status();
    let body = response
        .json()
        .await
        .context("surface response was not json")?;
    Ok((status, body))
}

#[allow(clippy::too_many_arguments)]
async fn append_surface_fact(
    client: &Client,
    target: &Target,
    case_id: Uuid,
    namespace: &str,
    key: &str,
    message: &str,
    sensitivity: &str,
    observed_at: &str,
    valid_from: &str,
    valid_until: Option<&str>,
    idempotency_key: &str,
) -> Result<()> {
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let episode_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{idempotency_key}-episode"))
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": observed_at,
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{idempotency_key}-episode"),
            },
            "sensitivity": sensitivity,
            "retention_policy_id": RETENTION_POLICY_ID,
            "payload": {"message": message},
        }))
        .send()
        .await
        .context("surface fact episode request failed")?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "surface fact episode returned {}, expected 201",
        episode_response.status()
    );
    let episode: Value = episode_response.json().await?;
    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let fact_response = client
        .post(&facts_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{idempotency_key}-fact"))
        .json(&json!({
            "case_id": case_id,
            "namespace": namespace,
            "key": key,
            "value": {"message": message},
            "observed_at": observed_at,
            "valid_time": {"from": valid_from, "until": valid_until},
            "evidence_episode_ids": [episode["episode_id"]],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 0.95,
            "sensitivity": sensitivity,
            "retention_policy_id": RETENTION_POLICY_ID,
        }))
        .send()
        .await
        .context("surface fact request failed")?;
    ensure!(
        fact_response.status() == StatusCode::CREATED,
        "surface fact returned {}, expected 201",
        fact_response.status()
    );
    Ok(())
}

/// Seeds the restore fence corpus: an active lifecycle and one canonical
/// episode for a dedicated restore scope (spec 014 A2).
async fn seed_restore_fence_fixture(migration_pool: &PgPool) -> Result<RestoreFixture> {
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000310")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000311")?;
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000312")?;
    let episode_id = Uuid::parse_str("019be000-0000-7000-8000-000000000313")?;
    let payload = restore_episode_payload();

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

/// Pins the canonical lexical-only ranking policy and its digest (mirrors
/// the server facade's hybrid_setup helper).
async fn verify_lexical_retrieval_policy(pool: &PgPool) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT policy_document, policy_sha256,
            encode(
                sha256(convert_to(policy_document::text, 'UTF8')),
                'hex'
            ) AS calculated_sha256
        FROM memory.retrieval_policies
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

/// Transitions a fact revision through `deletion_pending` to `deleted`
/// (mirrors the server facade's deletion_ops helper).
async fn transition_revision_to_deleted(
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

/// Deletes the lifecycle fixture's target revision under the retrieval test
/// scope (mirrors the server facade's deletion_ops helper).
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
    transition_revision_to_deleted(&mut transaction, target, fixture.deleted_revision_id).await?;
    transaction.commit().await?;
    Ok(())
}

/// Deletes the retrieval search document for a revision (index reproducibility).
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

/// Corrupts the projection digest for a revision (index reproducibility).
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

/// Corrupts the search vector for a revision (index reproducibility).
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

/// Rebuilds the retrieval search document from the canonical revision
/// (index reproducibility).
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
