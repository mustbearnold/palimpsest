//! review_queue — review-queue worker conformance (spec 017 P3, AC6).
//!
//! AC6 — a worker job scans the canonical fact metadata for pages whose
//! latest revision predates the staleness window, flags them in an advisory
//! surface, and leaves the canonical layer unchanged.

use anyhow::{Context, Result, ensure};
use palimpsest_application::{
    MemoryService, NewReviewQueueJob, NewSurfacePolicy, REVIEW_QUEUE_HOST_ID,
    REVIEW_QUEUE_PRINCIPAL_ID, REVIEW_QUEUE_STALE_AFTER_DAYS,
};
use palimpsest_domain::{
    AppendEpisode, CaseId, CreateFact, EpisodeId, EpisodeKind, FactId, FactKey, FactNamespace,
    PrincipalScope, Provenance, RetentionPolicyId, Sensitivity, SourceType, SubjectId, TenantId,
    ValidTime, WritePolicy, WritePolicyId, WritePolicyVersion,
};
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

use super::harness;

struct ReviewQueueHarness {
    service: MemoryService,
    principal: PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
}

async fn harness(
    pool: &PgPool,
    migration_pool: &PgPool,
    tenant_id: TenantId,
    subject_id: SubjectId,
    principal_name: &str,
) -> Result<ReviewQueueHarness> {
    harness::seed_active_lifecycle(migration_pool, tenant_id, subject_id).await?;
    let principal = harness::principal(
        principal_name,
        tenant_id,
        subject_id,
        &[],
        &[Sensitivity::try_from("internal".to_owned())?],
    );
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = harness::service(&repository)
        .with_surface_components(repository.clone())
        .with_review_queue(repository);
    let harness = ReviewQueueHarness {
        service,
        principal,
        tenant_id,
        subject_id,
    };
    harness
        .service
        .register_surface_policy(
            &harness.principal,
            harness.tenant_id,
            NewSurfacePolicy {
                host_id: REVIEW_QUEUE_HOST_ID.to_owned(),
                principal_id: REVIEW_QUEUE_PRINCIPAL_ID.to_owned(),
                enabled: true,
                max_items: 16,
                max_context_tokens: 512,
                max_result_tokens: 1024,
                sensitivity_ceiling: Some("internal".to_owned()),
                window_from: None,
                window_until: None,
                created_by_principal_id: harness.principal.principal_id.clone(),
            },
        )
        .await
        .context("register the review-queue surface policy")?;
    Ok(harness)
}

fn registered_policy() -> Result<WritePolicy> {
    Ok(WritePolicy {
        id: WritePolicyId::try_from("direct-evidence".to_owned())?,
        version: WritePolicyVersion::try_from("1".to_owned())?,
    })
}

fn observed_at() -> Result<OffsetDateTime> {
    Ok(OffsetDateTime::now_utc() - time::Duration::minutes(1))
}

/// Seed an episode and a fact, returning (episode_id, fact_id).
async fn seed_episode_and_fact(
    harness: &ReviewQueueHarness,
    episode_idem: &str,
    fact_idem: &str,
    key: &str,
    value: serde_json::Value,
) -> Result<(EpisodeId, FactId)> {
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for the review-queue corpus")?;
    let episode = harness
        .service
        .append_episode(
            &lease,
            &harness.principal,
            episode_idem.to_owned(),
            AppendEpisode {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x702)),
                kind: EpisodeKind::try_from("message".to_owned())?,
                observed_at: observed_at()?,
                provenance: Provenance {
                    source_type: SourceType::try_from("vault-conformance".to_owned())?,
                    source_uri: Some("urn:vault:review-queue".to_owned()),
                    external_id: Some(episode_idem.to_owned()),
                },
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
                payload: serde_json::json!({"marker": episode_idem}),
            },
        )
        .await
        .context("append review-queue corpus episode")?
        .episode
        .episode_id;
    let created = harness
        .service
        .create_fact(
            &lease,
            &harness.principal,
            fact_idem.to_owned(),
            CreateFact {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x702)),
                namespace: FactNamespace::try_from("wiki".to_owned())?,
                key: FactKey::try_from(key.to_owned())?,
                value,
                observed_at: observed_at()?,
                valid_time: ValidTime {
                    from: observed_at()?,
                    until: None,
                },
                evidence_episode_ids: vec![episode],
                write_policy: registered_policy()?,
                confidence: 0.9,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("create review-queue corpus fact")?;
    Ok((episode, created.view.fact_id))
}

/// AC6 — the worker flags pages untouched for the staleness window in an
/// advisory surface, keeps fresh pages out, and never writes the canonical
/// layer.
pub(crate) async fn review_queue_flags_stale_pages_in_an_advisory_surface(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::now_v7());
    let subject_id = SubjectId(Uuid::now_v7());
    let harness = harness(
        pool,
        migration_pool,
        tenant_id,
        subject_id,
        "review-queue-tenant-admin",
    )
    .await?;
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for review-queue reads")?;

    let (_, stale_fact_id) = seed_episode_and_fact(
        &harness,
        "review-queue-episode-stale",
        "review-queue-fact-stale",
        "stale-page",
        serde_json::json!({"title": "Stale page", "body": "untouched for months"}),
    )
    .await?;
    let (_, _fresh_fact_id) = seed_episode_and_fact(
        &harness,
        "review-queue-episode-fresh",
        "review-queue-fact-fresh",
        "fresh-page",
        serde_json::json!({"title": "Fresh page", "body": "touched today"}),
    )
    .await?;

    // Backdate the stale page's revision so its latest canonical revision
    // predates the staleness window. The append-only guard is disabled
    // transiently (test-only, same transaction) and re-enabled afterwards.
    let mut backdate_tx = migration_pool.begin().await.context("begin backdate")?;
    sqlx::query("ALTER TABLE memory.fact_revisions DISABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("disable the append-only guard")?;
    let _backdated = sqlx::query(
        r#"
        UPDATE memory.fact_revisions
        SET recorded_at = now() - ($2 * interval '1 day')
        WHERE fact_id = $1
        "#,
    )
    .bind(stale_fact_id.0)
    .bind(REVIEW_QUEUE_STALE_AFTER_DAYS + 1)
    .execute(&mut *backdate_tx)
    .await
    .context("backdate the stale page revision")?;
    sqlx::query("ALTER TABLE memory.fact_revisions ENABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("re-enable the append-only guard")?;
    backdate_tx.commit().await.context("commit backdate")?;
    let backdated_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory.fact_revisions WHERE fact_id = $1 AND recorded_at < now() - interval '29 days'",
    )
    .bind(stale_fact_id.0)
    .fetch_one(migration_pool)
    .await
    .context("verify the backdated revision")?;
    ensure!(
        backdated_count == 1,
        "the stale page must have exactly one backdated revision"
    );

    let stale_head_before = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            stale_fact_id,
        )
        .await
        .context("read the stale page before the worker runs")?
        .revision
        .context("the stale page must exist")?
        .revision_id;

    let job = harness
        .service
        .create_review_queue_job(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            NewReviewQueueJob {
                principal_id: harness.principal.principal_id.clone(),
            },
            "review-queue-1".to_owned(),
        )
        .await
        .context("create a review-queue job")?;
    ensure!(
        !job.replayed,
        "a fresh review-queue job must not be an idempotency replay"
    );

    let summary = harness
        .service
        .run_review_queue_worker_once()
        .await
        .context("run the review-queue worker once")?;
    ensure!(
        summary.lifecycle_state == "complete",
        "the worker must complete the job, got {:?}",
        summary.lifecycle_state
    );
    ensure!(
        summary.stale_pages == 1,
        "exactly one stale page must be flagged, got {}",
        summary.stale_pages
    );
    let surface_id = summary
        .surface_id
        .context("a non-empty review queue must record an advisory surface")?;

    let bundle = harness
        .service
        .get_surface(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            surface_id,
        )
        .await
        .context("read the advisory surface")?;
    let flagged: Vec<&String> = bundle.items.iter().map(|item| &item.fact_key).collect();
    ensure!(
        flagged.contains(&&"stale-page".to_owned()),
        "the stale page must be flagged in the advisory surface, got {:?}",
        flagged
    );
    ensure!(
        !flagged.contains(&&"fresh-page".to_owned()),
        "the fresh page must not be flagged, got {:?}",
        flagged
    );

    // The canonical layer is untouched: the stale page's head revision is
    // unchanged after the worker pass.
    let stale_head_after = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            stale_fact_id,
        )
        .await
        .context("read the stale page after the worker runs")?
        .revision
        .context("the stale page must still exist")?
        .revision_id;
    ensure!(
        stale_head_after == stale_head_before,
        "the worker must not rewrite the canonical layer"
    );

    // A second pass has nothing to claim.
    let idle = harness
        .service
        .run_review_queue_worker_once()
        .await
        .context("run the review-queue worker a second time")?;
    ensure!(
        idle.job_id == Uuid::nil(),
        "a second pass with no pending jobs must be idle, got {}",
        idle.job_id
    );

    Ok(())
}
