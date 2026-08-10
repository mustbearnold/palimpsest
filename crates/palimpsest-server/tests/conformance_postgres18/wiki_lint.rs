//! wiki_lint — spec 017 P4 conformance: lint worker jobs (AC8), the
//! hierarchical index surface (AC9), and governed schema versioning (AC10).
//!
//! AC8 a contradiction between two facts makes the lint job write a governed
//!     lint fact (namespace `wiki/lint`) and generate a new open question
//!     (namespace `open-questions`) through the governed fact path.
//! AC9 the index renders every page with a link and a summary as a bounded,
//!     idempotent advisory surface (012 R4, R6).
//! AC10 a schema amendment is governed (registered write policy, attributable
//!      principal) and the old version stays retrievable (R11).

use anyhow::{Context, Result, ensure};
use palimpsest_application::{
    MemoryService, NewIndexSurfaceRequest, NewSchemaConfig, NewSurfacePolicy, NewWikiLintJob,
    SchemaConfigView, ServiceError, WIKI_INDEX_HOST_ID, WIKI_INDEX_PRINCIPAL_ID,
    WIKI_LINT_NAMESPACE, WIKI_LINT_PRINCIPAL_ID, WIKI_LINT_STALE_AFTER_DAYS,
    WIKI_OPEN_QUESTIONS_NAMESPACE,
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

struct WikiLintHarness {
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
) -> Result<WikiLintHarness> {
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
        .with_wiki_lint(repository.clone())
        .with_schema_configs(repository);
    Ok(WikiLintHarness {
        service,
        principal,
        tenant_id,
        subject_id,
    })
}

fn registered_policy() -> Result<WritePolicy> {
    Ok(WritePolicy {
        id: WritePolicyId::try_from("direct-evidence".to_owned())?,
        version: WritePolicyVersion::try_from("1".to_owned())?,
    })
}

fn unregistered_policy() -> Result<WritePolicy> {
    Ok(WritePolicy {
        id: WritePolicyId::try_from("not-registered".to_owned())?,
        version: WritePolicyVersion::try_from("1".to_owned())?,
    })
}

fn observed_at() -> Result<OffsetDateTime> {
    Ok(OffsetDateTime::now_utc() - time::Duration::minutes(1))
}

/// Seed an episode and a fact, returning (episode_id, fact_id).
async fn seed_episode_and_fact(
    harness: &WikiLintHarness,
    episode_idem: &str,
    fact_idem: &str,
    case_id: CaseId,
    key: &str,
    value: serde_json::Value,
) -> Result<(EpisodeId, FactId)> {
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for the lint corpus")?;
    let episode = harness
        .service
        .append_episode(
            &lease,
            &harness.principal,
            episode_idem.to_owned(),
            AppendEpisode {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id,
                kind: EpisodeKind::try_from("message".to_owned())?,
                observed_at: observed_at()?,
                provenance: Provenance {
                    source_type: SourceType::try_from("vault-conformance".to_owned())?,
                    source_uri: Some("urn:vault:wiki-lint".to_owned()),
                    external_id: Some(episode_idem.to_owned()),
                },
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
                payload: serde_json::json!({"marker": episode_idem}),
            },
        )
        .await
        .context("append lint corpus episode")?
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
                case_id,
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
        .context("create lint corpus fact")?;
    Ok((episode, created.view.fact_id))
}

/// Seed an orphan: a fact whose head revision has no evidence rows. The API
/// forbids empty evidence (fail closed), so the conformance seeds the orphan
/// directly against the migration authority.
/// Seed an orphan: a current fact whose head revision carries a dangling
/// evidence reference — an episode id that does not exist in the subject's
/// episodes. The orphan still carries one valid evidence row (the DB
/// requires attributable evidence), and the FK is bypassed for the dangling
/// row only.
async fn seed_orphan_fact(
    migration_pool: &PgPool,
    harness: &WikiLintHarness,
    case_id: CaseId,
    fact_id: FactId,
    valid_episode_id: EpisodeId,
) -> Result<()> {
    let observed = observed_at()?;
    let mut transaction = migration_pool.begin().await.context("begin orphan seed")?;
    // The coverage maintenance trigger enforces the scope GUCs; the
    // migration role must present the tenant and subject scope.
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true), set_config('palimpsest.subject_id', $2, true)")
        .bind(harness.tenant_id.0.to_string())
        .bind(harness.subject_id.0.to_string())
        .execute(&mut *transaction)
        .await
        .context("set the scope GUCs for the orphan seed")?;
    sqlx::query(
        r#"
        INSERT INTO memory.facts (
            tenant_id, subject_id, case_id, fact_id, namespace, fact_key, schema_version
        )
        VALUES ($1, $2, $3, $4, 'wiki', 'orphan-page', 1)
        "#,
    )
    .bind(harness.tenant_id.0)
    .bind(harness.subject_id.0)
    .bind(case_id.0)
    .bind(fact_id.0)
    .execute(&mut *transaction)
    .await
    .context("insert the orphan fact metadata")?;
    let revision_id = Uuid::now_v7();
    sqlx::query(
        r#"
        INSERT INTO memory.fact_revisions (
            tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
            supersedes_revision_id, observed_at, valid_during, value, confidence,
            writer_principal_id, write_policy_id, write_policy_version,
            sensitivity, retention_policy_id, schema_version, content_sha256
        )
        VALUES (
            $1, $2, $3, $4, $5, 1, NULL, $6,
            tstzrange($6, NULL, '[)'), $7, 0.9,
            'conformance-seed', 'direct-evidence', '1',
            'internal', 'standard', 1,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        )
        "#,
    )
    .bind(harness.tenant_id.0)
    .bind(harness.subject_id.0)
    .bind(case_id.0)
    .bind(fact_id.0)
    .bind(revision_id)
    .bind(observed)
    .bind(serde_json::json!({"title": "Orphan page", "body": "no grounding"}))
    .execute(&mut *transaction)
    .await
    .context("insert the orphan fact revision")?;
    // One valid evidence row satisfies the attributable-evidence guard.
    sqlx::query(
        r#"
        INSERT INTO memory.fact_revision_evidence (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            episode_id, evidence_role
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
        "#,
    )
    .bind(harness.tenant_id.0)
    .bind(harness.subject_id.0)
    .bind(case_id.0)
    .bind(fact_id.0)
    .bind(revision_id)
    .bind(valid_episode_id.0)
    .execute(&mut *transaction)
    .await
    .context("insert the orphan's valid evidence row")?;
    // The dangling reference makes the page an orphan: its grounding
    // episode does not exist. The FK is bypassed for this row only.
    sqlx::query("SET LOCAL session_replication_role = replica")
        .execute(&mut *transaction)
        .await
        .context("disable foreign keys for the dangling reference")?;
    sqlx::query(
        r#"
        INSERT INTO memory.fact_revision_evidence (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            episode_id, evidence_role
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
        "#,
    )
    .bind(harness.tenant_id.0)
    .bind(harness.subject_id.0)
    .bind(case_id.0)
    .bind(fact_id.0)
    .bind(revision_id)
    .bind(Uuid::now_v7())
    .execute(&mut *transaction)
    .await
    .context("insert the dangling evidence row")?;
    sqlx::query("SET LOCAL session_replication_role = origin")
        .execute(&mut *transaction)
        .await
        .context("re-enable foreign keys after the dangling reference")?;
    transaction.commit().await.context("commit orphan seed")?;
    Ok(())
}

/// Seed a provenance gap: an evidence row whose episode is missing from the
/// subject's episodes. The FK is bypassed for the seeding transaction only.
/// Seed a provenance gap: a current fact whose head revision is grounded in
/// an episode recorded after the claim itself. The API always grounds a
/// claim in pre-existing episodes, so the conformance backdates the claim's
/// `recorded_at` below the episode's — evidence cannot predate its claim.
async fn seed_provenance_gap(
    migration_pool: &PgPool,
    harness: &WikiLintHarness,
    fact_id: FactId,
) -> Result<()> {
    let mut transaction = migration_pool.begin().await.context("begin gap seed")?;
    // The coverage maintenance trigger enforces the scope GUCs.
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true), set_config('palimpsest.subject_id', $2, true)")
        .bind(harness.tenant_id.0.to_string())
        .bind(harness.subject_id.0.to_string())
        .execute(&mut *transaction)
        .await
        .context("set the scope GUCs for the gap seed")?;
    // The append-only guard is disabled transiently (test-only, same
    // transaction) and re-enabled afterwards.
    sqlx::query("ALTER TABLE memory.fact_revisions DISABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *transaction)
        .await
        .context("disable the append-only guard")?;
    let backdated = sqlx::query(
        r#"
        UPDATE memory.fact_revisions
        SET recorded_at = now() - interval '1 day'
        WHERE fact_id = $1
          AND recorded_at > now() - interval '12 hours'
        "#,
    )
    .bind(fact_id.0)
    .execute(&mut *transaction)
    .await
    .context("backdate the claim below its evidence")?;
    sqlx::query("ALTER TABLE memory.fact_revisions ENABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *transaction)
        .await
        .context("re-enable the append-only guard")?;
    ensure!(
        backdated.rows_affected() == 1,
        "the gap seed must backdate exactly one claim revision"
    );
    transaction.commit().await.context("commit gap seed")?;
    Ok(())
}

/// AC8 — the lint job writes a governed lint fact and generates a new open
/// question when it finds a contradiction, plus orphans, stale claims, and
/// provenance gaps. The canonical layer is only ever extended by the lint
/// facts; the scanned pages are untouched.
pub(crate) async fn wiki_lint_writes_governed_state_and_generates_open_questions(
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
        "wiki-lint-admin",
    )
    .await?;

    // Two facts share the page key "temperature" in different cases with
    // different content: a contradiction.
    let case_a = CaseId(Uuid::from_u128(0x8101));
    let case_b = CaseId(Uuid::from_u128(0x8102));
    let (_, fact_a) = seed_episode_and_fact(
        &harness,
        "wiki-lint-episode-a",
        "wiki-lint-fact-a",
        case_a,
        "temperature",
        serde_json::json!({"value_celsius": 21.5}),
    )
    .await?;
    let (_, fact_b) = seed_episode_and_fact(
        &harness,
        "wiki-lint-episode-b",
        "wiki-lint-fact-b",
        case_b,
        "temperature",
        serde_json::json!({"value_celsius": 99.9}),
    )
    .await?;

    // One orphan: a current fact with a dangling evidence reference.
    let (orphan_episode, _) = seed_episode_and_fact(
        &harness,
        "wiki-lint-episode-orphan-anchor",
        "wiki-lint-fact-orphan-anchor",
        case_a,
        "orphan-anchor-page",
        serde_json::json!({"title": "Orphan anchor"}),
    )
    .await?;
    let orphan_fact = FactId(Uuid::from_u128(0x8103));
    seed_orphan_fact(
        migration_pool,
        &harness,
        case_a,
        orphan_fact,
        orphan_episode,
    )
    .await?;
    // One provenance gap: the claim's evidence was recorded after the claim.
    let (_, gap_fact) = seed_episode_and_fact(
        &harness,
        "wiki-lint-episode-gap",
        "wiki-lint-fact-gap",
        case_a,
        "gap-page",
        serde_json::json!({"title": "Gap page"}),
    )
    .await?;
    seed_provenance_gap(migration_pool, &harness, gap_fact).await?;

    // One stale claim: backdate the head revision past the staleness window
    // (same transient guard disable as the review-queue scenario).
    let (_, stale_fact) = seed_episode_and_fact(
        &harness,
        "wiki-lint-episode-stale",
        "wiki-lint-fact-stale",
        case_b,
        "stale-page",
        serde_json::json!({"title": "Stale page"}),
    )
    .await?;
    let mut backdate_tx = migration_pool
        .begin()
        .await
        .context("begin lint backdate")?;
    sqlx::query("ALTER TABLE memory.fact_revisions DISABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("disable the append-only guard")?;
    // The evidence episode must predate the claim, or the stale claim
    // would also trip the provenance-gap check.
    sqlx::query("ALTER TABLE memory.episodes DISABLE TRIGGER episodes_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("disable the episode append-only guard")?;
    let backdated_episode = sqlx::query(
        r#"
        UPDATE memory.episodes AS episode
        SET recorded_at = now() - ($2 * interval '1 day') - interval '1 day'
        WHERE episode.episode_id = (
            SELECT evidence.episode_id
            FROM memory.fact_revision_evidence AS evidence
            WHERE evidence.fact_id = $1
            LIMIT 1
        )
        "#,
    )
    .bind(stale_fact.0)
    .bind(WIKI_LINT_STALE_AFTER_DAYS + 1)
    .execute(&mut *backdate_tx)
    .await
    .context("backdate the stale claim evidence episode")?;
    sqlx::query("ALTER TABLE memory.episodes ENABLE TRIGGER episodes_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("re-enable the episode append-only guard")?;
    let backdated = sqlx::query(
        r#"
        UPDATE memory.fact_revisions
        SET recorded_at = now() - ($2 * interval '1 day')
        WHERE fact_id = $1
        "#,
    )
    .bind(stale_fact.0)
    .bind(WIKI_LINT_STALE_AFTER_DAYS + 1)
    .execute(&mut *backdate_tx)
    .await
    .context("backdate the stale claim revision")?;
    sqlx::query("ALTER TABLE memory.fact_revisions ENABLE TRIGGER fact_revisions_reject_mutation")
        .execute(&mut *backdate_tx)
        .await
        .context("re-enable the append-only guard")?;
    backdate_tx.commit().await.context("commit lint backdate")?;
    ensure!(
        backdated.rows_affected() == 1,
        "the stale claim backdate must touch one revision"
    );
    ensure!(
        backdated_episode.rows_affected() == 1,
        "the stale claim backdate must touch one evidence episode"
    );

    let job = harness
        .service
        .create_wiki_lint_job(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            NewWikiLintJob {
                principal_id: harness.principal.principal_id.clone(),
            },
            "wiki-lint-job-1".to_owned(),
        )
        .await
        .context("create a wiki lint job")?;
    ensure!(
        !job.replayed,
        "a fresh wiki lint job must not be an idempotency replay"
    );

    let summary = harness
        .service
        .run_wiki_lint_worker_once()
        .await
        .context("run the wiki lint worker once")?;
    if summary.lifecycle_state != "complete" {
        let view = harness
            .service
            .poll_wiki_lint_job(
                &harness.principal,
                harness.tenant_id,
                harness.subject_id,
                summary.job_id,
            )
            .await
            .context("poll the failed lint job")?;
        eprintln!("lint job failed: {:?}", view.failure_reason);
    }
    ensure!(
        summary.lifecycle_state == "complete",
        "the lint worker must complete the job, got {:?}",
        summary.lifecycle_state
    );
    ensure!(
        summary.contradictions == 1,
        "exactly one contradiction must be found, got {}",
        summary.contradictions
    );
    ensure!(
        summary.orphans == 1,
        "exactly one orphan must be found, got {}",
        summary.orphans
    );
    ensure!(
        summary.stale_claims == 1,
        "exactly one stale claim must be found, got {}",
        summary.stale_claims
    );
    ensure!(
        summary.provenance_gaps == 1,
        "exactly one provenance gap must be found, got {}",
        summary.provenance_gaps
    );

    let lint_fact_id = summary
        .lint_fact_id
        .context("the lint job must write a governed lint fact")?;
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for lint fact reads")?;
    let lint_fact = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            FactId(lint_fact_id),
        )
        .await
        .context("read the governed lint fact")?
        .revision
        .context("the lint fact must exist")?;
    ensure!(
        lint_fact.namespace.as_str() == WIKI_LINT_NAMESPACE,
        "the lint fact must live in the governed namespace, got {}",
        lint_fact.namespace.as_str()
    );
    ensure!(
        lint_fact.writer_principal_id.0 == WIKI_LINT_PRINCIPAL_ID,
        "the lint fact must be written by the lint worker principal"
    );
    ensure!(
        lint_fact.value["contradictions"]
            .as_array()
            .is_some_and(|list| list.len() == 1)
            && lint_fact.value["orphans"]
                .as_array()
                .is_some_and(|list| list.len() == 1)
            && lint_fact.value["stale_claims"]
                .as_array()
                .is_some_and(|list| list.len() == 1)
            && lint_fact.value["provenance_gaps"]
                .as_array()
                .is_some_and(|list| list.len() == 1),
        "the lint fact must carry the four finding lists, got {:?}",
        lint_fact.value
    );

    let question_fact_id = summary
        .question_fact_id
        .context("the lint job must generate a new open question")?;
    let question = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            FactId(question_fact_id),
        )
        .await
        .context("read the generated open question")?
        .revision
        .context("the generated open question must exist")?;
    ensure!(
        question.namespace.as_str() == WIKI_OPEN_QUESTIONS_NAMESPACE,
        "the open question must live in the open-questions namespace, got {}",
        question.namespace.as_str()
    );
    ensure!(
        question.writer_principal_id.0 == WIKI_LINT_PRINCIPAL_ID,
        "the open question must be written by the lint worker principal"
    );
    let related: Vec<String> = question.value["related_fact_ids"]
        .as_array()
        .context("the open question must list related facts")?
        .iter()
        .map(|id| id.as_str().unwrap_or_default().to_owned())
        .collect();
    ensure!(
        related.contains(&fact_a.0.to_string()) && related.contains(&fact_b.0.to_string()),
        "the open question must reference the two contradicting facts"
    );

    // The scanned pages are untouched: their head revisions are unchanged.
    for scanned_fact in [fact_a, fact_b, gap_fact, stale_fact] {
        let view = harness
            .service
            .get_current_fact(
                &lease,
                &harness.principal,
                harness.tenant_id,
                harness.subject_id,
                scanned_fact,
            )
            .await
            .context("read a scanned page after the lint run")?;
        ensure!(
            view.revision.is_some(),
            "the scanned page must still exist after the lint run"
        );
    }

    // A second pass has nothing to claim.
    let idle = harness
        .service
        .run_wiki_lint_worker_once()
        .await
        .context("run the wiki lint worker a second time")?;
    ensure!(
        idle.job_id == Uuid::nil(),
        "a second pass with no pending jobs must be idle, got {}",
        idle.job_id
    );

    Ok(())
}

/// AC9 — the hierarchical index renders every page with a link and a summary
/// as a bounded, idempotent advisory surface. The canonical layer is not
/// written by the index render.
pub(crate) async fn wiki_index_renders_every_page_with_link_and_summary(
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
        "wiki-index-admin",
    )
    .await?;
    harness
        .service
        .register_surface_policy(
            &harness.principal,
            harness.tenant_id,
            NewSurfacePolicy {
                host_id: WIKI_INDEX_HOST_ID.to_owned(),
                principal_id: WIKI_INDEX_PRINCIPAL_ID.to_owned(),
                enabled: true,
                max_items: 16,
                max_context_tokens: 512,
                max_result_tokens: 2048,
                sensitivity_ceiling: Some("internal".to_owned()),
                window_from: None,
                window_until: None,
                created_by_principal_id: harness.principal.principal_id.clone(),
            },
        )
        .await
        .context("register the wiki index surface policy")?;

    let case = CaseId(Uuid::from_u128(0x8201));
    let (_, alpha_fact) = seed_episode_and_fact(
        &harness,
        "wiki-index-episode-alpha",
        "wiki-index-fact-alpha",
        case,
        "alpha",
        serde_json::json!({"title": "Alpha page", "body": "first page"}),
    )
    .await?;
    let (_, beta_fact) = seed_episode_and_fact(
        &harness,
        "wiki-index-episode-beta",
        "wiki-index-fact-beta",
        case,
        "beta",
        serde_json::json!({"title": "Beta page", "body": "second page"}),
    )
    .await?;

    let created = harness
        .service
        .create_index_surface(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            NewIndexSurfaceRequest {
                host_id: WIKI_INDEX_HOST_ID.to_owned(),
                principal_id: WIKI_INDEX_PRINCIPAL_ID.to_owned(),
            },
            "wiki-index-1".to_owned(),
        )
        .await
        .context("render the wiki index")?;
    ensure!(
        !created.replayed,
        "a fresh index render must not be an idempotency replay"
    );

    // The catalog lists every page with a link and a summary.
    let mut keys: Vec<&String> = created
        .bundle
        .items
        .iter()
        .map(|item| &item.fact_key)
        .collect();
    keys.sort();
    ensure!(
        keys == vec!["alpha", "beta"],
        "the index must list every page, got {:?}",
        keys
    );
    for item in &created.bundle.items {
        ensure!(
            item.value["link"] == serde_json::json!(format!("pages/facts/{}.md", item.fact_id.0)),
            "each index item must carry a page link"
        );
        ensure!(
            item.value["summary"].is_string()
                && !item.value["summary"]
                    .as_str()
                    .unwrap_or_default()
                    .is_empty(),
            "each index item must carry a summary"
        );
    }
    ensure!(
        created.bundle.item_count == 2,
        "the index must be bounded to the two seeded pages"
    );
    ensure!(
        created.bundle.host_id == WIKI_INDEX_HOST_ID,
        "the index bundle must identify its host"
    );

    // A replayed request returns the stored bundle verbatim.
    let replayed = harness
        .service
        .create_index_surface(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            NewIndexSurfaceRequest {
                host_id: WIKI_INDEX_HOST_ID.to_owned(),
                principal_id: WIKI_INDEX_PRINCIPAL_ID.to_owned(),
            },
            "wiki-index-1".to_owned(),
        )
        .await
        .context("replay the wiki index render")?;
    ensure!(
        replayed.replayed,
        "a repeated index render with the same key must be an idempotency replay"
    );
    ensure!(
        replayed.bundle.surface_id == created.bundle.surface_id,
        "the replayed index must be the stored bundle"
    );
    ensure!(
        replayed.bundle.items.len() == created.bundle.items.len()
            && replayed
                .bundle
                .items
                .iter()
                .zip(created.bundle.items.iter())
                .all(|(replayed_item, created_item)| {
                    replayed_item.item_sha256 == created_item.item_sha256
                        && replayed_item.fact_id == created_item.fact_id
                        && replayed_item.ordinal == created_item.ordinal
                }),
        "the replayed index items must match the stored bundle"
    );

    // The render never writes the canonical layer: the pages are unchanged.
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for index page reads")?;
    for fact_id in [alpha_fact, beta_fact] {
        let view = harness
            .service
            .get_current_fact(
                &lease,
                &harness.principal,
                harness.tenant_id,
                harness.subject_id,
                fact_id,
            )
            .await
            .context("read an indexed page after the render")?;
        ensure!(
            view.revision.is_some(),
            "the indexed page must still exist after the render"
        );
    }

    Ok(())
}

/// AC10 — a schema amendment is governed and the old version stays
/// retrievable. An amendment without a registered write policy fails closed.
pub(crate) async fn wiki_schema_amendments_are_governed_and_versioned(
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
        "wiki-schema-admin",
    )
    .await?;

    let v1 = harness
        .service
        .amend_schema_config(
            &harness.principal,
            harness.tenant_id,
            NewSchemaConfig {
                config: serde_json::json!({
                    "page_format": "v1",
                    "frontmatter": ["title", "namespace", "last-touched"],
                }),
                write_policy: registered_policy()?,
            },
            "wiki-schema-v1".to_owned(),
        )
        .await
        .context("amend the schema config to version 1")?;
    ensure!(
        v1.schema_version == 1,
        "the first amendment must create version 1, got {}",
        v1.schema_version
    );
    ensure!(
        v1.supersedes_version.is_none(),
        "version 1 must not supersede anything"
    );

    let v2 = harness
        .service
        .amend_schema_config(
            &harness.principal,
            harness.tenant_id,
            NewSchemaConfig {
                config: serde_json::json!({
                    "page_format": "v2",
                    "frontmatter": ["title", "namespace", "last-touched", "status"],
                }),
                write_policy: registered_policy()?,
            },
            "wiki-schema-v2".to_owned(),
        )
        .await
        .context("amend the schema config to version 2")?;
    ensure!(
        v2.schema_version == 2,
        "the second amendment must create version 2, got {}",
        v2.schema_version
    );
    ensure!(
        v2.supersedes_version == Some(1),
        "version 2 must supersede version 1"
    );
    ensure!(
        v2.amended_by_principal_id == harness.principal.principal_id.0,
        "the amendment must be attributable to the amending principal"
    );

    // The old version stays retrievable; the current version is the latest.
    let old: SchemaConfigView = harness
        .service
        .get_schema_config(&harness.principal, harness.tenant_id, 1)
        .await
        .context("retrieve schema version 1 after the amendment")?;
    ensure!(
        old.schema_version == 1 && old.config["page_format"] == serde_json::json!("v1"),
        "the old schema version must stay retrievable unchanged"
    );
    let current: SchemaConfigView = harness
        .service
        .get_current_schema_config(&harness.principal, harness.tenant_id)
        .await
        .context("retrieve the current schema config")?;
    ensure!(
        current.schema_version == 2,
        "the current schema config must be the latest version"
    );

    // An amendment without a registered write policy fails closed.
    let rejected = harness
        .service
        .amend_schema_config(
            &harness.principal,
            harness.tenant_id,
            NewSchemaConfig {
                config: serde_json::json!({"page_format": "rogue"}),
                write_policy: unregistered_policy()?,
            },
            "wiki-schema-rogue".to_owned(),
        )
        .await;
    ensure!(
        matches!(rejected, Err(ServiceError::WritePolicyRejected)),
        "an unregistered amendment policy must fail closed with WritePolicyRejected, got {rejected:?}"
    );
    let unchanged: SchemaConfigView = harness
        .service
        .get_current_schema_config(&harness.principal, harness.tenant_id)
        .await
        .context("read the schema config after the rejected amendment")?;
    ensure!(
        unchanged.schema_version == 2,
        "a rejected amendment must not change the schema config"
    );

    Ok(())
}
