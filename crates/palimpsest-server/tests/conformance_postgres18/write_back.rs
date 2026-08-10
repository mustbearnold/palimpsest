//! write_back — spec 017 P2 conformance: attributed annotation/page-edit
//! write-back and filed answers (AC4, AC5).
//!
//! AC4 a registered write policy makes an annotation or page edit an
//!     attributable write (001 R9); a write without a registered policy
//!     fails closed (011 A4 pattern).
//! AC5 a filed answer is an agent write with attribution: the receipt records
//!     the filing agent as writer and provenance kind derived (011 R5).

use anyhow::{Context, Result, ensure};
use std::sync::Arc;

use palimpsest_application::{MemoryService, ServiceError};
use palimpsest_domain::{
    AppendEpisode, CaseId, CreateFact, EpisodeId, EpisodeKind, FactId, FactKey, FactNamespace,
    FileAnswer, PrincipalScope, Provenance, RetentionPolicyId, Sensitivity, SourceType, SubjectId,
    TenantId, ValidTime, WriteBackAnnotation, WriteBackPageEdit, WriteBackTarget, WritePolicy,
    WritePolicyId, WritePolicyVersion,
};
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

struct WriteBackHarness {
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
) -> Result<WriteBackHarness> {
    super::harness::seed_active_lifecycle(migration_pool, tenant_id, subject_id).await?;
    let principal = super::harness::principal(
        principal_name,
        tenant_id,
        subject_id,
        &[],
        &[Sensitivity::try_from("internal".to_owned())?],
    );
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = super::harness::service(&repository);
    Ok(WriteBackHarness {
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
    Ok(OffsetDateTime::parse(
        "2026-08-01T12:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )?)
}

async fn seed_episode_and_fact(harness: &WriteBackHarness) -> Result<(EpisodeId, FactId)> {
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for write-back corpus")?;
    let episode = harness
        .service
        .append_episode(
            &lease,
            &harness.principal,
            "write-back-episode".to_owned(),
            AppendEpisode {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x702)),
                kind: EpisodeKind::try_from("message".to_owned())?,
                observed_at: observed_at()?,
                provenance: Provenance {
                    source_type: SourceType::try_from("vault-conformance".to_owned())?,
                    source_uri: Some("urn:vault:write-back".to_owned()),
                    external_id: Some("vault-write-back-episode".to_owned()),
                },
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
                payload: serde_json::json!({"marker": "write-back-episode"}),
            },
        )
        .await
        .context("append write-back corpus episode")?
        .episode
        .episode_id;
    let created = harness
        .service
        .create_fact(
            &lease,
            &harness.principal,
            "write-back-fact".to_owned(),
            CreateFact {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x702)),
                namespace: FactNamespace::try_from("scratch".to_owned())?,
                key: FactKey::try_from("temperature".to_owned())?,
                value: serde_json::json!({"value_celsius": 21.5}),
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
        .context("create write-back corpus fact")?;
    Ok((episode, created.view.fact_id))
}

/// AC4 — attributed write-back with fail-closed policy gating.
pub(crate) async fn attributed_write_back_is_governed_and_fail_closed(
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
        "write-back-annotator",
    )
    .await?;
    let (episode, fact_id) = seed_episode_and_fact(&harness).await?;
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for annotation")?;

    // A registered write policy makes the annotation an attributable write.
    let annotation = harness
        .service
        .write_back_annotation(
            &lease,
            &harness.principal,
            "annotation-1".to_owned(),
            WriteBackAnnotation {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                target: WriteBackTarget::Fact { page_id: fact_id },
                body: "The reading looks plausible; follow up next week.".to_owned(),
                observed_at: observed_at()?,
                write_policy: registered_policy()?,
                confidence: 0.8,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("write-back annotation on fact page")?;
    let annotation_view = annotation
        .view
        .revision
        .context("annotation head revision")?;
    ensure!(
        annotation.view.namespace.as_str() == "wiki/annotations",
        "annotation must live in the wiki/annotations namespace, got {}",
        annotation.view.namespace.as_str()
    );
    ensure!(
        annotation_view.writer_principal_id == harness.principal.principal_id,
        "annotation must be attributable to the authenticated principal"
    );
    ensure!(
        annotation_view.evidence_episode_ids == vec![episode],
        "annotation must be grounded in the target page's evidence episodes"
    );
    ensure!(
        annotation_view.value["target"]["kind"] == serde_json::json!("fact")
            && annotation_view.value["target"]["page_id"]
                == serde_json::json!(fact_id.0.to_string()),
        "annotation value must reference the annotated page"
    );
    ensure!(
        annotation_view.value["body"]
            == serde_json::json!("The reading looks plausible; follow up next week."),
        "annotation value must carry the annotated body"
    );
    ensure!(
        !annotation.replayed,
        "a fresh annotation write must not be an idempotency replay"
    );

    // A write without a registered policy fails closed — nothing is written.
    let rejected = harness
        .service
        .write_back_annotation(
            &lease,
            &harness.principal,
            "annotation-unregistered".to_owned(),
            WriteBackAnnotation {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                target: WriteBackTarget::Fact { page_id: fact_id },
                body: "This must never land.".to_owned(),
                observed_at: observed_at()?,
                write_policy: unregistered_policy()?,
                confidence: 0.8,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await;
    ensure!(
        matches!(rejected, Err(ServiceError::WritePolicyRejected)),
        "an unregistered write policy must fail closed with WritePolicyRejected"
    );

    // An annotation on an episode page targets the raw layer page directly.
    let episode_annotation = harness
        .service
        .write_back_annotation(
            &lease,
            &harness.principal,
            "annotation-episode".to_owned(),
            WriteBackAnnotation {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                target: WriteBackTarget::Episode { page_id: episode },
                body: "Raw log note.".to_owned(),
                observed_at: observed_at()?,
                write_policy: registered_policy()?,
                confidence: 0.7,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("write-back annotation on episode page")?;
    ensure!(
        episode_annotation
            .view
            .revision
            .as_ref()
            .context("episode annotation head")?
            .evidence_episode_ids
            == vec![episode],
        "an episode-page annotation must be grounded in the episode itself"
    );

    // A page edit is an attributable supersede of the canonical fact.
    let before = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            fact_id,
        )
        .await
        .context("read fact head before page edit")?;
    let edited = harness
        .service
        .write_back_page_edit(
            &lease,
            &harness.principal,
            "page-edit-1".to_owned(),
            before.head_revision_id,
            WriteBackPageEdit {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                fact_id,
                value: serde_json::json!({"value_celsius": 22.5, "edited_note": "corrected sensor offset"}),
                observed_at: observed_at()?,
                valid_time: ValidTime {
                    from: observed_at()?,
                    until: None,
                },
                write_policy: registered_policy()?,
                confidence: 0.95,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("write-back page edit")?;
    ensure!(
        edited.view.head_revision_id != before.head_revision_id,
        "a page edit must produce a new head revision"
    );
    let edited_revision = edited.view.revision.context("edited head revision")?;
    ensure!(
        edited_revision.value["value_celsius"] == serde_json::json!(22.5),
        "the page edit must supersede the page's canonical value"
    );
    ensure!(
        edited_revision.writer_principal_id == harness.principal.principal_id,
        "the page edit must be attributable to the authenticated principal"
    );
    ensure!(
        edited_revision.evidence_episode_ids == vec![episode],
        "the page edit must preserve the page's evidence grounding"
    );

    // A page edit without a registered policy fails closed — nothing is written.
    let rejected_edit = harness
        .service
        .write_back_page_edit(
            &lease,
            &harness.principal,
            "page-edit-unregistered".to_owned(),
            edited_revision.revision_id,
            WriteBackPageEdit {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                fact_id,
                value: serde_json::json!({"value_celsius": 99.9}),
                observed_at: observed_at()?,
                valid_time: ValidTime {
                    from: observed_at()?,
                    until: None,
                },
                write_policy: unregistered_policy()?,
                confidence: 0.95,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await;
    ensure!(
        matches!(rejected_edit, Err(ServiceError::WritePolicyRejected)),
        "a page edit with an unregistered write policy must fail closed with WritePolicyRejected"
    );
    Ok(())
}

/// AC5 — filed answers are agent writes with attribution and derived provenance.
pub(crate) async fn filed_answers_record_agent_writer_and_derived_provenance(
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
        "write-back-agent",
    )
    .await?;
    let (episode, question_fact_id) = seed_episode_and_fact(&harness).await?;
    let answered_at = observed_at()?;
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for filed answer")?;
    let question_before = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            question_fact_id,
        )
        .await
        .context("read the question fact before filing the answer")?
        .revision
        .context("the seeded question fact must exist")?;

    let filed = harness
        .service
        .file_answer(
            &lease,
            &harness.principal,
            "answer-1".to_owned(),
            FileAnswer {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                question_fact_id,
                answer: serde_json::json!({
                    "summary": "Temperature settled at 22.5C after sensor offset correction.",
                    "sources": ["sensor-log-042"],
                }),
                observed_at: answered_at,
                write_policy: registered_policy()?,
                confidence: 0.9,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("file agent answer through write-back")?;

    // The receipt records the filing agent as writer and provenance kind derived.
    ensure!(
        filed.view.namespace.as_str() == "derived",
        "a filed answer must carry provenance kind derived (namespace), got {}",
        filed.view.namespace.as_str()
    );
    let answer_revision = filed.view.revision.context("answer head revision")?;
    ensure!(
        answer_revision.writer_principal_id == harness.principal.principal_id,
        "the receipt must record the filing agent as writer"
    );
    ensure!(
        answer_revision.value["provenance_kind"] == serde_json::json!("derived"),
        "the answer value must record provenance kind derived"
    );
    ensure!(
        answer_revision.value["question_fact_id"]
            == serde_json::json!(question_fact_id.0.to_string()),
        "the answer must reference the question it files"
    );
    ensure!(
        answer_revision.value["filed_by"] == serde_json::json!(harness.principal.principal_id.0),
        "the answer value must record the filing agent identity"
    );
    ensure!(
        answer_revision.evidence_episode_ids == vec![episode],
        "the filed answer must be grounded in the question's evidence episodes"
    );
    ensure!(
        !filed.replayed,
        "a fresh filed answer must not be an idempotency replay"
    );

    // AC7 (spec 017 P3) — answering supersedes the old question fact and
    // links the answer page, preserving the question's own metadata.
    let question_after = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            question_fact_id,
        )
        .await
        .context("read the question fact after filing the answer")?
        .revision
        .context("the question must still exist after being answered")?;
    ensure!(
        question_after.revision_id != question_before.revision_id,
        "answering must supersede the question fact with a new head revision"
    );
    ensure!(
        question_after.supersedes_revision_id == Some(question_before.revision_id),
        "the superseding revision must chain to the question's prior head"
    );
    ensure!(
        question_after.valid_time.until.is_none(),
        "the superseding revision stays open — supersession is carried by the revision chain"
    );
    ensure!(
        question_after.value["answered_by"]["fact_id"] == serde_json::json!(filed.view.fact_id.0),
        "the superseded question must link the answer page"
    );
    ensure!(
        question_after.value["answered_at"].is_string(),
        "the superseded question must record when it was answered"
    );
    ensure!(
        question_after.writer_principal_id == harness.principal.principal_id,
        "the superseding revision must record the answering agent"
    );
    ensure!(
        question_after.evidence_episode_ids == vec![episode],
        "the superseding revision must keep the question's evidence grounding"
    );
    ensure!(
        question_after.write_policy == registered_policy()?,
        "the superseding revision must keep the question's write policy"
    );

    // A replayed answer must not double-supersede the question.
    let replayed = harness
        .service
        .file_answer(
            &lease,
            &harness.principal,
            "answer-1".to_owned(),
            FileAnswer {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                question_fact_id,
                answer: serde_json::json!({
                    "summary": "Temperature settled at 22.5C after sensor offset correction.",
                    "sources": ["sensor-log-042"],
                }),
                observed_at: answered_at,
                write_policy: registered_policy()?,
                confidence: 0.9,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("replay the filed answer")?;
    ensure!(
        replayed.replayed,
        "the replayed answer must be an idempotency replay"
    );
    let question_still = harness
        .service
        .get_current_fact(
            &lease,
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            question_fact_id,
        )
        .await
        .context("read the question after the replayed answer")?
        .revision
        .context("the question must still exist after the replay")?;
    ensure!(
        question_still.revision_id == question_after.revision_id,
        "a replayed answer must not double-supersede the question"
    );

    // An unregistered policy fails closed for filed answers too.
    let rejected = harness
        .service
        .file_answer(
            &lease,
            &harness.principal,
            "answer-unregistered".to_owned(),
            FileAnswer {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                question_fact_id,
                answer: serde_json::json!({"summary": "This must never land."}),
                observed_at: observed_at()?,
                write_policy: unregistered_policy()?,
                confidence: 0.9,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await;
    ensure!(
        matches!(rejected, Err(ServiceError::WritePolicyRejected)),
        "an unregistered write policy on a filed answer must fail closed"
    );
    Ok(())
}
