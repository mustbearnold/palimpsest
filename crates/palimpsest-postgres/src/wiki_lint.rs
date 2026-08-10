//! wiki_lint — durable wiki lint worker persistence (spec 017 P4, AC8).
//! Mirrors the review-queue repository patterns: scoped transactions for
//! user-facing operations, SECURITY DEFINER claim function for the worker.

use async_trait::async_trait;
use palimpsest_application::{
    ClaimedWikiLintJob, CreateWikiLintJobOutcome, LintRepository, NewWikiLintJob, RepositoryError,
    WikiLintFindings, WikiLintJobView, WikiLintScanFact,
};
use palimpsest_domain::SubjectId;
use sqlx::{Row, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, unexpected};
use sha2::{Digest, Sha256};

fn map_wiki_lint_sqlx(error: sqlx::Error) -> RepositoryError {
    match error {
        sqlx::Error::RowNotFound => RepositoryError::NotFound,
        other => RepositoryError::Unexpected(other.to_string()),
    }
}

fn wiki_lint_job_view_from_row(row: &PgRow) -> Result<WikiLintJobView, RepositoryError> {
    Ok(WikiLintJobView {
        job_id: row.try_get("job_id").map_err(unexpected)?,
        lifecycle_state: row.try_get("lifecycle_state").map_err(unexpected)?,
        contradictions: row.try_get("contradictions").map_err(unexpected)?,
        orphans: row.try_get("orphans").map_err(unexpected)?,
        stale_claims: row.try_get("stale_claims").map_err(unexpected)?,
        provenance_gaps: row.try_get("provenance_gaps").map_err(unexpected)?,
        lint_fact_id: row.try_get("lint_fact_id").map_err(unexpected)?,
        question_fact_id: row.try_get("question_fact_id").map_err(unexpected)?,
        created_at: row.try_get("created_at").map_err(unexpected)?,
        completed_at: row.try_get("completed_at").map_err(unexpected)?,
        failure_reason: row.try_get("failure_reason").map_err(unexpected)?,
    })
}

#[async_trait]
impl LintRepository for PostgresMemoryRepository {
    async fn create_job(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        request: NewWikiLintJob,
        idempotency: palimpsest_application::IdempotencyRequest,
    ) -> Result<CreateWikiLintJobOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let key_digest = hex::encode(Sha256::digest(idempotency.key.as_bytes()));
        let job_id = Uuid::now_v7();
        let row = sqlx::query(
            r#"
            INSERT INTO memory.wiki_lint_jobs (
                tenant_id, subject_id, job_id, principal_id,
                idempotency_key_digest, request_fingerprint
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, subject_id, principal_id, idempotency_key_digest)
                DO NOTHING
            RETURNING job_id, lifecycle_state, contradictions, orphans,
                stale_claims, provenance_gaps, lint_fact_id, question_fact_id,
                created_at, completed_at, failure_reason
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(job_id)
        .bind(&request.principal_id.0)
        .bind(&key_digest)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_wiki_lint_sqlx)?;
        let (view, replayed) = if let Some(row) = row {
            (wiki_lint_job_view_from_row(&row)?, false)
        } else {
            let existing = sqlx::query(
                r#"
                SELECT request_fingerprint
                FROM memory.wiki_lint_jobs
                WHERE tenant_id = $1
                  AND subject_id = $2
                  AND principal_id = $3
                  AND idempotency_key_digest = $4
                "#,
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(&request.principal_id.0)
            .bind(&key_digest)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_wiki_lint_sqlx)?
            .ok_or(RepositoryError::Conflict)?;
            let existing_fingerprint: String = existing
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if existing_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::Conflict);
            }
            let row = sqlx::query(
                r#"
                SELECT job_id, lifecycle_state, contradictions, orphans,
                    stale_claims, provenance_gaps, lint_fact_id, question_fact_id,
                    created_at, completed_at, failure_reason
                FROM memory.wiki_lint_jobs
                WHERE tenant_id = $1
                  AND subject_id = $2
                  AND principal_id = $3
                  AND idempotency_key_digest = $4
                "#,
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(&request.principal_id.0)
            .bind(&key_digest)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_wiki_lint_sqlx)?
            .ok_or(RepositoryError::Conflict)?;
            (wiki_lint_job_view_from_row(&row)?, true)
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(CreateWikiLintJobOutcome {
            job_id: view.job_id,
            lifecycle_state: view.lifecycle_state,
            replayed,
        })
    }

    async fn poll_job(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<WikiLintJobView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(
            r#"
            SELECT job_id, lifecycle_state, contradictions, orphans,
                stale_claims, provenance_gaps, lint_fact_id, question_fact_id,
                created_at, completed_at, failure_reason
            FROM memory.wiki_lint_jobs
            WHERE tenant_id = $1 AND subject_id = $2 AND job_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_wiki_lint_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        wiki_lint_job_view_from_row(&row)
    }

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedWikiLintJob>, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT job_id, tenant_id, subject_id, lifecycle_state
            FROM memory.claim_next_wiki_lint_job($1, $2)
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds as i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_wiki_lint_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ClaimedWikiLintJob {
            tenant_id: palimpsest_domain::TenantId(row.try_get("tenant_id").map_err(unexpected)?),
            subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
            job_id: row.try_get("job_id").map_err(unexpected)?,
        }))
    }

    async fn complete_job(
        &self,
        job: &ClaimedWikiLintJob,
        worker_id: Uuid,
        findings: &WikiLintFindings,
        lint_fact_id: Option<Uuid>,
        question_fact_id: Option<Uuid>,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE memory.wiki_lint_jobs AS job_row
            SET lifecycle_state = 'complete',
                state_version = job_row.state_version + 1,
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                contradictions = $4,
                orphans = $5,
                stale_claims = $6,
                provenance_gaps = $7,
                lint_fact_id = $8,
                question_fact_id = $9,
                updated_at = clock_timestamp(),
                completed_at = clock_timestamp()
            WHERE job_row.tenant_id = $1
              AND job_row.subject_id = $2
              AND job_row.job_id = $3
              AND job_row.worker_lease_id = $10
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(findings.contradictions.len() as i32)
        .bind(findings.orphans.len() as i32)
        .bind(findings.stale_claims.len() as i32)
        .bind(findings.provenance_gaps.len() as i32)
        .bind(lint_fact_id)
        .bind(question_fact_id)
        .bind(worker_id)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        transaction.commit().await.map_err(unexpected)
    }

    async fn fail_job(
        &self,
        job: &ClaimedWikiLintJob,
        worker_id: Uuid,
        reason: &str,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE memory.wiki_lint_jobs AS job_row
            SET lifecycle_state = 'failed',
                state_version = job_row.state_version + 1,
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                failure_reason = $4,
                updated_at = clock_timestamp(),
                completed_at = clock_timestamp()
            WHERE job_row.tenant_id = $1
              AND job_row.subject_id = $2
              AND job_row.job_id = $3
              AND job_row.worker_lease_id = $5
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(reason)
        .bind(worker_id)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound);
        }
        transaction.commit().await.map_err(unexpected)
    }

    async fn list_findings(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        stale_cutoff: OffsetDateTime,
    ) -> Result<WikiLintFindings, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;

        // Contradiction: the same (namespace, fact_key) is current in two
        // different cases with different content digests.
        let contradictions = sqlx::query(
            r#"
            SELECT MIN(projection.fact_id::text)::uuid AS fact_id, projection.case_id,
                projection.namespace, projection.fact_key,
                projection.sensitivity,
                (ARRAY_AGG(DISTINCT pair.fact_id) || ARRAY[MIN(projection.fact_id::text)::uuid])
                    AS related_fact_ids,
                COALESCE(
                    ARRAY_AGG(DISTINCT evidence.episode_id) FILTER (WHERE evidence.episode_id IS NOT NULL),
                    ARRAY[]::uuid[]
                ) AS evidence_episode_ids
            FROM memory.authorized_current_projection AS projection
            JOIN memory.authorized_current_projection AS pair
              ON pair.tenant_id = projection.tenant_id
             AND pair.subject_id = projection.subject_id
             AND pair.namespace = projection.namespace
             AND pair.fact_key = projection.fact_key
             AND pair.case_id <> projection.case_id
             AND pair.content_sha256 <> projection.content_sha256
             AND pair.fact_id > projection.fact_id
            LEFT JOIN memory.fact_revision_evidence AS evidence
              ON evidence.tenant_id = projection.tenant_id
             AND evidence.subject_id = projection.subject_id
             AND evidence.case_id = projection.case_id
             AND evidence.fact_id = projection.fact_id
             AND evidence.revision_id = projection.revision_id
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
            GROUP BY projection.case_id, projection.namespace, projection.fact_key,
                projection.sensitivity
            ORDER BY MIN(projection.fact_id::text)
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        // Orphan: a current fact whose head revision carries an evidence
        // reference to an episode that is not present in the subject's
        // episode table (a dangling reference).
        let orphans = sqlx::query(
            r#"
            SELECT DISTINCT projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key,
                projection.sensitivity,
                ARRAY[]::uuid[] AS related_fact_ids,
                COALESCE(
                    ARRAY_AGG(DISTINCT episode.episode_id) FILTER (WHERE episode.episode_id IS NOT NULL),
                    ARRAY[]::uuid[]
                ) AS evidence_episode_ids
            FROM memory.authorized_current_projection AS projection
            JOIN memory.fact_revision_evidence AS evidence
              ON evidence.tenant_id = projection.tenant_id
             AND evidence.subject_id = projection.subject_id
             AND evidence.case_id = projection.case_id
             AND evidence.fact_id = projection.fact_id
             AND evidence.revision_id = projection.revision_id
            LEFT JOIN memory.episodes AS episode
              ON episode.tenant_id = evidence.tenant_id
             AND episode.subject_id = evidence.subject_id
             AND episode.case_id = evidence.case_id
             AND episode.episode_id = evidence.episode_id
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
              AND episode.episode_id IS NULL
            GROUP BY projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key, projection.sensitivity
            ORDER BY projection.fact_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        // Stale claim: a current fact whose latest revision predates the
        // staleness window.
        let stale_claims = sqlx::query(
            r#"
            SELECT projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key,
                projection.sensitivity,
                ARRAY[]::uuid[] AS related_fact_ids,
                COALESCE(ARRAY_AGG(DISTINCT evidence.episode_id), ARRAY[]::uuid[]) AS evidence_episode_ids
            FROM memory.authorized_current_projection AS projection
            JOIN memory.fact_revisions AS revision
              ON revision.tenant_id = projection.tenant_id
             AND revision.subject_id = projection.subject_id
             AND revision.fact_id = projection.fact_id
            LEFT JOIN memory.fact_revision_evidence AS evidence
              ON evidence.tenant_id = projection.tenant_id
             AND evidence.subject_id = projection.subject_id
             AND evidence.case_id = projection.case_id
             AND evidence.fact_id = projection.fact_id
             AND evidence.revision_id = projection.revision_id
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
            GROUP BY projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key, projection.sensitivity
            HAVING MAX(revision.recorded_at) < $3
            ORDER BY projection.fact_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(stale_cutoff)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        // Provenance gap: a head revision whose evidence episode was
        // recorded after the claim itself — evidence cannot predate its
        // claim. The API always grounds a claim in pre-existing episodes.
        // The comparison reads `recorded_at` from the canonical revisions
        // (the projection does not re-sync on UPDATE).
        let provenance_gaps = sqlx::query(
            r#"
            SELECT DISTINCT projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key,
                projection.sensitivity,
                ARRAY[]::uuid[] AS related_fact_ids,
                COALESCE(
                    ARRAY_AGG(DISTINCT episode.episode_id) FILTER (WHERE episode.episode_id IS NOT NULL),
                    ARRAY[]::uuid[]
                ) AS evidence_episode_ids
            FROM memory.authorized_current_projection AS projection
            JOIN memory.fact_revisions AS revision
              ON revision.tenant_id = projection.tenant_id
             AND revision.subject_id = projection.subject_id
             AND revision.case_id = projection.case_id
             AND revision.fact_id = projection.fact_id
             AND revision.revision_id = projection.revision_id
            JOIN memory.fact_revision_evidence AS evidence
              ON evidence.tenant_id = revision.tenant_id
             AND evidence.subject_id = revision.subject_id
             AND evidence.case_id = revision.case_id
             AND evidence.fact_id = revision.fact_id
             AND evidence.revision_id = revision.revision_id
            JOIN memory.episodes AS episode
              ON episode.tenant_id = evidence.tenant_id
             AND episode.subject_id = evidence.subject_id
             AND episode.case_id = evidence.case_id
             AND episode.episode_id = evidence.episode_id
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
              AND episode.recorded_at > revision.recorded_at
            GROUP BY projection.fact_id, projection.case_id,
                projection.namespace, projection.fact_key, projection.sensitivity
            ORDER BY projection.fact_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        let scan = |rows: Vec<PgRow>| -> Result<Vec<WikiLintScanFact>, RepositoryError> {
            rows.iter()
                .map(|row| {
                    let episode_ids: Vec<Uuid> =
                        row.try_get("evidence_episode_ids").map_err(unexpected)?;
                    Ok(WikiLintScanFact {
                        fact_id: palimpsest_domain::FactId(
                            row.try_get("fact_id").map_err(unexpected)?,
                        ),
                        case_id: palimpsest_domain::CaseId(
                            row.try_get("case_id").map_err(unexpected)?,
                        ),
                        namespace: row.try_get("namespace").map_err(unexpected)?,
                        key: row.try_get("fact_key").map_err(unexpected)?,
                        sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
                        related_fact_ids: {
                            let ids: Vec<Uuid> =
                                row.try_get("related_fact_ids").map_err(unexpected)?;
                            ids.into_iter().map(palimpsest_domain::FactId).collect()
                        },
                        evidence_episode_ids: episode_ids
                            .into_iter()
                            .map(palimpsest_domain::EpisodeId)
                            .collect(),
                    })
                })
                .collect()
        };
        Ok(WikiLintFindings {
            contradictions: scan(contradictions)?,
            orphans: scan(orphans)?,
            stale_claims: scan(stale_claims)?,
            provenance_gaps: scan(provenance_gaps)?,
        })
    }
}
