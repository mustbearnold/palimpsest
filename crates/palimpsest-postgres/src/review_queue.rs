//! review_queue — durable review-queue jobs (spec 017 P3, AC6).
//! Mirrors the consolidation repository patterns: scoped transactions for
//! user-facing operations, SECURITY DEFINER claim function for the worker.

use async_trait::async_trait;
use palimpsest_application::{
    ClaimedReviewQueueJob, CreateReviewQueueJobOutcome, NewReviewQueueJob, RepositoryError,
    ReviewQueueJobView, ReviewQueueRepository, ReviewQueueScanPage,
};
use palimpsest_domain::SubjectId;
use sqlx::{Row, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, unexpected};
use sha2::{Digest, Sha256};

fn map_review_queue_sqlx(error: sqlx::Error) -> RepositoryError {
    match error {
        sqlx::Error::RowNotFound => RepositoryError::NotFound,
        other => RepositoryError::Unexpected(other.to_string()),
    }
}

fn review_queue_job_view_from_row(row: &PgRow) -> Result<ReviewQueueJobView, RepositoryError> {
    Ok(ReviewQueueJobView {
        job_id: row.try_get("job_id").map_err(unexpected)?,
        lifecycle_state: row.try_get("lifecycle_state").map_err(unexpected)?,
        stale_pages: row.try_get("stale_pages").map_err(unexpected)?,
        surface_id: row.try_get("surface_id").map_err(unexpected)?,
        created_at: row.try_get("created_at").map_err(unexpected)?,
        completed_at: row.try_get("completed_at").map_err(unexpected)?,
        failure_reason: row.try_get("failure_reason").map_err(unexpected)?,
    })
}

#[async_trait]
impl ReviewQueueRepository for PostgresMemoryRepository {
    async fn create_job(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        request: NewReviewQueueJob,
        idempotency: palimpsest_application::IdempotencyRequest,
    ) -> Result<CreateReviewQueueJobOutcome, RepositoryError> {
        for attempt in 1..=3 {
            match self
                .create_review_queue_job_once(tenant_id, subject_id, &request, &idempotency)
                .await
            {
                Err(RepositoryError::SerializationRetry) if attempt < 3 => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded job creation retry loop always returns")
    }

    async fn poll_job(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<ReviewQueueJobView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(
            r#"
            SELECT job_id, lifecycle_state, stale_pages, surface_id,
                created_at, completed_at, failure_reason
            FROM memory.review_queue_jobs
            WHERE tenant_id = $1 AND subject_id = $2 AND job_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_review_queue_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        review_queue_job_view_from_row(&row)
    }

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedReviewQueueJob>, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT job_id, tenant_id, subject_id, lifecycle_state
            FROM memory.claim_next_review_queue_job($1, $2)
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds as i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_review_queue_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ClaimedReviewQueueJob {
            tenant_id: palimpsest_domain::TenantId(row.try_get("tenant_id").map_err(unexpected)?),
            subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
            job_id: row.try_get("job_id").map_err(unexpected)?,
        }))
    }

    async fn complete_job(
        &self,
        job: &ClaimedReviewQueueJob,
        worker_id: Uuid,
        stale_pages: i32,
        surface_id: Option<Uuid>,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE memory.review_queue_jobs AS job_row
            SET lifecycle_state = 'complete',
                state_version = job_row.state_version + 1,
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                stale_pages = $4,
                surface_id = $5,
                updated_at = clock_timestamp(),
                completed_at = clock_timestamp()
            WHERE job_row.tenant_id = $1
              AND job_row.subject_id = $2
              AND job_row.job_id = $3
              AND job_row.worker_lease_id = $6
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(stale_pages)
        .bind(surface_id)
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
        job: &ClaimedReviewQueueJob,
        worker_id: Uuid,
        reason: &str,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE memory.review_queue_jobs AS job_row
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

    async fn list_stale_pages(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        cutoff: OffsetDateTime,
    ) -> Result<Vec<ReviewQueueScanPage>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT projection.fact_id, projection.fact_key, projection.sensitivity
            FROM memory.authorized_current_projection AS projection
            JOIN memory.fact_revisions AS revision
              ON revision.tenant_id = projection.tenant_id
             AND revision.subject_id = projection.subject_id
             AND revision.fact_id = projection.fact_id
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
            GROUP BY projection.fact_id, projection.fact_key, projection.sensitivity
            HAVING MAX(revision.recorded_at) < $3
            ORDER BY projection.fact_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(cutoff)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let mut pages = Vec::with_capacity(rows.len());
        for row in &rows {
            pages.push(ReviewQueueScanPage {
                fact_id: palimpsest_domain::FactId(row.try_get("fact_id").map_err(unexpected)?),
                key: row.try_get("fact_key").map_err(unexpected)?,
                sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
            });
        }
        Ok(pages)
    }
}

impl PostgresMemoryRepository {
    async fn create_review_queue_job_once(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        subject_id: SubjectId,
        request: &NewReviewQueueJob,
        idempotency: &palimpsest_application::IdempotencyRequest,
    ) -> Result<CreateReviewQueueJobOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let key_digest = hex::encode(Sha256::digest(idempotency.key.as_bytes()));
        let job_id = Uuid::now_v7();
        let row = sqlx::query(
            r#"
            INSERT INTO memory.review_queue_jobs (
                tenant_id, subject_id, job_id, principal_id,
                idempotency_key_digest, request_fingerprint
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, subject_id, principal_id, idempotency_key_digest)
                DO NOTHING
            RETURNING job_id, lifecycle_state, stale_pages, surface_id,
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
        .map_err(map_review_queue_sqlx)?;
        let (view, replayed) = if let Some(row) = row {
            (review_queue_job_view_from_row(&row)?, false)
        } else {
            let existing = sqlx::query(
                r#"
                SELECT request_fingerprint
                FROM memory.review_queue_jobs
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
            .map_err(map_review_queue_sqlx)?
            .ok_or(RepositoryError::Conflict)?;
            let existing_fingerprint: String = existing
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if existing_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::Conflict);
            }
            let row = sqlx::query(
                r#"
                SELECT job_id, lifecycle_state, stale_pages, surface_id,
                    created_at, completed_at, failure_reason
                FROM memory.review_queue_jobs
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
            .map_err(map_review_queue_sqlx)?
            .ok_or(RepositoryError::Conflict)?;
            (review_queue_job_view_from_row(&row)?, true)
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(CreateReviewQueueJobOutcome {
            job_id: view.job_id,
            lifecycle_state: view.lifecycle_state,
            replayed,
        })
    }
}
