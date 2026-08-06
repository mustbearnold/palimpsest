//! projection — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use std::time::Duration;

use palimpsest_application::{
    EmbeddingProvider, EmbeddingRequest, RepositoryError, SubjectContentLeaseRepository,
    validate_embedding_response,
};
use palimpsest_domain::{
    EmbeddingInput, EmbeddingProfile, EmbeddingTask, PrincipalId, PrincipalScope, SubjectId,
    TenantId,
};
use pgvector::Vector;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, embedding_vector_sha256, unexpected};

#[derive(Clone)]
pub struct EmbeddingProjectionCoordinator {
    pool: PgPool,
    provider: std::sync::Arc<dyn EmbeddingProvider>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectionRebuildReport {
    pub attempted: usize,
    pub ready: usize,
    pub failed: usize,
}

pub(crate) const EMBEDDING_PROJECTION_LEASE_POLICY_ID: &str = "embedding-projection-v1";

#[derive(Clone, Debug)]
pub(crate) struct ProjectionJob {
    tenant_id: uuid::Uuid,
    subject_id: uuid::Uuid,
    case_id: uuid::Uuid,
    fact_id: uuid::Uuid,
    revision_id: uuid::Uuid,
    profile: EmbeddingProfile,
    projection_profile_id: String,
    projection_profile_version: String,
    projection_profile_sha256: String,
    projection_input_serialization: String,
    projection_input_schema_version: i32,
    generation_attempt_id: uuid::Uuid,
    source_content_sha256: String,
    source_projection_sha256: String,
    input_sha256: String,
    content: String,
}

impl EmbeddingProjectionCoordinator {
    pub fn new(pool: PgPool, provider: std::sync::Arc<dyn EmbeddingProvider>) -> Self {
        Self { pool, provider }
    }

    pub async fn rebuild_pending(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        batch_size: usize,
    ) -> Result<ProjectionRebuildReport, RepositoryError> {
        if batch_size == 0 {
            return Ok(ProjectionRebuildReport::default());
        }

        let lifecycle_repository = PostgresMemoryRepository::new(self.pool.clone());
        let principal = PrincipalScope {
            principal_id: PrincipalId("worker:embedding-projection".to_owned()),
            tenant_id,
            subject_ids: vec![subject_id],
            allowed_sensitivities: vec![],
            operation_grants: vec![],
        };
        let lease = lifecycle_repository
            .acquire_content_lease(&principal, tenant_id, subject_id)
            .await?;
        let rebuild = run_with_content_lease_deadline(
            lease.expires_at,
            self.rebuild_pending_with_lease(tenant_id, subject_id, batch_size),
        )
        .await;
        let release = lifecycle_repository.release_content_lease(&lease).await;
        match (rebuild, release) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(rebuild_error), Err(release_error)) => Err(RepositoryError::Unexpected(format!(
                "projection rebuild failed ({rebuild_error}); content lease release also failed ({release_error})"
            ))),
        }
    }

    async fn rebuild_pending_with_lease(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        batch_size: usize,
    ) -> Result<ProjectionRebuildReport, RepositoryError> {
        let limit = i64::try_from(batch_size).map_err(unexpected)?;
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let policy = sqlx::query(
            r#"
            SELECT lease_seconds, renewal_interval_seconds
            FROM memory.embedding_projection_lease_policies
            WHERE policy_id = $1
            "#,
        )
        .bind(EMBEDDING_PROJECTION_LEASE_POLICY_ID)
        .fetch_one(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let lease_seconds: i32 = policy.try_get("lease_seconds").map_err(unexpected)?;
        let renewal_interval_seconds: i32 = policy
            .try_get("renewal_interval_seconds")
            .map_err(unexpected)?;
        sqlx::query("SELECT memory.enqueue_missing_fact_revision_embedding_projections()")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        sqlx::query(
            r#"
            UPDATE memory.fact_revision_embedding_projections
            SET status = 'pending',
                generation_attempt_id = NULL,
                generation_started_at = NULL,
                generation_lease_expires_at = NULL
            WHERE tenant_id = $1
              AND subject_id = $2
              AND status = 'generating'
              AND (
                  generation_lease_expires_at IS NULL
                  OR generation_lease_expires_at <= clock_timestamp()
              )
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let generation_attempt_id = uuid::Uuid::now_v7();
        let rows = sqlx::query(
            r#"
            SELECT
                projection.tenant_id,
                projection.subject_id,
                projection.case_id,
                projection.fact_id,
                projection.revision_id,
                profile.profile_id,
                profile.profile_version,
                profile.provider,
                profile.model,
                profile.model_revision,
                profile.dimensions,
                profile.normalization,
                profile.normalization_tolerance::double precision
                    AS normalization_tolerance,
                profile.distance_metric,
                profile.scalar_type,
                profile.input_serialization,
                profile.query_task_mode,
                profile.document_task_mode,
                profile.provider_contract_schema_version,
                profile.profile_sha256,
                projection.embedding_projection_profile_id,
                projection.embedding_projection_profile_version,
                projection.embedding_projection_profile_sha256,
                projection_profile.input_serialization
                    AS projection_input_serialization,
                projection_profile.input_schema_version
                    AS projection_input_schema_version,
                projection.source_content_sha256,
                projection.source_projection_sha256,
                projection.input_sha256,
                '1' || chr(31) || fact.namespace || chr(31)
                    || fact.fact_key || chr(31) || revision.value::text AS content
            FROM memory.fact_revision_embedding_projections AS projection
            JOIN memory.embedding_profiles AS profile
              ON profile.profile_id = projection.embedding_profile_id
             AND profile.profile_version = projection.embedding_profile_version
             AND profile.profile_sha256 = projection.embedding_profile_sha256
            JOIN memory.embedding_projection_profiles AS projection_profile
              ON projection_profile.projection_profile_id
                    = projection.embedding_projection_profile_id
             AND projection_profile.projection_profile_version
                    = projection.embedding_projection_profile_version
             AND projection_profile.projection_profile_sha256
                    = projection.embedding_projection_profile_sha256
            JOIN memory.fact_revisions AS revision
              ON revision.tenant_id = projection.tenant_id
             AND revision.subject_id = projection.subject_id
             AND revision.case_id = projection.case_id
             AND revision.fact_id = projection.fact_id
             AND revision.revision_id = projection.revision_id
             AND revision.content_sha256 = projection.source_content_sha256
            JOIN memory.facts AS fact
              ON fact.tenant_id = revision.tenant_id
             AND fact.subject_id = revision.subject_id
             AND fact.case_id = revision.case_id
             AND fact.fact_id = revision.fact_id
            WHERE projection.tenant_id = $2
              AND projection.subject_id = $3
              AND projection.status = 'pending'
            ORDER BY projection.queued_at, projection.revision_id,
                projection.embedding_profile_id,
                projection.embedding_profile_version
            LIMIT $1
            FOR UPDATE OF projection SKIP LOCKED
            "#,
        )
        .bind(limit)
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(ProjectionJob {
                tenant_id: row.try_get("tenant_id").map_err(unexpected)?,
                subject_id: row.try_get("subject_id").map_err(unexpected)?,
                case_id: row.try_get("case_id").map_err(unexpected)?,
                fact_id: row.try_get("fact_id").map_err(unexpected)?,
                revision_id: row.try_get("revision_id").map_err(unexpected)?,
                profile: EmbeddingProfile {
                    id: row.try_get("profile_id").map_err(unexpected)?,
                    version: row.try_get("profile_version").map_err(unexpected)?,
                    provider: row.try_get("provider").map_err(unexpected)?,
                    model: row.try_get("model").map_err(unexpected)?,
                    model_revision: row.try_get("model_revision").map_err(unexpected)?,
                    dimensions: usize::try_from(
                        row.try_get::<i32, _>("dimensions").map_err(unexpected)?,
                    )
                    .map_err(unexpected)?,
                    normalization: row.try_get("normalization").map_err(unexpected)?,
                    normalization_tolerance: row
                        .try_get("normalization_tolerance")
                        .map_err(unexpected)?,
                    distance_metric: row.try_get("distance_metric").map_err(unexpected)?,
                    scalar_type: row.try_get("scalar_type").map_err(unexpected)?,
                    input_serialization: row.try_get("input_serialization").map_err(unexpected)?,
                    query_task: row.try_get("query_task_mode").map_err(unexpected)?,
                    document_task: row.try_get("document_task_mode").map_err(unexpected)?,
                    provider_contract_schema_version: u32::try_from(
                        row.try_get::<i32, _>("provider_contract_schema_version")
                            .map_err(unexpected)?,
                    )
                    .map_err(unexpected)?,
                    digest: row.try_get("profile_sha256").map_err(unexpected)?,
                },
                projection_profile_id: row
                    .try_get("embedding_projection_profile_id")
                    .map_err(unexpected)?,
                projection_profile_version: row
                    .try_get("embedding_projection_profile_version")
                    .map_err(unexpected)?,
                projection_profile_sha256: row
                    .try_get("embedding_projection_profile_sha256")
                    .map_err(unexpected)?,
                projection_input_serialization: row
                    .try_get("projection_input_serialization")
                    .map_err(unexpected)?,
                projection_input_schema_version: row
                    .try_get("projection_input_schema_version")
                    .map_err(unexpected)?,
                generation_attempt_id,
                source_content_sha256: row.try_get("source_content_sha256").map_err(unexpected)?,
                source_projection_sha256: row
                    .try_get("source_projection_sha256")
                    .map_err(unexpected)?,
                input_sha256: row.try_get("input_sha256").map_err(unexpected)?,
                content: row.try_get("content").map_err(unexpected)?,
            });
        }
        for job in &jobs {
            let claimed = sqlx::query(
                r#"
                UPDATE memory.fact_revision_embedding_projections
                SET status = 'generating',
                    generation_attempt_id = $12,
                    generation_started_at = clock_timestamp(),
                    generation_lease_expires_at = clock_timestamp()
                        + make_interval(secs => $13)
                WHERE tenant_id = $1
                  AND subject_id = $2
                  AND case_id = $3
                  AND fact_id = $4
                  AND revision_id = $5
                  AND embedding_profile_id = $6
                  AND embedding_profile_version = $7
                  AND embedding_profile_sha256 = $8
                  AND embedding_projection_profile_id = $9
                  AND embedding_projection_profile_version = $10
                  AND embedding_projection_profile_sha256 = $11
                  AND status = 'pending'
                "#,
            )
            .bind(job.tenant_id)
            .bind(job.subject_id)
            .bind(job.case_id)
            .bind(job.fact_id)
            .bind(job.revision_id)
            .bind(&job.profile.id)
            .bind(&job.profile.version)
            .bind(&job.profile.digest)
            .bind(&job.projection_profile_id)
            .bind(&job.projection_profile_version)
            .bind(&job.projection_profile_sha256)
            .bind(job.generation_attempt_id)
            .bind(lease_seconds)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
            if claimed.rows_affected() != 1 {
                return Err(RepositoryError::Unexpected(
                    "embedding projection claim was lost".to_owned(),
                ));
            }
        }
        transaction.commit().await.map_err(unexpected)?;

        let mut report = ProjectionRebuildReport {
            attempted: jobs.len(),
            ..ProjectionRebuildReport::default()
        };
        let mut offset = 0;
        while offset < jobs.len() {
            let profile = jobs[offset].profile.clone();
            let end = jobs[offset..]
                .iter()
                .position(|job| {
                    job.profile.id != profile.id
                        || job.profile.version != profile.version
                        || job.profile.digest != profile.digest
                })
                .map_or(jobs.len(), |relative| offset + relative);
            let profile_jobs = &jobs[offset..end];
            if profile.input_serialization != "utf8"
                || profile.provider_contract_schema_version != 1
                || profile_jobs.iter().any(|job| {
                    job.projection_input_serialization != "fact-projection-v1"
                        || job.projection_input_schema_version != 1
                })
            {
                for job in profile_jobs {
                    report.failed += usize::from(
                        self.mark_projection_failed(job, "projection_contract_unsupported")
                            .await?,
                    );
                }
                offset = end;
                continue;
            }
            let inputs = profile_jobs
                .iter()
                .map(|job| EmbeddingInput {
                    input_sha256: job.input_sha256.clone(),
                    content: job.content.clone(),
                })
                .collect::<Vec<_>>();
            let heartbeat = self.spawn_projection_lease_heartbeat(
                tenant_id,
                subject_id,
                generation_attempt_id,
                Duration::from_secs(u64::try_from(renewal_interval_seconds).map_err(unexpected)?),
            );
            let response = self
                .provider
                .embed(EmbeddingRequest {
                    profile: profile.clone(),
                    task: EmbeddingTask::Document,
                    inputs,
                })
                .await;
            heartbeat.abort();
            let _ = heartbeat.await;
            let outputs = match response.and_then(|response| {
                let expected = profile_jobs
                    .iter()
                    .map(|job| job.input_sha256.as_str())
                    .collect::<Vec<_>>();
                validate_embedding_response(&profile, &expected, response)
            }) {
                Ok(outputs) => outputs,
                Err(error) => {
                    let code = match error {
                        palimpsest_application::EmbeddingProviderError::Unavailable { .. } => {
                            "provider_unavailable"
                        }
                        palimpsest_application::EmbeddingProviderError::InvalidResponse {
                            ..
                        } => "provider_response_invalid",
                    };
                    for job in profile_jobs {
                        report.failed += usize::from(self.mark_projection_failed(job, code).await?);
                    }
                    offset = end;
                    continue;
                }
            };

            for (job, output) in profile_jobs.iter().zip(outputs) {
                let input_sha256 = hex::encode(Sha256::digest(job.content.as_bytes()));
                if input_sha256 != job.input_sha256 {
                    report.failed += usize::from(
                        self.mark_projection_failed(job, "input_digest_mismatch")
                            .await?,
                    );
                    continue;
                }
                let vector_sha256 = embedding_vector_sha256(&output.values);
                report.ready += usize::from(
                    self.mark_projection_ready(job, output.values, &vector_sha256)
                        .await?,
                );
            }
            offset = end;
        }
        Ok(report)
    }

    fn spawn_projection_lease_heartbeat(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        generation_attempt_id: uuid::Uuid,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match coordinator
                    .renew_projection_lease(tenant_id, subject_id, generation_attempt_id)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) | Err(_) => break,
                }
            }
        })
    }

    async fn renew_projection_lease(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        generation_attempt_id: uuid::Uuid,
    ) -> Result<bool, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let lease_seconds: i32 = sqlx::query_scalar(
            r#"
            SELECT lease_seconds
            FROM memory.embedding_projection_lease_policies
            WHERE policy_id = $1
            "#,
        )
        .bind(EMBEDDING_PROJECTION_LEASE_POLICY_ID)
        .fetch_one(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let updated = sqlx::query(
            r#"
            UPDATE memory.fact_revision_embedding_projections
            SET generation_lease_expires_at = clock_timestamp()
                + make_interval(secs => $4)
            WHERE tenant_id = $1
              AND subject_id = $2
              AND status = 'generating'
              AND generation_attempt_id = $3
              AND generation_lease_expires_at > clock_timestamp()
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(generation_attempt_id)
        .bind(lease_seconds)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(updated.rows_affected() > 0)
    }

    async fn mark_projection_ready(
        &self,
        job: &ProjectionJob,
        values: Vec<f32>,
        vector_sha256: &str,
    ) -> Result<bool, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(
            &mut transaction,
            TenantId(job.tenant_id),
            SubjectId(job.subject_id),
        )
        .await?;
        let result = sqlx::query(
            r#"
            WITH generated AS (SELECT clock_timestamp() AS at)
            UPDATE memory.fact_revision_embedding_projections AS projection
            SET status = 'ready',
                embedding = $17,
                vector_sha256 = $18,
                failure_code = NULL,
                generation_lease_expires_at = NULL,
                generated_at = generated.at
            FROM generated
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
              AND projection.case_id = $3
              AND projection.fact_id = $4
              AND projection.revision_id = $5
              AND projection.embedding_profile_id = $6
              AND projection.embedding_profile_version = $7
              AND projection.embedding_profile_sha256 = $8
              AND projection.embedding_projection_profile_id = $9
              AND projection.embedding_projection_profile_version = $10
              AND projection.embedding_projection_profile_sha256 = $11
              AND projection.source_content_sha256 = $12
              AND projection.source_projection_sha256 = $13
              AND projection.input_sha256 = $14
              AND projection.status = 'generating'
              AND projection.generation_attempt_id = $19
              AND projection.embedding_dimensions = $15
              AND projection.generation_schema_version = $16
            "#,
        )
        .bind(job.tenant_id)
        .bind(job.subject_id)
        .bind(job.case_id)
        .bind(job.fact_id)
        .bind(job.revision_id)
        .bind(&job.profile.id)
        .bind(&job.profile.version)
        .bind(&job.profile.digest)
        .bind(&job.projection_profile_id)
        .bind(&job.projection_profile_version)
        .bind(&job.projection_profile_sha256)
        .bind(&job.source_content_sha256)
        .bind(&job.source_projection_sha256)
        .bind(&job.input_sha256)
        .bind(i32::try_from(job.profile.dimensions).map_err(unexpected)?)
        .bind(1_i32)
        .bind(Vector::from(values))
        .bind(vector_sha256)
        .bind(job.generation_attempt_id)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(result.rows_affected() == 1)
    }

    async fn mark_projection_failed(
        &self,
        job: &ProjectionJob,
        failure_code: &str,
    ) -> Result<bool, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(
            &mut transaction,
            TenantId(job.tenant_id),
            SubjectId(job.subject_id),
        )
        .await?;
        let result = sqlx::query(
            r#"
            WITH failed AS (SELECT clock_timestamp() AS at)
            UPDATE memory.fact_revision_embedding_projections AS projection
            SET status = 'failed',
                embedding = NULL,
                vector_sha256 = NULL,
                failure_code = $15,
                generation_lease_expires_at = NULL,
                generated_at = NULL
            FROM failed
            WHERE projection.tenant_id = $1
              AND projection.subject_id = $2
              AND projection.case_id = $3
              AND projection.fact_id = $4
              AND projection.revision_id = $5
              AND projection.embedding_profile_id = $6
              AND projection.embedding_profile_version = $7
              AND projection.embedding_profile_sha256 = $8
              AND projection.embedding_projection_profile_id = $9
              AND projection.embedding_projection_profile_version = $10
              AND projection.embedding_projection_profile_sha256 = $11
              AND projection.source_content_sha256 = $12
              AND projection.source_projection_sha256 = $13
              AND projection.input_sha256 = $14
              AND projection.status = 'generating'
              AND projection.generation_attempt_id = $16
            "#,
        )
        .bind(job.tenant_id)
        .bind(job.subject_id)
        .bind(job.case_id)
        .bind(job.fact_id)
        .bind(job.revision_id)
        .bind(&job.profile.id)
        .bind(&job.profile.version)
        .bind(&job.profile.digest)
        .bind(&job.projection_profile_id)
        .bind(&job.projection_profile_version)
        .bind(&job.projection_profile_sha256)
        .bind(&job.source_content_sha256)
        .bind(&job.source_projection_sha256)
        .bind(&job.input_sha256)
        .bind(failure_code)
        .bind(job.generation_attempt_id)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(result.rows_affected() == 1)
    }
}

pub(crate) fn remaining_content_lease_duration(
    expires_at: OffsetDateTime,
) -> Result<std::time::Duration, RepositoryError> {
    let remaining = expires_at - OffsetDateTime::now_utc();
    if remaining <= time::Duration::ZERO {
        Ok(std::time::Duration::ZERO)
    } else {
        std::time::Duration::try_from(remaining).map_err(unexpected)
    }
}

pub(crate) async fn run_with_content_lease_deadline<T>(
    expires_at: OffsetDateTime,
    future: impl std::future::Future<Output = Result<T, RepositoryError>>,
) -> Result<T, RepositoryError> {
    let remaining = remaining_content_lease_duration(expires_at)?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| RepositoryError::Unexpected("projection content lease expired".to_owned()))?
}
