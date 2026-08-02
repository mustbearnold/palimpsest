use std::time::Duration;

use async_trait::async_trait;
use palimpsest_application::{
    AdvanceDeletionOutcome, AppendOutcome, CheckpointMutationOutcome, CheckpointRepository,
    ClaimedDeletionOperation, ClaimedDeletionTarget, CreateDeletionOutcome, CreateDeletionRequest,
    DeletionOperationView, DeletionOutcomeView, DeletionRepository, DeletionTargetView,
    EmbeddingProvider, EmbeddingRequest, EpisodeRepository, ExportCreateOutcome,
    ExportMaterialization, ExportOperationState, ExportOperationView, ExportPackageMetadata,
    ExportRecord, ExportRecordKind, ExportRepository, FactMutationOutcome, FactRepository,
    IdempotencyRequest, NewExport, RepositoryError, RetrievalMutationOutcome, RetrievalPreparation,
    RetrievalQueryEmbedding, RetrievalRepository, SubjectContentLeaseRepository,
    SubjectLifecycleControllerRepository, validate_embedding_response,
};
use palimpsest_domain::{
    AgentId, CaseId, CheckpointEffect, CheckpointId, CheckpointPrecondition, CheckpointRevisionId,
    CheckpointSnapshot, CheckpointView, ContentLeaseId, DeletionOperationId,
    DeletionOperationState, DeletionTargetCapability, DeletionTargetName, DeletionTargetState,
    DeletionTargetVerification, EffectId, EffectKey, EffectKind, EffectReceipt, EffectRecoveryMode,
    EffectStatus, EmbeddingInput, EmbeddingProfile, EmbeddingTask, Episode, EpisodeId, EpisodeKind,
    ExactIdentityTier, ExportId, FactId, FactKey, FactNamespace, FactRevision, FactView,
    NewCheckpointRevision, NewEffectTransition, NewEpisode, NewFact, NewRetrieval, PrincipalId,
    PrincipalScope, Provenance, Q63_EXP2_CONSTANTS_SHA256, RecencyProfile, RetentionPolicyId,
    RetrievalAuthorizationReceipt, RetrievalEmbeddingLineage, RetrievalId, RetrievalItem,
    RetrievalPerspective, RetrievalPolicy, RetrievalPolicyId, RetrievalQueryEmbeddingLineage,
    RetrievalReceipt, RetrievalScore, RevisionId, ScoreUnits, Sensitivity, SourceType,
    SubjectContentLease, SubjectId, SubjectLifecycle, SubjectLifecycleState, TemporalOrderKey,
    TemporalScoreInput, TenantId, ThreadId, ValidTime, WritePolicy, WritePolicyId,
    WritePolicyVersion, score_temporal_retrieval,
};
use pgvector::Vector;
use sha2::{Digest, Sha256};
use sqlx::{Decode, PgPool, Postgres, Row, Transaction, Type, postgres::PgRow};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone)]
pub struct PostgresMemoryRepository {
    pool: PgPool,
}

#[derive(Clone)]
pub struct PostgresSubjectLifecycleRepository {
    content: PostgresMemoryRepository,
    controller: PostgresMemoryRepository,
}

impl PostgresSubjectLifecycleRepository {
    pub fn new(content_pool: PgPool, controller_pool: PgPool) -> Self {
        Self {
            content: PostgresMemoryRepository::new(content_pool),
            controller: PostgresMemoryRepository::new(controller_pool),
        }
    }
}

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

const EMBEDDING_PROJECTION_LEASE_POLICY_ID: &str = "embedding-projection-v1";

#[derive(Clone, Debug)]
struct ProjectionJob {
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

fn remaining_content_lease_duration(
    expires_at: OffsetDateTime,
) -> Result<std::time::Duration, RepositoryError> {
    let remaining = expires_at - OffsetDateTime::now_utc();
    if remaining <= time::Duration::ZERO {
        Ok(std::time::Duration::ZERO)
    } else {
        std::time::Duration::try_from(remaining).map_err(unexpected)
    }
}

async fn run_with_content_lease_deadline<T>(
    expires_at: OffsetDateTime,
    future: impl std::future::Future<Output = Result<T, RepositoryError>>,
) -> Result<T, RepositoryError> {
    let remaining = remaining_content_lease_duration(expires_at)?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| RepositoryError::Unexpected("projection content lease expired".to_owned()))?
}

fn embedding_vector_sha256(values: &[f32]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palimpsest.embedding.float32-be.v1\0");
    for value in values {
        digest.update(value.to_bits().to_be_bytes());
    }
    hex::encode(digest.finalize())
}

fn required_column<T>(row: &PgRow, column: &str) -> Result<T, RepositoryError>
where
    for<'row> T: Decode<'row, Postgres> + Type<Postgres>,
{
    row.try_get::<Option<T>, _>(column)
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is incomplete".to_owned()))
}

fn hybrid_policy_plan(
    row: &PgRow,
    policy_version: String,
    policy_sha256: String,
    document: &serde_json::Value,
) -> Result<HybridPolicyPlan, RepositoryError> {
    fn integer(document: &serde_json::Value, pointer: &str) -> Result<i32, RepositoryError> {
        document
            .pointer(pointer)
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| {
                RepositoryError::Unexpected("hybrid retrieval policy is incomplete".to_owned())
            })
    }

    let policy_id: String = required_column(row, "policy_id")?;
    let scoring_mode: String = required_column(row, "scoring_mode")?;
    let temporal_scoring = match scoring_mode.as_str() {
        "channel-only" => false,
        "temporal-v1" => true,
        _ => {
            return Err(RepositoryError::Unexpected(
                "hybrid retrieval scoring mode is unsupported".to_owned(),
            ));
        }
    };
    let expected_rounding = if temporal_scoring {
        "half-even"
    } else {
        "half-away-from-zero"
    };

    if document
        .pointer("/fusion/method")
        .and_then(|value| value.as_str())
        != Some("reciprocal-rank")
        || document
            .pointer("/fts_configuration")
            .and_then(|value| value.as_str())
            != Some("pg_catalog.simple")
        || document
            .pointer("/fts_rank")
            .and_then(|value| value.as_str())
            != Some("ts_rank_cd")
        || document
            .pointer("/distance_metric")
            .and_then(|value| value.as_str())
            != Some("cosine")
        || document
            .pointer("/fallback")
            .and_then(|value| value.as_str())
            != Some("none")
        || document
            .pointer("/rounding")
            .and_then(|value| value.as_str())
            != Some(expected_rounding)
        || integer(document, "/fusion/weights/exact")? != 1
        || integer(document, "/fusion/weights/lexical")? != 1
        || integer(document, "/fusion/weights/vector")? != 1
    {
        return Err(RepositoryError::Unexpected(
            "hybrid retrieval policy is unsupported".to_owned(),
        ));
    }

    let plan = HybridPolicyPlan {
        policy_version,
        policy_sha256,
        exact_candidate_limit: integer(document, "/candidate_limits/exact")?,
        lexical_candidate_limit: integer(document, "/candidate_limits/lexical")?,
        vector_candidate_limit: integer(document, "/candidate_limits/vector")?,
        manifest_limit: integer(document, "/manifest_limit")?,
        fts_rank_normalization: integer(document, "/fts_rank_normalization")?,
        score_scale: integer(document, "/score_scale")?,
        rrf_k: integer(document, "/fusion/k")?,
        temporal_scoring,
        profile: EmbeddingProfile {
            id: required_column(row, "profile_id")?,
            version: required_column(row, "profile_version")?,
            provider: required_column(row, "provider")?,
            model: required_column(row, "model")?,
            model_revision: required_column(row, "model_revision")?,
            dimensions: usize::try_from(required_column::<i32>(row, "dimensions")?)
                .map_err(unexpected)?,
            normalization: required_column(row, "normalization")?,
            normalization_tolerance: required_column(row, "normalization_tolerance")?,
            distance_metric: required_column(row, "distance_metric")?,
            scalar_type: required_column(row, "scalar_type")?,
            input_serialization: required_column(row, "input_serialization")?,
            query_task: required_column(row, "query_task_mode")?,
            document_task: required_column(row, "document_task_mode")?,
            provider_contract_schema_version: u32::try_from(required_column::<i32>(
                row,
                "provider_contract_schema_version",
            )?)
            .map_err(unexpected)?,
            digest: required_column(row, "profile_sha256")?,
        },
        projection_profile_id: required_column(row, "embedding_projection_profile_id")?,
        projection_profile_version: required_column(row, "embedding_projection_profile_version")?,
        projection_profile_sha256: required_column(row, "embedding_projection_profile_sha256")?,
    };
    let expected_tie_break = if temporal_scoring {
        serde_json::json!([
            "exact_identity_rank_asc_nulls_last",
            "final_score_units_desc",
            "exact_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "vector_rank_asc_nulls_last",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ])
    } else {
        serde_json::json!([
            "fused_score_desc",
            "exact_identity_rank_asc_nulls_last",
            "exact_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "vector_rank_asc_nulls_last",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ])
    };
    let expected_channel_tie_breaks = serde_json::json!({
        "exact": [
            "exact_identity_rank_asc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ],
        "lexical": [
            "lexical_score_desc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ],
        "vector": [
            "vector_distance_asc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ]
    });
    let temporal_policy_supported = if temporal_scoring {
        let stable_profile_sha256: String = required_column(row, "stable_recency_profile_sha256")?;
        let active_profile_sha256: String = required_column(row, "active_recency_profile_sha256")?;
        temporal_policy_is_supported(document, &stable_profile_sha256, &active_profile_sha256)
    } else {
        true
    };
    let lexical_limit_supported = if policy_id == "retrieval-exact-vector-v1" {
        !temporal_scoring && plan.lexical_candidate_limit == 0
    } else {
        (1..=50).contains(&plan.lexical_candidate_limit)
    };
    if !(1..=50).contains(&plan.exact_candidate_limit)
        || !lexical_limit_supported
        || !(1..=50).contains(&plan.vector_candidate_limit)
        || !(1..=50).contains(&plan.manifest_limit)
        || plan.fts_rank_normalization != 32
        || plan.score_scale != 12
        || plan.rrf_k != 60
        || document
            .pointer("/exact_identity_precedence")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || document.pointer("/tie_break") != Some(&expected_tie_break)
        || document.pointer("/channel_tie_breaks") != Some(&expected_channel_tie_breaks)
        || document
            .pointer("/embedding_profile/id")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.id.as_str())
        || document
            .pointer("/embedding_profile/version")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.version.as_str())
        || document
            .pointer("/embedding_profile/digest")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.digest.as_str())
        || document
            .pointer("/projection_profile/id")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_id.as_str())
        || document
            .pointer("/projection_profile/version")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_version.as_str())
        || document
            .pointer("/projection_profile/digest")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_sha256.as_str())
        || !temporal_policy_supported
    {
        return Err(RepositoryError::Unexpected(
            "hybrid retrieval policy is unsupported".to_owned(),
        ));
    }
    Ok(plan)
}

fn temporal_policy_is_supported(
    document: &serde_json::Value,
    stable_profile_sha256: &str,
    active_profile_sha256: &str,
) -> bool {
    let value = |pointer: &str| document.pointer(pointer).and_then(|value| value.as_str());
    let integer = |pointer: &str| document.pointer(pointer).and_then(|value| value.as_i64());
    let expected_operation_order = serde_json::json!([
        "rrf-channel-half-even",
        "fused-exact-sum",
        "recency-half-even",
        "confidence-half-even",
        "importance-half-even",
        "exact-identity-bonus"
    ]);
    let expected_profile_lineage = serde_json::json!({
        "active-case-30d-v1": {
            "version": "1",
            "digest": active_profile_sha256
        },
        "stable-v1": {
            "version": "1",
            "digest": stable_profile_sha256
        }
    });
    value("/arithmetic/id") == Some("score-units-q63-v1")
        && integer("/arithmetic/score_scale") == Some(12)
        && value("/arithmetic/rounding") == Some("half-even")
        && value("/arithmetic/overflow") == Some("reject")
        && document.pointer("/arithmetic/operation_order") == Some(&expected_operation_order)
        && value("/temporal/axis") == Some("request.valid_at")
        && value("/temporal/anchor") == Some("fact_revision_governance.recency_anchor_at")
        && value("/temporal/age_unit") == Some("microsecond")
        && value("/temporal/negative_age") == Some("clamp_zero")
        && document.pointer("/temporal/profile_lineage") == Some(&expected_profile_lineage)
        && value("/temporal/profiles/stable-v1/kind") == Some("constant")
        && value("/temporal/profiles/stable-v1/factor_units") == Some("1000000000000")
        && value("/temporal/profiles/active-case-30d-v1/kind") == Some("continuous-half-life")
        && value("/temporal/profiles/active-case-30d-v1/half_life_us") == Some("2592000000000")
        && value("/temporal/profiles/active-case-30d-v1/floor_units") == Some("125000000000")
        && value("/temporal/profiles/active-case-30d-v1/arithmetic") == Some("q63-exp2-v1")
        && value("/temporal/profiles/active-case-30d-v1/constants_sha256")
            == Some(Q63_EXP2_CONSTANTS_SHA256)
        && value("/quality_factors/confidence/source") == Some("fact_revisions.confidence")
        && value("/quality_factors/confidence/formula") == Some("identity")
        && value("/quality_factors/confidence/minimum_units") == Some("0")
        && value("/quality_factors/confidence/maximum_units") == Some("1000000000000")
        && value("/quality_factors/importance/source")
            == Some("fact_revision_governance.importance")
        && value("/quality_factors/importance/formula") == Some("offset-plus-value")
        && value("/quality_factors/importance/offset_units") == Some("500000000000")
        && value("/quality_factors/importance/minimum_units") == Some("500000000000")
        && value("/quality_factors/importance/maximum_units") == Some("1500000000000")
        && value("/exact_identity_bonus_units/namespace_key") == Some("16393442623")
        && value("/exact_identity_bonus_units/key") == Some("8196721311")
        && value("/exact_identity_bonus_units/none") == Some("0")
}

#[derive(Debug)]
struct LexicalCandidate {
    case_id: uuid::Uuid,
    fact_id: uuid::Uuid,
    revision_id: uuid::Uuid,
    exact_identity_rank: Option<i16>,
    lexical_rank: Option<i64>,
    lexical_score: String,
    final_score: String,
    source_content_sha256: String,
    projection_sha256: String,
    item_sha256: String,
}

#[derive(Clone, Debug)]
struct HybridPolicyPlan {
    policy_version: String,
    policy_sha256: String,
    exact_candidate_limit: i32,
    lexical_candidate_limit: i32,
    vector_candidate_limit: i32,
    manifest_limit: i32,
    fts_rank_normalization: i32,
    score_scale: i32,
    rrf_k: i32,
    temporal_scoring: bool,
    profile: EmbeddingProfile,
    projection_profile_id: String,
    projection_profile_version: String,
    projection_profile_sha256: String,
}

#[derive(Clone, Debug)]
struct HybridCandidate {
    case_id: uuid::Uuid,
    fact_id: uuid::Uuid,
    revision_id: uuid::Uuid,
    exact_identity_rank: Option<i16>,
    exact_rank: Option<i64>,
    lexical_rank: Option<i64>,
    lexical_score: Option<String>,
    vector_rank: Option<i64>,
    vector_distance: Option<String>,
    vector_similarity: Option<String>,
    exact_rrf: String,
    lexical_rrf: String,
    vector_rrf: String,
    fused_score: String,
    source_content_sha256: String,
    projection_sha256: String,
    embedding_input_sha256: String,
    embedding_vector_sha256: String,
    temporal: Option<TemporalCandidate>,
    item_sha256: String,
}

#[derive(Clone, Debug)]
struct TemporalCandidate {
    recency_profile_id: String,
    recency_profile_version: String,
    recency_profile_sha256: String,
    recency_anchor_at: OffsetDateTime,
    recency_age_us: String,
    recency_factor: String,
    confidence_factor: String,
    importance_factor: String,
    temporal_adjustment: String,
    confidence_adjustment: String,
    importance_adjustment: String,
    exact_identity_bonus: String,
    final_score: String,
    order_key: TemporalOrderKey,
}

impl PostgresMemoryRepository {
    async fn create_receipt_once(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
        query_embedding: Option<RetrievalQueryEmbedding>,
    ) -> Result<RetrievalMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        set_retrieval_scope(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            &retrieval.principal_id,
            &retrieval.allowed_sensitivities,
        )
        .await?;

        let reserved = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_idempotency_reservations (
                tenant_id, subject_id, principal_id, idempotency_key,
                request_fingerprint, retrieval_id
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, principal_id, idempotency_key) DO NOTHING
            RETURNING retrieval_id
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .bind(retrieval.retrieval_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;
        if reserved.is_none() {
            let existing = sqlx::query(
                r#"
                SELECT subject_id, retrieval_id, request_fingerprint
                FROM memory.retrieval_idempotency_reservations
                WHERE tenant_id = $1
                  AND principal_id = $2
                  AND idempotency_key = $3
                "#,
            )
            .bind(retrieval.tenant_id.0)
            .bind(&retrieval.principal_id.0)
            .bind(&idempotency.key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_retrieval_sqlx)?;
            let stored_subject_id: uuid::Uuid =
                existing.try_get("subject_id").map_err(unexpected)?;
            let stored_fingerprint: String = existing
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if stored_subject_id != retrieval.subject_id.0
                || stored_fingerprint != idempotency.fingerprint
            {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let retrieval_id = RetrievalId(existing.try_get("retrieval_id").map_err(unexpected)?);
            let receipt = select_retrieval_receipt(
                &mut transaction,
                retrieval.tenant_id,
                retrieval.subject_id,
                retrieval_id,
                None,
                &retrieval.authorization_scope_sha256,
            )
            .await?
            .ok_or_else(|| {
                RepositoryError::Unexpected(
                    "completed retrieval receipt could not be reauthorized".to_owned(),
                )
            })?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalMutationOutcome {
                receipt,
                replayed: true,
            });
        }

        let evaluated_at: OffsetDateTime = sqlx::query("SELECT CURRENT_TIMESTAMP AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?
            .try_get("evaluated_at")
            .map_err(unexpected)?;
        let (perspective, valid_at, recorded_at) = match &retrieval.perspective {
            RetrievalPerspective::Current => ("current", evaluated_at, evaluated_at),
            RetrievalPerspective::AsOf {
                valid_at,
                recorded_at,
            } => {
                if *recorded_at > evaluated_at {
                    return Err(RepositoryError::FutureRecordedTime);
                }
                ("as_of", *valid_at, *recorded_at)
            }
        };

        let policy = sqlx::query(
            r#"
            SELECT policy.policy_id, policy.policy_version, policy.policy_sha256,
                policy.retrieval_mode, policy.scoring_mode, policy.policy_document,
                (policy_document ->> 'candidate_limit')::integer AS candidate_limit,
                (policy_document ->> 'fts_rank_normalization')::integer
                    AS fts_rank_normalization,
                (policy_document ->> 'score_scale')::integer AS score_scale,
                profile.profile_id, profile.profile_version, profile.provider,
                profile.model, profile.model_revision, profile.dimensions,
                profile.normalization,
                profile.normalization_tolerance::double precision
                    AS normalization_tolerance,
                profile.distance_metric, profile.scalar_type,
                profile.input_serialization,
                profile.query_task_mode, profile.document_task_mode,
                profile.provider_contract_schema_version, profile.profile_sha256,
                policy.embedding_projection_profile_id,
                policy.embedding_projection_profile_version,
                policy.embedding_projection_profile_sha256,
                stable_recency.profile_sha256
                    AS stable_recency_profile_sha256,
                active_recency.profile_sha256
                    AS active_recency_profile_sha256
            FROM memory.retrieval_policies AS policy
            LEFT JOIN memory.embedding_profiles AS profile
              ON profile.profile_id = policy.embedding_profile_id
             AND profile.profile_version = policy.embedding_profile_version
             AND profile.profile_sha256 = policy.embedding_profile_sha256
            LEFT JOIN memory.recency_profiles AS stable_recency
              ON stable_recency.profile_id = 'stable-v1'
             AND stable_recency.profile_version = '1'
            LEFT JOIN memory.recency_profiles AS active_recency
              ON active_recency.profile_id = 'active-case-30d-v1'
             AND active_recency.profile_version = '1'
            WHERE policy.policy_id = $1 AND policy.policy_version = '1'
            "#,
        )
        .bind(retrieval.policy_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is unavailable".to_owned()))?;
        let policy_version: String = policy.try_get("policy_version").map_err(unexpected)?;
        let policy_sha256: String = policy.try_get("policy_sha256").map_err(unexpected)?;
        let retrieval_mode: String = policy.try_get("retrieval_mode").map_err(unexpected)?;
        let projection = sqlx::query(
            r#"
            SELECT projection_schema_version, projection_sha256
            FROM memory.search_projection_schemas
            WHERE projection_schema_version = 1
            "#,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| {
            RepositoryError::Unexpected("search projection schema is unavailable".to_owned())
        })?;
        let projection_schema_version: i32 = projection
            .try_get("projection_schema_version")
            .map_err(unexpected)?;
        let projection_schema_sha256: String = projection
            .try_get("projection_sha256")
            .map_err(unexpected)?;

        if retrieval_mode == "hybrid" {
            let policy_document: serde_json::Value =
                policy.try_get("policy_document").map_err(unexpected)?;
            let plan =
                hybrid_policy_plan(&policy, policy_version, policy_sha256, &policy_document)?;
            let receipt = self
                .create_hybrid_receipt_in_transaction(
                    &mut transaction,
                    &retrieval,
                    &idempotency,
                    query_embedding.as_ref(),
                    perspective,
                    valid_at,
                    recorded_at,
                    evaluated_at,
                    projection_schema_version,
                    &projection_schema_sha256,
                    &plan,
                )
                .await?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalMutationOutcome {
                receipt,
                replayed: false,
            });
        }
        if retrieval_mode != "lexical" || query_embedding.is_some() {
            return Err(RepositoryError::Unexpected(
                "retrieval policy execution plan is invalid".to_owned(),
            ));
        }
        let candidate_limit: i32 = policy.try_get("candidate_limit").map_err(unexpected)?;
        let fts_rank_normalization: i32 = policy
            .try_get("fts_rank_normalization")
            .map_err(unexpected)?;
        let score_scale: i32 = policy.try_get("score_scale").map_err(unexpected)?;

        let case_ids = retrieval
            .filters
            .case_ids
            .as_ref()
            .map(|values| values.iter().map(|value| value.0).collect::<Vec<_>>());
        let namespaces = retrieval.filters.namespaces.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let keys = retrieval.filters.keys.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let requested_sensitivities = retrieval.filters.sensitivities.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let allowed_sensitivities = retrieval
            .allowed_sensitivities
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>();
        let candidate_started = std::time::Instant::now();
        let rows = sqlx::query(
            r#"
            WITH effective AS MATERIALIZED (
                SELECT DISTINCT ON (revision.fact_id)
                    revision.tenant_id,
                    revision.subject_id,
                    revision.case_id,
                    revision.fact_id,
                    revision.revision_id,
                    fact.namespace,
                    fact.fact_key,
                    revision.value,
                    revision.sensitivity,
                    revision.content_sha256
                FROM memory.fact_revisions AS revision
                JOIN memory.facts AS fact
                  ON fact.tenant_id = revision.tenant_id
                 AND fact.subject_id = revision.subject_id
                 AND fact.case_id = revision.case_id
                 AND fact.fact_id = revision.fact_id
                WHERE revision.tenant_id = $1
                  AND revision.subject_id = $2
                  AND revision.recorded_at <= $3
                  AND revision.valid_during @> $4::timestamptz
                  AND ($5::uuid[] IS NULL OR revision.case_id = ANY($5))
                  AND ($6::text[] IS NULL OR fact.namespace = ANY($6))
                  AND ($7::text[] IS NULL OR fact.fact_key = ANY($7))
                ORDER BY revision.fact_id, revision.revision_no DESC, revision.revision_id
            ),
            authorized AS MATERIALIZED (
                SELECT effective.*
                FROM effective
                JOIN memory.fact_revision_governance AS governance
                  ON governance.tenant_id = effective.tenant_id
                 AND governance.subject_id = effective.subject_id
                 AND governance.case_id = effective.case_id
                 AND governance.fact_id = effective.fact_id
                 AND governance.revision_id = effective.revision_id
                WHERE governance.lifecycle_state = 'active'
                  AND (
                      governance.retention_expires_at IS NULL
                      OR governance.retention_expires_at > $8
                  )
                  AND effective.sensitivity = ANY($9::text[])
                  AND (
                      $10::text[] IS NULL
                      OR effective.sensitivity = ANY($10)
                  )
            ),
            projected AS MATERIALIZED (
                SELECT authorized.*, document.search_vector,
                    document.projection_sha256,
                    (
                        document.revision_id IS NOT NULL
                        AND document.projection_schema_sha256 = $12
                        AND document.source_content_sha256 = authorized.content_sha256
                        AND document.projection_sha256 =
                            memory.fact_projection_sha256_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                        AND document.search_vector = memory.fact_search_vector_v1(
                            authorized.namespace,
                            authorized.fact_key,
                            authorized.value
                        )
                    ) AS projection_ready
                FROM authorized
                LEFT JOIN memory.fact_revision_search_documents AS document
                  ON document.tenant_id = authorized.tenant_id
                 AND document.subject_id = authorized.subject_id
                 AND document.case_id = authorized.case_id
                 AND document.fact_id = authorized.fact_id
                 AND document.revision_id = authorized.revision_id
                 AND document.projection_schema_version = $11
            ),
            coverage AS (
                SELECT COALESCE(bool_or(NOT projection_ready), false)
                    AS coverage_missing
                FROM projected
            ),
            eligible AS MATERIALIZED (
                SELECT *
                FROM projected
                WHERE projection_ready
            ),
            scored AS (
                SELECT eligible.*,
                    CASE
                        WHEN lower(eligible.namespace || ':' || eligible.fact_key)
                            = lower(btrim($13)) THEN 1::smallint
                        WHEN lower(eligible.fact_key) = lower(btrim($13)) THEN 2::smallint
                        ELSE NULL::smallint
                    END AS exact_identity_rank,
                    eligible.search_vector
                        @@ websearch_to_tsquery('pg_catalog.simple', $13)
                        AS lexical_match,
                    ts_rank_cd(
                        eligible.search_vector,
                        websearch_to_tsquery('pg_catalog.simple', $13),
                        $14
                    )::double precision AS lexical_score
                FROM eligible
            ),
            ranked AS (
                SELECT scored.*,
                    CASE WHEN lexical_match THEN
                        row_number() OVER (
                            PARTITION BY lexical_match
                            ORDER BY lexical_score DESC, fact_id, revision_id
                        )
                    END AS lexical_rank
                FROM scored
                WHERE exact_identity_rank IS NOT NULL OR lexical_match
            ),
            limited AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id, exact_identity_rank,
                    lexical_rank,
                    round(lexical_score::numeric, $15)::text AS lexical_score,
                    content_sha256,
                    projection_sha256
                FROM ranked
                ORDER BY exact_identity_rank ASC NULLS LAST,
                    lexical_rank ASC NULLS LAST, fact_id, revision_id
                LIMIT $16
            )
            SELECT coverage.coverage_missing,
                candidate.fact_id IS NOT NULL AS candidate_present,
                candidate.case_id, candidate.fact_id, candidate.revision_id,
                candidate.exact_identity_rank, candidate.lexical_rank,
                candidate.lexical_score, candidate.content_sha256,
                candidate.projection_sha256
            FROM coverage
            LEFT JOIN limited AS candidate
              ON NOT coverage.coverage_missing
            ORDER BY candidate.exact_identity_rank ASC NULLS LAST,
                candidate.lexical_rank ASC NULLS LAST,
                candidate.fact_id, candidate.revision_id
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(recorded_at)
        .bind(valid_at)
        .bind(case_ids)
        .bind(namespaces)
        .bind(keys)
        .bind(evaluated_at)
        .bind(allowed_sensitivities)
        .bind(requested_sensitivities)
        .bind(projection_schema_version)
        .bind(&projection_schema_sha256)
        .bind(retrieval.query.as_str())
        .bind(fts_rank_normalization)
        .bind(score_scale)
        .bind(candidate_limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unexpected)?;

        let coverage_missing = rows
            .first()
            .ok_or_else(|| {
                RepositoryError::Unexpected("retrieval query returned no rows".to_owned())
            })?
            .try_get::<bool, _>("coverage_missing")
            .map_err(unexpected)?;
        if coverage_missing {
            return Err(RepositoryError::Unexpected(
                "retrieval index is not ready".to_owned(),
            ));
        }
        let mut candidates = Vec::new();
        for row in &rows {
            if row
                .try_get::<bool, _>("candidate_present")
                .map_err(unexpected)?
            {
                candidates.push(lexical_candidate_from_row(row)?);
            }
        }
        let manifest_sha256 = hex::encode(Sha256::digest(
            candidates
                .iter()
                .map(|candidate| candidate.item_sha256.as_str())
                .collect::<String>()
                .as_bytes(),
        ));
        let outcome = if candidates.is_empty() {
            "abstention"
        } else {
            "results"
        };
        let abstention_reason = candidates.is_empty().then_some("no_authorized_match");
        let stage_timings_ms = serde_json::json!({
            "candidate_generation": candidate_started.elapsed().as_secs_f64() * 1000.0
        });
        let _inserted = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_receipts (
                tenant_id, subject_id, retrieval_id, principal_id,
                idempotency_key, request_fingerprint, query_sha256,
                perspective, valid_at, recorded_at, evaluated_at,
                policy_id, policy_version, policy_sha256,
                projection_schema_version, projection_schema_sha256,
                authorization_scope_sha256, authorization_policy_version,
                outcome, abstention_reason, stage_timings_ms, manifest_sha256,
                page_size, schema_version
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, 'principal-scope-v1',
                $18, $19, $20, $21, $22, 1
            )
            RETURNING retrieval_id
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(retrieval.retrieval_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .bind(&retrieval.query_sha256)
        .bind(perspective)
        .bind(valid_at)
        .bind(recorded_at)
        .bind(evaluated_at)
        .bind(retrieval.policy_id.as_str())
        .bind(&policy_version)
        .bind(&policy_sha256)
        .bind(projection_schema_version)
        .bind(&projection_schema_sha256)
        .bind(&retrieval.authorization_scope_sha256)
        .bind(outcome)
        .bind(abstention_reason)
        .bind(&stage_timings_ms)
        .bind(&manifest_sha256)
        .bind(i16::try_from(retrieval.page_size).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;

        for (index, candidate) in candidates.iter().enumerate() {
            let ordinal = i16::try_from(index + 1).map_err(unexpected)?;
            sqlx::query(
                r#"
                INSERT INTO memory.retrieval_manifest_items (
                    tenant_id, subject_id, retrieval_id, principal_id,
                    ordinal, case_id, fact_id, revision_id,
                    exact_identity_rank, lexical_rank, lexical_score,
                    final_rank, final_score, source_content_sha256,
                    projection_sha256, item_sha256, schema_version
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11::numeric, $5, $12::numeric, $13, $14, $15, 1
                )
                "#,
            )
            .bind(retrieval.tenant_id.0)
            .bind(retrieval.subject_id.0)
            .bind(retrieval.retrieval_id.0)
            .bind(&retrieval.principal_id.0)
            .bind(ordinal)
            .bind(candidate.case_id)
            .bind(candidate.fact_id)
            .bind(candidate.revision_id)
            .bind(candidate.exact_identity_rank)
            .bind(candidate.lexical_rank)
            .bind(&candidate.lexical_score)
            .bind(&candidate.final_score)
            .bind(&candidate.source_content_sha256)
            .bind(&candidate.projection_sha256)
            .bind(&candidate.item_sha256)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        }

        let receipt = select_retrieval_receipt(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            retrieval.retrieval_id,
            None,
            &retrieval.authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(map_retrieval_sqlx)?;
        Ok(RetrievalMutationOutcome {
            receipt,
            replayed: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_hybrid_receipt_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        retrieval: &NewRetrieval,
        idempotency: &IdempotencyRequest,
        query_embedding: Option<&RetrievalQueryEmbedding>,
        perspective: &str,
        valid_at: OffsetDateTime,
        recorded_at: OffsetDateTime,
        evaluated_at: OffsetDateTime,
        projection_schema_version: i32,
        projection_schema_sha256: &str,
        plan: &HybridPolicyPlan,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        let query_embedding = query_embedding.ok_or_else(|| {
            RepositoryError::Unexpected("query embedding is unavailable".to_owned())
        })?;
        if query_embedding.profile != plan.profile
            || query_embedding.output.input_sha256 != retrieval.query_sha256
            || query_embedding.output.values.len() != plan.profile.dimensions
        {
            return Err(RepositoryError::Unexpected(
                "query embedding does not match the retrieval plan".to_owned(),
            ));
        }
        let query_vector_sha256 = embedding_vector_sha256(&query_embedding.output.values);
        let query_vector = Vector::from(query_embedding.output.values.clone());
        let case_ids = retrieval
            .filters
            .case_ids
            .as_ref()
            .map(|values| values.iter().map(|value| value.0).collect::<Vec<_>>());
        let namespaces = retrieval.filters.namespaces.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let keys = retrieval.filters.keys.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let requested_sensitivities = retrieval.filters.sensitivities.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let allowed_sensitivities = retrieval
            .allowed_sensitivities
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>();
        let candidate_started = std::time::Instant::now();
        let rows = sqlx::query(
            r#"
            WITH effective AS MATERIALIZED (
                SELECT DISTINCT ON (revision.fact_id)
                    revision.tenant_id,
                    revision.subject_id,
                    revision.case_id,
                    revision.fact_id,
                    revision.revision_id,
                    fact.namespace,
                    fact.fact_key,
                    revision.value,
                    revision.observed_at,
                    revision.confidence,
                    revision.sensitivity,
                    revision.content_sha256
                FROM memory.fact_revisions AS revision
                JOIN memory.facts AS fact
                  ON fact.tenant_id = revision.tenant_id
                 AND fact.subject_id = revision.subject_id
                 AND fact.case_id = revision.case_id
                 AND fact.fact_id = revision.fact_id
                WHERE revision.tenant_id = $1
                  AND revision.subject_id = $2
                  AND revision.recorded_at <= $3
                  AND revision.valid_during @> $4::timestamptz
                  AND ($5::uuid[] IS NULL OR revision.case_id = ANY($5))
                  AND ($6::text[] IS NULL OR fact.namespace = ANY($6))
                  AND ($7::text[] IS NULL OR fact.fact_key = ANY($7))
                ORDER BY revision.fact_id, revision.revision_no DESC,
                    revision.revision_id
            ),
            authorized AS MATERIALIZED (
                SELECT effective.*,
                    governance.recency_profile_id,
                    governance.recency_profile_version,
                    governance.recency_profile_sha256,
                    governance.recency_anchor_at,
                    governance.importance,
                    greatest(
                        0::numeric,
                        extract(epoch FROM (
                            $4::timestamptz - governance.recency_anchor_at
                        )) * 1000000
                    )::numeric(30, 0) AS recency_age_us
                FROM effective
                JOIN memory.fact_revision_governance AS governance
                  ON governance.tenant_id = effective.tenant_id
                 AND governance.subject_id = effective.subject_id
                 AND governance.case_id = effective.case_id
                 AND governance.fact_id = effective.fact_id
                 AND governance.revision_id = effective.revision_id
                WHERE governance.lifecycle_state = 'active'
                  AND (
                      governance.retention_expires_at IS NULL
                      OR governance.retention_expires_at > $8
                  )
                  AND effective.sensitivity = ANY($9::text[])
                  AND (
                      $10::text[] IS NULL
                      OR effective.sensitivity = ANY($10)
                  )
            ),
            projected AS MATERIALIZED (
                SELECT authorized.*,
                    document.search_vector,
                    document.projection_sha256,
                    embedding.embedding,
                    embedding.embedding_profile_sha256,
                    embedding.embedding_projection_profile_sha256,
                    embedding.input_sha256 AS embedding_input_sha256,
                    embedding.vector_sha256 AS embedding_vector_sha256,
                    (
                        document.revision_id IS NOT NULL
                        AND document.projection_schema_sha256 = $12
                        AND document.source_content_sha256
                            = authorized.content_sha256
                        AND document.projection_sha256 =
                            memory.fact_projection_sha256_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                        AND document.search_vector =
                            memory.fact_search_vector_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                    ) AS lexical_ready,
                    (
                        embedding.revision_id IS NOT NULL
                        AND embedding.embedding_profile_id = $16
                        AND embedding.embedding_profile_version = $17
                        AND embedding.embedding_profile_sha256 = $18
                        AND embedding.embedding_dimensions = $19
                        AND embedding.embedding_projection_profile_id = $20
                        AND embedding.embedding_projection_profile_version = $21
                        AND embedding.embedding_projection_profile_sha256 = $22
                        AND embedding.source_content_sha256
                            = authorized.content_sha256
                        AND embedding.source_projection_sha256
                            = document.projection_sha256
                    ) AS embedding_ready
                FROM authorized
                LEFT JOIN memory.fact_revision_search_documents AS document
                  ON document.tenant_id = authorized.tenant_id
                 AND document.subject_id = authorized.subject_id
                 AND document.case_id = authorized.case_id
                 AND document.fact_id = authorized.fact_id
                 AND document.revision_id = authorized.revision_id
                 AND document.projection_schema_version = $11
                LEFT JOIN memory.retrieval_ready_fact_revision_embeddings AS embedding
                  ON embedding.tenant_id = authorized.tenant_id
                 AND embedding.subject_id = authorized.subject_id
                 AND embedding.case_id = authorized.case_id
                 AND embedding.fact_id = authorized.fact_id
                 AND embedding.revision_id = authorized.revision_id
                 AND embedding.embedding_profile_id = $16
                 AND embedding.embedding_profile_version = $17
                 AND embedding.embedding_projection_profile_id = $20
                 AND embedding.embedding_projection_profile_version = $21
            ),
            coverage AS (
                SELECT COALESCE(
                    bool_or(NOT lexical_ready OR NOT embedding_ready),
                    false
                ) AS coverage_missing
                FROM projected
            ),
            eligible AS MATERIALIZED (
                SELECT *
                FROM projected
                WHERE lexical_ready AND embedding_ready
            ),
            scored AS MATERIALIZED (
                SELECT eligible.*,
                    CASE
                        WHEN lower(eligible.namespace || ':' || eligible.fact_key)
                            = lower(btrim($13)) THEN 1::smallint
                        WHEN lower(eligible.fact_key) = lower(btrim($13))
                            THEN 2::smallint
                        ELSE NULL::smallint
                    END AS exact_identity_rank,
                    eligible.search_vector
                        @@ websearch_to_tsquery('pg_catalog.simple', $13)
                        AS lexical_match,
                    ts_rank_cd(
                        eligible.search_vector,
                        websearch_to_tsquery('pg_catalog.simple', $13),
                        $14
                    )::double precision AS lexical_score,
                    eligible.embedding <=> $15 AS vector_distance
                FROM eligible
            ),
            exact_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.exact_identity_rank,
                    row_number() OVER (
                        ORDER BY scored.exact_identity_rank,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS exact_rank
                FROM scored
                WHERE scored.exact_identity_rank IS NOT NULL
                ORDER BY scored.exact_identity_rank,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $23
            ),
            lexical_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.lexical_score,
                    row_number() OVER (
                        ORDER BY scored.lexical_score DESC,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS lexical_rank
                FROM scored
                WHERE scored.lexical_match
                ORDER BY scored.lexical_score DESC,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $24
            ),
            vector_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.vector_distance,
                    row_number() OVER (
                        ORDER BY scored.vector_distance,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS vector_rank
                FROM scored
                ORDER BY scored.vector_distance,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $25
            ),
            candidate_keys AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id FROM exact_channel
                UNION
                SELECT case_id, fact_id, revision_id FROM lexical_channel
                UNION
                SELECT case_id, fact_id, revision_id FROM vector_channel
            ),
            fusion AS MATERIALIZED (
                SELECT eligible.case_id, eligible.fact_id,
                    eligible.revision_id,
                    exact_channel.exact_identity_rank,
                    exact_channel.exact_rank,
                    lexical_channel.lexical_rank,
                    lexical_channel.lexical_score,
                    vector_channel.vector_rank,
                    vector_channel.vector_distance,
                    CASE WHEN vector_channel.vector_distance IS NULL
                        THEN NULL::double precision
                        ELSE 1.0 - vector_channel.vector_distance
                    END AS vector_similarity,
                    CASE WHEN exact_channel.exact_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + exact_channel.exact_rank),
                            $26
                        )
                    END AS exact_rrf,
                    CASE WHEN lexical_channel.lexical_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + lexical_channel.lexical_rank),
                            $26
                        )
                    END AS lexical_rrf,
                    CASE WHEN vector_channel.vector_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + vector_channel.vector_rank),
                            $26
                        )
                    END AS vector_rrf,
                    eligible.recency_profile_id,
                    eligible.recency_profile_version,
                    eligible.recency_profile_sha256,
                    eligible.recency_anchor_at,
                    eligible.recency_age_us,
                    eligible.confidence,
                    eligible.importance,
                    eligible.content_sha256,
                    eligible.projection_sha256,
                    eligible.embedding_input_sha256,
                    eligible.embedding_vector_sha256
                FROM candidate_keys
                JOIN eligible
                  ON eligible.case_id = candidate_keys.case_id
                 AND eligible.fact_id = candidate_keys.fact_id
                 AND eligible.revision_id = candidate_keys.revision_id
                LEFT JOIN exact_channel
                  ON exact_channel.case_id = candidate_keys.case_id
                 AND exact_channel.fact_id = candidate_keys.fact_id
                 AND exact_channel.revision_id = candidate_keys.revision_id
                LEFT JOIN lexical_channel
                  ON lexical_channel.case_id = candidate_keys.case_id
                 AND lexical_channel.fact_id = candidate_keys.fact_id
                 AND lexical_channel.revision_id = candidate_keys.revision_id
                LEFT JOIN vector_channel
                  ON vector_channel.case_id = candidate_keys.case_id
                 AND vector_channel.fact_id = candidate_keys.fact_id
                 AND vector_channel.revision_id = candidate_keys.revision_id
            ),
            ranked AS MATERIALIZED (
                SELECT fusion.*,
                    fusion.exact_rrf + fusion.lexical_rrf + fusion.vector_rrf
                        AS fused_score,
                    row_number() OVER (
                        ORDER BY
                            fusion.exact_rrf + fusion.lexical_rrf
                                + fusion.vector_rrf DESC,
                            fusion.exact_identity_rank ASC NULLS LAST,
                            fusion.exact_rank ASC NULLS LAST,
                            fusion.lexical_rank ASC NULLS LAST,
                            fusion.vector_rank ASC NULLS LAST,
                            fusion.case_id, fusion.fact_id, fusion.revision_id
                    ) AS final_rank
                FROM fusion
            ),
            limited AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id,
                    exact_identity_rank, exact_rank, lexical_rank,
                    CASE WHEN lexical_rank IS NULL THEN NULL::text
                        ELSE round(lexical_score::numeric, $26)::text
                    END AS lexical_score,
                    vector_rank,
                    CASE WHEN vector_rank IS NULL THEN NULL::text
                        ELSE round(vector_distance::numeric, $26)::text
                    END AS vector_distance,
                    CASE WHEN vector_rank IS NULL THEN NULL::text
                        ELSE round(vector_similarity::numeric, $26)::text
                    END AS vector_similarity,
                    round(exact_rrf, $26)::text AS exact_rrf,
                    round(lexical_rrf, $26)::text AS lexical_rrf,
                    round(vector_rrf, $26)::text AS vector_rrf,
                    round(fused_score, $26)::text AS fused_score,
                    content_sha256, projection_sha256,
                    embedding_input_sha256, embedding_vector_sha256,
                    recency_profile_id, recency_profile_version,
                    recency_profile_sha256, recency_anchor_at,
                    recency_age_us, confidence, importance,
                    final_rank
                FROM ranked
                ORDER BY final_rank
                LIMIT CASE WHEN $29 THEN 150 ELSE $28 END
            )
            SELECT coverage.coverage_missing,
                candidate.fact_id IS NOT NULL AS candidate_present,
                candidate.case_id, candidate.fact_id, candidate.revision_id,
                candidate.exact_identity_rank, candidate.exact_rank,
                candidate.lexical_rank, candidate.lexical_score,
                candidate.vector_rank, candidate.vector_distance,
                candidate.vector_similarity, candidate.exact_rrf,
                candidate.lexical_rrf, candidate.vector_rrf,
                candidate.fused_score, candidate.content_sha256,
                candidate.projection_sha256,
                candidate.embedding_input_sha256,
                candidate.embedding_vector_sha256,
                candidate.recency_profile_id,
                candidate.recency_profile_version,
                candidate.recency_profile_sha256,
                candidate.recency_anchor_at,
                candidate.recency_age_us::text AS recency_age_us,
                (candidate.confidence * 10000)::bigint AS confidence_basis_points,
                (candidate.importance * 10000)::bigint AS importance_basis_points,
                candidate.final_rank
            FROM coverage
            LEFT JOIN limited AS candidate
              ON NOT coverage.coverage_missing
            ORDER BY candidate.final_rank
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(recorded_at)
        .bind(valid_at)
        .bind(case_ids)
        .bind(namespaces)
        .bind(keys)
        .bind(evaluated_at)
        .bind(allowed_sensitivities)
        .bind(requested_sensitivities)
        .bind(projection_schema_version)
        .bind(projection_schema_sha256)
        .bind(retrieval.query.as_str())
        .bind(plan.fts_rank_normalization)
        .bind(query_vector)
        .bind(&plan.profile.id)
        .bind(&plan.profile.version)
        .bind(&plan.profile.digest)
        .bind(i32::try_from(plan.profile.dimensions).map_err(unexpected)?)
        .bind(&plan.projection_profile_id)
        .bind(&plan.projection_profile_version)
        .bind(&plan.projection_profile_sha256)
        .bind(plan.exact_candidate_limit)
        .bind(plan.lexical_candidate_limit)
        .bind(plan.vector_candidate_limit)
        .bind(plan.score_scale)
        .bind(plan.rrf_k)
        .bind(plan.manifest_limit)
        .bind(plan.temporal_scoring)
        .fetch_all(&mut **transaction)
        .await
        .map_err(unexpected)?;
        let coverage_missing = rows
            .first()
            .ok_or_else(|| {
                RepositoryError::Unexpected("hybrid retrieval query returned no rows".to_owned())
            })?
            .try_get::<bool, _>("coverage_missing")
            .map_err(unexpected)?;
        if coverage_missing {
            return Err(RepositoryError::Unexpected(
                "retrieval index is not ready".to_owned(),
            ));
        }
        let mut candidates = Vec::new();
        for row in &rows {
            if row
                .try_get::<bool, _>("candidate_present")
                .map_err(unexpected)?
            {
                candidates.push(hybrid_candidate_from_row(row, plan.temporal_scoring)?);
            }
        }
        if plan.temporal_scoring {
            candidates.sort_by(|left, right| {
                left.temporal
                    .as_ref()
                    .expect("temporal policy creates temporal candidates")
                    .order_key
                    .cmp(
                        &right
                            .temporal
                            .as_ref()
                            .expect("temporal policy creates temporal candidates")
                            .order_key,
                    )
            });
            candidates.truncate(usize::try_from(plan.manifest_limit).map_err(unexpected)?);
        }
        let manifest_sha256 = hex::encode(Sha256::digest(
            candidates
                .iter()
                .map(|candidate| candidate.item_sha256.as_str())
                .collect::<String>()
                .as_bytes(),
        ));
        let outcome = if candidates.is_empty() {
            "abstention"
        } else {
            "results"
        };
        let abstention_reason = candidates.is_empty().then_some("no_authorized_match");
        let stage_timings_ms = serde_json::json!({
            "candidate_generation": candidate_started.elapsed().as_secs_f64() * 1000.0
        });
        sqlx::query(
            r#"
            INSERT INTO memory.retrieval_receipts (
                tenant_id, subject_id, retrieval_id, principal_id,
                idempotency_key, request_fingerprint, query_sha256,
                perspective, valid_at, recorded_at, evaluated_at,
                policy_id, policy_version, policy_sha256,
                projection_schema_version, projection_schema_sha256,
                authorization_scope_sha256, authorization_policy_version,
                outcome, abstention_reason, stage_timings_ms, manifest_sha256,
                page_size, schema_version,
                embedding_profile_id, embedding_profile_version,
                embedding_profile_sha256,
                embedding_projection_profile_id,
                embedding_projection_profile_version,
                embedding_projection_profile_sha256,
                query_input_sha256, query_vector_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, 'principal-scope-v1',
                $18, $19, $20, $21, $22, 1,
                $23, $24, $25, $26, $27, $28, $29, $30
            )
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(retrieval.retrieval_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .bind(&retrieval.query_sha256)
        .bind(perspective)
        .bind(valid_at)
        .bind(recorded_at)
        .bind(evaluated_at)
        .bind(retrieval.policy_id.as_str())
        .bind(&plan.policy_version)
        .bind(&plan.policy_sha256)
        .bind(projection_schema_version)
        .bind(projection_schema_sha256)
        .bind(&retrieval.authorization_scope_sha256)
        .bind(outcome)
        .bind(abstention_reason)
        .bind(&stage_timings_ms)
        .bind(&manifest_sha256)
        .bind(i16::try_from(retrieval.page_size).map_err(unexpected)?)
        .bind(&plan.profile.id)
        .bind(&plan.profile.version)
        .bind(&plan.profile.digest)
        .bind(&plan.projection_profile_id)
        .bind(&plan.projection_profile_version)
        .bind(&plan.projection_profile_sha256)
        .bind(&retrieval.query_sha256)
        .bind(&query_vector_sha256)
        .execute(&mut **transaction)
        .await
        .map_err(map_retrieval_sqlx)?;

        for (index, candidate) in candidates.iter().enumerate() {
            let ordinal = i16::try_from(index + 1).map_err(unexpected)?;
            sqlx::query(
                r#"
                INSERT INTO memory.retrieval_manifest_items (
                    tenant_id, subject_id, retrieval_id, principal_id,
                    ordinal, case_id, fact_id, revision_id,
                    exact_identity_rank, exact_rank, lexical_rank,
                    lexical_score, vector_rank, vector_distance,
                    vector_similarity, exact_rrf_contribution,
                    lexical_rrf_contribution, vector_rrf_contribution,
                    fused_score, final_rank, final_score,
                    source_content_sha256, projection_sha256, item_sha256,
                    embedding_profile_sha256,
                    embedding_projection_profile_sha256,
                    embedding_input_sha256, embedding_vector_sha256,
                    recency_profile_id, recency_profile_version,
                    recency_profile_sha256, recency_anchor_at,
                    recency_age_us, recency_factor, confidence_factor,
                    importance_factor, temporal_adjustment,
                    confidence_adjustment, importance_adjustment,
                    exact_identity_bonus,
                    schema_version
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, COALESCE($12::numeric, 0),
                    $13, $14::numeric, $15::numeric,
                    $16::numeric, $17::numeric, $18::numeric,
                    $19::numeric, $5, $27::numeric,
                    $20, $21, $22, $23, $24, $25, $26,
                    $28, $29, $30, $31, $32::numeric,
                    $33::numeric, $34::numeric, $35::numeric,
                    $36::numeric, $37::numeric, $38::numeric, $39::numeric,
                    $40
                )
                "#,
            )
            .bind(retrieval.tenant_id.0)
            .bind(retrieval.subject_id.0)
            .bind(retrieval.retrieval_id.0)
            .bind(&retrieval.principal_id.0)
            .bind(ordinal)
            .bind(candidate.case_id)
            .bind(candidate.fact_id)
            .bind(candidate.revision_id)
            .bind(candidate.exact_identity_rank)
            .bind(candidate.exact_rank)
            .bind(candidate.lexical_rank)
            .bind(candidate.lexical_score.as_deref())
            .bind(candidate.vector_rank)
            .bind(candidate.vector_distance.as_deref())
            .bind(candidate.vector_similarity.as_deref())
            .bind(&candidate.exact_rrf)
            .bind(&candidate.lexical_rrf)
            .bind(&candidate.vector_rrf)
            .bind(&candidate.fused_score)
            .bind(&candidate.source_content_sha256)
            .bind(&candidate.projection_sha256)
            .bind(&candidate.item_sha256)
            .bind(&plan.profile.digest)
            .bind(&plan.projection_profile_sha256)
            .bind(&candidate.embedding_input_sha256)
            .bind(&candidate.embedding_vector_sha256)
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map_or(candidate.fused_score.as_str(), |value| {
                        value.final_score.as_str()
                    }),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_id.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_version.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_sha256.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_anchor_at),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_age_us.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.confidence_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.importance_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.temporal_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.confidence_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.importance_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.exact_identity_bonus.as_str()),
            )
            .bind(if candidate.temporal.is_some() {
                2_i32
            } else {
                1_i32
            })
            .execute(&mut **transaction)
            .await
            .map_err(unexpected)?;
        }

        select_retrieval_receipt(
            transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            retrieval.retrieval_id,
            None,
            &retrieval.authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)
    }

    async fn get_receipt_once(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
        authorization_scope_sha256: &str,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        set_retrieval_scope(
            &mut transaction,
            tenant_id,
            subject_id,
            &principal.principal_id,
            &principal.allowed_sensitivities,
        )
        .await?;
        let receipt = select_retrieval_receipt(
            &mut transaction,
            tenant_id,
            subject_id,
            retrieval_id,
            cursor.as_deref(),
            authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(receipt)
    }
}

#[async_trait]
impl RetrievalRepository for PostgresMemoryRepository {
    async fn prepare_receipt(
        &self,
        retrieval: &NewRetrieval,
        idempotency: &IdempotencyRequest,
    ) -> Result<RetrievalPreparation, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        set_retrieval_scope(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            &retrieval.principal_id,
            &retrieval.allowed_sensitivities,
        )
        .await?;

        let reservation = sqlx::query(
            r#"
            SELECT subject_id, retrieval_id, request_fingerprint
            FROM memory.retrieval_idempotency_reservations
            WHERE tenant_id = $1
              AND principal_id = $2
              AND idempotency_key = $3
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;
        if let Some(reservation) = reservation {
            let stored_subject_id: uuid::Uuid =
                reservation.try_get("subject_id").map_err(unexpected)?;
            let stored_fingerprint: String = reservation
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if stored_subject_id != retrieval.subject_id.0
                || stored_fingerprint != idempotency.fingerprint
            {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let retrieval_id =
                RetrievalId(reservation.try_get("retrieval_id").map_err(unexpected)?);
            let receipt = select_retrieval_receipt(
                &mut transaction,
                retrieval.tenant_id,
                retrieval.subject_id,
                retrieval_id,
                None,
                &retrieval.authorization_scope_sha256,
            )
            .await?
            .ok_or(RepositoryError::IdempotencyInProgress)?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalPreparation::Replay(RetrievalMutationOutcome {
                receipt,
                replayed: true,
            }));
        }

        let policy = sqlx::query(
            r#"
            SELECT policy.retrieval_mode,
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
                profile.profile_sha256
            FROM memory.retrieval_policies AS policy
            LEFT JOIN memory.embedding_profiles AS profile
              ON profile.profile_id = policy.embedding_profile_id
             AND profile.profile_version = policy.embedding_profile_version
             AND profile.profile_sha256 = policy.embedding_profile_sha256
            WHERE policy.policy_id = $1
              AND policy.policy_version = '1'
            "#,
        )
        .bind(retrieval.policy_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is unavailable".to_owned()))?;
        let retrieval_mode: String = policy.try_get("retrieval_mode").map_err(unexpected)?;
        let embedding_profile = match retrieval_mode.as_str() {
            "lexical" => None,
            "hybrid" => Some(EmbeddingProfile {
                id: required_column(&policy, "profile_id")?,
                version: required_column(&policy, "profile_version")?,
                provider: required_column(&policy, "provider")?,
                model: required_column(&policy, "model")?,
                model_revision: required_column(&policy, "model_revision")?,
                dimensions: usize::try_from(required_column::<i32>(&policy, "dimensions")?)
                    .map_err(unexpected)?,
                normalization: required_column(&policy, "normalization")?,
                normalization_tolerance: required_column(&policy, "normalization_tolerance")?,
                distance_metric: required_column(&policy, "distance_metric")?,
                scalar_type: required_column(&policy, "scalar_type")?,
                input_serialization: required_column(&policy, "input_serialization")?,
                query_task: required_column(&policy, "query_task_mode")?,
                document_task: required_column(&policy, "document_task_mode")?,
                provider_contract_schema_version: u32::try_from(required_column::<i32>(
                    &policy,
                    "provider_contract_schema_version",
                )?)
                .map_err(unexpected)?,
                digest: required_column(&policy, "profile_sha256")?,
            }),
            _ => {
                return Err(RepositoryError::Unexpected(
                    "retrieval policy mode is invalid".to_owned(),
                ));
            }
        };
        transaction.commit().await.map_err(map_retrieval_sqlx)?;
        Ok(RetrievalPreparation::Execute { embedding_profile })
    }

    async fn create_receipt(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
        query_embedding: Option<RetrievalQueryEmbedding>,
    ) -> Result<RetrievalMutationOutcome, RepositoryError> {
        const MAX_SERIALIZATION_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_SERIALIZATION_ATTEMPTS {
            match self
                .create_receipt_once(
                    retrieval.clone(),
                    idempotency.clone(),
                    query_embedding.clone(),
                )
                .await
            {
                Err(RepositoryError::SerializationRetry)
                    if attempt < MAX_SERIALIZATION_ATTEMPTS => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded serialization retry loop always returns")
    }

    async fn get_receipt(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
        authorization_scope_sha256: String,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        self.get_receipt_once(
            principal,
            tenant_id,
            subject_id,
            retrieval_id,
            cursor,
            &authorization_scope_sha256,
        )
        .await
    }
}

#[async_trait]
impl FactRepository for PostgresMemoryRepository {
    async fn create(
        &self,
        fact: NewFact,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, fact.tenant_id, fact.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: fact.tenant_id,
            subject_id: fact.subject_id,
            principal_id: &fact.writer_principal_id.0,
            operation_id: "createFact",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let view: FactView = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(FactMutationOutcome {
                view,
                replayed: true,
            });
        }

        sqlx::query(
            r#"
            INSERT INTO memory.facts (
                tenant_id, subject_id, case_id, fact_id, namespace, fact_key, schema_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(fact.tenant_id.0)
        .bind(fact.subject_id.0)
        .bind(fact.case_id.0)
        .bind(fact.fact_id.0)
        .bind(fact.namespace.as_str())
        .bind(fact.key.as_str())
        .bind(i32::try_from(fact.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        let revision_row = sqlx::query(
            r#"
            INSERT INTO memory.fact_revisions (
                tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
                supersedes_revision_id, observed_at, valid_during, value, confidence,
                writer_principal_id, write_policy_id, write_policy_version,
                sensitivity, retention_policy_id, schema_version, content_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, 1, NULL, $6,
                tstzrange($7, $8, '[)'), $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            RETURNING recorded_at
            "#,
        )
        .bind(fact.tenant_id.0)
        .bind(fact.subject_id.0)
        .bind(fact.case_id.0)
        .bind(fact.fact_id.0)
        .bind(fact.revision_id.0)
        .bind(fact.observed_at)
        .bind(fact.valid_time.from)
        .bind(fact.valid_time.until)
        .bind(&fact.value)
        .bind(fact.confidence)
        .bind(&fact.writer_principal_id.0)
        .bind(fact.write_policy.id.as_str())
        .bind(fact.write_policy.version.as_str())
        .bind(fact.sensitivity.as_str())
        .bind(fact.retention_policy_id.as_str())
        .bind(i32::try_from(fact.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .bind(&fact.value_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let recorded_at: OffsetDateTime =
            revision_row.try_get("recorded_at").map_err(unexpected)?;

        for episode_id in &fact.evidence_episode_ids {
            sqlx::query(
                r#"
                INSERT INTO memory.fact_revision_evidence (
                    tenant_id, subject_id, case_id, fact_id, revision_id,
                    episode_id, evidence_role
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
                "#,
            )
            .bind(fact.tenant_id.0)
            .bind(fact.subject_id.0)
            .bind(fact.case_id.0)
            .bind(fact.fact_id.0)
            .bind(fact.revision_id.0)
            .bind(episode_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        }

        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: fact.tenant_id,
                subject_id: fact.subject_id,
                case_id: fact.case_id,
                principal_id: &fact.writer_principal_id.0,
                operation_id: "createFact",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: None,
                resource_fact_id: Some(fact.fact_id),
                resource_revision_id: Some(fact.revision_id),
                resource_checkpoint: None,
                event_type: "memory.fact.created.v1",
            },
        )
        .await?;

        let view = select_fact_view(
            &mut transaction,
            fact.tenant_id,
            fact.subject_id,
            fact.fact_id,
            recorded_at,
            recorded_at,
            recorded_at,
        )
        .await?
        .ok_or_else(|| {
            RepositoryError::Unexpected("created fact could not be reconstructed".to_owned())
        })?;
        let response_body = serde_json::to_value(&view).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", view.head_revision_id.0);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/facts/{}",
            view.tenant_id.0, view.subject_id.0, view.fact_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: None,
                resource_fact_id: Some(fact.fact_id),
                resource_checkpoint: None,
                status: 201,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(FactMutationOutcome {
            view,
            replayed: false,
        })
    }

    async fn get_current(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
    ) -> Result<FactView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let evaluation_row = sqlx::query("SELECT clock_timestamp() AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let evaluated_at: OffsetDateTime =
            evaluation_row.try_get("evaluated_at").map_err(unexpected)?;
        let view = select_fact_view(
            &mut transaction,
            tenant_id,
            subject_id,
            fact_id,
            evaluated_at,
            evaluated_at,
            evaluated_at,
        )
        .await?;
        transaction.commit().await.map_err(unexpected)?;
        view.ok_or(RepositoryError::NotFound)
    }

    async fn supersede(
        &self,
        revision: palimpsest_domain::NewFactRevision,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, revision.tenant_id, revision.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: revision.tenant_id,
            subject_id: revision.subject_id,
            principal_id: &revision.writer_principal_id.0,
            operation_id: "supersedeFact",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let view: FactView = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(FactMutationOutcome {
                view,
                replayed: true,
            });
        }

        let lock_key = format!(
            "{}:{}:{}",
            revision.tenant_id.0, revision.subject_id.0, revision.fact_id.0
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let head = sqlx::query(
            r#"
            SELECT facts.case_id, head.revision_id, head.revision_no
            FROM memory.facts AS facts
            JOIN LATERAL (
                SELECT revision_id, revision_no
                FROM memory.fact_revisions
                WHERE tenant_id = facts.tenant_id
                  AND subject_id = facts.subject_id
                  AND case_id = facts.case_id
                  AND fact_id = facts.fact_id
                ORDER BY revision_no DESC
                LIMIT 1
            ) AS head ON true
            WHERE facts.tenant_id = $1
              AND facts.subject_id = $2
              AND facts.fact_id = $3
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(revision.fact_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or(RepositoryError::NotFound)?;
        let case_id = CaseId(head.try_get("case_id").map_err(unexpected)?);
        let head_revision_id = RevisionId(head.try_get("revision_id").map_err(unexpected)?);
        let head_revision_number: i64 = head.try_get("revision_no").map_err(unexpected)?;
        if revision.expected_head_revision_id != head_revision_id {
            return Err(RepositoryError::PreconditionFailed);
        }
        if revision.supersedes_revision_id != head_revision_id {
            return Err(RepositoryError::SupersessionConflict);
        }
        let next_revision_number = head_revision_number.checked_add(1).ok_or_else(|| {
            RepositoryError::Unexpected("fact revision number overflowed".to_owned())
        })?;

        let revision_row = sqlx::query(
            r#"
            INSERT INTO memory.fact_revisions (
                tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
                supersedes_revision_id, observed_at, valid_during, value, confidence,
                writer_principal_id, write_policy_id, write_policy_version,
                sensitivity, retention_policy_id, schema_version, content_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                tstzrange($9, $10, '[)'), $11, $12, $13, $14, $15, $16, $17, $18, $19
            )
            RETURNING recorded_at
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(case_id.0)
        .bind(revision.fact_id.0)
        .bind(revision.revision_id.0)
        .bind(next_revision_number)
        .bind(revision.supersedes_revision_id.0)
        .bind(revision.observed_at)
        .bind(revision.valid_time.from)
        .bind(revision.valid_time.until)
        .bind(&revision.value)
        .bind(revision.confidence)
        .bind(&revision.writer_principal_id.0)
        .bind(revision.write_policy.id.as_str())
        .bind(revision.write_policy.version.as_str())
        .bind(revision.sensitivity.as_str())
        .bind(revision.retention_policy_id.as_str())
        .bind(i32::try_from(revision.schema_version).map_err(unexpected)?)
        .bind(&revision.value_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let recorded_at: OffsetDateTime =
            revision_row.try_get("recorded_at").map_err(unexpected)?;

        for episode_id in &revision.evidence_episode_ids {
            sqlx::query(
                r#"
                INSERT INTO memory.fact_revision_evidence (
                    tenant_id, subject_id, case_id, fact_id, revision_id,
                    episode_id, evidence_role
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
                "#,
            )
            .bind(revision.tenant_id.0)
            .bind(revision.subject_id.0)
            .bind(case_id.0)
            .bind(revision.fact_id.0)
            .bind(revision.revision_id.0)
            .bind(episode_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        }

        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: revision.tenant_id,
                subject_id: revision.subject_id,
                case_id,
                principal_id: &revision.writer_principal_id.0,
                operation_id: "supersedeFact",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: None,
                resource_fact_id: Some(revision.fact_id),
                resource_revision_id: Some(revision.revision_id),
                resource_checkpoint: None,
                event_type: "memory.fact.superseded.v1",
            },
        )
        .await?;

        let view = select_fact_view(
            &mut transaction,
            revision.tenant_id,
            revision.subject_id,
            revision.fact_id,
            recorded_at,
            recorded_at,
            recorded_at,
        )
        .await?
        .ok_or_else(|| {
            RepositoryError::Unexpected("superseded fact could not be reconstructed".to_owned())
        })?;
        let response_body = serde_json::to_value(&view).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", view.head_revision_id.0);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/facts/{}",
            view.tenant_id.0, view.subject_id.0, view.fact_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: None,
                resource_fact_id: Some(revision.fact_id),
                resource_checkpoint: None,
                status: 200,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(FactMutationOutcome {
            view,
            replayed: false,
        })
    }

    async fn get_as_of(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
        valid_at: OffsetDateTime,
        recorded_at: OffsetDateTime,
    ) -> Result<FactView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let evaluation_row = sqlx::query("SELECT clock_timestamp() AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let evaluated_at: OffsetDateTime =
            evaluation_row.try_get("evaluated_at").map_err(unexpected)?;
        if recorded_at > evaluated_at {
            return Err(RepositoryError::FutureRecordedTime);
        }
        let view = select_fact_view(
            &mut transaction,
            tenant_id,
            subject_id,
            fact_id,
            valid_at,
            recorded_at,
            evaluated_at,
        )
        .await?;
        transaction.commit().await.map_err(unexpected)?;
        view.ok_or(RepositoryError::NotFound)
    }
}

#[async_trait]
impl CheckpointRepository for PostgresMemoryRepository {
    async fn save(
        &self,
        revision: NewCheckpointRevision,
        idempotency: IdempotencyRequest,
    ) -> Result<CheckpointMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, revision.tenant_id, revision.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: revision.tenant_id,
            subject_id: revision.subject_id,
            principal_id: &revision.writer_principal_id.0,
            operation_id: "saveCheckpoint",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let view: CheckpointView = serde_json::from_value(response_body).map_err(unexpected)?;
            if !checkpoint_revision_is_active(&mut transaction, &view).await? {
                return Err(RepositoryError::CheckpointExpired);
            }
            transaction.commit().await.map_err(unexpected)?;
            return Ok(CheckpointMutationOutcome {
                view,
                replayed: true,
            });
        }

        let lock_key = format!(
            "checkpoint:{}:{}:{}:{}",
            revision.tenant_id.0, revision.subject_id.0, revision.agent_id.0, revision.thread_id.0
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;

        let current = select_checkpoint_head_for_update(
            &mut transaction,
            revision.tenant_id,
            revision.subject_id,
            revision.agent_id,
            revision.thread_id,
        )
        .await?;
        let (checkpoint_id, revision_number, status) = match (revision.precondition, current) {
            (CheckpointPrecondition::Create, Some(head)) => {
                if head.expired {
                    return Err(RepositoryError::CheckpointExpired);
                }
                return Err(RepositoryError::CheckpointAlreadyExists);
            }
            (CheckpointPrecondition::Create, None) => (revision.checkpoint_id, 1_i64, 201_i16),
            (CheckpointPrecondition::Match(_), None) => return Err(RepositoryError::NotFound),
            (CheckpointPrecondition::Match(expected), Some(head)) => {
                if head.expired {
                    return Err(RepositoryError::CheckpointExpired);
                }
                if expected != head.revision_id {
                    return Err(RepositoryError::CheckpointPreconditionFailed);
                }
                if revision.parent_revision_id != Some(head.revision_id) {
                    return Err(RepositoryError::CheckpointParentConflict);
                }
                if revision.case_id != head.case_id {
                    return Err(RepositoryError::CheckpointCaseConflict);
                }
                let next_revision_number =
                    head.revision_number.checked_add(1).ok_or_else(|| {
                        RepositoryError::Unexpected(
                            "checkpoint revision number overflowed".to_owned(),
                        )
                    })?;
                (head.checkpoint_id, next_revision_number, 200_i16)
            }
        };

        let revision_row = sqlx::query(
            r#"
            INSERT INTO memory.checkpoint_revisions (
                tenant_id, subject_id, case_id, agent_id, thread_id,
                checkpoint_id, revision_id, revision_number, parent_revision_id,
                state, state_schema_version, state_sha256,
                source_type, source_uri, external_id,
                sensitivity, retention_policy_id, expires_at,
                writer_principal_id, schema_version
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15,
                $16, $17, clock_timestamp(),
                $18, $19
            )
            RETURNING expires_at
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(revision.case_id.0)
        .bind(revision.agent_id.0)
        .bind(revision.thread_id.0)
        .bind(checkpoint_id.0)
        .bind(revision.revision_id.0)
        .bind(revision_number)
        .bind(revision.parent_revision_id.map(|id| id.0))
        .bind(&revision.state)
        .bind(i32::try_from(revision.state_schema_version).map_err(unexpected)?)
        .bind(&revision.state_sha256)
        .bind(revision.provenance.source_type.as_str())
        .bind(&revision.provenance.source_uri)
        .bind(&revision.provenance.external_id)
        .bind(revision.sensitivity.as_str())
        .bind(revision.retention_policy_id.as_str())
        .bind(&revision.writer_principal_id.0)
        .bind(i32::try_from(revision.schema_version).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_checkpoint_sqlx)?;
        let expires_at: OffsetDateTime = revision_row.try_get("expires_at").map_err(unexpected)?;

        persist_effect_transitions(&mut transaction, &revision, checkpoint_id).await?;

        match revision.precondition {
            CheckpointPrecondition::Create => {
                sqlx::query(
                    r#"
                    INSERT INTO memory.checkpoints (
                        tenant_id, subject_id, case_id, agent_id, thread_id,
                        checkpoint_id, head_revision_id, head_revision_number,
                        retention_policy_id, expires_at, schema_version
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $9, $10)
                    "#,
                )
                .bind(revision.tenant_id.0)
                .bind(revision.subject_id.0)
                .bind(revision.case_id.0)
                .bind(revision.agent_id.0)
                .bind(revision.thread_id.0)
                .bind(checkpoint_id.0)
                .bind(revision.revision_id.0)
                .bind(revision.retention_policy_id.as_str())
                .bind(expires_at)
                .bind(i32::try_from(revision.schema_version).map_err(unexpected)?)
                .execute(&mut *transaction)
                .await
                .map_err(map_checkpoint_sqlx)?;
            }
            CheckpointPrecondition::Match(_) => {
                let result = sqlx::query(
                    r#"
                    UPDATE memory.checkpoints
                    SET head_revision_id = $1
                    WHERE tenant_id = $2
                      AND subject_id = $3
                      AND agent_id = $4
                      AND thread_id = $5
                      AND checkpoint_id = $6
                    "#,
                )
                .bind(revision.revision_id.0)
                .bind(revision.tenant_id.0)
                .bind(revision.subject_id.0)
                .bind(revision.agent_id.0)
                .bind(revision.thread_id.0)
                .bind(checkpoint_id.0)
                .execute(&mut *transaction)
                .await
                .map_err(map_checkpoint_sqlx)?;
                if result.rows_affected() != 1 {
                    return Err(RepositoryError::CheckpointPreconditionFailed);
                }
            }
        }

        let checkpoint_resource = CheckpointResource {
            agent_id: revision.agent_id,
            thread_id: revision.thread_id,
            checkpoint_id,
            revision_id: revision.revision_id,
        };
        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: revision.tenant_id,
                subject_id: revision.subject_id,
                case_id: revision.case_id,
                principal_id: &revision.writer_principal_id.0,
                operation_id: "saveCheckpoint",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: None,
                resource_fact_id: None,
                resource_revision_id: None,
                resource_checkpoint: Some(checkpoint_resource),
                event_type: "memory.checkpoint.saved.v1",
            },
        )
        .await?;

        let view = select_checkpoint_view(
            &mut transaction,
            revision.tenant_id,
            revision.subject_id,
            revision.agent_id,
            revision.thread_id,
        )
        .await?
        .ok_or_else(|| {
            RepositoryError::Unexpected("saved checkpoint could not be reconstructed".to_owned())
        })?;
        let response_body = serde_json::to_value(&view).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", view.checkpoint_revision_id.0);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/agents/{}/threads/{}/checkpoint",
            view.tenant_id.0, view.subject_id.0, view.agent_id.0, view.thread_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: None,
                resource_fact_id: None,
                resource_checkpoint: Some(checkpoint_resource),
                status,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(map_checkpoint_sqlx)?;
        Ok(CheckpointMutationOutcome {
            view,
            replayed: false,
        })
    }

    async fn get_current(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        agent_id: AgentId,
        thread_id: ThreadId,
    ) -> Result<CheckpointView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let view =
            select_checkpoint_view(&mut transaction, tenant_id, subject_id, agent_id, thread_id)
                .await?;
        transaction.commit().await.map_err(unexpected)?;
        view.ok_or(RepositoryError::NotFound)
    }
}

async fn checkpoint_revision_is_active(
    transaction: &mut Transaction<'_, Postgres>,
    view: &CheckpointView,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM memory.checkpoint_revisions AS revision
            JOIN memory.checkpoints AS checkpoint
              ON checkpoint.tenant_id = revision.tenant_id
             AND checkpoint.subject_id = revision.subject_id
             AND checkpoint.agent_id = revision.agent_id
             AND checkpoint.thread_id = revision.thread_id
             AND checkpoint.checkpoint_id = revision.checkpoint_id
            WHERE revision.tenant_id = $1
              AND revision.subject_id = $2
              AND revision.agent_id = $3
              AND revision.thread_id = $4
              AND revision.checkpoint_id = $5
              AND revision.revision_id = $6
              AND revision.expires_at > clock_timestamp()
              AND checkpoint.expires_at > clock_timestamp()
        )
        "#,
    )
    .bind(view.tenant_id.0)
    .bind(view.subject_id.0)
    .bind(view.agent_id.0)
    .bind(view.thread_id.0)
    .bind(view.checkpoint_id.0)
    .bind(view.checkpoint_revision_id.0)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unexpected)
}

impl PostgresMemoryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

#[async_trait]
impl SubjectContentLeaseRepository for PostgresMemoryRepository {
    async fn acquire_content_lease(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectContentLease, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
            .bind(&principal.principal_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;

        let lease_id = ContentLeaseId(uuid::Uuid::now_v7());
        let row = sqlx::query(
            r#"
            SELECT acquired_at, expires_at
            FROM memory.acquire_subject_content_lease($1, $2, $3, $4)
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(lease_id.0)
        .bind(&principal.principal_id.0)
        .fetch_one(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let lease = SubjectContentLease {
            tenant_id,
            subject_id,
            lease_id,
            principal_id: principal.principal_id.clone(),
            acquired_at: row.try_get("acquired_at").map_err(unexpected)?,
            expires_at: row.try_get("expires_at").map_err(unexpected)?,
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(lease)
    }

    async fn release_content_lease(
        &self,
        lease: &SubjectContentLease,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, lease.tenant_id, lease.subject_id).await?;
        sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
            .bind(&lease.principal_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        sqlx::query(
            r#"
            SELECT memory.release_subject_content_lease($1, $2, $3, $4)
            "#,
        )
        .bind(lease.tenant_id.0)
        .bind(lease.subject_id.0)
        .bind(lease.lease_id.0)
        .bind(&lease.principal_id.0)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }
}

#[async_trait]
impl SubjectLifecycleControllerRepository for PostgresMemoryRepository {
    async fn transition_to_deletion_pending(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        self.transition_subject_lifecycle(
            tenant_id,
            subject_id,
            SubjectLifecycleState::DeletionPending,
        )
        .await
    }

    async fn transition_to_deleted(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        self.transition_subject_lifecycle(tenant_id, subject_id, SubjectLifecycleState::Deleted)
            .await
    }
}

#[async_trait]
impl DeletionRepository for PostgresMemoryRepository {
    async fn create_deletion_operation(
        &self,
        request: CreateDeletionRequest,
    ) -> Result<CreateDeletionOutcome, RepositoryError> {
        const MAX_SERIALIZATION_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_SERIALIZATION_ATTEMPTS {
            match self.create_deletion_operation_once(request.clone()).await {
                Err(RepositoryError::SerializationRetry)
                    if attempt < MAX_SERIALIZATION_ATTEMPTS => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded deletion creation retry loop always returns")
    }

    async fn poll_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
    ) -> Result<DeletionOperationView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(
            r#"
            SELECT lifecycle_state, state_version, retry_count, failure_reason,
                   targets, outcome, updated_at, expired
            FROM memory.poll_deletion_operation($1, $2, $3)
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(operation_id.0)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        let state: String = row.try_get("lifecycle_state").map_err(unexpected)?;
        let expired: bool = row.try_get("expired").map_err(unexpected)?;
        let lifecycle_state = if expired {
            DeletionOperationState::Expired
        } else {
            parse_deletion_state(&state)?
        };
        let failure_reason: Option<String> = row.try_get("failure_reason").map_err(unexpected)?;
        let targets: serde_json::Value = row.try_get("targets").map_err(unexpected)?;
        let mut outcome = row
            .try_get::<Option<serde_json::Value>, _>("outcome")
            .map_err(unexpected)?
            .map(serde_json::from_value::<DeletionOutcomeView>)
            .transpose()
            .map_err(unexpected)?;
        if expired && outcome.is_none() {
            outcome = Some(DeletionOutcomeView::fenced_not_verified());
        }
        Ok(DeletionOperationView {
            operation_id,
            lifecycle_state,
            state_version: u64::try_from(
                row.try_get::<i64, _>("state_version").map_err(unexpected)?,
            )
            .map_err(unexpected)?,
            retry_count: u32::try_from(row.try_get::<i32, _>("retry_count").map_err(unexpected)?)
                .map_err(unexpected)?,
            failure_reason: if expired { None } else { failure_reason },
            targets: parse_deletion_targets(targets, lifecycle_state)?,
            outcome,
            updated_at: row.try_get("updated_at").map_err(unexpected)?,
            expired,
        })
    }

    async fn repair_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
        reason_code: &str,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        sqlx::query("SELECT memory.repair_deletion_operation($1, $2, $3, $4)")
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(operation_id.0)
            .bind(reason_code)
            .execute(&mut *transaction)
            .await
            .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn claim_next_deletion_operation(
        &self,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionOperation>, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, subject_id, operation_id, lifecycle_state, state_version
            FROM memory.claim_next_deletion_operation($1, $2)
            "#,
        )
        .bind(worker_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_deletion_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let state: String = row.try_get("lifecycle_state").map_err(unexpected)?;
        Ok(Some(ClaimedDeletionOperation {
            tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
            subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
            operation_id: DeletionOperationId(row.try_get("operation_id").map_err(unexpected)?),
            lifecycle_state: parse_deletion_state(&state)?,
            state_version: u64::try_from(
                row.try_get::<i64, _>("state_version").map_err(unexpected)?,
            )
            .map_err(unexpected)?,
        }))
    }

    async fn renew_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, claimed.tenant_id, claimed.subject_id).await?;
        sqlx::query(
            r#"
            SELECT memory.renew_deletion_operation_lease($1, $2, $3, $4, $5)
            "#,
        )
        .bind(claimed.tenant_id.0)
        .bind(claimed.subject_id.0)
        .bind(claimed.operation_id.0)
        .bind(worker_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn claim_next_deletion_target(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionTarget>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, claimed.tenant_id, claimed.subject_id).await?;
        let target_lease_id = uuid::Uuid::now_v7();
        let row = sqlx::query(
            r#"
            SELECT target_name, target_key_digest, target_lease_id,
                   attempts, lease_expires_at
            FROM memory.claim_next_deletion_target($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(claimed.tenant_id.0)
        .bind(claimed.subject_id.0)
        .bind(claimed.operation_id.0)
        .bind(worker_id)
        .bind(target_lease_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let target_name = DeletionTargetName::try_from_str(
            row.try_get::<String, _>("target_name")
                .map_err(unexpected)?
                .as_str(),
        )
        .ok_or_else(|| unexpected("deletion target returned an unknown target name"))?;
        Ok(Some(ClaimedDeletionTarget {
            tenant_id: claimed.tenant_id,
            subject_id: claimed.subject_id,
            operation_id: claimed.operation_id,
            worker_id,
            target_name,
            target_key_digest: row.try_get("target_key_digest").map_err(unexpected)?,
            target_lease_id: row.try_get("target_lease_id").map_err(unexpected)?,
            attempts: u32::try_from(row.try_get::<i32, _>("attempts").map_err(unexpected)?)
                .map_err(unexpected)?,
            lease_expires_at: row.try_get("lease_expires_at").map_err(unexpected)?,
        }))
    }

    async fn renew_deletion_target_lease(
        &self,
        target: &ClaimedDeletionTarget,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, target.tenant_id, target.subject_id).await?;
        sqlx::query(
            r#"
            SELECT memory.renew_deletion_target_lease($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(target.tenant_id.0)
        .bind(target.subject_id.0)
        .bind(target.operation_id.0)
        .bind(target.worker_id)
        .bind(&target.target_key_digest)
        .bind(target.target_lease_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn apply_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, target.tenant_id, target.subject_id).await?;
        sqlx::query("SELECT set_config('palimpsest.deletion_workflow', $1, true)")
            .bind(target.operation_id.0.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        sqlx::query("SELECT memory.purge_deletion_target($1, $2, $3)")
            .bind(target.tenant_id.0)
            .bind(target.subject_id.0)
            .bind(target.target_name.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn fail_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        sanitized_error: &str,
        max_attempts: u32,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, target.tenant_id, target.subject_id).await?;
        sqlx::query(
            r#"
            SELECT memory.fail_deletion_target(
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
            "#,
        )
        .bind(target.tenant_id.0)
        .bind(target.subject_id.0)
        .bind(target.operation_id.0)
        .bind(target.worker_id)
        .bind(target.target_name.as_str())
        .bind(&target.target_key_digest)
        .bind(target.target_lease_id)
        .bind(sanitized_error)
        .bind(i32::try_from(max_attempts).map_err(unexpected)?)
        .execute(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn complete_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        effect_receipt_sha256: &str,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, target.tenant_id, target.subject_id).await?;
        sqlx::query(
            r#"
            SELECT memory.complete_deletion_target($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(target.tenant_id.0)
        .bind(target.subject_id.0)
        .bind(target.operation_id.0)
        .bind(target.worker_id)
        .bind(target.target_name.as_str())
        .bind(&target.target_key_digest)
        .bind(target.target_lease_id)
        .bind(effect_receipt_sha256)
        .execute(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn advance_deletion_operation(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        max_attempts: u32,
    ) -> Result<AdvanceDeletionOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, claimed.tenant_id, claimed.subject_id).await?;
        let row = sqlx::query(
            r#"
            SELECT lifecycle_state, state_version, next_poll_seconds
            FROM memory.advance_deletion_operation($1, $2, $3, $4, $5)
            "#,
        )
        .bind(claimed.tenant_id.0)
        .bind(claimed.subject_id.0)
        .bind(claimed.operation_id.0)
        .bind(worker_id)
        .bind(i32::try_from(max_attempts).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        let state: String = row.try_get("lifecycle_state").map_err(unexpected)?;
        Ok(AdvanceDeletionOutcome {
            lifecycle_state: parse_deletion_state(&state)?,
            state_version: u64::try_from(
                row.try_get::<i64, _>("state_version").map_err(unexpected)?,
            )
            .map_err(unexpected)?,
            next_poll_seconds: u32::try_from(
                row.try_get::<i32, _>("next_poll_seconds")
                    .map_err(unexpected)?,
            )
            .map_err(unexpected)?,
        })
    }
}

#[async_trait]
impl SubjectContentLeaseRepository for PostgresSubjectLifecycleRepository {
    async fn acquire_content_lease(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectContentLease, RepositoryError> {
        self.content
            .acquire_content_lease(principal, tenant_id, subject_id)
            .await
    }

    async fn release_content_lease(
        &self,
        lease: &SubjectContentLease,
    ) -> Result<(), RepositoryError> {
        self.content.release_content_lease(lease).await
    }
}

#[async_trait]
impl SubjectLifecycleControllerRepository for PostgresSubjectLifecycleRepository {
    async fn transition_to_deletion_pending(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        self.controller
            .transition_to_deletion_pending(tenant_id, subject_id)
            .await
    }

    async fn transition_to_deleted(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        self.controller
            .transition_to_deleted(tenant_id, subject_id)
            .await
    }
}

#[async_trait]
impl DeletionRepository for PostgresSubjectLifecycleRepository {
    async fn create_deletion_operation(
        &self,
        request: CreateDeletionRequest,
    ) -> Result<CreateDeletionOutcome, RepositoryError> {
        self.controller.create_deletion_operation(request).await
    }

    async fn poll_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
    ) -> Result<DeletionOperationView, RepositoryError> {
        self.controller
            .poll_deletion_operation(tenant_id, subject_id, operation_id)
            .await
    }

    async fn repair_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
        reason_code: &str,
    ) -> Result<(), RepositoryError> {
        self.controller
            .repair_deletion_operation(tenant_id, subject_id, operation_id, reason_code)
            .await
    }

    async fn claim_next_deletion_operation(
        &self,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionOperation>, RepositoryError> {
        self.controller
            .claim_next_deletion_operation(worker_id, lease_seconds)
            .await
    }

    async fn renew_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError> {
        self.controller
            .renew_deletion_operation_lease(claimed, worker_id, lease_seconds)
            .await
    }

    async fn claim_next_deletion_target(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionTarget>, RepositoryError> {
        self.controller
            .claim_next_deletion_target(claimed, worker_id, lease_seconds)
            .await
    }

    async fn renew_deletion_target_lease(
        &self,
        target: &ClaimedDeletionTarget,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError> {
        self.controller
            .renew_deletion_target_lease(target, lease_seconds)
            .await
    }

    async fn apply_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
    ) -> Result<(), RepositoryError> {
        self.controller.apply_deletion_target(target).await
    }

    async fn fail_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        sanitized_error: &str,
        max_attempts: u32,
    ) -> Result<(), RepositoryError> {
        self.controller
            .fail_deletion_target(target, sanitized_error, max_attempts)
            .await
    }

    async fn complete_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        effect_receipt_sha256: &str,
    ) -> Result<(), RepositoryError> {
        self.controller
            .complete_deletion_target(target, effect_receipt_sha256)
            .await
    }

    async fn advance_deletion_operation(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
        max_attempts: u32,
    ) -> Result<AdvanceDeletionOutcome, RepositoryError> {
        self.controller
            .advance_deletion_operation(claimed, worker_id, max_attempts)
            .await
    }
}

#[async_trait]
impl ExportRepository for PostgresMemoryRepository {
    async fn create_export(
        &self,
        request: NewExport,
    ) -> Result<ExportCreateOutcome, RepositoryError> {
        const MAX_SERIALIZATION_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_SERIALIZATION_ATTEMPTS {
            match self.create_export_once(request.clone()).await {
                Err(RepositoryError::SerializationRetry)
                    if attempt < MAX_SERIALIZATION_ATTEMPTS => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded export creation retry loop always returns")
    }

    async fn get_export(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<ExportOperationView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(export_operation_select_sql())
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?
            .ok_or(RepositoryError::NotFound)?;
        let mut operation = export_operation_from_row(&row)?;
        if operation.expires_at <= OffsetDateTime::now_utc()
            && !matches!(
                operation.state,
                ExportOperationState::Expired | ExportOperationState::Revoked
            )
        {
            let updated = sqlx::query(
                r#"
                UPDATE memory.export_operations
                SET state = 'expired',
                    status_version = status_version + 1,
                    worker_lease_id = NULL,
                    worker_lease_expires_at = NULL,
                    content_sha256 = NULL,
                    package_size_bytes = NULL,
                    record_count = NULL,
                    updated_at = clock_timestamp()
                WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
                  AND state NOT IN ('revoked', 'expired')
                "#,
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
            if updated.rows_affected() == 1 {
                operation.state = ExportOperationState::Expired;
                operation.status_version = operation.status_version.saturating_add(1);
                operation.worker_lease_id = None;
                operation.content_sha256 = None;
                operation.size_bytes = None;
                operation.record_count = None;
            }
        }
        transaction.commit().await.map_err(unexpected)?;
        Ok(operation)
    }

    async fn list_export_ids_for_subject(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<Vec<ExportId>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT export_id
            FROM memory.export_operations
            WHERE tenant_id = $1 AND subject_id = $2
            ORDER BY created_at, export_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        let export_ids = rows
            .into_iter()
            .map(|row| row.try_get("export_id").map(ExportId).map_err(unexpected))
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(export_ids)
    }

    async fn claim_export_for_materialization(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<Option<ExportMaterialization>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(export_operation_select_sql())
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
        let Some(row) = row else {
            transaction.commit().await.map_err(unexpected)?;
            return Err(RepositoryError::NotFound);
        };
        let state: String = row.try_get("state").map_err(unexpected)?;
        let expires_at: OffsetDateTime = row.try_get("expires_at").map_err(unexpected)?;
        if expires_at <= OffsetDateTime::now_utc() {
            sqlx::query(
                r#"
                UPDATE memory.export_operations
                SET state = 'expired',
                    status_version = status_version + 1,
                    worker_lease_id = NULL,
                    worker_lease_expires_at = NULL,
                    content_sha256 = NULL,
                    package_size_bytes = NULL,
                    record_count = NULL,
                    updated_at = clock_timestamp()
                WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
                  AND state NOT IN ('revoked', 'expired')
                "#,
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(None);
        }
        let lease_available = match state.as_str() {
            "queued" => true,
            "materializing" => {
                let lease_expires_at: Option<OffsetDateTime> =
                    row.try_get("worker_lease_expires_at").map_err(unexpected)?;
                lease_expires_at.is_none_or(|value| value <= OffsetDateTime::now_utc())
            }
            _ => false,
        };
        if !lease_available {
            transaction.commit().await.map_err(unexpected)?;
            return Ok(None);
        }
        let worker_lease_id = uuid::Uuid::now_v7();
        let operation_row = sqlx::query(
            r#"
            UPDATE memory.export_operations
            SET state = 'materializing',
                status_version = status_version + 1,
                worker_lease_id = $4,
                worker_lease_expires_at = clock_timestamp() + interval '30 seconds',
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
              AND (
                  state = 'queued'
                  OR (
                      state = 'materializing'
                      AND (worker_lease_expires_at IS NULL
                           OR worker_lease_expires_at <= clock_timestamp())
                  )
              )
            RETURNING *
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(export_id.0)
        .bind(worker_lease_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?
        .ok_or(RepositoryError::Conflict)?;
        let operation = export_operation_from_row(&operation_row)?;
        let records =
            load_export_records(&mut transaction, tenant_id, subject_id, export_id).await?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(Some(ExportMaterialization { operation, records }))
    }

    async fn claim_next_export_for_materialization(
        &self,
        worker_lease_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ExportMaterialization>, RepositoryError> {
        let row = sqlx::query(
            "SELECT tenant_id, subject_id, export_id\n             FROM memory.claim_next_export_operation($1, $2)",
        )
        .bind(worker_lease_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_export_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let tenant_id = TenantId(row.try_get("tenant_id").map_err(unexpected)?);
        let subject_id = SubjectId(row.try_get("subject_id").map_err(unexpected)?);
        let export_id = ExportId(row.try_get("export_id").map_err(unexpected)?);
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let operation_row = sqlx::query(export_operation_select_sql())
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
        let operation = export_operation_from_row(&operation_row)?;
        let records =
            load_export_records(&mut transaction, tenant_id, subject_id, export_id).await?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(Some(ExportMaterialization { operation, records }))
    }

    async fn claim_next_expired_export_for_cleanup(
        &self,
        worker_lease_id: uuid::Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ExportOperationView>, RepositoryError> {
        let row = sqlx::query(
            "SELECT tenant_id, subject_id, export_id\n             FROM memory.claim_next_expired_export_operation($1, $2)",
        )
        .bind(worker_lease_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_export_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let tenant_id = TenantId(row.try_get("tenant_id").map_err(unexpected)?);
        let subject_id = SubjectId(row.try_get("subject_id").map_err(unexpected)?);
        let export_id = ExportId(row.try_get("export_id").map_err(unexpected)?);
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        let operation_row = sqlx::query(export_operation_select_sql())
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(export_id.0)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
        let Some(operation_row) = operation_row else {
            transaction.commit().await.map_err(unexpected)?;
            return Ok(None);
        };
        let operation = export_operation_from_row(&operation_row)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(Some(operation))
    }

    async fn mark_export_cleanup_complete(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: uuid::Uuid,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        sqlx::query(
            r#"
            UPDATE memory.export_operations
            SET worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                package_cleanup_completed_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
              AND state = 'expired'
              AND worker_lease_id = $4
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(export_id.0)
        .bind(worker_lease_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn mark_export_ready(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: uuid::Uuid,
        metadata: ExportPackageMetadata,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let size_bytes = i64::try_from(metadata.size_bytes).map_err(unexpected)?;
        let record_count = i64::try_from(metadata.record_count).map_err(unexpected)?;
        let result = sqlx::query(
            r#"
            UPDATE memory.export_operations
            SET state = 'ready',
                status_version = status_version + 1,
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                content_sha256 = $4,
                package_size_bytes = $5,
                record_count = $6,
                failure_code = NULL,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
              AND state = 'materializing'
              AND worker_lease_id = $7
              AND expires_at > clock_timestamp()
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(export_id.0)
        .bind(metadata.content_sha256)
        .bind(size_bytes)
        .bind(record_count)
        .bind(worker_lease_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(RepositoryError::Conflict);
        }
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn mark_export_failed(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: uuid::Uuid,
        failure_code: &str,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE memory.export_operations
            SET state = 'failed',
                status_version = status_version + 1,
                worker_lease_id = NULL,
                worker_lease_expires_at = NULL,
                failure_code = $4,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
              AND state = 'materializing'
              AND worker_lease_id = $5
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(export_id.0)
        .bind(failure_code)
        .bind(worker_lease_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(RepositoryError::Conflict);
        }
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ExportManifestItem {
    kind: ExportRecordKind,
    record_id: uuid::Uuid,
    recorded_at: OffsetDateTime,
    source_content_sha256: String,
}

async fn load_export_records(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    export_id: ExportId,
) -> Result<Vec<ExportRecord>, RepositoryError> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT record_kind, record_id, recorded_at, source_content_sha256
        FROM memory.export_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
        ORDER BY record_kind, recorded_at, record_id
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(export_id.0)
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_export_sqlx)?;
    let mut manifest = Vec::with_capacity(manifest_rows.len());
    for row in manifest_rows {
        manifest.push(ExportManifestItem {
            kind: parse_export_record_kind(
                row.try_get::<String, _>("record_kind")
                    .map_err(unexpected)?
                    .as_str(),
            )?,
            record_id: row.try_get("record_id").map_err(unexpected)?,
            recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
            source_content_sha256: row
                .try_get::<String, _>("source_content_sha256")
                .map_err(unexpected)?
                .trim()
                .to_owned(),
        });
    }

    let mut records = Vec::with_capacity(manifest.len());
    let episode_rows = sqlx::query(
        r#"
        SELECT item.record_id, item.recorded_at, item.source_content_sha256,
               episode.payload_sha256,
               jsonb_build_object(
                   'schema_version', 1,
                   'record_kind', 'episode',
                   'origin_class', 'observed',
                   'id', episode.episode_id,
                   'scope', jsonb_build_object(
                       'tenant_id', episode.tenant_id,
                       'subject_id', episode.subject_id,
                       'case_id', episode.case_id
                   ),
                   'temporal', jsonb_build_object(
                       'observed_at', episode.observed_at,
                       'recorded_at', episode.recorded_at
                   ),
                   'governance', jsonb_build_object(
                       'sensitivity', episode.sensitivity,
                       'retention_policy_id', episode.retention_policy_id,
                       'schema_version', episode.schema_version
                   ),
                   'provenance', jsonb_build_object(
                       'writer_principal_id', episode.writer_principal_id,
                       'source_type', episode.source_type,
                       'source_uri', episode.source_uri,
                       'external_id', episode.external_id
                   ),
                   'relations', '{}'::jsonb,
                   'payload', episode.payload
               ) AS value
        FROM memory.export_manifest_items AS item
        JOIN memory.episodes AS episode
          ON episode.tenant_id = item.tenant_id
         AND episode.subject_id = item.subject_id
         AND episode.episode_id = item.record_id
        WHERE item.tenant_id = $1 AND item.subject_id = $2
          AND item.export_id = $3 AND item.record_kind = 'episode'
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(export_id.0)
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_export_sqlx)?;
    for row in episode_rows {
        let record_id: uuid::Uuid = row.try_get("record_id").map_err(unexpected)?;
        let expected = manifest_item(&manifest, ExportRecordKind::Episode, record_id)?;
        let source_digest: String = row
            .try_get::<String, _>("payload_sha256")
            .map_err(unexpected)?
            .trim()
            .to_owned();
        if source_digest != expected.source_content_sha256 {
            return Err(RepositoryError::Unexpected(
                "export episode source digest changed".to_owned(),
            ));
        }
        records.push(ExportRecord {
            kind: ExportRecordKind::Episode,
            id: record_id,
            recorded_at: expected.recorded_at,
            value: row.try_get("value").map_err(unexpected)?,
        });
    }
    verify_export_kind_count(
        &manifest,
        ExportRecordKind::Episode,
        records
            .iter()
            .filter(|record| record.kind == ExportRecordKind::Episode)
            .count(),
    )?;

    let fact_rows = sqlx::query(
        r#"
        SELECT item.record_id, item.recorded_at, item.source_content_sha256,
               revision.content_sha256,
               jsonb_build_object(
                   'schema_version', 1,
                   'record_kind', 'fact_revision',
                   'origin_class', 'derived',
                   'id', revision.revision_id,
                   'scope', jsonb_build_object(
                       'tenant_id', revision.tenant_id,
                       'subject_id', revision.subject_id,
                       'case_id', revision.case_id,
                       'fact_id', revision.fact_id,
                       'namespace', fact.namespace,
                       'key', fact.fact_key
                   ),
                   'temporal', jsonb_build_object(
                       'observed_at', revision.observed_at,
                       'recorded_at', revision.recorded_at,
                       'valid_from', lower(revision.valid_during),
                       'valid_to', CASE
                           WHEN upper_inf(revision.valid_during) THEN NULL
                           ELSE upper(revision.valid_during)
                       END
                   ),
                   'governance', jsonb_build_object(
                       'sensitivity', revision.sensitivity,
                       'retention_policy_id', revision.retention_policy_id,
                       'schema_version', revision.schema_version,
                       'lifecycle_state', governance.lifecycle_state,
                       'importance', governance.importance
                   ),
                   'provenance', jsonb_build_object(
                       'writer_principal_id', revision.writer_principal_id,
                       'write_policy_id', revision.write_policy_id,
                       'write_policy_version', revision.write_policy_version,
                       'evidence', COALESCE((
                           SELECT jsonb_agg(
                               jsonb_build_object(
                                   'episode_id', evidence.episode_id,
                                   'role', evidence.evidence_role
                               ) ORDER BY evidence.episode_id
                           )
                           FROM memory.fact_revision_evidence AS evidence
                           WHERE evidence.tenant_id = revision.tenant_id
                             AND evidence.subject_id = revision.subject_id
                             AND evidence.case_id = revision.case_id
                             AND evidence.fact_id = revision.fact_id
                             AND evidence.revision_id = revision.revision_id
                       ), '[]'::jsonb)
                   ),
                   'relations', jsonb_build_object(
                       'supersedes_id', revision.supersedes_revision_id
                   ),
                   'payload', revision.value
               ) AS value
        FROM memory.export_manifest_items AS item
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = item.tenant_id
         AND revision.subject_id = item.subject_id
         AND revision.revision_id = item.record_id
        JOIN memory.facts AS fact
          ON fact.tenant_id = revision.tenant_id
         AND fact.subject_id = revision.subject_id
         AND fact.case_id = revision.case_id
         AND fact.fact_id = revision.fact_id
        JOIN memory.fact_revision_governance AS governance
          ON governance.tenant_id = revision.tenant_id
         AND governance.subject_id = revision.subject_id
         AND governance.case_id = revision.case_id
         AND governance.fact_id = revision.fact_id
         AND governance.revision_id = revision.revision_id
        WHERE item.tenant_id = $1 AND item.subject_id = $2
          AND item.export_id = $3 AND item.record_kind = 'fact_revision'
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(export_id.0)
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_export_sqlx)?;
    for row in fact_rows {
        let record_id: uuid::Uuid = row.try_get("record_id").map_err(unexpected)?;
        let expected = manifest_item(&manifest, ExportRecordKind::FactRevision, record_id)?;
        let source_digest: String = row
            .try_get::<String, _>("content_sha256")
            .map_err(unexpected)?
            .trim()
            .to_owned();
        if source_digest != expected.source_content_sha256 {
            return Err(RepositoryError::Unexpected(
                "export fact revision source digest changed".to_owned(),
            ));
        }
        records.push(ExportRecord {
            kind: ExportRecordKind::FactRevision,
            id: record_id,
            recorded_at: expected.recorded_at,
            value: row.try_get("value").map_err(unexpected)?,
        });
    }
    verify_export_kind_count(
        &manifest,
        ExportRecordKind::FactRevision,
        records
            .iter()
            .filter(|record| record.kind == ExportRecordKind::FactRevision)
            .count(),
    )?;

    let checkpoint_rows = sqlx::query(
        r#"
        SELECT item.record_id, item.recorded_at, item.source_content_sha256,
               revision.state_sha256,
               jsonb_build_object(
                   'schema_version', 1,
                   'record_kind', 'checkpoint',
                   'origin_class', 'provided',
                   'id', revision.revision_id,
                   'scope', jsonb_build_object(
                       'tenant_id', revision.tenant_id,
                       'subject_id', revision.subject_id,
                       'case_id', revision.case_id,
                       'agent_id', revision.agent_id,
                       'thread_id', revision.thread_id,
                       'checkpoint_id', revision.checkpoint_id
                   ),
                   'temporal', jsonb_build_object(
                       'recorded_at', revision.recorded_at
                   ),
                   'governance', jsonb_build_object(
                       'sensitivity', revision.sensitivity,
                       'retention_policy_id', revision.retention_policy_id,
                       'schema_version', revision.schema_version,
                       'state_schema_version', revision.state_schema_version
                   ),
                   'provenance', jsonb_build_object(
                       'writer_principal_id', revision.writer_principal_id,
                       'source_type', revision.source_type,
                       'source_uri', revision.source_uri,
                       'external_id', revision.external_id
                   ),
                   'relations', jsonb_build_object(
                       'parent_revision_id', revision.parent_revision_id,
                       'created_at', checkpoint.created_at,
                       'expires_at', revision.expires_at
                   ),
                   'payload', revision.state
               ) AS value
        FROM memory.export_manifest_items AS item
        JOIN memory.checkpoint_revisions AS revision
          ON revision.tenant_id = item.tenant_id
         AND revision.subject_id = item.subject_id
         AND revision.revision_id = item.record_id
        JOIN memory.checkpoints AS checkpoint
          ON checkpoint.tenant_id = revision.tenant_id
         AND checkpoint.subject_id = revision.subject_id
         AND checkpoint.case_id = revision.case_id
         AND checkpoint.agent_id = revision.agent_id
         AND checkpoint.thread_id = revision.thread_id
         AND checkpoint.checkpoint_id = revision.checkpoint_id
        WHERE item.tenant_id = $1 AND item.subject_id = $2
          AND item.export_id = $3 AND item.record_kind = 'checkpoint'
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(export_id.0)
    .fetch_all(&mut **transaction)
    .await
    .map_err(map_export_sqlx)?;
    for row in checkpoint_rows {
        let record_id: uuid::Uuid = row.try_get("record_id").map_err(unexpected)?;
        let expected = manifest_item(&manifest, ExportRecordKind::Checkpoint, record_id)?;
        let source_digest: String = row
            .try_get::<String, _>("state_sha256")
            .map_err(unexpected)?
            .trim()
            .to_owned();
        if source_digest != expected.source_content_sha256 {
            return Err(RepositoryError::Unexpected(
                "export checkpoint source digest changed".to_owned(),
            ));
        }
        records.push(ExportRecord {
            kind: ExportRecordKind::Checkpoint,
            id: record_id,
            recorded_at: expected.recorded_at,
            value: row.try_get("value").map_err(unexpected)?,
        });
    }
    verify_export_kind_count(
        &manifest,
        ExportRecordKind::Checkpoint,
        records
            .iter()
            .filter(|record| record.kind == ExportRecordKind::Checkpoint)
            .count(),
    )?;
    verify_export_kind_count(&manifest, ExportRecordKind::Procedure, 0)?;
    verify_export_kind_count(&manifest, ExportRecordKind::ArtifactReference, 0)?;
    Ok(records)
}

fn manifest_item(
    manifest: &[ExportManifestItem],
    kind: ExportRecordKind,
    record_id: uuid::Uuid,
) -> Result<&ExportManifestItem, RepositoryError> {
    manifest
        .iter()
        .find(|item| item.kind == kind && item.record_id == record_id)
        .ok_or_else(|| RepositoryError::Unexpected("export membership is inconsistent".to_owned()))
}

fn verify_export_kind_count(
    manifest: &[ExportManifestItem],
    kind: ExportRecordKind,
    actual: usize,
) -> Result<(), RepositoryError> {
    let expected = manifest.iter().filter(|item| item.kind == kind).count();
    if expected == actual {
        Ok(())
    } else {
        Err(RepositoryError::Unexpected(format!(
            "export membership source row is unavailable for {kind:?}: expected {expected}, got {actual}"
        )))
    }
}

fn parse_export_record_kind(value: &str) -> Result<ExportRecordKind, RepositoryError> {
    match value {
        "episode" => Ok(ExportRecordKind::Episode),
        "checkpoint" => Ok(ExportRecordKind::Checkpoint),
        "fact_revision" => Ok(ExportRecordKind::FactRevision),
        "procedure" => Ok(ExportRecordKind::Procedure),
        "artifact_reference" => Ok(ExportRecordKind::ArtifactReference),
        _ => Err(RepositoryError::Unexpected(
            "export manifest contains an unknown record kind".to_owned(),
        )),
    }
}

fn export_operation_select_sql() -> &'static str {
    r#"
    SELECT *
    FROM memory.export_operations
    WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
    "#
}

fn export_operation_from_row(row: &PgRow) -> Result<ExportOperationView, RepositoryError> {
    let state = parse_export_operation_state(
        row.try_get::<String, _>("state")
            .map_err(unexpected)?
            .as_str(),
    )?;
    let status_version = u64::try_from(
        row.try_get::<i64, _>("status_version")
            .map_err(unexpected)?,
    )
    .map_err(unexpected)?;
    let size_bytes = row
        .try_get::<Option<i64>, _>("package_size_bytes")
        .map_err(unexpected)?
        .map(u64::try_from)
        .transpose()
        .map_err(unexpected)?;
    let record_count = row
        .try_get::<Option<i64>, _>("record_count")
        .map_err(unexpected)?
        .map(u64::try_from)
        .transpose()
        .map_err(unexpected)?;
    Ok(ExportOperationView {
        export_id: ExportId(row.try_get("export_id").map_err(unexpected)?),
        profile: row.try_get("profile").map_err(unexpected)?,
        state,
        status_version,
        created_at: row.try_get("created_at").map_err(unexpected)?,
        expires_at: row.try_get("expires_at").map_err(unexpected)?,
        content_sha256: row
            .try_get::<Option<String>, _>("content_sha256")
            .map_err(unexpected)?
            .map(|value| value.trim().to_owned()),
        size_bytes,
        record_count,
        failure_code: row.try_get("failure_code").map_err(unexpected)?,
        tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
        subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
        principal_id: PrincipalId(row.try_get("principal_id").map_err(unexpected)?),
        allowed_sensitivities: row.try_get("allowed_sensitivities").map_err(unexpected)?,
        authorization_scope_sha256: row
            .try_get::<String, _>("authorization_scope_sha256")
            .map_err(unexpected)?
            .trim()
            .to_owned(),
        worker_lease_id: row.try_get("worker_lease_id").map_err(unexpected)?,
    })
}

fn parse_export_operation_state(value: &str) -> Result<ExportOperationState, RepositoryError> {
    match value {
        "queued" => Ok(ExportOperationState::Queued),
        "materializing" => Ok(ExportOperationState::Materializing),
        "ready" => Ok(ExportOperationState::Ready),
        "failed" => Ok(ExportOperationState::Failed),
        "revoked" => Ok(ExportOperationState::Revoked),
        "expired" => Ok(ExportOperationState::Expired),
        _ => Err(RepositoryError::Unexpected(
            "export operation returned an unknown state".to_owned(),
        )),
    }
}

impl PostgresMemoryRepository {
    async fn create_export_once(
        &self,
        request: NewExport,
    ) -> Result<ExportCreateOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
        set_scope(&mut transaction, request.tenant_id, request.subject_id).await?;
        sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
            .bind(&request.principal_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;

        let existing = sqlx::query(
            r#"
            SELECT *
            FROM memory.export_operations
            WHERE tenant_id = $1 AND subject_id = $2
              AND principal_id = $3 AND idempotency_key = $4
            FOR UPDATE
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(&request.principal_id.0)
        .bind(&request.idempotency.key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        if let Some(row) = existing {
            let fingerprint: String = row
                .try_get::<String, _>("request_fingerprint_sha256")
                .map_err(unexpected)?;
            if fingerprint.trim() != request.idempotency.fingerprint {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let operation = export_operation_from_row(&row)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(ExportCreateOutcome {
                operation,
                replayed: true,
            });
        }

        let allowed_sensitivities = request.allowed_sensitivities;
        sqlx::query(
            r#"
            INSERT INTO memory.export_operations (
                tenant_id, subject_id, export_id, principal_id, allowed_sensitivities, profile,
                idempotency_key, request_fingerprint_sha256,
                authorization_scope_sha256, state, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'queued', $10)
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(request.export_id.0)
        .bind(&request.principal_id.0)
        .bind(&allowed_sensitivities)
        .bind(&request.profile)
        .bind(&request.idempotency.key)
        .bind(&request.idempotency.fingerprint)
        .bind(&request.authorization_scope_sha256)
        .bind(request.expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;

        sqlx::query(
            r#"
            INSERT INTO memory.export_manifest_items (
                tenant_id, subject_id, export_id, record_kind, record_id,
                recorded_at, source_content_sha256
            )
            SELECT e.tenant_id, e.subject_id, $3, 'episode', e.episode_id,
                   e.recorded_at, e.payload_sha256
            FROM memory.episodes AS e
            WHERE e.tenant_id = $1 AND e.subject_id = $2
              AND e.sensitivity = ANY($4::text[])
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(request.export_id.0)
        .bind(&allowed_sensitivities)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        sqlx::query(
            r#"
            INSERT INTO memory.export_manifest_items (
                tenant_id, subject_id, export_id, record_kind, record_id,
                recorded_at, source_content_sha256
            )
            SELECT revision.tenant_id, revision.subject_id, $3, 'fact_revision',
                   revision.revision_id, revision.recorded_at, revision.content_sha256
            FROM memory.fact_revisions AS revision
            JOIN memory.fact_revision_governance AS governance
              ON governance.tenant_id = revision.tenant_id
             AND governance.subject_id = revision.subject_id
             AND governance.case_id = revision.case_id
             AND governance.fact_id = revision.fact_id
             AND governance.revision_id = revision.revision_id
            WHERE revision.tenant_id = $1 AND revision.subject_id = $2
              AND revision.sensitivity = ANY($4::text[])
              AND governance.lifecycle_state = 'active'
              AND (
                  governance.retention_expires_at IS NULL
                  OR governance.retention_expires_at > clock_timestamp()
              )
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(request.export_id.0)
        .bind(&allowed_sensitivities)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;
        sqlx::query(
            r#"
            INSERT INTO memory.export_manifest_items (
                tenant_id, subject_id, export_id, record_kind, record_id,
                recorded_at, source_content_sha256
            )
            SELECT revision.tenant_id, revision.subject_id, $3, 'checkpoint',
                   revision.revision_id, revision.recorded_at, revision.state_sha256
            FROM memory.checkpoint_revisions AS revision
            WHERE revision.tenant_id = $1 AND revision.subject_id = $2
              AND revision.sensitivity = ANY($4::text[])
              AND revision.expires_at > clock_timestamp()
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(request.export_id.0)
        .bind(&allowed_sensitivities)
        .execute(&mut *transaction)
        .await
        .map_err(map_export_sqlx)?;

        let row = sqlx::query(export_operation_select_sql())
            .bind(request.tenant_id.0)
            .bind(request.subject_id.0)
            .bind(request.export_id.0)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_export_sqlx)?;
        let operation = export_operation_from_row(&row)?;
        transaction.commit().await.map_err(map_export_sqlx)?;
        Ok(ExportCreateOutcome {
            operation,
            replayed: false,
        })
    }
}

impl PostgresMemoryRepository {
    async fn create_deletion_operation_once(
        &self,
        request: CreateDeletionRequest,
    ) -> Result<CreateDeletionOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await
            .map_err(map_deletion_sqlx)?;
        set_scope_context(&mut transaction, request.tenant_id, request.subject_id).await?;
        sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
            .bind(&request.principal_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let target_names = request
            .configured_targets
            .iter()
            .map(|target| target.as_str().to_owned())
            .collect::<Vec<_>>();
        let operation_id = DeletionOperationId(uuid::Uuid::now_v7());
        let row = sqlx::query(
            r#"
            SELECT operation_id, lifecycle_state, state_version, replayed, targets
            FROM memory.create_deletion_operation(
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            "#,
        )
        .bind(request.tenant_id.0)
        .bind(request.subject_id.0)
        .bind(operation_id.0)
        .bind(&request.principal_id.0)
        .bind(&request.idempotency_key)
        .bind(&request.request_fingerprint_sha256)
        .bind(target_names)
        .bind(i32::try_from(request.retention_hours).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_deletion_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        let state: String = row.try_get("lifecycle_state").map_err(unexpected)?;
        let lifecycle_state = parse_deletion_state(&state)?;
        let targets: serde_json::Value = row.try_get("targets").map_err(unexpected)?;
        Ok(CreateDeletionOutcome {
            operation_id: DeletionOperationId(row.try_get("operation_id").map_err(unexpected)?),
            lifecycle_state,
            state_version: u64::try_from(
                row.try_get::<i64, _>("state_version").map_err(unexpected)?,
            )
            .map_err(unexpected)?,
            replayed: row.try_get("replayed").map_err(unexpected)?,
            targets: parse_deletion_targets(targets, lifecycle_state)?,
        })
    }
}

impl PostgresMemoryRepository {
    async fn transition_subject_lifecycle(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        target: SubjectLifecycleState,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        const MAX_SERIALIZATION_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_SERIALIZATION_ATTEMPTS {
            match self
                .transition_subject_lifecycle_once(tenant_id, subject_id, target)
                .await
            {
                Err(RepositoryError::SerializationRetry)
                    if attempt < MAX_SERIALIZATION_ATTEMPTS => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded lifecycle serialization retry loop always returns")
    }

    async fn transition_subject_lifecycle_once(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        target: SubjectLifecycleState,
    ) -> Result<SubjectLifecycle, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await
            .map_err(map_lifecycle_sqlx)?;
        set_scope_context(&mut transaction, tenant_id, subject_id).await?;
        let state_version = match target {
            SubjectLifecycleState::DeletionPending => sqlx::query_scalar::<_, i64>(
                "SELECT memory.transition_subject_to_deletion_pending($1, $2)",
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_lifecycle_sqlx)?,
            SubjectLifecycleState::Deleted => {
                sqlx::query_scalar::<_, i64>("SELECT memory.transition_subject_to_deleted($1, $2)")
                    .bind(tenant_id.0)
                    .bind(subject_id.0)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(map_lifecycle_sqlx)?
            }
            SubjectLifecycleState::Active => {
                return Err(RepositoryError::Unexpected(
                    "active is not a lifecycle transition target".to_owned(),
                ));
            }
        };
        transaction.commit().await.map_err(map_lifecycle_sqlx)?;
        Ok(SubjectLifecycle {
            tenant_id,
            subject_id,
            state: target,
            state_version: u64::try_from(state_version).map_err(unexpected)?,
        })
    }
}

#[async_trait]
impl EpisodeRepository for PostgresMemoryRepository {
    async fn append(
        &self,
        episode: NewEpisode,
        idempotency: IdempotencyRequest,
    ) -> Result<AppendOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, episode.tenant_id, episode.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: episode.tenant_id,
            subject_id: episode.subject_id,
            principal_id: &episode.writer_principal_id.0,
            operation_id: "appendEpisode",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let stored: Episode = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(AppendOutcome {
                episode: stored,
                replayed: true,
            });
        }

        let row = sqlx::query(
            r#"
            INSERT INTO memory.episodes (
                tenant_id, subject_id, case_id, episode_id, kind, observed_at,
                writer_principal_id, source_type, source_uri, external_id,
                sensitivity, retention_policy_id, schema_version, payload, payload_sha256
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING tenant_id, subject_id, case_id, episode_id, kind, observed_at,
                recorded_at, writer_principal_id, source_type, source_uri, external_id,
                sensitivity, retention_policy_id, schema_version, payload, payload_sha256
            "#,
        )
        .bind(episode.tenant_id.0)
        .bind(episode.subject_id.0)
        .bind(episode.case_id.0)
        .bind(episode.episode_id.0)
        .bind(episode.kind.as_str())
        .bind(episode.observed_at)
        .bind(&episode.writer_principal_id.0)
        .bind(episode.provenance.source_type.as_str())
        .bind(&episode.provenance.source_uri)
        .bind(&episode.provenance.external_id)
        .bind(episode.sensitivity.as_str())
        .bind(episode.retention_policy_id.as_str())
        .bind(i32::try_from(episode.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .bind(&episode.payload)
        .bind(&episode.payload_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: episode.tenant_id,
                subject_id: episode.subject_id,
                case_id: episode.case_id,
                principal_id: &episode.writer_principal_id.0,
                operation_id: "appendEpisode",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: Some(episode.episode_id),
                resource_fact_id: None,
                resource_revision_id: None,
                resource_checkpoint: None,
                event_type: "memory.episode.appended.v1",
            },
        )
        .await?;

        let stored_episode = episode_from_row(&row)?;
        let response_body = serde_json::to_value(&stored_episode).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", stored_episode.payload_sha256);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/episodes/{}",
            stored_episode.tenant_id.0, stored_episode.subject_id.0, stored_episode.episode_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: Some(episode.episode_id),
                resource_fact_id: None,
                resource_checkpoint: None,
                status: 201,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(AppendOutcome {
            episode: stored_episode,
            replayed: false,
        })
    }

    async fn get(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        episode_id: EpisodeId,
    ) -> Result<Episode, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;

        let row = select_episode(&mut transaction, tenant_id, subject_id, episode_id).await?;

        transaction.commit().await.map_err(unexpected)?;
        row.ok_or(RepositoryError::NotFound)
    }
}

struct CheckpointHead {
    case_id: CaseId,
    checkpoint_id: CheckpointId,
    revision_id: CheckpointRevisionId,
    revision_number: i64,
    expired: bool,
}

async fn select_checkpoint_head_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    agent_id: AgentId,
    thread_id: ThreadId,
) -> Result<Option<CheckpointHead>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT case_id, checkpoint_id, head_revision_id, head_revision_number,
            expires_at <= clock_timestamp() AS expired
        FROM memory.checkpoints
        WHERE tenant_id = $1
          AND subject_id = $2
          AND agent_id = $3
          AND thread_id = $4
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(agent_id.0)
    .bind(thread_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    row.as_ref()
        .map(|row| {
            Ok(CheckpointHead {
                case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
                checkpoint_id: CheckpointId(row.try_get("checkpoint_id").map_err(unexpected)?),
                revision_id: CheckpointRevisionId(
                    row.try_get("head_revision_id").map_err(unexpected)?,
                ),
                revision_number: row.try_get("head_revision_number").map_err(unexpected)?,
                expired: row.try_get("expired").map_err(unexpected)?,
            })
        })
        .transpose()
}

async fn persist_effect_transitions(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &NewCheckpointRevision,
    checkpoint_id: CheckpointId,
) -> Result<(), RepositoryError> {
    for transition in &revision.effect_transitions {
        match transition {
            NewEffectTransition::Prepare(effect) => {
                let recovery_mode = match effect.recovery_mode {
                    EffectRecoveryMode::IdempotencyKey => "idempotency_key",
                    EffectRecoveryMode::Reconcile => "reconcile",
                };
                sqlx::query(
                    r#"
                    INSERT INTO memory.checkpoint_effect_intents (
                        tenant_id, subject_id, agent_id, thread_id, checkpoint_id,
                        effect_id, effect_key, kind, recovery_mode, prepared_revision_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    "#,
                )
                .bind(revision.tenant_id.0)
                .bind(revision.subject_id.0)
                .bind(revision.agent_id.0)
                .bind(revision.thread_id.0)
                .bind(checkpoint_id.0)
                .bind(effect.effect_id.0)
                .bind(effect.effect_key.as_str())
                .bind(effect.kind.as_str())
                .bind(recovery_mode)
                .bind(revision.revision_id.0)
                .execute(&mut **transaction)
                .await
                .map_err(map_checkpoint_sqlx)?;
            }
            NewEffectTransition::Complete(effect) => {
                let receipt = serde_json::to_value(&effect.receipt).map_err(unexpected)?;
                sqlx::query(
                    r#"
                    INSERT INTO memory.checkpoint_effect_receipts (
                        tenant_id, subject_id, agent_id, thread_id, checkpoint_id,
                        effect_id, completed_revision_id, completed_at,
                        receipt, receipt_sha256
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    "#,
                )
                .bind(revision.tenant_id.0)
                .bind(revision.subject_id.0)
                .bind(revision.agent_id.0)
                .bind(revision.thread_id.0)
                .bind(checkpoint_id.0)
                .bind(effect.effect_id.0)
                .bind(revision.revision_id.0)
                .bind(effect.receipt.observed_at)
                .bind(receipt)
                .bind(&effect.receipt.outcome_sha256)
                .execute(&mut **transaction)
                .await
                .map_err(map_checkpoint_sqlx)?;
            }
        }
    }
    Ok(())
}

async fn select_checkpoint_view(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    agent_id: AgentId,
    thread_id: ThreadId,
) -> Result<Option<CheckpointView>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT checkpoint.case_id, checkpoint.checkpoint_id,
            revision.revision_id, revision.parent_revision_id,
            revision.revision_number, revision.recorded_at,
            revision.state, revision.state_schema_version, revision.state_sha256,
            revision.source_type, revision.source_uri, revision.external_id,
            revision.sensitivity, revision.retention_policy_id,
            revision.expires_at, revision.writer_principal_id, revision.schema_version
        FROM memory.checkpoints AS checkpoint
        JOIN memory.checkpoint_revisions AS revision
          ON revision.tenant_id = checkpoint.tenant_id
         AND revision.subject_id = checkpoint.subject_id
         AND revision.case_id = checkpoint.case_id
         AND revision.agent_id = checkpoint.agent_id
         AND revision.thread_id = checkpoint.thread_id
         AND revision.checkpoint_id = checkpoint.checkpoint_id
         AND revision.revision_id = checkpoint.head_revision_id
        WHERE checkpoint.tenant_id = $1
          AND checkpoint.subject_id = $2
          AND checkpoint.agent_id = $3
          AND checkpoint.thread_id = $4
          AND checkpoint.expires_at > clock_timestamp()
          AND revision.expires_at > clock_timestamp()
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(agent_id.0)
    .bind(thread_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let checkpoint_id = CheckpointId(row.try_get("checkpoint_id").map_err(unexpected)?);
    let effect_rows = sqlx::query(
        r#"
        SELECT intent.effect_id, intent.effect_key, intent.kind,
            intent.recovery_mode, intent.prepared_at,
            receipt.completed_at, receipt.receipt
        FROM memory.checkpoint_effect_intents AS intent
        LEFT JOIN memory.checkpoint_effect_receipts AS receipt
          ON receipt.tenant_id = intent.tenant_id
         AND receipt.subject_id = intent.subject_id
         AND receipt.agent_id = intent.agent_id
         AND receipt.thread_id = intent.thread_id
         AND receipt.checkpoint_id = intent.checkpoint_id
         AND receipt.effect_id = intent.effect_id
        WHERE intent.tenant_id = $1
          AND intent.subject_id = $2
          AND intent.agent_id = $3
          AND intent.thread_id = $4
          AND intent.checkpoint_id = $5
        ORDER BY intent.prepared_at, intent.effect_id
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(agent_id.0)
    .bind(thread_id.0)
    .bind(checkpoint_id.0)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let effects = effect_rows
        .iter()
        .map(checkpoint_effect_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let parent_revision_id: Option<uuid::Uuid> =
        row.try_get("parent_revision_id").map_err(unexpected)?;
    let revision_number: i64 = row.try_get("revision_number").map_err(unexpected)?;
    let state_schema_version: i32 = row.try_get("state_schema_version").map_err(unexpected)?;
    let schema_version: i32 = row.try_get("schema_version").map_err(unexpected)?;
    Ok(Some(CheckpointView {
        tenant_id,
        subject_id,
        agent_id,
        thread_id,
        case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
        checkpoint_id,
        checkpoint_revision_id: CheckpointRevisionId(
            row.try_get("revision_id").map_err(unexpected)?,
        ),
        parent_revision_id: parent_revision_id.map(CheckpointRevisionId),
        revision_number: u64::try_from(revision_number).map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        snapshot: CheckpointSnapshot {
            state: row.try_get("state").map_err(unexpected)?,
            effects,
        },
        state_schema_version: u32::try_from(state_schema_version).map_err(unexpected)?,
        provenance: Provenance {
            source_type: text_value_from_row::<SourceType>(&row, "source_type")?,
            source_uri: row.try_get("source_uri").map_err(unexpected)?,
            external_id: row.try_get("external_id").map_err(unexpected)?,
        },
        sensitivity: text_value_from_row::<Sensitivity>(&row, "sensitivity")?,
        retention_policy_id: text_value_from_row::<RetentionPolicyId>(&row, "retention_policy_id")?,
        expires_at: row.try_get("expires_at").map_err(unexpected)?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        schema_version: u32::try_from(schema_version).map_err(unexpected)?,
        state_sha256: row.try_get("state_sha256").map_err(unexpected)?,
    }))
}

fn checkpoint_effect_from_row(row: &PgRow) -> Result<CheckpointEffect, RepositoryError> {
    let recovery_mode: String = row.try_get("recovery_mode").map_err(unexpected)?;
    let recovery_mode = match recovery_mode.as_str() {
        "idempotency_key" => EffectRecoveryMode::IdempotencyKey,
        "reconcile" => EffectRecoveryMode::Reconcile,
        value => {
            return Err(RepositoryError::Unexpected(format!(
                "stored checkpoint recovery mode is invalid: {value}"
            )));
        }
    };
    let receipt: Option<serde_json::Value> = row.try_get("receipt").map_err(unexpected)?;
    let receipt = receipt
        .map(serde_json::from_value::<EffectReceipt>)
        .transpose()
        .map_err(unexpected)?;
    let completed_at: Option<OffsetDateTime> = row.try_get("completed_at").map_err(unexpected)?;
    Ok(CheckpointEffect {
        effect_id: EffectId(row.try_get("effect_id").map_err(unexpected)?),
        effect_key: text_value_from_row::<EffectKey>(row, "effect_key")?,
        kind: text_value_from_row::<EffectKind>(row, "kind")?,
        recovery_mode,
        status: if receipt.is_some() {
            EffectStatus::Completed
        } else {
            EffectStatus::Prepared
        },
        prepared_at: row.try_get("prepared_at").map_err(unexpected)?,
        completed_at,
        receipt,
    })
}

async fn select_fact_view(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    fact_id: FactId,
    valid_at: OffsetDateTime,
    recorded_at: OffsetDateTime,
    evaluated_at: OffsetDateTime,
) -> Result<Option<FactView>, RepositoryError> {
    let metadata = sqlx::query(
        r#"
        SELECT case_id, namespace, fact_key
        FROM memory.facts
        WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let case_id = CaseId(metadata.try_get("case_id").map_err(unexpected)?);
    let namespace = text_value_from_row::<FactNamespace>(&metadata, "namespace")?;
    let key = text_value_from_row::<FactKey>(&metadata, "fact_key")?;

    let head = sqlx::query(
        r#"
        SELECT revision_id
        FROM memory.fact_revisions
        WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3
          AND recorded_at <= $4
        ORDER BY revision_no DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .bind(recorded_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(head) = head else {
        return Ok(None);
    };
    let head_revision_id = RevisionId(head.try_get("revision_id").map_err(unexpected)?);

    let revision = sqlx::query(
        r#"
        SELECT fr.case_id, fr.revision_id, fr.revision_no,
            fr.supersedes_revision_id, fr.observed_at, fr.recorded_at,
            lower(fr.valid_during) AS valid_from,
            CASE WHEN upper_inf(fr.valid_during) THEN NULL
                 ELSE upper(fr.valid_during) END AS valid_until,
            fr.value, fr.confidence::double precision AS confidence,
            fr.writer_principal_id, fr.write_policy_id, fr.write_policy_version,
            fr.sensitivity, fr.retention_policy_id, fr.schema_version,
            ARRAY(
                SELECT evidence.episode_id
                FROM memory.fact_revision_evidence AS evidence
                WHERE evidence.tenant_id = fr.tenant_id
                  AND evidence.subject_id = fr.subject_id
                  AND evidence.case_id = fr.case_id
                  AND evidence.fact_id = fr.fact_id
                  AND evidence.revision_id = fr.revision_id
                ORDER BY evidence.episode_id
            ) AS evidence_episode_ids
        FROM memory.fact_revisions AS fr
        WHERE fr.tenant_id = $1 AND fr.subject_id = $2 AND fr.fact_id = $3
          AND fr.valid_during @> $4::timestamptz
          AND fr.recorded_at <= $5
        ORDER BY fr.revision_no DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .bind(valid_at)
    .bind(recorded_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let revision = revision
        .as_ref()
        .map(|row| fact_revision_from_row(row, tenant_id, subject_id, fact_id, &namespace, &key))
        .transpose()?;

    Ok(Some(FactView {
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        namespace,
        key,
        head_revision_id,
        evaluated_at,
        valid_at,
        recorded_at,
        revision,
    }))
}

fn text_value_from_row<T>(row: &PgRow, column: &'static str) -> Result<T, RepositoryError>
where
    T: TryFrom<String>,
    T::Error: std::fmt::Display,
{
    let raw: String = row.try_get(column).map_err(unexpected)?;
    T::try_from(raw).map_err(unexpected)
}

fn fact_revision_from_row(
    row: &PgRow,
    tenant_id: TenantId,
    subject_id: SubjectId,
    fact_id: FactId,
    namespace: &FactNamespace,
    key: &FactKey,
) -> Result<FactRevision, RepositoryError> {
    let revision_number: i64 = row.try_get("revision_no").map_err(unexpected)?;
    let schema_version: i32 = row.try_get("schema_version").map_err(unexpected)?;
    let supersedes_revision_id: Option<uuid::Uuid> =
        row.try_get("supersedes_revision_id").map_err(unexpected)?;
    let evidence_episode_ids: Vec<uuid::Uuid> =
        row.try_get("evidence_episode_ids").map_err(unexpected)?;
    Ok(FactRevision {
        tenant_id,
        subject_id,
        case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
        fact_id,
        revision_id: RevisionId(row.try_get("revision_id").map_err(unexpected)?),
        revision_number: u64::try_from(revision_number).map_err(unexpected)?,
        supersedes_revision_id: supersedes_revision_id.map(RevisionId),
        namespace: namespace.clone(),
        key: key.clone(),
        value: row.try_get("value").map_err(unexpected)?,
        observed_at: row.try_get("observed_at").map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        valid_time: ValidTime {
            from: row.try_get("valid_from").map_err(unexpected)?,
            until: row.try_get("valid_until").map_err(unexpected)?,
        },
        evidence_episode_ids: evidence_episode_ids.into_iter().map(EpisodeId).collect(),
        write_policy: WritePolicy {
            id: text_value_from_row::<WritePolicyId>(row, "write_policy_id")?,
            version: text_value_from_row::<WritePolicyVersion>(row, "write_policy_version")?,
        },
        confidence: row.try_get("confidence").map_err(unexpected)?,
        sensitivity: text_value_from_row::<Sensitivity>(row, "sensitivity")?,
        retention_policy_id: text_value_from_row::<RetentionPolicyId>(row, "retention_policy_id")?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        schema_version: u32::try_from(schema_version).map_err(unexpected)?,
    })
}

async fn select_episode(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    episode_id: EpisodeId,
) -> Result<Option<Episode>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, subject_id, case_id, episode_id, kind, observed_at,
            recorded_at, writer_principal_id, source_type, source_uri, external_id,
            sensitivity, retention_policy_id, schema_version, payload, payload_sha256
        FROM memory.episodes
        WHERE tenant_id = $1 AND subject_id = $2 AND episode_id = $3
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(episode_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    row.as_ref().map(episode_from_row).transpose()
}

struct GovernedWrite<'a> {
    tenant_id: TenantId,
    subject_id: SubjectId,
    case_id: CaseId,
    principal_id: &'a str,
    operation_id: &'a str,
    request_fingerprint: &'a str,
    resource_episode_id: Option<EpisodeId>,
    resource_fact_id: Option<FactId>,
    resource_revision_id: Option<RevisionId>,
    resource_checkpoint: Option<CheckpointResource>,
    event_type: &'a str,
}

#[derive(Clone, Copy)]
struct CheckpointResource {
    agent_id: AgentId,
    thread_id: ThreadId,
    checkpoint_id: CheckpointId,
    revision_id: CheckpointRevisionId,
}

#[derive(Clone, Copy)]
struct IdempotencyScope<'a> {
    tenant_id: TenantId,
    subject_id: SubjectId,
    principal_id: &'a str,
    operation_id: &'a str,
}

struct IdempotencyCompletion<'a> {
    scope: IdempotencyScope<'a>,
    key: &'a str,
    resource_episode_id: Option<EpisodeId>,
    resource_fact_id: Option<FactId>,
    resource_checkpoint: Option<CheckpointResource>,
    status: i16,
    body: serde_json::Value,
    etag: &'a str,
    location: &'a str,
}

async fn reserve_idempotency(
    transaction: &mut Transaction<'_, Postgres>,
    scope: IdempotencyScope<'_>,
    idempotency: &IdempotencyRequest,
) -> Result<Option<serde_json::Value>, RepositoryError> {
    sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
        .bind(scope.principal_id)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    let reserved = sqlx::query(
        r#"
        INSERT INTO memory.idempotency_receipts (
            tenant_id, subject_id, principal_id, operation_id,
            idempotency_key, request_fingerprint, state
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'in_progress')
        ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key)
            DO NOTHING
        RETURNING true AS reserved
        "#,
    )
    .bind(scope.tenant_id.0)
    .bind(scope.subject_id.0)
    .bind(scope.principal_id)
    .bind(scope.operation_id)
    .bind(&idempotency.key)
    .bind(&idempotency.fingerprint)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?
    .is_some();
    if reserved {
        return Ok(None);
    }

    let receipt = sqlx::query(
        r#"
        SELECT request_fingerprint, state, response_body
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND principal_id = $2
          AND operation_id = $3
          AND idempotency_key = $4
        FOR UPDATE
        "#,
    )
    .bind(scope.tenant_id.0)
    .bind(scope.principal_id)
    .bind(scope.operation_id)
    .bind(&idempotency.key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let stored_fingerprint: String = receipt.try_get("request_fingerprint").map_err(unexpected)?;
    if stored_fingerprint != idempotency.fingerprint {
        return Err(RepositoryError::IdempotencyKeyReused);
    }
    let state: String = receipt.try_get("state").map_err(unexpected)?;
    if state != "completed" {
        return Err(RepositoryError::IdempotencyInProgress);
    }
    receipt
        .try_get("response_body")
        .map(Some)
        .map_err(unexpected)
}

async fn complete_idempotency(
    transaction: &mut Transaction<'_, Postgres>,
    completion: IdempotencyCompletion<'_>,
) -> Result<(), RepositoryError> {
    let checkpoint = completion.resource_checkpoint;
    let result = sqlx::query(
        r#"
        UPDATE memory.idempotency_receipts
        SET state = 'completed', resource_episode_id = $1, resource_fact_id = $2,
            resource_checkpoint_agent_id = $3, resource_checkpoint_thread_id = $4,
            resource_checkpoint_id = $5, resource_checkpoint_revision_id = $6,
            response_status = $7, response_body = $8, response_etag = $9,
            response_location = $10, completed_at = clock_timestamp()
        WHERE tenant_id = $11
          AND subject_id = $12
          AND principal_id = $13
          AND operation_id = $14
          AND idempotency_key = $15
          AND state = 'in_progress'
        "#,
    )
    .bind(completion.resource_episode_id.map(|id| id.0))
    .bind(completion.resource_fact_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .bind(completion.status)
    .bind(completion.body)
    .bind(completion.etag)
    .bind(completion.location)
    .bind(completion.scope.tenant_id.0)
    .bind(completion.scope.subject_id.0)
    .bind(completion.scope.principal_id)
    .bind(completion.scope.operation_id)
    .bind(completion.key)
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    if result.rows_affected() != 1 {
        return Err(RepositoryError::Unexpected(
            "idempotency receipt completion did not update one row".to_owned(),
        ));
    }
    Ok(())
}

async fn record_governed_write(
    transaction: &mut Transaction<'_, Postgres>,
    write: GovernedWrite<'_>,
) -> Result<(), RepositoryError> {
    let checkpoint = write.resource_checkpoint;
    sqlx::query(
        r#"
        INSERT INTO memory.write_audit_receipts (
            tenant_id, subject_id, case_id, principal_id, operation_id,
            authorization_decision, authorization_context, request_fingerprint,
            resource_episode_id, resource_fact_id, resource_revision_id,
            resource_checkpoint_agent_id, resource_checkpoint_thread_id,
            resource_checkpoint_id, resource_checkpoint_revision_id
        )
        VALUES (
            $1, $2, $3, $4, $5, 'authorized',
            jsonb_build_object(
                'principal_id', $4::text,
                'tenant_id', $1::uuid,
                'subject_id', $2::uuid
            ),
            $6, $7, $8, $9, $10, $11, $12, $13
        )
        "#,
    )
    .bind(write.tenant_id.0)
    .bind(write.subject_id.0)
    .bind(write.case_id.0)
    .bind(write.principal_id)
    .bind(write.operation_id)
    .bind(write.request_fingerprint)
    .bind(write.resource_episode_id.map(|id| id.0))
    .bind(write.resource_fact_id.map(|id| id.0))
    .bind(write.resource_revision_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;

    sqlx::query(
        r#"
        INSERT INTO memory.outbox_intents (
            tenant_id, subject_id, case_id, event_type,
            resource_episode_id, resource_fact_id, resource_revision_id,
            resource_checkpoint_agent_id, resource_checkpoint_thread_id,
            resource_checkpoint_id, resource_checkpoint_revision_id, payload
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            jsonb_strip_nulls(jsonb_build_object(
                'schema_version', 1,
                'tenant_id', $1::uuid,
                'subject_id', $2::uuid,
                'case_id', $3::uuid,
                'episode_id', $5::uuid,
                'fact_id', $6::uuid,
                'revision_id', $7::uuid,
                'agent_id', $8::uuid,
                'thread_id', $9::uuid,
                'checkpoint_id', $10::uuid,
                'checkpoint_revision_id', $11::uuid
            ))
        )
        "#,
    )
    .bind(write.tenant_id.0)
    .bind(write.subject_id.0)
    .bind(write.case_id.0)
    .bind(write.event_type)
    .bind(write.resource_episode_id.map(|id| id.0))
    .bind(write.resource_fact_id.map(|id| id.0))
    .bind(write.resource_revision_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    Ok(())
}

async fn set_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), RepositoryError> {
    set_scope_context(transaction, tenant_id, subject_id).await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared(\
            hashtextextended($1::text || ':' || $2::text, 0)\
        )",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let lifecycle_state = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lifecycle_state
        FROM memory.subject_lifecycles
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    if lifecycle_state.is_some_and(|state| state != "active") {
        return Err(RepositoryError::SubjectUnavailable);
    }
    Ok(())
}

async fn set_scope_context(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

async fn set_retrieval_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    principal_id: &PrincipalId,
    allowed_sensitivities: &[Sensitivity],
) -> Result<(), RepositoryError> {
    set_scope(transaction, tenant_id, subject_id).await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
        .bind(&principal_id.0)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    let allowed_sensitivities = serde_json::to_string(
        &allowed_sensitivities
            .iter()
            .map(Sensitivity::as_str)
            .collect::<Vec<_>>(),
    )
    .map_err(unexpected)?;
    sqlx::query("SELECT set_config('palimpsest.allowed_sensitivities', $1, true)")
        .bind(allowed_sensitivities)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

fn lexical_candidate_from_row(row: &PgRow) -> Result<LexicalCandidate, RepositoryError> {
    let exact_identity_rank: Option<i16> =
        row.try_get("exact_identity_rank").map_err(unexpected)?;
    let lexical_rank: Option<i64> = row.try_get("lexical_rank").map_err(unexpected)?;
    let lexical_score: String = row.try_get("lexical_score").map_err(unexpected)?;
    let final_score = lexical_score.clone();
    let case_id: uuid::Uuid = row.try_get("case_id").map_err(unexpected)?;
    let fact_id: uuid::Uuid = row.try_get("fact_id").map_err(unexpected)?;
    let revision_id: uuid::Uuid = row.try_get("revision_id").map_err(unexpected)?;
    let source_content_sha256: String = row.try_get("content_sha256").map_err(unexpected)?;
    let projection_sha256: String = row.try_get("projection_sha256").map_err(unexpected)?;
    let item_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&serde_json::json!({
            "case_id": case_id,
            "fact_id": fact_id,
            "revision_id": revision_id,
            "exact_identity_rank": exact_identity_rank,
            "lexical_rank": lexical_rank,
            "lexical_score": lexical_score,
            "final_score": final_score,
            "source_content_sha256": source_content_sha256,
            "projection_sha256": projection_sha256,
        }))
        .map_err(unexpected)?,
    ));
    Ok(LexicalCandidate {
        case_id,
        fact_id,
        revision_id,
        exact_identity_rank,
        lexical_rank,
        lexical_score,
        final_score,
        source_content_sha256,
        projection_sha256,
        item_sha256,
    })
}

fn hybrid_candidate_from_row(
    row: &PgRow,
    temporal_scoring: bool,
) -> Result<HybridCandidate, RepositoryError> {
    let case_id = row.try_get("case_id").map_err(unexpected)?;
    let fact_id = row.try_get("fact_id").map_err(unexpected)?;
    let revision_id = row.try_get("revision_id").map_err(unexpected)?;
    let exact_identity_rank: Option<i16> =
        row.try_get("exact_identity_rank").map_err(unexpected)?;
    let exact_rank: Option<i64> = row.try_get("exact_rank").map_err(unexpected)?;
    let lexical_rank: Option<i64> = row.try_get("lexical_rank").map_err(unexpected)?;
    let lexical_score = row.try_get("lexical_score").map_err(unexpected)?;
    let vector_rank: Option<i64> = row.try_get("vector_rank").map_err(unexpected)?;
    let vector_distance = row.try_get("vector_distance").map_err(unexpected)?;
    let vector_similarity = row.try_get("vector_similarity").map_err(unexpected)?;
    let mut exact_rrf: String = row.try_get("exact_rrf").map_err(unexpected)?;
    let mut lexical_rrf: String = row.try_get("lexical_rrf").map_err(unexpected)?;
    let mut vector_rrf: String = row.try_get("vector_rrf").map_err(unexpected)?;
    let mut fused_score: String = row.try_get("fused_score").map_err(unexpected)?;
    let source_content_sha256 = row.try_get("content_sha256").map_err(unexpected)?;
    let projection_sha256 = row.try_get("projection_sha256").map_err(unexpected)?;
    let embedding_input_sha256 = row.try_get("embedding_input_sha256").map_err(unexpected)?;
    let embedding_vector_sha256 = row.try_get("embedding_vector_sha256").map_err(unexpected)?;
    let temporal = if temporal_scoring {
        let recency_profile_id: String = row.try_get("recency_profile_id").map_err(unexpected)?;
        let recency_profile_version: String =
            row.try_get("recency_profile_version").map_err(unexpected)?;
        let recency_profile_sha256: String =
            row.try_get("recency_profile_sha256").map_err(unexpected)?;
        let recency_profile = match (
            recency_profile_id.as_str(),
            recency_profile_version.as_str(),
        ) {
            ("stable-v1", "1") => RecencyProfile::StableV1,
            ("active-case-30d-v1", "1") => RecencyProfile::ActiveCase30dV1,
            _ => {
                return Err(RepositoryError::Unexpected(
                    "temporal retrieval recency profile is unsupported".to_owned(),
                ));
            }
        };
        let recency_anchor_at = row.try_get("recency_anchor_at").map_err(unexpected)?;
        let recency_age_us: String = row.try_get("recency_age_us").map_err(unexpected)?;
        let age_us = recency_age_us.parse::<i128>().map_err(unexpected)?;
        let confidence_basis_points: i64 =
            row.try_get("confidence_basis_points").map_err(unexpected)?;
        let importance_basis_points: i64 =
            row.try_get("importance_basis_points").map_err(unexpected)?;
        let confidence_factor = ScoreUnits::from_ratio(i128::from(confidence_basis_points), 10_000)
            .map_err(score_math_unexpected)?;
        let importance = ScoreUnits::from_ratio(i128::from(importance_basis_points), 10_000)
            .map_err(score_math_unexpected)?;
        let exact_identity = match exact_identity_rank {
            Some(1) => ExactIdentityTier::NamespaceAndKey,
            Some(2) => ExactIdentityTier::KeyOnly,
            None => ExactIdentityTier::None,
            Some(_) => {
                return Err(RepositoryError::Unexpected(
                    "temporal retrieval exact identity rank is invalid".to_owned(),
                ));
            }
        };
        let score = score_temporal_retrieval(TemporalScoreInput {
            exact_rank: temporal_rank(exact_rank)?,
            lexical_rank: temporal_rank(lexical_rank)?,
            vector_rank: temporal_rank(vector_rank)?,
            recency_profile,
            valid_at_us: age_us,
            recency_anchor_at_us: 0,
            confidence_factor,
            importance,
            exact_identity,
        })
        .map_err(score_math_unexpected)?;
        exact_rrf = score.exact_rrf.to_string();
        lexical_rrf = score.lexical_rrf.to_string();
        vector_rrf = score.vector_rrf.to_string();
        fused_score = score.fused_score.to_string();
        Some(TemporalCandidate {
            recency_profile_id,
            recency_profile_version,
            recency_profile_sha256,
            recency_anchor_at,
            recency_age_us,
            recency_factor: score.recency_factor.to_string(),
            confidence_factor: score.confidence_factor.to_string(),
            importance_factor: score.importance_factor.to_string(),
            temporal_adjustment: score.temporal_adjustment.to_string(),
            confidence_adjustment: score.confidence_adjustment.to_string(),
            importance_adjustment: score.importance_adjustment.to_string(),
            exact_identity_bonus: score.exact_identity_bonus.to_string(),
            final_score: score.final_score.to_string(),
            order_key: TemporalOrderKey {
                exact_identity_rank: exact_identity_rank
                    .map(u32::try_from)
                    .transpose()
                    .map_err(unexpected)?,
                final_score: score.final_score,
                exact_rank: temporal_rank(exact_rank)?,
                lexical_rank: temporal_rank(lexical_rank)?,
                vector_rank: temporal_rank(vector_rank)?,
                case_id: CaseId(case_id),
                fact_id: FactId(fact_id),
                revision_id: RevisionId(revision_id),
            },
        })
    } else {
        None
    };
    let mut item_document = serde_json::json!({
        "case_id": case_id,
        "fact_id": fact_id,
        "revision_id": revision_id,
        "exact_identity_rank": exact_identity_rank,
        "exact_rank": exact_rank,
        "lexical_rank": lexical_rank,
        "lexical_score": lexical_score,
        "vector_rank": vector_rank,
        "vector_distance": vector_distance,
        "vector_similarity": vector_similarity,
        "exact_rrf": exact_rrf,
        "lexical_rrf": lexical_rrf,
        "vector_rrf": vector_rrf,
        "fused_score": fused_score,
        "source_content_sha256": source_content_sha256,
        "projection_sha256": projection_sha256,
        "embedding_input_sha256": embedding_input_sha256,
        "embedding_vector_sha256": embedding_vector_sha256,
    });
    if let Some(temporal) = &temporal {
        let object = item_document.as_object_mut().ok_or_else(|| {
            RepositoryError::Unexpected("temporal item document is invalid".to_owned())
        })?;
        object.insert(
            "recency_profile_id".to_owned(),
            serde_json::json!(temporal.recency_profile_id),
        );
        object.insert(
            "recency_profile_version".to_owned(),
            serde_json::json!(temporal.recency_profile_version),
        );
        object.insert(
            "recency_profile_sha256".to_owned(),
            serde_json::json!(temporal.recency_profile_sha256),
        );
        object.insert(
            "recency_anchor_at_unix_nanos".to_owned(),
            serde_json::json!(
                temporal
                    .recency_anchor_at
                    .unix_timestamp_nanos()
                    .to_string()
            ),
        );
        object.insert(
            "recency_age_us".to_owned(),
            serde_json::json!(temporal.recency_age_us),
        );
        for (name, value) in [
            ("recency_factor", &temporal.recency_factor),
            ("confidence_factor", &temporal.confidence_factor),
            ("importance_factor", &temporal.importance_factor),
            ("temporal_adjustment", &temporal.temporal_adjustment),
            ("confidence_adjustment", &temporal.confidence_adjustment),
            ("importance_adjustment", &temporal.importance_adjustment),
            ("exact_identity_bonus", &temporal.exact_identity_bonus),
            ("final_score", &temporal.final_score),
        ] {
            object.insert(name.to_owned(), serde_json::json!(value));
        }
    }
    let item_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&item_document).map_err(unexpected)?,
    ));
    Ok(HybridCandidate {
        case_id,
        fact_id,
        revision_id,
        exact_identity_rank,
        exact_rank,
        lexical_rank,
        lexical_score,
        vector_rank,
        vector_distance,
        vector_similarity,
        exact_rrf,
        lexical_rrf,
        vector_rrf,
        fused_score,
        source_content_sha256,
        projection_sha256,
        embedding_input_sha256,
        embedding_vector_sha256,
        temporal,
        item_sha256,
    })
}

fn temporal_rank(rank: Option<i64>) -> Result<Option<u32>, RepositoryError> {
    rank.map(|rank| u32::try_from(rank).map_err(unexpected))
        .transpose()
}

fn score_math_unexpected(error: palimpsest_domain::ScoreMathError) -> RepositoryError {
    RepositoryError::Unexpected(format!("temporal retrieval score is invalid: {error:?}"))
}

async fn select_retrieval_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    retrieval_id: RetrievalId,
    cursor: Option<&str>,
    authorization_scope_sha256: &str,
) -> Result<Option<RetrievalReceipt>, RepositoryError> {
    let receipt = sqlx::query(
        r#"
        SELECT evaluated_at, valid_at, recorded_at, policy_id, policy_version,
            policy_sha256, projection_schema_version,
            authorization_scope_sha256, page_size,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            query_input_sha256, query_vector_sha256
        FROM memory.retrieval_receipts
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
          AND principal_id = NULLIF(
              current_setting('palimpsest.principal_id', true),
              ''
          )
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(retrieval_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    let page_size: i16 = receipt.try_get("page_size").map_err(unexpected)?;
    let after_ordinal = if let Some(cursor) = cursor {
        let Ok(cursor) = uuid::Uuid::parse_str(cursor) else {
            return Ok(None);
        };
        let cursor_row = sqlx::query(
            r#"
            SELECT ordinal
            FROM memory.retrieval_manifest_items
            WHERE tenant_id = $1
              AND subject_id = $2
              AND retrieval_id = $3
              AND principal_id = NULLIF(
                  current_setting('palimpsest.principal_id', true),
                  ''
              )
              AND cursor_token = $4
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(retrieval_id.0)
        .bind(cursor)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(unexpected)?;
        let Some(cursor_row) = cursor_row else {
            return Ok(None);
        };
        cursor_row
            .try_get::<i16, _>("ordinal")
            .map_err(unexpected)?
    } else {
        0
    };
    let rows = sqlx::query(
        r#"
        SELECT manifest.ordinal, manifest.cursor_token, manifest.fact_id,
            manifest.revision_id, manifest.exact_identity_rank,
            manifest.lexical_rank, manifest.lexical_score::text AS lexical_score,
            manifest.final_rank, manifest.final_score::text AS final_score,
            manifest.exact_rank, manifest.vector_rank,
            manifest.vector_distance::text AS vector_distance,
            manifest.vector_similarity::text AS vector_similarity,
            manifest.exact_rrf_contribution::text AS exact_rrf_contribution,
            manifest.lexical_rrf_contribution::text AS lexical_rrf_contribution,
            manifest.vector_rrf_contribution::text AS vector_rrf_contribution,
            manifest.fused_score::text AS fused_score,
            manifest.recency_profile_id,
            manifest.recency_profile_version,
            manifest.recency_profile_sha256,
            manifest.recency_anchor_at,
            manifest.recency_age_us::text AS recency_age_us,
            manifest.recency_factor::text AS recency_factor,
            manifest.confidence_factor::text AS confidence_factor,
            manifest.importance_factor::text AS importance_factor,
            manifest.temporal_adjustment::text AS temporal_adjustment,
            manifest.confidence_adjustment::text AS confidence_adjustment,
            manifest.importance_adjustment::text AS importance_adjustment,
            manifest.exact_identity_bonus::text AS exact_identity_bonus,
            manifest.embedding_input_sha256,
            manifest.embedding_vector_sha256,
            receipt.embedding_profile_id,
            receipt.embedding_profile_version,
            receipt.embedding_profile_sha256,
            receipt.embedding_projection_profile_sha256,
            fact.namespace, fact.fact_key, revision.value,
            ARRAY(
                SELECT evidence.episode_id
                FROM memory.fact_revision_evidence AS evidence
                WHERE evidence.tenant_id = manifest.tenant_id
                  AND evidence.subject_id = manifest.subject_id
                  AND evidence.case_id = manifest.case_id
                  AND evidence.fact_id = manifest.fact_id
                  AND evidence.revision_id = manifest.revision_id
                ORDER BY evidence.episode_id
            ) AS evidence_episode_ids
        FROM memory.authorized_retrieval_manifest AS manifest
        JOIN memory.retrieval_receipts AS receipt
          ON receipt.tenant_id = manifest.tenant_id
         AND receipt.subject_id = manifest.subject_id
         AND receipt.retrieval_id = manifest.retrieval_id
         AND receipt.principal_id = manifest.principal_id
        JOIN memory.facts AS fact
          ON fact.tenant_id = manifest.tenant_id
         AND fact.subject_id = manifest.subject_id
         AND fact.case_id = manifest.case_id
         AND fact.fact_id = manifest.fact_id
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = manifest.tenant_id
         AND revision.subject_id = manifest.subject_id
         AND revision.case_id = manifest.case_id
         AND revision.fact_id = manifest.fact_id
         AND revision.revision_id = manifest.revision_id
        WHERE manifest.tenant_id = $1
          AND manifest.subject_id = $2
          AND manifest.retrieval_id = $3
          AND manifest.ordinal > $4
        ORDER BY manifest.ordinal
        LIMIT $5
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(retrieval_id.0)
    .bind(after_ordinal)
    .bind(i32::from(page_size) + 1)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let has_more = rows.len() > usize::try_from(page_size).map_err(unexpected)?;
    let visible_rows = rows
        .iter()
        .take(usize::try_from(page_size).map_err(unexpected)?)
        .collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| visible_rows.last())
        .flatten()
        .map(|row| row.try_get::<uuid::Uuid, _>("cursor_token"))
        .transpose()
        .map_err(unexpected)?
        .map(|cursor| cursor.to_string());
    let items = visible_rows
        .into_iter()
        .map(retrieval_item_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let policy_id = RetrievalPolicyId::try_from(
        receipt
            .try_get::<String, _>("policy_id")
            .map_err(unexpected)?,
    )
    .map_err(unexpected)?;
    let projection_schema_version: i32 = receipt
        .try_get("projection_schema_version")
        .map_err(unexpected)?;
    let query_embedding = receipt
        .try_get::<Option<String>, _>("embedding_profile_id")
        .map_err(unexpected)?
        .map(|profile_id| {
            Ok(RetrievalQueryEmbeddingLineage {
                profile_id,
                profile_version: required_column(&receipt, "embedding_profile_version")?,
                profile_digest: required_column(&receipt, "embedding_profile_sha256")?,
                projection_profile_id: required_column(
                    &receipt,
                    "embedding_projection_profile_id",
                )?,
                projection_profile_version: required_column(
                    &receipt,
                    "embedding_projection_profile_version",
                )?,
                projection_profile_digest: required_column(
                    &receipt,
                    "embedding_projection_profile_sha256",
                )?,
                input_sha256: required_column(&receipt, "query_input_sha256")?,
                vector_sha256: required_column(&receipt, "query_vector_sha256")?,
            })
        })
        .transpose()?;
    Ok(Some(RetrievalReceipt {
        tenant_id,
        subject_id,
        retrieval_id,
        status: if items.is_empty() {
            "abstained".to_owned()
        } else {
            "results".to_owned()
        },
        evaluated_at: receipt.try_get("evaluated_at").map_err(unexpected)?,
        valid_at: receipt.try_get("valid_at").map_err(unexpected)?,
        recorded_at: receipt.try_get("recorded_at").map_err(unexpected)?,
        policy: RetrievalPolicy {
            id: policy_id,
            version: receipt.try_get("policy_version").map_err(unexpected)?,
            digest: receipt.try_get("policy_sha256").map_err(unexpected)?,
        },
        authorization: RetrievalAuthorizationReceipt {
            decision: "authorized".to_owned(),
            scope_digest: authorization_scope_sha256.to_owned(),
        },
        document_schema_version: u32::try_from(projection_schema_version).map_err(unexpected)?,
        query_embedding,
        items,
        next_cursor,
    }))
}

fn retrieval_item_from_row(row: &PgRow) -> Result<RetrievalItem, RepositoryError> {
    let mut scores = Vec::new();
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("exact_identity_rank")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "exact_identity_rank".to_owned(),
            value: rank.to_string(),
        });
    }
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("exact_rank")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "exact_rank".to_owned(),
            value: rank.to_string(),
        });
        scores.push(RetrievalScore {
            component: "exact_rrf".to_owned(),
            value: row.try_get("exact_rrf_contribution").map_err(unexpected)?,
        });
    }
    let lexical_rank = row
        .try_get::<Option<i64>, _>("lexical_rank")
        .map_err(unexpected)?;
    if let Some(rank) = lexical_rank {
        scores.push(RetrievalScore {
            component: "lexical_rank".to_owned(),
            value: rank.to_string(),
        });
        scores.push(RetrievalScore {
            component: "lexical_score".to_owned(),
            value: row.try_get("lexical_score").map_err(unexpected)?,
        });
        if row
            .try_get::<Option<String>, _>("fused_score")
            .map_err(unexpected)?
            .is_some()
        {
            scores.push(RetrievalScore {
                component: "lexical_rrf".to_owned(),
                value: row
                    .try_get("lexical_rrf_contribution")
                    .map_err(unexpected)?,
            });
        }
    }
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("vector_rank")
        .map_err(unexpected)?
    {
        scores.extend([
            RetrievalScore {
                component: "vector_rank".to_owned(),
                value: rank.to_string(),
            },
            RetrievalScore {
                component: "vector_distance".to_owned(),
                value: row.try_get("vector_distance").map_err(unexpected)?,
            },
            RetrievalScore {
                component: "vector_similarity".to_owned(),
                value: row.try_get("vector_similarity").map_err(unexpected)?,
            },
            RetrievalScore {
                component: "vector_rrf".to_owned(),
                value: row.try_get("vector_rrf_contribution").map_err(unexpected)?,
            },
        ]);
    }
    if let Some(fused_score) = row
        .try_get::<Option<String>, _>("fused_score")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "fused_score".to_owned(),
            value: fused_score,
        });
    }
    if row
        .try_get::<Option<String>, _>("recency_profile_id")
        .map_err(unexpected)?
        .is_some()
    {
        for (component, column) in [
            ("recency_factor", "recency_factor"),
            ("confidence_factor", "confidence_factor"),
            ("importance_factor", "importance_factor"),
            ("temporal_adjustment", "temporal_adjustment"),
            ("confidence_adjustment", "confidence_adjustment"),
            ("importance_adjustment", "importance_adjustment"),
            ("exact_identity_bonus", "exact_identity_bonus"),
        ] {
            scores.push(RetrievalScore {
                component: component.to_owned(),
                value: required_column(row, column)?,
            });
        }
    }
    scores.extend([
        RetrievalScore {
            component: "final_rank".to_owned(),
            value: row
                .try_get::<i16, _>("final_rank")
                .map_err(unexpected)?
                .to_string(),
        },
        RetrievalScore {
            component: "final_score".to_owned(),
            value: row.try_get("final_score").map_err(unexpected)?,
        },
    ]);
    let embedding = row
        .try_get::<Option<String>, _>("embedding_profile_id")
        .map_err(unexpected)?
        .map(|profile_id| {
            Ok(RetrievalEmbeddingLineage {
                profile_id,
                profile_version: required_column(row, "embedding_profile_version")?,
                profile_digest: required_column(row, "embedding_profile_sha256")?,
                projection_sha256: required_column(row, "embedding_projection_profile_sha256")?,
                input_sha256: required_column(row, "embedding_input_sha256")?,
                vector_sha256: required_column(row, "embedding_vector_sha256")?,
            })
        })
        .transpose()?;
    let evidence_episode_ids: Vec<uuid::Uuid> =
        row.try_get("evidence_episode_ids").map_err(unexpected)?;
    Ok(RetrievalItem {
        memory_kind: "fact_revision".to_owned(),
        fact_id: FactId(row.try_get("fact_id").map_err(unexpected)?),
        revision_id: RevisionId(row.try_get("revision_id").map_err(unexpected)?),
        namespace: text_value_from_row::<FactNamespace>(row, "namespace")?,
        key: text_value_from_row::<FactKey>(row, "fact_key")?,
        value: row.try_get("value").map_err(unexpected)?,
        evidence_episode_ids: evidence_episode_ids.into_iter().map(EpisodeId).collect(),
        scores,
        embedding,
    })
}

fn episode_from_row(row: &PgRow) -> Result<Episode, RepositoryError> {
    let schema_version: i32 = row.try_get("schema_version").map_err(unexpected)?;
    Ok(Episode {
        tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
        subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
        case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
        episode_id: EpisodeId(row.try_get("episode_id").map_err(unexpected)?),
        kind: text_value_from_row::<EpisodeKind>(row, "kind")?,
        observed_at: row.try_get("observed_at").map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        provenance: Provenance {
            source_type: text_value_from_row::<SourceType>(row, "source_type")?,
            source_uri: row.try_get("source_uri").map_err(unexpected)?,
            external_id: row.try_get("external_id").map_err(unexpected)?,
        },
        sensitivity: text_value_from_row::<Sensitivity>(row, "sensitivity")?,
        retention_policy_id: text_value_from_row::<RetentionPolicyId>(row, "retention_policy_id")?,
        schema_version: u32::try_from(schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("stored schema version is invalid: {error}"))
        })?,
        payload: row.try_get("payload").map_err(unexpected)?,
        payload_sha256: row.try_get("payload_sha256").map_err(unexpected)?,
    })
}

fn map_sqlx(error: sqlx::Error) -> RepositoryError {
    if error
        .as_database_error()
        .and_then(|database_error| database_error.constraint())
        == Some("fact_retrieval_metadata_policy_known")
    {
        return RepositoryError::WritePolicyRejected;
    }
    if error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "23505")
    {
        RepositoryError::Conflict
    } else {
        unexpected(error)
    }
}

fn map_retrieval_sqlx(error: sqlx::Error) -> RepositoryError {
    if error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "40001")
    {
        RepositoryError::SerializationRetry
    } else {
        unexpected(error)
    }
}

fn map_lifecycle_sqlx(error: sqlx::Error) -> RepositoryError {
    let code = error
        .as_database_error()
        .and_then(|database_error| database_error.code());
    match code.as_deref() {
        Some("40001") => RepositoryError::SerializationRetry,
        Some("P0002") => RepositoryError::NotFound,
        Some("23000" | "55000") => RepositoryError::Conflict,
        _ => unexpected(error),
    }
}

fn map_deletion_sqlx(error: sqlx::Error) -> RepositoryError {
    let code = error
        .as_database_error()
        .and_then(|database_error| database_error.code());
    match code.as_deref() {
        Some("40001") => RepositoryError::SerializationRetry,
        Some("P0002") => RepositoryError::NotFound,
        Some("P0004") => RepositoryError::IdempotencyKeyReused,
        Some("42501") => RepositoryError::NotFound,
        Some("23000" | "55000" | "23503" | "23505" | "23514") => RepositoryError::Conflict,
        _ => unexpected(error),
    }
}

fn map_export_sqlx(error: sqlx::Error) -> RepositoryError {
    let code = error
        .as_database_error()
        .and_then(|database_error| database_error.code());
    match code.as_deref() {
        Some("40001") => RepositoryError::SerializationRetry,
        Some("P0002" | "42501") => RepositoryError::NotFound,
        Some("P0004") => RepositoryError::IdempotencyKeyReused,
        Some("23000" | "55000" | "23503" | "23505" | "23514") => RepositoryError::Conflict,
        _ => unexpected(error),
    }
}

fn parse_deletion_state(value: &str) -> Result<DeletionOperationState, RepositoryError> {
    match value {
        "draining" => Ok(DeletionOperationState::Draining),
        "fenced" => Ok(DeletionOperationState::Fenced),
        "purging" => Ok(DeletionOperationState::Purging),
        "retry_wait" => Ok(DeletionOperationState::RetryWait),
        "verifying" => Ok(DeletionOperationState::Verifying),
        "completed" => Ok(DeletionOperationState::Completed),
        "failed" => Ok(DeletionOperationState::Failed),
        "expired" => Ok(DeletionOperationState::Expired),
        _ => Err(unexpected(format!(
            "deletion operation returned an unknown lifecycle state {value}"
        ))),
    }
}

fn parse_deletion_targets(
    value: serde_json::Value,
    lifecycle_state: DeletionOperationState,
) -> Result<Vec<DeletionTargetView>, RepositoryError> {
    let Some(array) = value.as_array() else {
        return Err(unexpected(
            "deletion operation returned a non-array target ledger",
        ));
    };
    let mut targets = Vec::with_capacity(array.len());
    for entry in array {
        let Some(object) = entry.as_object() else {
            return Err(unexpected(
                "deletion operation returned a non-object target ledger entry",
            ));
        };
        let target_name = match object
            .get("target_name")
            .and_then(serde_json::Value::as_str)
            .and_then(DeletionTargetName::try_from_str)
        {
            Some(target_name) => target_name,
            None => {
                return Err(unexpected(
                    "deletion operation returned an unknown target name",
                ));
            }
        };
        let capability = match object.get("capability").and_then(serde_json::Value::as_str) {
            Some("configured") => DeletionTargetCapability::Configured,
            Some("not_configured") => DeletionTargetCapability::NotConfigured,
            _ => {
                return Err(unexpected(
                    "deletion operation returned an unknown target capability",
                ));
            }
        };
        let state = match object.get("state").and_then(serde_json::Value::as_str) {
            Some("pending") => DeletionTargetState::Pending,
            Some("leased") => DeletionTargetState::Leased,
            Some("done") => DeletionTargetState::Done,
            Some("failed") => DeletionTargetState::Failed,
            Some("not_configured") => DeletionTargetState::NotConfigured,
            _ => {
                return Err(unexpected(
                    "deletion operation returned an unknown target state",
                ));
            }
        };
        let verification = match (capability, lifecycle_state, state) {
            (DeletionTargetCapability::NotConfigured, _, _) => {
                DeletionTargetVerification::NotConfigured
            }
            (
                _,
                DeletionOperationState::Completed | DeletionOperationState::Expired,
                DeletionTargetState::Done,
            ) => DeletionTargetVerification::Verified,
            (_, _, DeletionTargetState::Failed) => DeletionTargetVerification::NotVerified,
            _ => DeletionTargetVerification::Pending,
        };
        let target_key_digest = object
            .get("target_key_digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| unexpected("deletion target omitted its key digest"))?
            .to_owned();
        let lease_id = object
            .get("lease_id")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(unexpected)?;
        let lease_expires_at = object
            .get("lease_expires_at")
            .filter(|value| !value.is_null())
            .and_then(serde_json::Value::as_str)
            .map(|value| OffsetDateTime::parse(value, &Rfc3339))
            .transpose()
            .map_err(unexpected)?;
        targets.push(DeletionTargetView {
            target_name,
            target_key_digest,
            capability,
            state,
            verification,
            attempts: u32::try_from(
                object
                    .get("attempts")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0),
            )
            .map_err(unexpected)?,
            lease_id,
            lease_expires_at,
            effect_receipt_sha256: object
                .get("effect_receipt_sha256")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            sanitized_error: object
                .get("sanitized_error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        });
    }
    Ok(targets)
}

fn map_checkpoint_sqlx(error: sqlx::Error) -> RepositoryError {
    let Some(database_error) = error.as_database_error() else {
        return unexpected(error);
    };
    let code = database_error.code().map(|code| code.into_owned());
    let constraint = database_error.constraint().unwrap_or_default();
    let table = database_error.table().unwrap_or_default();

    if constraint == "checkpoint_retention_policy_active" {
        return RepositoryError::RetentionPolicyRejected;
    }
    if table == "checkpoint_effect_intents" && code.as_deref() == Some("23505") {
        return RepositoryError::EffectKeyConflict;
    }
    if constraint.contains("checkpoint_effect") {
        return RepositoryError::InvalidEffectTransition;
    }
    if constraint.contains("checkpoint_revision_parent_is_head")
        || constraint.contains("checkpoint_head_advances_linearly")
        || constraint.contains("checkpoint_revisions_one_successor")
    {
        return RepositoryError::CheckpointParentConflict;
    }
    if constraint.contains("checkpoints_pkey") {
        return RepositoryError::CheckpointAlreadyExists;
    }
    if code.as_deref() == Some("40001") {
        return RepositoryError::CheckpointParentConflict;
    }
    if code.as_deref() == Some("23505") {
        return RepositoryError::Conflict;
    }
    unexpected(error)
}

fn unexpected(error: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::Unexpected(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn projection_work_is_cancelled_at_the_content_lease_deadline() {
        let result = run_with_content_lease_deadline::<()>(
            OffsetDateTime::now_utc() + time::Duration::milliseconds(10),
            std::future::pending(),
        )
        .await;
        assert!(
            matches!(result, Err(RepositoryError::Unexpected(message)) if message == "projection content lease expired")
        );
    }
}
