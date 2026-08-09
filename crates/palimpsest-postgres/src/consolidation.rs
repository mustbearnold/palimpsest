//! consolidation — durable governed consolidation jobs (spec 011).
//! Mirrors the export/delete repository patterns: scoped transactions for
//! user-facing operations, SECURITY DEFINER claim functions for the worker.

use async_trait::async_trait;
use palimpsest_application::{
    ClaimedConsolidationClaim, ClaimedConsolidationJob, ConsolidationInterpreterConfigView,
    ConsolidationJobView, ConsolidationPolicyView, ConsolidationRepository,
    CreateConsolidationJobOutcome, NewConsolidationInterpreterConfig, NewConsolidationJob,
    NewConsolidationPolicy, PendingConsolidationClaim, RepositoryError, WorkerPolicySnapshot,
};
use palimpsest_domain::{CaseId, EpisodeId, FactId, PrincipalId, RevisionId, SubjectId, TenantId};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, unexpected};

fn map_consolidation_sqlx(error: sqlx::Error) -> RepositoryError {
    match error {
        sqlx::Error::RowNotFound => RepositoryError::NotFound,
        other => RepositoryError::Unexpected(other.to_string()),
    }
}

fn consolidation_job_view_from_row(row: &PgRow) -> Result<ConsolidationJobView, RepositoryError> {
    Ok(ConsolidationJobView {
        job_id: row.try_get("job_id").map_err(unexpected)?,
        source_kind: row.try_get("source_kind").map_err(unexpected)?,
        policy_id: row.try_get("policy_id").map_err(unexpected)?,
        policy_version: row.try_get("policy_version").map_err(unexpected)?,
        lifecycle_state: row.try_get("lifecycle_state").map_err(unexpected)?,
        claims_total: row.try_get("claims_total").map_err(unexpected)?,
        claims_done: row.try_get("claims_done").map_err(unexpected)?,
        claim_cap: row.try_get("claim_cap").map_err(unexpected)?,
        created_at: row.try_get("created_at").map_err(unexpected)?,
        completed_at: row.try_get("completed_at").map_err(unexpected)?,
        failure_reason: row.try_get("failure_reason").map_err(unexpected)?,
    })
}

async fn set_worker_claim_context(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT memory.set_worker_claim_context()")
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

#[async_trait]
impl ConsolidationRepository for PostgresMemoryRepository {
    async fn register_interpreter_config(
        &self,
        tenant_id: TenantId,
        request: NewConsolidationInterpreterConfig,
    ) -> Result<ConsolidationInterpreterConfigView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let config_digest = hex::encode(sha256_bytes(
            format!(
                "{}/{}",
                request.provider_kind, request.prompt_policy_version
            )
            .as_bytes(),
        ));
        let interpreter_config_id = Uuid::now_v7();
        let row = sqlx::query(
            r#"
            INSERT INTO memory.consolidation_interpreter_configs (
                tenant_id, interpreter_config_id, provider_kind,
                prompt_policy_version, config_digest, created_by_principal_id
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, interpreter_config_id) DO NOTHING
            RETURNING tenant_id, interpreter_config_id, provider_kind,
                prompt_policy_version, config_digest
            "#,
        )
        .bind(tenant_id.0)
        .bind(interpreter_config_id)
        .bind(&request.provider_kind)
        .bind(&request.prompt_policy_version)
        .bind(&config_digest)
        .bind(request.created_by_principal_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        let row = match row {
            Some(row) => row,
            None => sqlx::query(
                r#"
                    SELECT tenant_id, interpreter_config_id, provider_kind,
                        prompt_policy_version, config_digest
                    FROM memory.consolidation_interpreter_configs
                    WHERE tenant_id = $1 AND interpreter_config_id = $2
                    "#,
            )
            .bind(tenant_id.0)
            .bind(interpreter_config_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_consolidation_sqlx)?,
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(ConsolidationInterpreterConfigView {
            tenant_id,
            interpreter_config_id: row.try_get("interpreter_config_id").map_err(unexpected)?,
            provider_kind: row.try_get("provider_kind").map_err(unexpected)?,
            prompt_policy_version: row.try_get("prompt_policy_version").map_err(unexpected)?,
            config_digest: row.try_get("config_digest").map_err(unexpected)?,
        })
    }

    async fn get_interpreter_config(
        &self,
        tenant_id: TenantId,
        interpreter_config_id: Uuid,
    ) -> Result<ConsolidationInterpreterConfigView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, interpreter_config_id, provider_kind,
                prompt_policy_version, config_digest
            FROM memory.consolidation_interpreter_configs
            WHERE tenant_id = $1 AND interpreter_config_id = $2
            "#,
        )
        .bind(tenant_id.0)
        .bind(interpreter_config_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(ConsolidationInterpreterConfigView {
            tenant_id,
            interpreter_config_id: row.try_get("interpreter_config_id").map_err(unexpected)?,
            provider_kind: row.try_get("provider_kind").map_err(unexpected)?,
            prompt_policy_version: row.try_get("prompt_policy_version").map_err(unexpected)?,
            config_digest: row.try_get("config_digest").map_err(unexpected)?,
        })
    }

    async fn register_policy(
        &self,
        tenant_id: TenantId,
        request: NewConsolidationPolicy,
    ) -> Result<ConsolidationPolicyView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let row = sqlx::query(
            r#"
            INSERT INTO memory.consolidation_policies (
                tenant_id, source_kind, policy_id, interpreter_config_id,
                write_policy_id, write_policy_version, retention_policy_id,
                confidence_auto_promote_min, created_by_principal_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, source_kind, policy_id) DO NOTHING
            RETURNING tenant_id, source_kind, policy_id, interpreter_config_id,
                write_policy_id, write_policy_version, retention_policy_id,
                confidence_auto_promote_min, enabled
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.source_kind)
        .bind(&request.policy_id)
        .bind(request.interpreter_config_id)
        .bind(&request.write_policy_id)
        .bind(&request.write_policy_version)
        .bind(&request.retention_policy_id)
        .bind(request.confidence_auto_promote_min)
        .bind(request.created_by_principal_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        let row = match row {
            Some(row) => row,
            None => sqlx::query(
                r#"
                    SELECT tenant_id, source_kind, policy_id, interpreter_config_id,
                        write_policy_id, write_policy_version, retention_policy_id,
                        confidence_auto_promote_min, enabled
                    FROM memory.consolidation_policies
                    WHERE tenant_id = $1 AND source_kind = $2 AND policy_id = $3
                    "#,
            )
            .bind(tenant_id.0)
            .bind(&request.source_kind)
            .bind(&request.policy_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_consolidation_sqlx)?,
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(ConsolidationPolicyView {
            tenant_id,
            source_kind: row.try_get("source_kind").map_err(unexpected)?,
            policy_id: row.try_get("policy_id").map_err(unexpected)?,
            interpreter_config_id: row.try_get("interpreter_config_id").map_err(unexpected)?,
            write_policy_id: row.try_get("write_policy_id").map_err(unexpected)?,
            write_policy_version: row.try_get("write_policy_version").map_err(unexpected)?,
            retention_policy_id: row.try_get("retention_policy_id").map_err(unexpected)?,
            confidence_auto_promote_min: row
                .try_get("confidence_auto_promote_min")
                .map_err(unexpected)?,
            enabled: row.try_get("enabled").map_err(unexpected)?,
        })
    }

    async fn get_policy(
        &self,
        tenant_id: TenantId,
        source_kind: &str,
        policy_id: &str,
    ) -> Result<ConsolidationPolicyView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, source_kind, policy_id, interpreter_config_id,
                write_policy_id, write_policy_version, retention_policy_id,
                confidence_auto_promote_min, enabled
            FROM memory.consolidation_policies
            WHERE tenant_id = $1 AND source_kind = $2 AND policy_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(source_kind)
        .bind(policy_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(ConsolidationPolicyView {
            tenant_id,
            source_kind: row.try_get("source_kind").map_err(unexpected)?,
            policy_id: row.try_get("policy_id").map_err(unexpected)?,
            interpreter_config_id: row.try_get("interpreter_config_id").map_err(unexpected)?,
            write_policy_id: row.try_get("write_policy_id").map_err(unexpected)?,
            write_policy_version: row.try_get("write_policy_version").map_err(unexpected)?,
            retention_policy_id: row.try_get("retention_policy_id").map_err(unexpected)?,
            confidence_auto_promote_min: row
                .try_get("confidence_auto_promote_min")
                .map_err(unexpected)?,
            enabled: row.try_get("enabled").map_err(unexpected)?,
        })
    }

    async fn create_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewConsolidationJob,
        idempotency: palimpsest_application::IdempotencyRequest,
    ) -> Result<CreateConsolidationJobOutcome, RepositoryError> {
        for attempt in 1..=3 {
            match self
                .create_job_once(tenant_id, subject_id, &request, &idempotency)
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
        tenant_id: TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<ConsolidationJobView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let row = sqlx::query(
            r#"
            SELECT job_id, source_kind, policy_id, policy_version,
                lifecycle_state, claims_total, claims_done, claim_cap,
                created_at, completed_at, failure_reason
            FROM memory.consolidation_jobs
            WHERE tenant_id = $1 AND subject_id = $2 AND job_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        consolidation_job_view_from_row(&row)
    }

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedConsolidationJob>, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, subject_id, job_id, source_kind, policy_id,
                policy_version, window_from, window_until, claim_cap, principal_id
            FROM memory.claim_next_consolidation_job($1, $2)
            "#,
        )
        .bind(worker_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ClaimedConsolidationJob {
            tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
            subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
            job_id: row.try_get("job_id").map_err(unexpected)?,
            source_kind: row.try_get("source_kind").map_err(unexpected)?,
            policy_id: row.try_get("policy_id").map_err(unexpected)?,
            policy_version: row.try_get("policy_version").map_err(unexpected)?,
            window_from: row.try_get("window_from").map_err(unexpected)?,
            window_until: row.try_get("window_until").map_err(unexpected)?,
            claim_cap: row.try_get("claim_cap").map_err(unexpected)?,
            principal_id: PrincipalId(row.try_get("principal_id").map_err(unexpected)?),
        }))
    }

    async fn worker_policy_snapshot(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<WorkerPolicySnapshot, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_worker_claim_context(&mut transaction).await?;
        let row = sqlx::query(
            r#"
            SELECT policy.interpreter_config_id, policy.write_policy_id,
                policy.write_policy_version, policy.retention_policy_id,
                policy.confidence_auto_promote_min, config.provider_kind,
                config.prompt_policy_version, config.config_digest
            FROM memory.consolidation_policies AS policy
            JOIN memory.consolidation_interpreter_configs AS config
                ON config.tenant_id = policy.tenant_id
               AND config.interpreter_config_id = policy.interpreter_config_id
            WHERE policy.tenant_id = $1
              AND policy.source_kind = $2
              AND policy.policy_id = $3
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(&job.source_kind)
        .bind(&job.policy_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(WorkerPolicySnapshot {
            interpreter_config_id: row.try_get("interpreter_config_id").map_err(unexpected)?,
            write_policy_id: row.try_get("write_policy_id").map_err(unexpected)?,
            write_policy_version: row.try_get("write_policy_version").map_err(unexpected)?,
            retention_policy_id: row.try_get("retention_policy_id").map_err(unexpected)?,
            confidence_auto_promote_min: row
                .try_get("confidence_auto_promote_min")
                .map_err(unexpected)?,
            provider_kind: row.try_get("provider_kind").map_err(unexpected)?,
            prompt_policy_version: row.try_get("prompt_policy_version").map_err(unexpected)?,
            config_digest: row.try_get("config_digest").map_err(unexpected)?,
        })
    }

    async fn select_window_episodes(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<Vec<palimpsest_application::InterpreterEpisode>, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT episode_id, case_id, observed_at, source_type,
                encode(sha256(convert_to(coalesce(payload::text, ''), 'UTF8')), 'hex')
                    AS payload_digest
            FROM memory.episodes
            WHERE tenant_id = $1
              AND subject_id = $2
              AND source_type = $3
              AND recorded_at >= $4
              AND recorded_at < $5
            ORDER BY recorded_at
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(&job.source_kind)
        .bind(job.window_from)
        .bind(job.window_until)
        .fetch_all(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        transaction.commit().await.map_err(unexpected)?;
        rows.into_iter()
            .map(|row| {
                Ok(palimpsest_application::InterpreterEpisode {
                    episode_id: EpisodeId(row.try_get("episode_id").map_err(unexpected)?),
                    case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
                    observed_at: row.try_get("observed_at").map_err(unexpected)?,
                    source_type: row.try_get("source_type").map_err(unexpected)?,
                    payload_digest: row.try_get("payload_digest").map_err(unexpected)?,
                })
            })
            .collect()
    }

    async fn insert_claims(
        &self,
        job: &ClaimedConsolidationJob,
        claims: &[PendingConsolidationClaim],
    ) -> Result<(), RepositoryError> {
        if claims.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, job.tenant_id, job.subject_id).await?;
        let mut inserted_total: i64 = 0;
        for claim in claims {
            let inserted = sqlx::query(
                r#"
                INSERT INTO memory.consolidation_claims (
                    tenant_id, subject_id, case_id, job_id, claim_id,
                    episode_ids, content_hash, confidence, sensitivity,
                    valid_from, valid_until, observed_at, value, model_identity,
                    prompt_policy_version
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (tenant_id, subject_id, job_id, claim_id) DO NOTHING
                "#,
            )
            .bind(job.tenant_id.0)
            .bind(job.subject_id.0)
            .bind(claim.case_id.0)
            .bind(job.job_id)
            .bind(claim.claim_id)
            .bind(claim.episode_ids.iter().map(|id| id.0).collect::<Vec<_>>())
            .bind(&claim.content_hash)
            .bind(claim.confidence)
            .bind(&claim.sensitivity)
            .bind(claim.valid_from)
            .bind(claim.valid_until)
            .bind(claim.observed_at)
            .bind(&claim.value)
            .bind(&claim.model_identity)
            .bind(&claim.prompt_policy_version)
            .execute(&mut *transaction)
            .await
            .map_err(map_consolidation_sqlx)?;
            inserted_total += i64::try_from(inserted.rows_affected()).map_err(unexpected)?;
        }
        let updated = sqlx::query(
            r#"
            UPDATE memory.consolidation_jobs
            SET claims_total = claims_total + $4,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND subject_id = $2 AND job_id = $3
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(i32::try_from(inserted_total).map_err(unexpected)?)
        .execute(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(unexpected)?;
            return Err(RepositoryError::NotFound);
        }
        transaction.commit().await.map_err(unexpected)?;
        Ok(())
    }

    async fn claim_next_claim(
        &self,
        job: &ClaimedConsolidationJob,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedConsolidationClaim>, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT claim_id, case_id, episode_ids, content_hash, confidence,
                sensitivity, valid_from, valid_until, observed_at, value,
                model_identity, prompt_policy_version
            FROM memory.claim_next_consolidation_claim(
                $1, $2, $3, $4, $5
            )
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(worker_id)
        .bind(i32::try_from(lease_seconds).map_err(unexpected)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(ClaimedConsolidationClaim {
            tenant_id: job.tenant_id,
            subject_id: job.subject_id,
            job_id: job.job_id,
            claim_id: row.try_get("claim_id").map_err(unexpected)?,
            case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
            episode_ids: row
                .try_get::<Vec<Uuid>, _>("episode_ids")
                .map_err(unexpected)?
                .into_iter()
                .map(EpisodeId)
                .collect(),
            content_hash: row.try_get("content_hash").map_err(unexpected)?,
            confidence: row.try_get("confidence").map_err(unexpected)?,
            sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
            valid_from: row.try_get("valid_from").map_err(unexpected)?,
            valid_until: row.try_get("valid_until").map_err(unexpected)?,
            observed_at: row.try_get("observed_at").map_err(unexpected)?,
            value: row.try_get("value").map_err(unexpected)?,
            model_identity: row.try_get("model_identity").map_err(unexpected)?,
            prompt_policy_version: row.try_get("prompt_policy_version").map_err(unexpected)?,
        }))
    }

    async fn complete_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
        fact_id: FactId,
        revision_id: RevisionId,
    ) -> Result<bool, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT memory.complete_consolidation_claim($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(claim.tenant_id.0)
        .bind(claim.subject_id.0)
        .bind(claim.job_id)
        .bind(claim.claim_id)
        .bind(fact_id.0)
        .bind(revision_id.0)
        .fetch_one(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        Ok(row.try_get::<bool, _>(0).map_err(unexpected)?)
    }

    async fn skip_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
        reason: &str,
    ) -> Result<bool, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT memory.skip_consolidation_claim($1, $2, $3, $4, $5)
            "#,
        )
        .bind(claim.tenant_id.0)
        .bind(claim.subject_id.0)
        .bind(claim.job_id)
        .bind(claim.claim_id)
        .bind(reason)
        .fetch_one(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        Ok(row.try_get::<bool, _>(0).map_err(unexpected)?)
    }

    async fn release_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
    ) -> Result<bool, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT memory.release_consolidation_claim($1, $2, $3, $4)
            "#,
        )
        .bind(claim.tenant_id.0)
        .bind(claim.subject_id.0)
        .bind(claim.job_id)
        .bind(claim.claim_id)
        .fetch_one(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        Ok(row.try_get::<bool, _>(0).map_err(unexpected)?)
    }

    async fn complete_job(&self, job: &ClaimedConsolidationJob) -> Result<bool, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT memory.complete_consolidation_job($1, $2, $3)
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        Ok(row.try_get::<bool, _>(0).map_err(unexpected)?)
    }

    async fn has_in_flight_claims(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<bool, RepositoryError> {
        // The claims table is RLS-protected; set the worker-claim context
        // exactly like the SECURITY DEFINER functions do.
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
            .bind(job.tenant_id.0.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
            .bind(job.subject_id.0.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        sqlx::query("SELECT set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true)")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let exists = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM memory.consolidation_claims
                WHERE tenant_id = $1
                  AND subject_id = $2
                  AND job_id = $3
                  AND lifecycle_state = 'leased'
                  AND lease_expires_at > clock_timestamp()
            )
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        transaction.rollback().await.map_err(unexpected)?;
        Ok(exists)
    }

    async fn fail_job(
        &self,
        job: &ClaimedConsolidationJob,
        reason: &str,
    ) -> Result<bool, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT memory.fail_consolidation_job($1, $2, $3, $4)
            "#,
        )
        .bind(job.tenant_id.0)
        .bind(job.subject_id.0)
        .bind(job.job_id)
        .bind(reason)
        .fetch_one(&self.pool)
        .await
        .map_err(map_consolidation_sqlx)?;
        Ok(row.try_get::<bool, _>(0).map_err(unexpected)?)
    }
}

impl PostgresMemoryRepository {
    async fn create_job_once(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: &NewConsolidationJob,
        idempotency: &palimpsest_application::IdempotencyRequest,
    ) -> Result<CreateConsolidationJobOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let key_digest = hex::encode(sha256_bytes(idempotency.key.as_bytes()));
        let job_id = Uuid::now_v7();
        let policy_row = sqlx::query(
            r#"
            SELECT write_policy_version
            FROM memory.consolidation_policies
            WHERE tenant_id = $1 AND source_kind = $2 AND policy_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.source_kind)
        .bind(&request.policy_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        let policy_version: String = policy_row
            .try_get("write_policy_version")
            .map_err(unexpected)?;
        let row = sqlx::query(
            r#"
            INSERT INTO memory.consolidation_jobs (
                tenant_id, subject_id, job_id, source_kind, policy_id,
                policy_version, window_from, window_until, claim_cap,
                principal_id, idempotency_key_digest, request_fingerprint
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (tenant_id, subject_id, principal_id, idempotency_key_digest)
                DO NOTHING
            RETURNING job_id, source_kind, policy_id, policy_version,
                lifecycle_state, claims_total, claims_done, claim_cap,
                created_at, completed_at, failure_reason
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(job_id)
        .bind(&request.source_kind)
        .bind(&request.policy_id)
        .bind(&policy_version)
        .bind(request.window_from)
        .bind(request.window_until)
        .bind(palimpsest_application::CONSOLIDATION_CLAIM_CAP)
        .bind(&request.principal_id.0)
        .bind(&key_digest)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_consolidation_sqlx)?;
        let (view, replayed) = if let Some(row) = row {
            (consolidation_job_view_from_row(&row)?, false)
        } else {
            let existing = sqlx::query(
                r#"
                SELECT request_fingerprint
                FROM memory.consolidation_jobs
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
            .map_err(map_consolidation_sqlx)?
            .ok_or(RepositoryError::Unexpected(
                "consolidation job idempotency replay lost its reservation".to_owned(),
            ))?;
            let stored_fingerprint: String = existing
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if stored_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let row = sqlx::query(
                r#"
                SELECT job_id, source_kind, policy_id, policy_version,
                    lifecycle_state, claims_total, claims_done, claim_cap,
                    created_at, completed_at, failure_reason
                FROM memory.consolidation_jobs
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
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_consolidation_sqlx)?;
            (consolidation_job_view_from_row(&row)?, true)
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(CreateConsolidationJobOutcome {
            job_id: view.job_id,
            lifecycle_state: view.lifecycle_state,
            claims_total: view.claims_total,
            claim_cap: view.claim_cap,
            replayed,
        })
    }
}

fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}
