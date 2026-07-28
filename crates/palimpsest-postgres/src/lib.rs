use async_trait::async_trait;
use palimpsest_application::{
    AppendOutcome, CheckpointMutationOutcome, CheckpointRepository, EpisodeRepository,
    FactMutationOutcome, FactRepository, IdempotencyRequest, RepositoryError,
};
use palimpsest_domain::{
    AgentId, CaseId, CheckpointEffect, CheckpointId, CheckpointPrecondition, CheckpointRevisionId,
    CheckpointSnapshot, CheckpointView, EffectId, EffectKey, EffectKind, EffectReceipt,
    EffectRecoveryMode, EffectStatus, Episode, EpisodeId, EpisodeKind, FactId, FactKey,
    FactNamespace, FactRevision, FactView, NewCheckpointRevision, NewEffectTransition, NewEpisode,
    NewFact, PrincipalId, Provenance, RetentionPolicyId, RevisionId, Sensitivity, SourceType,
    SubjectId, TenantId, ThreadId, ValidTime, WritePolicy, WritePolicyId, WritePolicyVersion,
};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

#[derive(Clone)]
pub struct PostgresMemoryRepository {
    pool: PgPool,
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
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "23505")
    {
        RepositoryError::Conflict
    } else {
        unexpected(error)
    }
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
