//! checkpoints — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    CheckpointMutationOutcome, CheckpointRepository, IdempotencyRequest, RepositoryError,
};
use palimpsest_domain::{
    AgentId, CaseId, CheckpointEffect, CheckpointId, CheckpointPrecondition, CheckpointRevisionId,
    CheckpointSnapshot, CheckpointView, EffectId, EffectKey, EffectKind, EffectReceipt,
    EffectRecoveryMode, EffectStatus, NewCheckpointRevision, NewEffectTransition, PrincipalId,
    Provenance, RetentionPolicyId, Sensitivity, SourceType, SubjectId, TenantId, ThreadId,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

use super::retrieval::set_scope;
use super::write_path::{
    CheckpointResource, GovernedWrite, IdempotencyCompletion, IdempotencyScope,
    complete_idempotency, record_governed_write, reserve_idempotency,
};
use super::{PostgresMemoryRepository, text_value_from_row, unexpected};

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

pub(crate) async fn checkpoint_revision_is_active(
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

pub(crate) struct CheckpointHead {
    case_id: CaseId,
    checkpoint_id: CheckpointId,
    revision_id: CheckpointRevisionId,
    revision_number: i64,
    expired: bool,
}

pub(crate) async fn select_checkpoint_head_for_update(
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

pub(crate) async fn persist_effect_transitions(
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

pub(crate) async fn select_checkpoint_view(
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

pub(crate) fn checkpoint_effect_from_row(row: &PgRow) -> Result<CheckpointEffect, RepositoryError> {
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

pub(crate) fn map_checkpoint_sqlx(error: sqlx::Error) -> RepositoryError {
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
