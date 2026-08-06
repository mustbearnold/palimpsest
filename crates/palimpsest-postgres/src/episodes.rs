//! episodes — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    AppendOutcome, CreateDeletionOutcome, CreateDeletionRequest, EpisodeRepository,
    ExportCreateOutcome, IdempotencyRequest, NewExport, RepositoryError,
};
use palimpsest_domain::{
    CaseId, DeletionOperationId, Episode, EpisodeId, EpisodeKind, NewEpisode, PrincipalId,
    Provenance, RetentionPolicyId, Sensitivity, SourceType, SubjectId, SubjectLifecycle,
    SubjectLifecycleState, TenantId,
};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};

use super::export::{export_operation_from_row, export_operation_select_sql, map_export_sqlx};
use super::lifecycle::{
    map_deletion_sqlx, map_lifecycle_sqlx, parse_deletion_state, parse_deletion_targets,
};
use super::retrieval::{set_scope, set_scope_context};
use super::write_path::{
    GovernedWrite, IdempotencyCompletion, IdempotencyScope, complete_idempotency,
    record_governed_write, reserve_idempotency,
};
use super::{
    PostgresMemoryRepository, RestoreFenceReplayReport, map_sqlx, text_value_from_row, unexpected,
};

impl PostgresMemoryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn replay_restore_fence_ledger(
        &self,
        ledger_bytes: &[u8],
        expected_ledger_sha256: &str,
    ) -> Result<RestoreFenceReplayReport, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT scopes_found, scopes_purged, residual_rows, ledger_sha256
            FROM memory.replay_restore_fence_ledger($1, $2)
            "#,
        )
        .bind(ledger_bytes.to_vec())
        .bind(expected_ledger_sha256)
        .fetch_one(&self.pool)
        .await
        .map_err(unexpected)?;
        let scopes_found =
            u64::try_from(row.try_get::<i64, _>("scopes_found").map_err(unexpected)?)
                .map_err(unexpected)?;
        let scopes_purged =
            u64::try_from(row.try_get::<i64, _>("scopes_purged").map_err(unexpected)?)
                .map_err(unexpected)?;
        let residual_rows =
            u64::try_from(row.try_get::<i64, _>("residual_rows").map_err(unexpected)?)
                .map_err(unexpected)?;
        Ok(RestoreFenceReplayReport {
            scopes_found,
            scopes_purged,
            residual_rows,
            ledger_sha256: row.try_get("ledger_sha256").map_err(unexpected)?,
        })
    }
}

impl PostgresMemoryRepository {
    pub(crate) async fn create_export_once(
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
    pub(crate) async fn create_deletion_operation_once(
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
    pub(crate) async fn transition_subject_lifecycle(
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

    pub(crate) async fn transition_subject_lifecycle_once(
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

pub(crate) async fn select_episode(
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

pub(crate) fn episode_from_row(row: &PgRow) -> Result<Episode, RepositoryError> {
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
