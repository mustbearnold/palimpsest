//! export — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    ExportCreateOutcome, ExportMaterialization, ExportOperationState, ExportOperationView,
    ExportPackageMetadata, ExportRecord, ExportRecordKind, ExportRepository, NewExport,
    RepositoryError,
};
use palimpsest_domain::{ExportId, PrincipalId, SubjectId, TenantId};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

use super::retrieval::{set_scope, set_scope_context};
use super::{PostgresMemoryRepository, unexpected};

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
pub(crate) struct ExportManifestItem {
    kind: ExportRecordKind,
    record_id: uuid::Uuid,
    recorded_at: OffsetDateTime,
    source_content_sha256: String,
}

pub(crate) async fn load_export_records(
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

pub(crate) fn manifest_item(
    manifest: &[ExportManifestItem],
    kind: ExportRecordKind,
    record_id: uuid::Uuid,
) -> Result<&ExportManifestItem, RepositoryError> {
    manifest
        .iter()
        .find(|item| item.kind == kind && item.record_id == record_id)
        .ok_or_else(|| RepositoryError::Unexpected("export membership is inconsistent".to_owned()))
}

pub(crate) fn verify_export_kind_count(
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

pub(crate) fn parse_export_record_kind(value: &str) -> Result<ExportRecordKind, RepositoryError> {
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

pub(crate) fn export_operation_select_sql() -> &'static str {
    r#"
    SELECT *
    FROM memory.export_operations
    WHERE tenant_id = $1 AND subject_id = $2 AND export_id = $3
    "#
}

pub(crate) fn export_operation_from_row(
    row: &PgRow,
) -> Result<ExportOperationView, RepositoryError> {
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

pub(crate) fn parse_export_operation_state(
    value: &str,
) -> Result<ExportOperationState, RepositoryError> {
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

pub(crate) fn map_export_sqlx(error: sqlx::Error) -> RepositoryError {
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
