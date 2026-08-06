//! lifecycle — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    AdvanceDeletionOutcome, ClaimedDeletionOperation, ClaimedDeletionTarget, CreateDeletionOutcome,
    CreateDeletionRequest, DeletionOperationView, DeletionOutcomeView, DeletionRepository,
    DeletionTargetView, RepositoryError, SubjectContentLeaseRepository,
    SubjectLifecycleControllerRepository,
};
use palimpsest_domain::{
    ContentLeaseId, DeletionOperationId, DeletionOperationState, DeletionTargetCapability,
    DeletionTargetName, DeletionTargetState, DeletionTargetVerification, PrincipalScope,
    SubjectContentLease, SubjectId, SubjectLifecycle, SubjectLifecycleState, TenantId,
};
use sqlx::Row;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::retrieval::{set_scope, set_scope_context};
use super::{PostgresMemoryRepository, PostgresSubjectLifecycleRepository, unexpected};

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

    async fn release_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
    ) -> Result<(), RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope_context(&mut transaction, claimed.tenant_id, claimed.subject_id).await?;
        sqlx::query(
            r#"
            SELECT memory.release_deletion_operation_lease($1, $2, $3, $4)
            "#,
        )
        .bind(claimed.tenant_id.0)
        .bind(claimed.subject_id.0)
        .bind(claimed.operation_id.0)
        .bind(worker_id)
        .execute(&mut *transaction)
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

    async fn release_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: uuid::Uuid,
    ) -> Result<(), RepositoryError> {
        self.controller
            .release_deletion_operation_lease(claimed, worker_id)
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

pub(crate) fn map_lifecycle_sqlx(error: sqlx::Error) -> RepositoryError {
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

pub(crate) fn map_deletion_sqlx(error: sqlx::Error) -> RepositoryError {
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

pub(crate) fn parse_deletion_state(value: &str) -> Result<DeletionOperationState, RepositoryError> {
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

pub(crate) fn parse_deletion_targets(
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
