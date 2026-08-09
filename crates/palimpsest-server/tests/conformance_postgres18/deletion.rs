//! deletion — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use std::{env, fs, sync::Arc, time::Duration};
use time::{Duration as TimeDuration, OffsetDateTime};

use palimpsest_application::{
    CANONICAL_HISTORY_EXPORT_PROFILE, CreateDeletionRequest, DeletionRepository,
    ExportOperationState, ExportPackageMetadata, ExportRepository, FileExportPackageStore,
    IdempotencyRequest, InMemoryExportPackageStore, MemoryService, NewExport, ServiceError,
};
use palimpsest_conformance::Target;
use palimpsest_domain::{
    DeletionOperationId, DeletionOperationState, DeletionTargetCapability, DeletionTargetName,
    DeletionTargetState, DeletionTargetVerification, OperationGrant, PrincipalId, PrincipalScope,
    Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::fixtures::{DenyingExportWorkerAuthorizer, StaticExportWorkerAuthorizer};

pub(crate) async fn deletion_target_lease_recovers_after_worker_expiry(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000030")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000031")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-recovery-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-target-lease-recovery".to_owned(),
            request_fingerprint_sha256: "1".repeat(64),
            configured_targets: vec![
                palimpsest_domain::DeletionTargetName::Canonical,
                palimpsest_domain::DeletionTargetName::Projections,
            ],
            retention_hours: 24 * 90,
        })
        .await
        .context("create deletion operation")?;
    let seed_counts: (i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT count(*) FROM memory.deletion_tombstone_seeds
             WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3),
            (SELECT count(*) FROM memory.deletion_audit_seeds
             WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3)
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(seed_counts == (1, 1));
    let first_worker = Uuid::now_v7();
    let claimed = repository
        .claim_next_deletion_operation(first_worker, 5)
        .await?
        .context("deletion operation was not claimable")?;
    ensure!(claimed.operation_id == created.operation_id);
    let advanced = repository
        .advance_deletion_operation(&claimed, first_worker, 5)
        .await
        .context("advance operation into purging")?;
    ensure!(advanced.lifecycle_state == DeletionOperationState::Purging);
    let first_target = repository
        .claim_next_deletion_target(&claimed, first_worker, 1)
        .await
        .context("claim first deletion target")?
        .context("first deletion target was not claimable")?;
    let leased_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll leased deletion target")?;
    let leased_target = leased_view
        .targets
        .iter()
        .find(|target| target.target_name == first_target.target_name)
        .context("claimed target was not visible in the public deletion view")?;
    ensure!(leased_target.state == DeletionTargetState::Leased);
    ensure!(leased_target.verification == DeletionTargetVerification::Pending);
    ensure!(leased_target.target_key_digest == first_target.target_key_digest);
    ensure!(leased_target.lease_id == Some(first_target.target_lease_id));

    // The renewed leases use one second. The waits below stay short while
    // they still prove live lease expiry.
    repository
        .renew_deletion_operation_lease(&claimed, first_worker, 1)
        .await
        .context("renew deletion operation lease")?;
    repository
        .renew_deletion_target_lease(&first_target, 1)
        .await
        .context("renew deletion target lease")?;
    crate::sleep_budget::sleep(Duration::from_millis(400)).await;
    let second_worker = Uuid::now_v7();
    ensure!(
        repository
            .claim_next_deletion_operation(second_worker, 1)
            .await?
            .is_none(),
        "a renewed deletion operation lease was reclaimed early"
    );
    crate::sleep_budget::sleep(Duration::from_millis(1_300)).await;
    let reclaimed_operation = repository
        .claim_next_deletion_operation(second_worker, 1)
        .await
        .context("reclaim expired deletion operation")?
        .context("expired deletion operation lease was not reclaimable")?;
    let reclaimed_target = repository
        .claim_next_deletion_target(&reclaimed_operation, second_worker, 1)
        .await
        .context("reclaim expired deletion target")?
        .context("expired deletion target lease was not reclaimable")?;
    ensure!(reclaimed_target.target_name == first_target.target_name);
    ensure!(reclaimed_target.target_key_digest == first_target.target_key_digest);
    ensure!(reclaimed_target.target_lease_id != first_target.target_lease_id);
    ensure!(reclaimed_target.attempts == first_target.attempts + 1);

    // Probe before any lease drain. While the recovered leases are live, the
    // worker must make no progress: two runs still leave the recovered
    // target leased by the recovery worker.
    service
        .run_deletion_worker_once()
        .await
        .context("probe recovered deletion before the lease drain")?;
    let probe_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll recovered deletion before the lease drain")?;
    ensure!(
        probe_view.lifecycle_state == DeletionOperationState::Purging,
        "the recovered deletion left purging before the lease drain"
    );
    let probe_target = probe_view
        .targets
        .iter()
        .find(|target| target.target_name == reclaimed_target.target_name)
        .context("probe view missed the recovered target")?;
    ensure!(
        probe_target.state == DeletionTargetState::Leased
            && probe_target.lease_id == Some(reclaimed_target.target_lease_id),
        "the worker reclaimed a live recovered target lease early"
    );
    service
        .run_deletion_worker_once()
        .await
        .context("probe recovered deletion a second time")?;
    let held_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll recovered deletion against the held leases")?;
    let held_target = held_view
        .targets
        .iter()
        .find(|target| target.target_name == reclaimed_target.target_name)
        .context("held view missed the recovered target")?;
    ensure!(
        held_view.lifecycle_state == DeletionOperationState::Purging
            && held_target.state == DeletionTargetState::Leased
            && held_target.lease_id == Some(reclaimed_target.target_lease_id),
        "a live recovered target lease was reclaimed early"
    );

    // Finish the recovered deletion. The recovery lease is the smallest
    // allowed duration, so the drain poll stays under one second plus one
    // poll interval. The two second deadline bounds the conditional-wait
    // poll; the loop must outlast the recovered target lease, not the retry
    // budget.
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let completed = {
        let mut completed = None;
        while std::time::Instant::now() < drain_deadline {
            if completed.is_some() {
                break;
            }
            service
                .run_deletion_worker_once()
                .await
                .context("finish recovered deletion")?;
            let view = service
                .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
                .await
                .context("poll recovered deletion")?;
            match view.lifecycle_state {
                DeletionOperationState::Completed => {
                    completed = Some(view);
                }
                DeletionOperationState::RetryWait | DeletionOperationState::Purging => {
                    crate::sleep_budget::poll_sleep(Duration::from_millis(100)).await;
                }
                state => bail!("recovered deletion entered unexpected state {state:?}"),
            }
        }
        completed.context("recovered deletion did not reach completed")?
    };
    ensure!(
        completed
            .targets
            .iter()
            .filter(|target| target.capability
                == palimpsest_domain::DeletionTargetCapability::Configured)
            .all(|target| {
                target.state == DeletionTargetState::Done
                    && target.verification
                        == palimpsest_domain::DeletionTargetVerification::Verified
            })
    );
    ensure!(
        completed
            .targets
            .iter()
            .filter(|target| target.capability
                == palimpsest_domain::DeletionTargetCapability::Configured)
            .all(|target| target.effect_receipt_sha256.is_some())
    );
    let operation_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.deletion_operations WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(operation_rows == 0);
    let tombstone = sqlx::query(
        "SELECT scope_digest, target_summary, idempotency_key_digest, request_fingerprint_sha256
         FROM memory.deletion_tombstones WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id.0)
    .bind(created.operation_id.0)
    .fetch_one(migration_pool)
    .await?;
    let scope_digest: String = tombstone.try_get("scope_digest")?;
    ensure!(scope_digest.starts_with("v1:"));
    ensure!(scope_digest.len() == 67);
    let target_summary: serde_json::Value = tombstone.try_get("target_summary")?;
    ensure!(target_summary.is_array());
    ensure!(
        tombstone
            .try_get::<String, _>("idempotency_key_digest")?
            .trim()
            .len()
            == 64
    );
    ensure!(
        tombstone
            .try_get::<String, _>("request_fingerprint_sha256")?
            .trim()
            .len()
            == 64
    );
    Ok(())
}

/// Spec 018 AC4: a deletion retry backoff is a stored deadline. The suite
/// proves the deadline holds, rewinds it, and proves the operation advances.
/// The verification failure is deterministic: one live content lease survives
/// the target purges and fails the residual check.
pub(crate) async fn deletion_retry_backoff_rewinds_instead_of_waiting(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000032")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000033")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-retry-backoff-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-retry-backoff-rewind".to_owned(),
            request_fingerprint_sha256: "2".repeat(64),
            configured_targets: vec![
                palimpsest_domain::DeletionTargetName::Canonical,
                palimpsest_domain::DeletionTargetName::Projections,
            ],
            retention_hours: 24 * 90,
        })
        .await
        .context("create deletion operation for the retry backoff rewind")?;

    // Drive the purge at the repository level so a residual row can survive
    // between the target effects and the verification step. The operation
    // lease stays held by this worker for the whole drive.
    let worker = Uuid::now_v7();
    let claimed = repository
        .claim_next_deletion_operation(worker, 5)
        .await
        .context("claim deletion operation for the retry backoff rewind")?
        .context("deletion operation was not claimable for the retry backoff rewind")?;
    ensure!(claimed.operation_id == created.operation_id);
    let advanced = repository
        .advance_deletion_operation(&claimed, worker, 5)
        .await
        .context("advance deletion operation into purging")?;
    ensure!(advanced.lifecycle_state == DeletionOperationState::Purging);
    loop {
        let Some(target) = repository
            .claim_next_deletion_target(&claimed, worker, 1)
            .await
            .context("claim deletion target for the retry backoff rewind")?
        else {
            break;
        };
        repository
            .apply_deletion_target(&target)
            .await
            .context("apply deletion target effect")?;
        let effect_receipt_sha256 = MemoryService::deletion_target_effect_receipt_sha256(
            target.operation_id,
            &target.target_key_digest,
            target.attempts,
        );
        repository
            .complete_deletion_target(&target, &effect_receipt_sha256)
            .await
            .context("complete deletion target")?;
    }

    // One live content lease survives the purges. Verification must fail on
    // it and park the operation in retry_wait with a future backoff.
    let residual_lease_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO memory.subject_content_leases
            (tenant_id, subject_id, lease_id, principal_id, expires_at)
         VALUES ($1, $2, $3, 'deletion-retry-backoff-principal',
                 clock_timestamp() + interval '1 hour')",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(residual_lease_id)
    .execute(migration_pool)
    .await
    .context("insert the residual content lease")?;
    let failed = repository
        .advance_deletion_operation(&claimed, worker, 5)
        .await
        .context("advance deletion operation into verification")?;
    ensure!(
        failed.lifecycle_state == DeletionOperationState::RetryWait,
        "deletion verification did not fail into retry_wait: {:?}",
        failed.lifecycle_state
    );

    // The backoff deadline must hold the worker: one run before the rewind
    // makes no progress.
    let held_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll the held retry backoff")?;
    ensure!(
        held_view.lifecycle_state == DeletionOperationState::RetryWait
            && held_view.retry_count == 1,
        "the retry backoff did not hold the operation"
    );
    service
        .run_deletion_worker_once()
        .await
        .context("probe the retry backoff before the rewind")?;
    let probe_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll the retry backoff before the rewind")?;
    ensure!(
        probe_view.lifecycle_state == DeletionOperationState::RetryWait
            && probe_view.retry_count == 1,
        "the worker advanced the operation before the retry backoff rewind"
    );

    // Rewind the backoff deadline instead of waiting for it.
    rewind_deletion_retry_deadline(migration_pool, tenant_id, subject_id, created.operation_id)
        .await?;

    // The residual still fails verification, so the operation advances into
    // a second backoff with an incremented retry count.
    service
        .run_deletion_worker_once()
        .await
        .context("advance the rewound deletion operation")?;
    let advanced_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll the advanced deletion operation")?;
    ensure!(
        advanced_view.lifecycle_state == DeletionOperationState::RetryWait
            && advanced_view.retry_count == 2,
        "the rewound deletion operation did not advance into the second backoff"
    );

    // Remove the residual, rewind once more, and let the worker complete.
    sqlx::query(
        "DELETE FROM memory.subject_content_leases
         WHERE tenant_id = $1 AND subject_id = $2 AND lease_id = $3",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(residual_lease_id)
    .execute(migration_pool)
    .await
    .context("remove the residual content lease")?;
    rewind_deletion_retry_deadline(migration_pool, tenant_id, subject_id, created.operation_id)
        .await?;
    service
        .run_deletion_worker_once()
        .await
        .context("complete the rewound deletion operation")?;
    let completed = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll the completed deletion operation")?;
    ensure!(
        completed.lifecycle_state == DeletionOperationState::Completed,
        "the rewound deletion operation did not complete"
    );
    ensure!(
        completed
            .targets
            .iter()
            .filter(|target| target.capability
                == palimpsest_domain::DeletionTargetCapability::Configured)
            .all(|target| {
                target.state == DeletionTargetState::Done
                    && target.verification
                        == palimpsest_domain::DeletionTargetVerification::Verified
                    && target.effect_receipt_sha256.is_some()
            })
    );
    let operation_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.deletion_operations WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(operation_rows == 0);
    let tombstone = sqlx::query(
        "SELECT scope_digest FROM memory.deletion_tombstones
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id.0)
    .bind(created.operation_id.0)
    .fetch_one(migration_pool)
    .await?;
    let scope_digest: String = tombstone.try_get("scope_digest")?;
    ensure!(scope_digest.starts_with("v1:"));
    ensure!(scope_digest.len() == 67);
    Ok(())
}

/// Rewinds the retry deadline of one retry_wait deletion operation into the
/// past by one second, and proves that exactly one deadline moved. The one
/// second margin satisfies the spec 018 safety margin of at least 100
/// milliseconds.
async fn rewind_deletion_retry_deadline(
    migration_pool: &PgPool,
    tenant_id: TenantId,
    subject_id: SubjectId,
    operation_id: DeletionOperationId,
) -> Result<()> {
    let rewound = sqlx::query(
        "UPDATE memory.deletion_operations
         SET retry_at = clock_timestamp() - interval '1 second'
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3
             AND lifecycle_state = 'retry_wait'",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(operation_id.0)
    .execute(migration_pool)
    .await
    .context("rewind the deletion retry deadline")?;
    ensure!(
        rewound.rows_affected() == 1,
        "the retry deadline rewind missed the operation"
    );
    Ok(())
}

pub(crate) async fn export_worker_lease_recovery_fences_stale_completion(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000060")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000061")?);
    let principal_id = PrincipalId("export-lease-recovery-principal".to_owned());
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active export lease-recovery lifecycle")?;

    let repository = PostgresMemoryRepository::new(pool.clone());
    let created = repository
        .create_export(NewExport {
            tenant_id,
            subject_id,
            export_id: palimpsest_domain::ExportId(Uuid::now_v7()),
            principal_id: principal_id.clone(),
            profile: CANONICAL_HISTORY_EXPORT_PROFILE.to_owned(),
            idempotency: IdempotencyRequest {
                key: "export-worker-lease-recovery".to_owned(),
                fingerprint: "b".repeat(64),
            },
            authorization_scope_sha256: "a".repeat(64),
            allowed_sensitivities: vec!["internal".to_owned()],
            expires_at: OffsetDateTime::now_utc() + TimeDuration::hours(1),
        })
        .await
        .context("create export lease-recovery operation")?;
    ensure!(!created.replayed);

    let first_worker = Uuid::now_v7();
    let first_claim = repository
        .claim_next_export_for_materialization(first_worker, 1)
        .await
        .context("claim export with first worker")?
        .context("export was not claimable by first worker")?;
    ensure!(first_claim.operation.state == ExportOperationState::Materializing);
    ensure!(first_claim.operation.worker_lease_id == Some(first_worker));

    let second_worker = Uuid::now_v7();
    ensure!(
        repository
            .claim_next_export_for_materialization(second_worker, 1)
            .await
            .context("check live export worker lease")?
            .is_none(),
        "a live export worker lease was reclaimed early"
    );
    crate::sleep_budget::sleep(Duration::from_millis(1_500)).await;

    let recovered_claim = repository
        .claim_next_export_for_materialization(second_worker, 1)
        .await
        .context("reclaim expired export worker lease")?
        .context("expired export worker lease was not recoverable")?;
    ensure!(recovered_claim.operation.worker_lease_id == Some(second_worker));
    ensure!(
        recovered_claim.operation.status_version == first_claim.operation.status_version + 1,
        "export lease recovery did not advance the durable status version"
    );

    let metadata = ExportPackageMetadata {
        content_sha256: "0".repeat(64),
        size_bytes: 0,
        record_count: 0,
    };
    ensure!(
        repository
            .mark_export_ready(
                tenant_id,
                subject_id,
                created.operation.export_id,
                first_worker,
                metadata.clone(),
            )
            .await
            .is_err(),
        "stale export worker finalized after its lease was reclaimed"
    );
    repository
        .mark_export_ready(
            tenant_id,
            subject_id,
            created.operation.export_id,
            second_worker,
            metadata,
        )
        .await
        .context("finalize export under recovered worker lease")?;
    let ready = repository
        .get_export(tenant_id, subject_id, created.operation.export_id)
        .await
        .context("read recovered export")?;
    ensure!(ready.state == ExportOperationState::Ready);
    ensure!(ready.worker_lease_id.is_none());
    Ok(())
}

pub(crate) async fn export_worker_fails_closed_on_store_failure(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000062")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000063")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("export-store-failure-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
        operation_grants: vec![OperationGrant::CanonicalHistoryExport],
    };
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active export store-failure lifecycle")?;

    let authenticator = Arc::new(StaticAuthenticator::new([(
        "export-store-failure-token".to_owned(),
        principal.clone(),
    )]));
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let fault_path =
        env::temp_dir().join(format!("palimpsest-export-store-fault-{}", Uuid::now_v7()));
    fs::write(&fault_path, b"the export root is intentionally a file")
        .context("seed export store failure")?;
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    )
    .with_export_components(
        repository.clone(),
        Arc::new(FileExportPackageStore::new(fault_path.clone())),
    )
    .with_export_worker_authorizer(Arc::new(StaticExportWorkerAuthorizer { authenticator }));

    let created = service
        .create_export(
            &principal,
            tenant_id,
            subject_id,
            "export-store-failure".to_owned(),
        )
        .await
        .context("create export with failing package store")?;
    let worker_result = service.run_export_worker_once().await;
    let operation = service
        .get_export(
            &principal,
            tenant_id,
            subject_id,
            created.operation.export_id,
        )
        .await
        .context("read failed export operation")?;
    let _ = fs::remove_file(&fault_path);
    ensure!(
        matches!(worker_result, Err(ServiceError::Unavailable)),
        "export store failure did not fail closed: {worker_result:?}"
    );
    ensure!(operation.state == ExportOperationState::Failed);
    ensure!(operation.failure_code.as_deref() == Some("package_store_failed"));
    ensure!(operation.content_sha256.is_none());
    ensure!(operation.size_bytes.is_none());
    ensure!(operation.record_count.is_none());
    Ok(())
}

pub(crate) async fn export_worker_fails_closed_on_authorization_revocation(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-0000-8000-000000000066")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-0000-8000-000000000067")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("export-authorization-revocation-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
        operation_grants: vec![OperationGrant::CanonicalHistoryExport],
    };
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active export authorization-revocation lifecycle")?;

    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    )
    .with_export_components(
        repository.clone(),
        Arc::new(InMemoryExportPackageStore::default()),
    )
    .with_export_worker_authorizer(Arc::new(DenyingExportWorkerAuthorizer));
    let created = service
        .create_export(
            &principal,
            tenant_id,
            subject_id,
            "export-authorization-revocation".to_owned(),
        )
        .await
        .context("create export before authorization revocation")?;

    ensure!(service.run_export_worker_once().await?);
    let operation = repository
        .get_export(tenant_id, subject_id, created.operation.export_id)
        .await
        .context("read authorization-revoked export operation")?;
    ensure!(operation.state == ExportOperationState::Failed);
    ensure!(operation.failure_code.as_deref() == Some("authorization_revoked"));
    ensure!(operation.worker_lease_id.is_none());
    ensure!(operation.content_sha256.is_none());
    ensure!(operation.size_bytes.is_none());
    ensure!(operation.record_count.is_none());
    Ok(())
}

pub(crate) async fn deletion_worker_fails_closed_when_export_store_is_unavailable(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000064")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000065")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-export-store-failure-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
        operation_grants: vec![
            OperationGrant::CanonicalHistoryExport,
            OperationGrant::SubjectDelete,
        ],
    };
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active deletion export-store-failure lifecycle")?;

    let authenticator = Arc::new(StaticAuthenticator::new([(
        "deletion-export-store-failure-token".to_owned(),
        principal.clone(),
    )]));
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let fault_path = env::temp_dir().join(format!(
        "palimpsest-deletion-export-store-fault-{}",
        Uuid::now_v7()
    ));
    fs::write(&fault_path, b"the export root is intentionally a file")
        .context("seed deletion export store failure")?;
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    )
    .with_export_components(
        repository.clone(),
        Arc::new(FileExportPackageStore::new(fault_path.clone())),
    )
    .with_export_worker_authorizer(Arc::new(StaticExportWorkerAuthorizer { authenticator }));

    let _export = service
        .create_export(
            &principal,
            tenant_id,
            subject_id,
            "deletion-export-store-failure-export".to_owned(),
        )
        .await
        .context("create export before deletion store failure")?;
    let deletion = service
        .create_subject_deletion(
            &principal,
            tenant_id,
            subject_id,
            "deletion-export-store-failure".to_owned(),
        )
        .await
        .context("create deletion before export store failure")?;
    let fenced_lease = service
        .acquire_subject_content_lease(&principal, tenant_id, subject_id)
        .await;
    ensure!(
        matches!(fenced_lease, Err(ServiceError::NotFound)),
        "deletion fence allowed a new subject content lease: {fenced_lease:?}"
    );
    let wrong_subject_id = Uuid::now_v7();
    let mut authorization_transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut *authorization_transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.0.to_string())
        .execute(&mut *authorization_transaction)
        .await?;
    let unauthorized_release =
        sqlx::query("SELECT memory.release_deletion_operation_lease($1, $2, $3, $4)")
            .bind(tenant_id.0)
            .bind(wrong_subject_id)
            .bind(deletion.operation_id.0)
            .bind(Uuid::now_v7())
            .execute(&mut *authorization_transaction)
            .await;
    let denied_by_scope = match unauthorized_release {
        Err(sqlx::Error::Database(error)) => error.code().as_deref() == Some("42501"),
        Ok(_) | Err(_) => false,
    };
    authorization_transaction.rollback().await?;
    ensure!(
        denied_by_scope,
        "deletion lease release did not reject a mismatched subject scope"
    );
    let worker_result = service.run_deletion_worker_once().await;
    let operation = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, deletion.operation_id)
        .await
        .context("read deletion after export store failure")?;
    let _ = fs::remove_file(&fault_path);
    ensure!(
        matches!(worker_result, Err(ServiceError::Unavailable)),
        "deletion export-target failure did not fail closed: {worker_result:?}"
    );
    ensure!(operation.lifecycle_state == DeletionOperationState::Purging);
    ensure!(operation.failure_reason.is_none());
    let exports_target = operation
        .targets
        .iter()
        .find(|target| target.target_name == DeletionTargetName::Exports)
        .context("deletion omitted configured export target")?;
    ensure!(exports_target.state == DeletionTargetState::Pending);
    ensure!(exports_target.verification == DeletionTargetVerification::Pending);
    ensure!(exports_target.sanitized_error.as_deref() == Some("target_effect_failed"));

    fs::create_dir_all(&fault_path).context("repair export store after injected failure")?;
    let mut recovered_operation = None;
    let mut last_recovered_view = None;
    for _ in 0..8 {
        service
            .run_deletion_worker_once()
            .await
            .context("resume deletion after export store repair")?;
        let view = service
            .poll_subject_deletion(&principal, tenant_id, subject_id, deletion.operation_id)
            .await
            .context("read deletion after export store repair")?;
        last_recovered_view = Some(view.clone());
        if view.lifecycle_state == DeletionOperationState::Completed {
            recovered_operation = Some(view);
            break;
        }
        ensure!(view.lifecycle_state == DeletionOperationState::Purging);
        crate::sleep_budget::poll_sleep(Duration::from_millis(25)).await;
    }
    let recovered_operation = match recovered_operation {
        Some(operation) => operation,
        None => bail!(
            "repaired deletion did not reach completed within the worker budget: {last_recovered_view:?}"
        ),
    };
    let _ = fs::remove_dir_all(&fault_path);
    ensure!(recovered_operation.lifecycle_state == DeletionOperationState::Completed);
    let recovered_exports_target = recovered_operation
        .targets
        .iter()
        .find(|target| target.target_name == DeletionTargetName::Exports)
        .context("recovered deletion omitted configured export target")?;
    ensure!(recovered_exports_target.state == DeletionTargetState::Done);
    ensure!(recovered_exports_target.verification == DeletionTargetVerification::Verified);
    ensure!(recovered_exports_target.sanitized_error.is_none());
    Ok(())
}

pub(crate) async fn deletion_failed_operation_can_be_repaired_and_resumed(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000040")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000041")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-repair-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-repair".to_owned(),
            request_fingerprint_sha256: "2".repeat(64),
            configured_targets: vec![palimpsest_domain::DeletionTargetName::Canonical],
            retention_hours: 24 * 90,
        })
        .await
        .context("create repairable deletion")?;

    sqlx::query(
        "UPDATE memory.deletion_operations
         SET lifecycle_state = 'failed', failure_reason = 'injected_failure',
             completed_at = clock_timestamp()
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;
    sqlx::query(
        "UPDATE memory.deletion_targets
         SET state = 'failed', sanitized_error = 'injected_failure'
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3
           AND capability = 'configured'",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;

    let repaired = service
        .repair_subject_deletion(
            &principal,
            tenant_id,
            subject_id,
            created.operation_id,
            "operator_retry".to_owned(),
        )
        .await
        .context("repair failed deletion")?;
    ensure!(repaired.lifecycle_state == DeletionOperationState::RetryWait);
    ensure!(repaired.failure_reason.is_none());
    ensure!(repaired.targets.iter().all(|target| {
        target.capability == palimpsest_domain::DeletionTargetCapability::NotConfigured
            || target.state == DeletionTargetState::Pending
    }));

    service
        .run_deletion_worker_once()
        .await
        .context("resume repaired deletion")?;
    let completed = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll repaired deletion")?;
    ensure!(completed.lifecycle_state == DeletionOperationState::Completed);
    let tombstone_text: String = sqlx::query_scalar(
        "SELECT target_summary::text || ':' || verification_digest
         FROM memory.deletion_tombstones
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id.0)
    .bind(created.operation_id.0)
    .fetch_optional(migration_pool)
    .await?
    .context("repairable deletion tombstone is missing")?;
    ensure!(
        !tombstone_text.contains("operator_retry"),
        "repair reason must not enter the tombstone"
    );
    Ok(())
}

pub(crate) async fn deletion_target_retry_exhaustion_remains_fenced(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000050")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000051")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-retry-exhaustion-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-retry-exhaustion".to_owned(),
            request_fingerprint_sha256: "3".repeat(64),
            configured_targets: vec![palimpsest_domain::DeletionTargetName::Canonical],
            retention_hours: 24 * 90,
        })
        .await
        .context("create retry-exhaustion deletion")?;
    let worker_id = Uuid::now_v7();
    let claimed = repository
        .claim_next_deletion_operation(worker_id, 30)
        .await?
        .context("retry-exhaustion deletion was not claimable")?;
    let advanced = repository
        .advance_deletion_operation(&claimed, worker_id, 5)
        .await
        .context("advance retry-exhaustion deletion")?;
    ensure!(advanced.lifecycle_state == DeletionOperationState::Purging);

    for attempt in 1..=5 {
        let target = repository
            .claim_next_deletion_target(&claimed, worker_id, 30)
            .await?
            .context("retry-exhaustion target was not claimable")?;
        repository
            .fail_deletion_target(&target, "injected_failure", 5)
            .await
            .context("record retry-exhaustion target failure")?;
        let view = service
            .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
            .await?;
        let target_view = view
            .targets
            .iter()
            .find(|candidate| candidate.target_name == target.target_name)
            .context("retry-exhaustion target disappeared")?;
        if attempt < 5 {
            ensure!(target_view.state == DeletionTargetState::Pending);
        } else {
            ensure!(target_view.state == DeletionTargetState::Failed);
            ensure!(target_view.sanitized_error.as_deref() == Some("injected_failure"));
        }
    }

    let failed = repository
        .advance_deletion_operation(&claimed, worker_id, 5)
        .await
        .context("fail retry-exhaustion deletion")?;
    ensure!(failed.lifecycle_state == DeletionOperationState::Failed);
    sqlx::query(
        "UPDATE memory.deletion_operations
         SET expires_at = clock_timestamp() - interval '1 second'
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;
    let view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await?;
    ensure!(view.lifecycle_state == DeletionOperationState::Failed);
    ensure!(!view.expired);
    ensure!(
        view.targets
            .iter()
            .filter(|target| target.capability == DeletionTargetCapability::Configured)
            .all(|target| target.verification == DeletionTargetVerification::NotVerified)
    );
    let outcome = view
        .outcome
        .as_ref()
        .context("failed deletion omitted terminal outcome")?;
    ensure!(
        outcome.live_disposition == palimpsest_domain::DeletionLiveDisposition::FencedNotVerified
    );
    ensure!(
        outcome.backup_disposition == palimpsest_domain::DeletionBackupDisposition::NotConfigured
    );
    ensure!(outcome.verification_digest.is_none());
    let lifecycle_state: String = sqlx::query_scalar(
        "SELECT lifecycle_state FROM memory.subject_lifecycles
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(lifecycle_state == "deletion_pending");
    Ok(())
}

pub(crate) async fn exercise_export_and_deletion_http(
    target: &Target,
    scope: &Target,
    bearer_token: &str,
    migration_pool: &PgPool,
) -> Result<()> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let base_url = target.base_url.trim_end_matches('/');
    let secondary_subject = scope.principal_a_secondary_subject_id;
    let secondary_prefix = format!(
        "{base_url}/v1/tenants/{}/subjects/{secondary_subject}",
        target.tenant_id
    );

    let cross_tenant_deletion = client
        .post(format!(
            "{base_url}/v1/tenants/{}/subjects/{}/deletions",
            scope.principal_b_tenant_id, scope.principal_b_subject_id
        ))
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "cross-tenant-deletion-attempt")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(
        cross_tenant_deletion.status() == StatusCode::NOT_FOUND,
        "cross-tenant deletion disclosed an operation: {}",
        cross_tenant_deletion.status()
    );

    let episode_url = format!("{secondary_prefix}/episodes");
    let episode_body = json!({
        "case_id": Uuid::from_u128(0x501),
        "kind": "message",
        "observed_at": "2026-07-31T09:00:00Z",
        "provenance": {
            "source_type": "export-deletion-conformance",
            "source_uri": null,
            "external_id": "export-delete-episode"
        },
        "sensitivity": "internal",
        "retention_policy_id": "standard",
        "payload": {"marker": "export-delete-private-marker"}
    });
    let episode_response = client
        .post(&episode_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-episode")
        .json(&episode_body)
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode_location = episode_response
        .headers()
        .get(header::LOCATION)
        .context("export/deletion episode omitted Location")?
        .to_str()?
        .to_owned();
    let episode_location = if episode_location.starts_with("http") {
        episode_location
    } else {
        format!("{base_url}{episode_location}")
    };
    let episode_id = episode_location
        .rsplit('/')
        .next()
        .context("export/deletion episode Location omitted its identifier")?
        .to_owned();

    let export_url = format!("{secondary_prefix}/exports");
    let no_export_grant = client
        .post(&export_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "export-delete-no-grant")
        .send()
        .await?;
    let no_export_grant_status = no_export_grant.status();
    let no_export_grant_body = no_export_grant.text().await?;
    ensure!(
        no_export_grant_status == StatusCode::NOT_FOUND,
        "same-scope export without a grant disclosed an operation: {}",
        no_export_grant_status
    );
    for forbidden in [
        episode_id.as_str(),
        "export-delete-private-marker",
        "export-delete-no-grant",
    ] {
        ensure!(
            !no_export_grant_body.contains(forbidden),
            "same-scope export denial disclosed {forbidden}"
        );
    }
    let export_response = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-export")
        .send()
        .await?;
    if export_response.status() != StatusCode::ACCEPTED {
        let status = export_response.status();
        let body = export_response.text().await?;
        bail!("export creation returned {status}: {body}");
    }
    ensure!(
        export_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let export_status_url = export_response
        .headers()
        .get(header::LOCATION)
        .context("export creation omitted Location")?
        .to_str()?
        .to_owned();
    let export_operation: Value = export_response.json().await?;
    let export_id = export_operation["export_id"]
        .as_str()
        .context("export response omitted export_id")?
        .to_owned();
    let export_status_url = if export_status_url.starts_with("http") {
        export_status_url
    } else {
        format!("{base_url}{export_status_url}")
    };
    let replay = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-export")
        .send()
        .await?;
    ensure!(replay.status() == StatusCode::ACCEPTED);
    ensure!(
        replay.headers().get("idempotency-replayed")
            == Some(&header::HeaderValue::from_static("true"))
    );
    let replay_operation: Value = replay.json().await?;
    ensure!(replay_operation["export_id"] == export_id);

    let mut ready_etag = None;
    let mut content_url = None;
    let mut ready_operation = None;
    let mut last_export_body = Value::Null;
    for _ in 0..100 {
        let response = client
            .get(&export_status_url)
            .bearer_auth(bearer_token)
            .send()
            .await?;
        if response.status() == StatusCode::SEE_OTHER {
            ready_etag = response
                .headers()
                .get(header::ETAG)
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()?;
            let location = response
                .headers()
                .get(header::LOCATION)
                .context("ready export omitted content Location")?
                .to_str()?
                .to_owned();
            content_url = Some(if location.starts_with("http") {
                location
            } else {
                format!("{base_url}{location}")
            });
            ready_operation = Some(response);
            break;
        }
        ensure!(response.status() == StatusCode::OK);
        let body: Value = response.json().await?;
        last_export_body = body.clone();
        ensure!(body["state"] != "failed", "export failed: {body}");
        crate::sleep_budget::poll_sleep(Duration::from_millis(25)).await;
    }
    let _ready_response = ready_operation
        .with_context(|| format!("export did not become ready; last status: {last_export_body}"))?;
    let ready_etag = ready_etag.context("ready export omitted ETag")?;
    let content_url = content_url.context("ready export omitted content URL")?;
    let not_modified = client
        .get(&export_status_url)
        .bearer_auth(bearer_token)
        .header(header::IF_NONE_MATCH, &ready_etag)
        .send()
        .await?;
    ensure!(not_modified.status() == StatusCode::NOT_MODIFIED);
    ensure!(not_modified.headers().get(header::CACHE_CONTROL).is_some());

    let content_response = client
        .get(&content_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(content_response.status() == StatusCode::OK);
    ensure!(content_response.headers().get(header::ETAG).is_some());
    let content = content_response.bytes().await?;
    ensure!(
        String::from_utf8_lossy(&content).contains("export-delete-private-marker"),
        "export package omitted the authorized marker"
    );
    let no_export_read = client
        .get(&export_status_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    let no_export_read_status = no_export_read.status();
    let no_export_read_body = no_export_read.text().await?;
    ensure!(
        no_export_read_status == StatusCode::NOT_FOUND,
        "same-scope export read without a grant disclosed an operation: {}",
        no_export_read_status
    );
    for forbidden in [
        export_id.as_str(),
        episode_id.as_str(),
        "export-delete-private-marker",
    ] {
        ensure!(
            !no_export_read_body.contains(forbidden),
            "same-scope export read disclosed {forbidden}"
        );
    }

    let hidden_export = client
        .get(format!(
            "{base_url}/v1/tenants/{}/subjects/{}/exports/{export_id}",
            scope.principal_b_tenant_id, scope.principal_b_subject_id
        ))
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(hidden_export.status() == StatusCode::NOT_FOUND);

    let deletion_url = format!("{secondary_prefix}/deletions");
    let deletion_response = client
        .post(&deletion_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-deletion")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(
        deletion_response.status() == StatusCode::ACCEPTED,
        "deletion creation returned {}",
        deletion_response.status()
    );
    ensure!(
        deletion_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let export_after_fence = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-after-deletion-fence")
        .send()
        .await?;
    ensure!(
        export_after_fence.status() == StatusCode::NOT_FOUND,
        "export creation after deletion fence disclosed a new operation: {}",
        export_after_fence.status()
    );
    let deletion_status_url = deletion_response
        .headers()
        .get(header::LOCATION)
        .context("deletion creation omitted Location")?
        .to_str()?
        .to_owned();
    let deletion_body: Value = deletion_response.json().await?;
    let deletion_id = deletion_body["operation_id"]
        .as_str()
        .context("deletion response omitted operation_id")?
        .to_owned();
    let deletion_status_url = if deletion_status_url.starts_with("http") {
        deletion_status_url
    } else {
        format!("{base_url}{deletion_status_url}")
    };
    let no_deletion_grant_read = client
        .get(&deletion_status_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    let no_deletion_grant_status = no_deletion_grant_read.status();
    let no_deletion_grant_body = no_deletion_grant_read.text().await?;
    ensure!(
        no_deletion_grant_status == StatusCode::NOT_FOUND,
        "same-scope deletion read without a grant disclosed an operation: {}",
        no_deletion_grant_status
    );
    for forbidden in [
        deletion_id.as_str(),
        episode_id.as_str(),
        "export-delete-private-marker",
        "export-delete-deletion",
    ] {
        ensure!(
            !no_deletion_grant_body.contains(forbidden),
            "same-scope deletion denial disclosed {forbidden}"
        );
    }
    let deletion_replay = client
        .post(&deletion_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-deletion")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(deletion_replay.status() == StatusCode::ACCEPTED);
    ensure!(
        deletion_replay.headers().get("idempotency-replayed")
            == Some(&header::HeaderValue::from_static("true"))
    );
    let deletion_replay_body: Value = deletion_replay.json().await?;
    ensure!(deletion_replay_body["operation_id"] == deletion_id);

    let mut completed_etag = None;
    let mut last_deletion_body = Value::Null;
    for _ in 0..200 {
        let response = client
            .get(&deletion_status_url)
            .bearer_auth(bearer_token)
            .send()
            .await?;
        ensure!(response.status() == StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .context("deletion status omitted ETag")?
            .to_str()?
            .to_owned();
        let body: Value = response.json().await?;
        last_deletion_body = body.clone();
        if body["lifecycle_state"] == "completed" {
            let outcome = body["outcome"]
                .as_object()
                .context("completed deletion omitted terminal outcome")?;
            ensure!(outcome["live_disposition"] == "purged_and_verified");
            ensure!(outcome["backup_disposition"] == "not_configured");
            ensure!(outcome["backup_policy_id"].is_null());
            ensure!(outcome["deletion_watermark"].is_null());
            ensure!(outcome["restore_gate_version"].is_null());
            ensure!(
                body["targets"]
                    .as_array()
                    .context("completed deletion omitted target ledger")?
                    .iter()
                    .filter(|target| target["capability"] == "configured")
                    .all(|target| target["verification"] == "verified")
            );
            ensure!(
                outcome["verification_digest"]
                    .as_str()
                    .is_some_and(|digest| digest.len() == 64)
            );
            completed_etag = Some(etag);
            break;
        }
        ensure!(
            body["lifecycle_state"] != "failed",
            "deletion failed: {body}"
        );
        crate::sleep_budget::poll_sleep(Duration::from_millis(25)).await;
    }
    let completed_etag = completed_etag
        .with_context(|| format!("deletion did not complete: {last_deletion_body}"))?;
    let deletion_not_modified = client
        .get(&deletion_status_url)
        .bearer_auth(bearer_token)
        .header(header::IF_NONE_MATCH, completed_etag)
        .send()
        .await?;
    ensure!(deletion_not_modified.status() == StatusCode::NOT_MODIFIED);
    ensure!(
        deletion_not_modified
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some()
    );

    sqlx::query(
        "UPDATE memory.deletion_tombstones
         SET expires_at = clock_timestamp() - interval '1 second'
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(target.tenant_id)
    .bind(Uuid::parse_str(&deletion_id)?)
    .execute(migration_pool)
    .await?;
    let expired_status = client
        .get(&deletion_status_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(expired_status.status() == StatusCode::OK);
    let expired_body: Value = expired_status.json().await?;
    ensure!(expired_body["lifecycle_state"] == "expired");
    ensure!(expired_body["outcome"]["live_disposition"] == "purged_and_verified");

    let tombstone_material: String = sqlx::query_scalar(
        "SELECT concat_ws(' ', scope_digest, idempotency_key_digest,
            request_fingerprint_sha256, policy_version, worker_release,
            target_summary::text, verification_digest, backup_policy_id)
         FROM memory.deletion_tombstones
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(target.tenant_id)
    .bind(Uuid::parse_str(&deletion_id)?)
    .fetch_one(migration_pool)
    .await?;
    for forbidden in [
        episode_id.as_str(),
        "export-delete-episode",
        "export-delete-private-marker",
        "export-delete-deletion",
    ] {
        ensure!(
            !tombstone_material.contains(forbidden),
            "deletion tombstone retained {forbidden}"
        );
    }

    let deleted_episode = client
        .get(&episode_location)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(deleted_episode.status() == StatusCode::NOT_FOUND);
    let revoked_export = client
        .get(&content_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(
        matches!(
            revoked_export.status(),
            StatusCode::NOT_FOUND | StatusCode::GONE
        ),
        "revoked export remained readable: {}",
        revoked_export.status()
    );
    let deleted_export_status = client
        .get(&export_status_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(deleted_export_status.status() == StatusCode::NOT_FOUND);
    Ok(())
}
