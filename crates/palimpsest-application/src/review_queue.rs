//! review_queue — durable review-queue jobs (spec 017 P3, AC6).
//!
//! A review-queue job scans the canonical fact metadata for pages whose
//! latest revision predates the staleness window. The job follows the
//! spec 011 worker pattern (jobs and claims, bounded leases,
//! crash-resumable). Its output is an advisory surface (spec 012):
//! the canonical layer is never written by the worker.

use palimpsest_domain::{SubjectId, TenantId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Identity of the review-queue surface host (spec 012 host id).
pub const REVIEW_QUEUE_HOST_ID: &str = "palimpsest-review-queue";
/// Principal recorded as the writer of the advisory surface.
pub const REVIEW_QUEUE_PRINCIPAL_ID: &str = "palimpsest-review-queue-worker";
/// Pages untouched for this many days are flagged.
pub const REVIEW_QUEUE_STALE_AFTER_DAYS: i64 = 30;
/// Worker lease for a claimed job, in seconds.
pub const REVIEW_QUEUE_LEASE_SECONDS: u32 = 30;
/// Deterministic worker identity for review-queue claims.
pub const REVIEW_QUEUE_WORKER_ID: Uuid = Uuid::from_u128(0x7265766965775f71756575655f303031);

#[derive(Debug, Clone)]
pub struct NewReviewQueueJob {
    pub principal_id: palimpsest_domain::PrincipalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateReviewQueueJobOutcome {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewQueueJobView {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub stale_pages: i32,
    pub surface_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimedReviewQueueJob {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub job_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct ReviewQueueScanPage {
    pub fact_id: palimpsest_domain::FactId,
    pub key: String,
    pub sensitivity: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReviewQueueRunSummary {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub stale_pages: i32,
    pub surface_id: Option<Uuid>,
}

#[async_trait::async_trait]
pub trait ReviewQueueRepository: Send + Sync {
    async fn create_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewReviewQueueJob,
        idempotency: crate::IdempotencyRequest,
    ) -> Result<CreateReviewQueueJobOutcome, crate::RepositoryError>;

    async fn poll_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<ReviewQueueJobView, crate::RepositoryError>;

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedReviewQueueJob>, crate::RepositoryError>;

    async fn complete_job(
        &self,
        job: &ClaimedReviewQueueJob,
        worker_id: Uuid,
        stale_pages: i32,
        surface_id: Option<Uuid>,
    ) -> Result<(), crate::RepositoryError>;

    async fn fail_job(
        &self,
        job: &ClaimedReviewQueueJob,
        worker_id: Uuid,
        reason: &str,
    ) -> Result<(), crate::RepositoryError>;

    /// Scan the canonical fact metadata: current pages whose latest
    /// revision predates the cutoff. Reads only, never writes facts.
    async fn list_stale_pages(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        cutoff: OffsetDateTime,
    ) -> Result<Vec<ReviewQueueScanPage>, crate::RepositoryError>;
}

impl ReviewQueueRunSummary {
    /// Construct an idle summary: no job was available to claim.
    pub fn idle() -> Self {
        Self {
            job_id: Uuid::nil(),
            lifecycle_state: "idle".to_owned(),
            stale_pages: 0,
            surface_id: None,
        }
    }

    /// Construct a failure summary for a job that could not run.
    pub fn failed(job_id: Uuid) -> Self {
        Self {
            job_id,
            lifecycle_state: "failed".to_owned(),
            stale_pages: 0,
            surface_id: None,
        }
    }
}

/// Parse a tenant id from a SQL text value (worker claim path).
pub fn parse_tenant_id(value: String) -> Result<TenantId, crate::RepositoryError> {
    Uuid::parse_str(&value)
        .map(TenantId)
        .map_err(|error| crate::RepositoryError::Unexpected(error.to_string()))
}

/// Parse a subject id from a SQL text value (worker claim path).
pub fn parse_subject_id(value: String) -> Result<SubjectId, crate::RepositoryError> {
    Uuid::parse_str(&value)
        .map(SubjectId)
        .map_err(|error| crate::RepositoryError::Unexpected(error.to_string()))
}
