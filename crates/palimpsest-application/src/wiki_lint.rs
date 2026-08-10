//! wiki_lint — durable wiki lint worker jobs (spec 017 P4, AC8).
//!
//! The lint pass is an operation, not state (R9). A periodic worker job
//! checks contradictions, orphans, stale claims, and provenance gaps. The
//! job follows the spec 011 worker pattern (jobs and claims, bounded
//! leases, crash-resumable). The worker writes lint state to the governed
//! fact namespace `wiki/lint` and generates a new open question in the
//! `open-questions` namespace through the governed fact path (001 R9).
//!
//! The worker materializes facts like the consolidation worker does: the
//! same repository (facts slot) writes the facts, and the write policy is
//! the registered `direct-evidence` policy seeded in migration 0008.

use palimpsest_domain::{CaseId, EpisodeId, FactId, SubjectId, TenantId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Governed fact namespace for lint state (R9).
pub const WIKI_LINT_NAMESPACE: &str = "wiki/lint";
/// Fact namespace for open questions (R8). The lint worker generates new
/// open questions here.
pub const WIKI_OPEN_QUESTIONS_NAMESPACE: &str = "open-questions";
/// Principal recorded as the writer of lint facts and open questions.
pub const WIKI_LINT_PRINCIPAL_ID: &str = "palimpsest-wiki-lint-worker";
/// Registered write policy id used by the lint worker (seeded in 0008).
pub const WIKI_LINT_WRITE_POLICY_ID: &str = "direct-evidence";
/// Registered write policy version used by the lint worker.
pub const WIKI_LINT_WRITE_POLICY_VERSION: &str = "1";
/// Claims untouched for this many days are stale.
pub const WIKI_LINT_STALE_AFTER_DAYS: i64 = 30;
/// Worker lease for a claimed job, in seconds.
pub const WIKI_LINT_LEASE_SECONDS: u32 = 30;
/// Deterministic worker identity for wiki lint claims.
pub const WIKI_LINT_WORKER_ID: Uuid = Uuid::from_u128(0x77696b695f6c696e745f776f726b6572);

#[derive(Debug, Clone)]
pub struct NewWikiLintJob {
    pub principal_id: palimpsest_domain::PrincipalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateWikiLintJobOutcome {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiLintJobView {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub contradictions: i32,
    pub orphans: i32,
    pub stale_claims: i32,
    pub provenance_gaps: i32,
    pub lint_fact_id: Option<Uuid>,
    pub question_fact_id: Option<Uuid>,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimedWikiLintJob {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub job_id: Uuid,
}

/// One scanned fact that a lint check flagged.
#[derive(Debug, Clone)]
pub struct WikiLintScanFact {
    pub fact_id: FactId,
    pub case_id: CaseId,
    pub namespace: String,
    pub key: String,
    pub sensitivity: String,
    /// The head revision's evidence episodes (grounding for the lint fact).
    pub evidence_episode_ids: Vec<EpisodeId>,
    /// All fact ids of the contradiction pair (empty for other findings).
    pub related_fact_ids: Vec<FactId>,
}

/// The deterministic lint findings for one subject.
#[derive(Debug, Clone, Default)]
pub struct WikiLintFindings {
    pub contradictions: Vec<WikiLintScanFact>,
    pub orphans: Vec<WikiLintScanFact>,
    pub stale_claims: Vec<WikiLintScanFact>,
    pub provenance_gaps: Vec<WikiLintScanFact>,
}

impl WikiLintFindings {
    /// Whether any finding exists.
    pub fn is_empty(&self) -> bool {
        self.contradictions.is_empty()
            && self.orphans.is_empty()
            && self.stale_claims.is_empty()
            && self.provenance_gaps.is_empty()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WikiLintRunSummary {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub contradictions: i32,
    pub orphans: i32,
    pub stale_claims: i32,
    pub provenance_gaps: i32,
    pub lint_fact_id: Option<Uuid>,
    pub question_fact_id: Option<Uuid>,
}

impl WikiLintRunSummary {
    /// Construct an idle summary: no job was available to claim.
    pub fn idle() -> Self {
        Self {
            job_id: Uuid::nil(),
            lifecycle_state: "idle".to_owned(),
            contradictions: 0,
            orphans: 0,
            stale_claims: 0,
            provenance_gaps: 0,
            lint_fact_id: None,
            question_fact_id: None,
        }
    }

    /// Construct a failure summary for a job that could not run.
    pub fn failed(job_id: Uuid) -> Self {
        Self {
            job_id,
            lifecycle_state: "failed".to_owned(),
            contradictions: 0,
            orphans: 0,
            stale_claims: 0,
            provenance_gaps: 0,
            lint_fact_id: None,
            question_fact_id: None,
        }
    }
}

/// Deterministic idempotency key for the lint fact of one job.
pub fn wiki_lint_fact_idempotency_key(tenant_id: TenantId, job_id: Uuid) -> String {
    format!("wiki-lint:{}:{}", tenant_id.0, job_id)
}

/// Deterministic idempotency key for the open question of one job.
pub fn wiki_lint_question_idempotency_key(tenant_id: TenantId, job_id: Uuid) -> String {
    format!("wiki-lint-question:{}:{}", tenant_id.0, job_id)
}

/// The case and evidence grounding for the lint facts of one job: the
/// first finding's case and head-revision evidence. Every fact revision
/// requires attributable evidence, so the lint facts are grounded in the
/// first finding; the value records the full trace.
pub fn wiki_lint_grounding(findings: &WikiLintFindings) -> (CaseId, Vec<EpisodeId>) {
    let first = findings
        .contradictions
        .first()
        .or_else(|| findings.orphans.first())
        .or_else(|| findings.stale_claims.first())
        .or_else(|| findings.provenance_gaps.first());
    match first {
        Some(finding) => (finding.case_id, finding.evidence_episode_ids.clone()),
        None => (CaseId(Uuid::nil()), Vec::new()),
    }
}

#[async_trait::async_trait]
pub trait LintRepository: Send + Sync {
    async fn create_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewWikiLintJob,
        idempotency: crate::IdempotencyRequest,
    ) -> Result<CreateWikiLintJobOutcome, crate::RepositoryError>;

    async fn poll_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<WikiLintJobView, crate::RepositoryError>;

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedWikiLintJob>, crate::RepositoryError>;

    async fn complete_job(
        &self,
        job: &ClaimedWikiLintJob,
        worker_id: Uuid,
        findings: &WikiLintFindings,
        lint_fact_id: Option<Uuid>,
        question_fact_id: Option<Uuid>,
    ) -> Result<(), crate::RepositoryError>;

    async fn fail_job(
        &self,
        job: &ClaimedWikiLintJob,
        worker_id: Uuid,
        reason: &str,
    ) -> Result<(), crate::RepositoryError>;

    /// Scan the canonical fact metadata for the four lint checks. Reads
    /// only; the worker writes findings through the governed fact path.
    async fn list_findings(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        stale_cutoff: OffsetDateTime,
    ) -> Result<WikiLintFindings, crate::RepositoryError>;
}
