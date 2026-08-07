//! Governed consolidation (spec 011): the interpreter boundary and the
//! service types for durable consolidation jobs.

use std::collections::HashMap;

use async_trait::async_trait;
use palimpsest_domain::{CaseId, EpisodeId, FactId, PrincipalId, RevisionId, SubjectId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub const FIXTURE_DETERMINISTIC_INTERPRETER: &str = "fixture-deterministic-v1";
pub const CONSOLIDATION_WORKER_LEASE_SECONDS: u32 = 30;
pub const CONSOLIDATION_MAX_CLAIMS_PER_RUN: u32 = 64;
pub const CONSOLIDATION_CLAIM_CAP: i32 = 100_000;
pub const CONSOLIDATION_WRITER_PRINCIPAL_ID: &str = "palimpsest-consolidation-worker";
pub const CONSOLIDATION_DERIVED_FACT_NAMESPACE: &str = "derived";
pub const CONSOLIDATION_PROVENANCE_KIND: &str = "derived";
pub const CONSOLIDATION_SKIP_REASON_LOW_CONFIDENCE: &str = "low_confidence";

#[derive(Debug, Clone, Serialize)]
pub struct ConsolidationInterpreterConfigView {
    pub tenant_id: TenantId,
    pub interpreter_config_id: Uuid,
    pub provider_kind: String,
    pub prompt_policy_version: String,
    pub config_digest: String,
}

#[derive(Debug, Clone)]
pub struct NewConsolidationInterpreterConfig {
    pub provider_kind: String,
    pub prompt_policy_version: String,
    pub created_by_principal_id: PrincipalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsolidationPolicyView {
    pub tenant_id: TenantId,
    pub source_kind: String,
    pub policy_id: String,
    pub interpreter_config_id: Uuid,
    pub write_policy_id: String,
    pub write_policy_version: String,
    pub retention_policy_id: String,
    pub confidence_auto_promote_min: f64,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewConsolidationPolicy {
    pub source_kind: String,
    pub policy_id: String,
    pub interpreter_config_id: Uuid,
    pub write_policy_id: String,
    pub write_policy_version: String,
    pub retention_policy_id: String,
    pub confidence_auto_promote_min: f64,
    pub created_by_principal_id: PrincipalId,
}

#[derive(Debug, Clone)]
pub struct NewConsolidationJob {
    pub source_kind: String,
    pub policy_id: String,
    pub window_from: OffsetDateTime,
    pub window_until: OffsetDateTime,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateConsolidationJobOutcome {
    pub job_id: Uuid,
    pub lifecycle_state: String,
    pub claims_total: i32,
    pub claim_cap: i32,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationJobView {
    pub job_id: Uuid,
    pub source_kind: String,
    pub policy_id: String,
    pub policy_version: String,
    pub lifecycle_state: String,
    pub claims_total: i32,
    pub claims_done: i32,
    pub claim_cap: i32,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimedConsolidationJob {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub job_id: Uuid,
    pub source_kind: String,
    pub policy_id: String,
    pub policy_version: String,
    pub window_from: OffsetDateTime,
    pub window_until: OffsetDateTime,
    pub claim_cap: i32,
    pub principal_id: PrincipalId,
}

#[derive(Debug, Clone)]
pub struct PendingConsolidationClaim {
    pub claim_id: Uuid,
    pub case_id: CaseId,
    pub episode_ids: Vec<EpisodeId>,
    pub content_hash: String,
    pub confidence: f64,
    pub sensitivity: String,
    pub observed_at: OffsetDateTime,
    pub valid_from: OffsetDateTime,
    pub valid_until: Option<OffsetDateTime>,
    pub model_identity: String,
    pub prompt_policy_version: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ClaimedConsolidationClaim {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub job_id: Uuid,
    pub claim_id: Uuid,
    pub case_id: CaseId,
    pub episode_ids: Vec<EpisodeId>,
    pub content_hash: String,
    pub confidence: f64,
    pub sensitivity: String,
    pub observed_at: OffsetDateTime,
    pub valid_from: OffsetDateTime,
    pub valid_until: Option<OffsetDateTime>,
    pub model_identity: String,
    pub prompt_policy_version: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct WorkerPolicySnapshot {
    pub interpreter_config_id: Uuid,
    pub write_policy_id: String,
    pub write_policy_version: String,
    pub retention_policy_id: String,
    pub confidence_auto_promote_min: f64,
    pub provider_kind: String,
    pub prompt_policy_version: String,
    pub config_digest: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ConsolidationWorkerRunSummary {
    pub jobs_processed: u32,
    pub claims_written: u32,
    pub claims_done: u32,
    pub claims_skipped: u32,
    pub failed_jobs: u32,
}

// --- Interpreter boundary (R2, R7) -------------------------------------

#[derive(Debug, Clone)]
pub struct InterpreterEpisode {
    pub episode_id: EpisodeId,
    pub case_id: CaseId,
    pub observed_at: OffsetDateTime,
    pub source_type: String,
    pub payload_digest: String,
}

#[derive(Debug)]
pub struct InterpreterContext<'a> {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub source_kind: &'a str,
    pub policy_id: &'a str,
    pub policy_version: &'a str,
    pub model_identity: &'a str,
    pub prompt_policy_version: &'a str,
}

#[derive(Debug, Clone)]
pub struct InterpreterClaim {
    pub case_id: CaseId,
    pub episode_ids: Vec<EpisodeId>,
    pub observed_at: OffsetDateTime,
    pub valid_from: OffsetDateTime,
    pub valid_until: Option<OffsetDateTime>,
    pub confidence: f64,
    pub sensitivity: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum InterpreterError {
    #[error("the interpreter provider is not registered: {0}")]
    ProviderNotRegistered(String),
    #[error("the interpreter failed: {0}")]
    Failed(String),
}

#[async_trait]
pub trait ConsolidationInterpreter: Send + Sync {
    fn provider_kind(&self) -> &'static str;
    async fn interpret(
        &self,
        context: &InterpreterContext<'_>,
        episodes: &[InterpreterEpisode],
    ) -> Result<Vec<InterpreterClaim>, InterpreterError>;
}

/// Deterministic fixture interpreter. One claim per episode. The claim id is
/// derived from the scope and the source episode, so a replay of the same
/// episodes under the same policy yields the same claims (R3). This provider
/// exists for tests and for the neutrality default: no external model is
/// configured by default.
#[derive(Debug, Default)]
pub struct FixtureDeterministicInterpreter;

#[async_trait]
impl ConsolidationInterpreter for FixtureDeterministicInterpreter {
    fn provider_kind(&self) -> &'static str {
        FIXTURE_DETERMINISTIC_INTERPRETER
    }

    async fn interpret(
        &self,
        _context: &InterpreterContext<'_>,
        episodes: &[InterpreterEpisode],
    ) -> Result<Vec<InterpreterClaim>, InterpreterError> {
        Ok(episodes
            .iter()
            .map(|episode| {
                let value = json!({
                    "kind": "fixture-summary",
                    "episode_id": episode.episode_id.0,
                    "episode_digest": episode.payload_digest,
                });
                InterpreterClaim {
                    case_id: episode.case_id,
                    episode_ids: vec![episode.episode_id],
                    observed_at: episode.observed_at,
                    valid_from: episode.observed_at,
                    valid_until: None,
                    confidence: 0.9,
                    sensitivity: "internal".to_owned(),
                    value,
                }
            })
            .collect())
    }
}

/// Deterministic claim id from the scope, the policy, and the source episode.
/// Independent of the job id, so a replayed job over the same episodes under
/// the same policy derives the same claim ids and reuses the same fact
/// idempotency keys (R3, A2).
pub fn consolidation_claim_id(
    tenant_id: TenantId,
    subject_id: SubjectId,
    source_kind: &str,
    policy_id: &str,
    episode_id: EpisodeId,
) -> Uuid {
    let namespace_input = format!(
        "palimpsest.consolidation-claim/v1:{}:{}:{}:{}",
        tenant_id.0, subject_id.0, source_kind, policy_id
    );
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, namespace_input.as_bytes());
    Uuid::new_v5(&namespace, episode_id.0.as_bytes())
}

/// Deterministic per-claim fact idempotency key. Independent of the job id.
pub fn consolidation_claim_idempotency_key(
    tenant_id: TenantId,
    subject_id: SubjectId,
    source_kind: &str,
    policy_id: &str,
    claim_id: Uuid,
) -> String {
    format!(
        "palimpsest.consolidation/v1:{}:{}:{}:{}:{}",
        tenant_id.0, subject_id.0, source_kind, policy_id, claim_id
    )
}

/// Content hash of a derived claim: sha256 of the canonical JSON value.
pub fn consolidation_content_hash(value: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palimpsest.consolidation-content/v1:");
    hasher.update(serde_json::to_vec(value).expect("claim value is serializable"));
    hex::encode(hasher.finalize())
}

/// Deterministic fact key for a derived claim.
pub fn consolidation_fact_key(source_kind: &str, claim_id: Uuid) -> String {
    format!("{source_kind}:{claim_id}")
}

#[derive(Default)]
pub struct InterpreterRegistry {
    providers: HashMap<&'static str, Box<dyn ConsolidationInterpreter>>,
}

impl InterpreterRegistry {
    pub fn register(&mut self, interpreter: Box<dyn ConsolidationInterpreter>) {
        self.providers
            .insert(interpreter.provider_kind(), interpreter);
    }

    pub fn resolve(
        &self,
        provider_kind: &str,
    ) -> Result<&dyn ConsolidationInterpreter, InterpreterError> {
        self.providers
            .get(provider_kind)
            .map(|provider| provider.as_ref())
            .ok_or_else(|| InterpreterError::ProviderNotRegistered(provider_kind.to_owned()))
    }
}

/// Repository port. The worker and the API surface both use it.
#[async_trait]
pub trait ConsolidationRepository: Send + Sync {
    async fn register_interpreter_config(
        &self,
        tenant_id: TenantId,
        request: NewConsolidationInterpreterConfig,
    ) -> Result<ConsolidationInterpreterConfigView, crate::RepositoryError>;

    async fn get_interpreter_config(
        &self,
        tenant_id: TenantId,
        interpreter_config_id: Uuid,
    ) -> Result<ConsolidationInterpreterConfigView, crate::RepositoryError>;

    async fn register_policy(
        &self,
        tenant_id: TenantId,
        request: NewConsolidationPolicy,
    ) -> Result<ConsolidationPolicyView, crate::RepositoryError>;

    async fn get_policy(
        &self,
        tenant_id: TenantId,
        source_kind: &str,
        policy_id: &str,
    ) -> Result<ConsolidationPolicyView, crate::RepositoryError>;

    async fn create_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewConsolidationJob,
        idempotency: crate::IdempotencyRequest,
    ) -> Result<CreateConsolidationJobOutcome, crate::RepositoryError>;

    async fn poll_job(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<ConsolidationJobView, crate::RepositoryError>;

    async fn claim_next_job(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedConsolidationJob>, crate::RepositoryError>;

    async fn worker_policy_snapshot(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<WorkerPolicySnapshot, crate::RepositoryError>;

    async fn select_window_episodes(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<Vec<InterpreterEpisode>, crate::RepositoryError>;

    async fn insert_claims(
        &self,
        job: &ClaimedConsolidationJob,
        claims: &[PendingConsolidationClaim],
    ) -> Result<(), crate::RepositoryError>;

    async fn claim_next_claim(
        &self,
        job: &ClaimedConsolidationJob,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedConsolidationClaim>, crate::RepositoryError>;

    async fn complete_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
        fact_id: FactId,
        revision_id: RevisionId,
    ) -> Result<bool, crate::RepositoryError>;

    async fn skip_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
        reason: &str,
    ) -> Result<bool, crate::RepositoryError>;

    async fn release_claim(
        &self,
        claim: &ClaimedConsolidationClaim,
    ) -> Result<bool, crate::RepositoryError>;

    async fn complete_job(
        &self,
        job: &ClaimedConsolidationJob,
    ) -> Result<bool, crate::RepositoryError>;

    async fn fail_job(
        &self,
        job: &ClaimedConsolidationJob,
        reason: &str,
    ) -> Result<bool, crate::RepositoryError>;
}
