use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use palimpsest_domain::{
    AgentId, AppendEpisode, CheckpointPrecondition, CheckpointRevisionId, CheckpointView,
    CompleteEffectTransition, CreateFact, CreateRetrieval, DeletionBackupDisposition,
    DeletionLiveDisposition, DeletionOperationId, DeletionOperationState, DeletionTargetCapability,
    DeletionTargetName, DeletionTargetState, DeletionTargetVerification, EffectId,
    EffectTransition, EmbeddingInput, EmbeddingOutput, EmbeddingProfile, EmbeddingTask, Episode,
    EpisodeId, ExportId, FactId, FactKey, FactNamespace, FactView, HotCache, HotCacheKind,
    NewCheckpointRevision, NewEffectTransition, NewEpisode, NewFact, NewFactRevision,
    NewPreparedEffect, NewRetrieval, NoopHotCache, OperationGrant, PrincipalId, PrincipalScope,
    RetentionPolicyId, RetrievalFilters, RetrievalId, RetrievalPolicyId, RetrievalQuery,
    RetrievalReceipt, RevisionId, SaveCheckpoint, Sensitivity, SubjectContentLease, SubjectId,
    SubjectLifecycle, SupersedeFact, TenantId, ThreadId, ValidTime, WritePolicy, WritePolicyId,
    WritePolicyVersion,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub mod backup;
pub mod consolidation;
pub mod export;
pub mod recovery;
pub mod surface;

pub use consolidation::{
    CONSOLIDATION_CLAIM_CAP, CONSOLIDATION_DERIVED_FACT_NAMESPACE,
    CONSOLIDATION_MAX_CLAIMS_PER_RUN, CONSOLIDATION_PROVENANCE_KIND,
    CONSOLIDATION_SKIP_REASON_LOW_CONFIDENCE, CONSOLIDATION_WORKER_LEASE_SECONDS,
    CONSOLIDATION_WRITER_PRINCIPAL_ID, ClaimedConsolidationClaim, ClaimedConsolidationJob,
    ConsolidationInterpreter, ConsolidationInterpreterConfigView, ConsolidationJobView,
    ConsolidationPolicyView, ConsolidationRepository, ConsolidationWorkerRunSummary,
    CreateConsolidationJobOutcome, FixtureDeterministicInterpreter, InterpreterContext,
    InterpreterEpisode, InterpreterError, InterpreterRegistry, NewConsolidationInterpreterConfig,
    NewConsolidationJob, NewConsolidationPolicy, PendingConsolidationClaim, WorkerPolicySnapshot,
    consolidation_claim_id, consolidation_claim_idempotency_key, consolidation_content_hash,
    consolidation_fact_key,
};

pub use export::{
    CANONICAL_HISTORY_EXPORT_PROFILE, CanonicalHistoryPackage, EXPORT_RETENTION_HOURS,
    ExportCreateOutcome, ExportMaterialization, ExportOperationState, ExportOperationView,
    ExportPackage, ExportPackageError, ExportPackageMetadata, ExportPackageStore,
    ExportProcessingContext, ExportRecord, ExportRecordKind, ExportRepository, ExportStoreError,
    FileExportPackageStore, InMemoryExportPackageStore, NewExport, S3_EXPORT_PACKAGE_STORE_PROFILE,
    S3ExportPackageStore, S3ExportPackageStoreConfig, S3ExportPackageStoreConfigError,
    WIKI_VAULT_EXPORT_PROFILE, WikiVaultPackage, is_supported_export_profile,
};
pub use recovery::{
    RESTORE_FENCE_LEDGER_PROFILE, RESTORE_FENCE_LEDGER_SCHEMA_VERSION, RestoreFenceEntry,
    RestoreFenceLedger, RestoreFenceLedgerError, verify_restore_fence_ledger,
};
pub use surface::{
    CreateSurfaceOutcome, NewSurfacePolicy, NewSurfaceRequest, SURFACE_DEFAULT_MAX_CONTEXT_TOKENS,
    SURFACE_DEFAULT_MAX_ITEMS, SURFACE_DEFAULT_MAX_RESULT_TOKENS, SURFACE_MAX_CONTEXT_TERMS,
    SURFACE_MAX_ITEMS, SURFACE_MAX_TERM_LENGTH, SurfaceBundle, SurfaceBundleItem,
    SurfacePolicyView, SurfaceRepository, surface_request_fingerprint,
};

const MAX_CHECKPOINT_STATE_BYTES: usize = 1_048_576;
const MAX_CHECKPOINT_EFFECT_TRANSITIONS: usize = 100;
const MAX_RETRIEVAL_QUERY_BYTES: usize = 4096;
const MAX_RETRIEVAL_FILTER_VALUES: usize = 100;
pub const DELETION_RETENTION_HOURS: u32 = 24 * 90;
pub const DELETION_WORKER_LEASE_SECONDS: u32 = 30;
pub const DELETION_MAX_ATTEMPTS: u32 = 5;
const DELETION_WORKER_MAX_CLAIMS_PER_RUN: u32 = 64;

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("record not found")]
    NotFound,
    #[error("subject content is unavailable")]
    SubjectUnavailable,
    #[error("record conflicts with existing data")]
    Conflict,
    #[error("record has expired")]
    Expired,
    #[error("idempotency key was reused for a different request")]
    IdempotencyKeyReused,
    #[error("idempotent request is still in progress")]
    IdempotencyInProgress,
    #[error("fact precondition did not match the current head")]
    PreconditionFailed,
    #[error("supersession does not name the current head")]
    SupersessionConflict,
    #[error("recorded-time coordinate is in the future")]
    FutureRecordedTime,
    #[error("checkpoint precondition did not match the current head")]
    CheckpointPreconditionFailed,
    #[error("checkpoint parent does not name the current head")]
    CheckpointParentConflict,
    #[error("checkpoint case does not match the existing lineage")]
    CheckpointCaseConflict,
    #[error("checkpoint already exists")]
    CheckpointAlreadyExists,
    #[error("checkpoint has expired")]
    CheckpointExpired,
    #[error("effect key conflicts with an existing effect")]
    EffectKeyConflict,
    #[error("effect transition is invalid")]
    InvalidEffectTransition,
    #[error("retention policy rejected the checkpoint")]
    RetentionPolicyRejected,
    #[error("fact write policy is not registered for retrieval metadata")]
    WritePolicyRejected,
    #[error("transaction serialization must be retried")]
    SerializationRetry,
    #[error("repository failure: {0}")]
    Unexpected(String),
}

#[async_trait]
pub trait EpisodeRepository: Send + Sync {
    async fn append(
        &self,
        episode: NewEpisode,
        idempotency: IdempotencyRequest,
    ) -> Result<AppendOutcome, RepositoryError>;

    async fn get(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        episode_id: EpisodeId,
    ) -> Result<Episode, RepositoryError>;
}

#[async_trait]
pub trait FactRepository: Send + Sync {
    async fn create(
        &self,
        fact: NewFact,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError>;

    async fn get_current(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
    ) -> Result<FactView, RepositoryError>;

    async fn supersede(
        &self,
        revision: NewFactRevision,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError>;

    async fn get_as_of(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
        valid_at: time::OffsetDateTime,
        recorded_at: time::OffsetDateTime,
    ) -> Result<FactView, RepositoryError>;
}

#[async_trait]
pub trait CheckpointRepository: Send + Sync {
    async fn save(
        &self,
        revision: NewCheckpointRevision,
        idempotency: IdempotencyRequest,
    ) -> Result<CheckpointMutationOutcome, RepositoryError>;

    async fn get_current(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        agent_id: AgentId,
        thread_id: ThreadId,
    ) -> Result<CheckpointView, RepositoryError>;
}

#[async_trait]
pub trait RetrievalRepository: Send + Sync {
    async fn prepare_receipt(
        &self,
        retrieval: &NewRetrieval,
        idempotency: &IdempotencyRequest,
    ) -> Result<RetrievalPreparation, RepositoryError>;

    async fn create_receipt(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
        query_embedding: Option<RetrievalQueryEmbedding>,
    ) -> Result<RetrievalMutationOutcome, RepositoryError>;

    async fn get_receipt(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
        authorization_scope_sha256: String,
    ) -> Result<RetrievalReceipt, RepositoryError>;
}

#[async_trait]
pub trait SubjectContentLeaseRepository: Send + Sync {
    async fn acquire_content_lease(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectContentLease, RepositoryError>;

    async fn release_content_lease(
        &self,
        lease: &SubjectContentLease,
    ) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait SubjectLifecycleControllerRepository: Send + Sync {
    async fn transition_to_deletion_pending(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError>;

    async fn transition_to_deleted(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, RepositoryError>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeletionTargetView {
    pub target_name: DeletionTargetName,
    pub target_key_digest: String,
    pub capability: DeletionTargetCapability,
    pub state: DeletionTargetState,
    pub verification: DeletionTargetVerification,
    pub attempts: u32,
    pub lease_id: Option<Uuid>,
    pub lease_expires_at: Option<time::OffsetDateTime>,
    pub effect_receipt_sha256: Option<String>,
    pub sanitized_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletionOutcomeView {
    pub live_disposition: DeletionLiveDisposition,
    pub backup_disposition: DeletionBackupDisposition,
    pub backup_policy_id: Option<String>,
    pub deletion_watermark: Option<String>,
    pub earliest_backup_expiry: Option<time::OffsetDateTime>,
    pub restore_gate_version: Option<String>,
    pub verification_digest: Option<String>,
}

impl DeletionOutcomeView {
    pub fn fenced_not_verified() -> Self {
        Self {
            live_disposition: DeletionLiveDisposition::FencedNotVerified,
            backup_disposition: DeletionBackupDisposition::NotConfigured,
            backup_policy_id: None,
            deletion_watermark: None,
            earliest_backup_expiry: None,
            restore_gate_version: None,
            verification_digest: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DeletionOperationView {
    pub operation_id: DeletionOperationId,
    pub lifecycle_state: DeletionOperationState,
    pub state_version: u64,
    pub retry_count: u32,
    pub failure_reason: Option<String>,
    pub targets: Vec<DeletionTargetView>,
    pub outcome: Option<DeletionOutcomeView>,
    pub updated_at: time::OffsetDateTime,
    pub expired: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDeletionOutcome {
    pub operation_id: DeletionOperationId,
    pub lifecycle_state: DeletionOperationState,
    pub state_version: u64,
    pub replayed: bool,
    pub targets: Vec<DeletionTargetView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedDeletionOperation {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub operation_id: DeletionOperationId,
    pub lifecycle_state: DeletionOperationState,
    pub state_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedDeletionTarget {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub operation_id: DeletionOperationId,
    pub worker_id: Uuid,
    pub target_name: DeletionTargetName,
    pub target_key_digest: String,
    pub target_lease_id: Uuid,
    pub attempts: u32,
    pub lease_expires_at: time::OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceDeletionOutcome {
    pub lifecycle_state: DeletionOperationState,
    pub state_version: u64,
    pub next_poll_seconds: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeletionWorkerRunSummary {
    pub processed: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDeletionRequest {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub principal_id: PrincipalId,
    pub idempotency_key: String,
    pub request_fingerprint_sha256: String,
    pub configured_targets: Vec<DeletionTargetName>,
    pub retention_hours: u32,
}

#[async_trait]
pub trait DeletionRepository: Send + Sync {
    async fn create_deletion_operation(
        &self,
        request: CreateDeletionRequest,
    ) -> Result<CreateDeletionOutcome, RepositoryError>;

    async fn poll_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
    ) -> Result<DeletionOperationView, RepositoryError>;

    async fn repair_deletion_operation(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
        reason_code: &str,
    ) -> Result<(), RepositoryError>;

    async fn claim_next_deletion_operation(
        &self,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionOperation>, RepositoryError>;

    async fn renew_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError>;

    async fn release_deletion_operation_lease(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: Uuid,
    ) -> Result<(), RepositoryError>;

    async fn claim_next_deletion_target(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ClaimedDeletionTarget>, RepositoryError>;

    async fn renew_deletion_target_lease(
        &self,
        target: &ClaimedDeletionTarget,
        lease_seconds: u32,
    ) -> Result<(), RepositoryError>;

    async fn apply_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
    ) -> Result<(), RepositoryError>;

    async fn fail_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        sanitized_error: &str,
        max_attempts: u32,
    ) -> Result<(), RepositoryError>;

    async fn complete_deletion_target(
        &self,
        target: &ClaimedDeletionTarget,
        effect_receipt_sha256: &str,
    ) -> Result<(), RepositoryError>;

    async fn advance_deletion_operation(
        &self,
        claimed: &ClaimedDeletionOperation,
        worker_id: Uuid,
        max_attempts: u32,
    ) -> Result<AdvanceDeletionOutcome, RepositoryError>;
}

pub trait SubjectLifecycleRepository:
    SubjectContentLeaseRepository + SubjectLifecycleControllerRepository + DeletionRepository
{
}

impl<T> SubjectLifecycleRepository for T where
    T: SubjectContentLeaseRepository + SubjectLifecycleControllerRepository + DeletionRepository
{
}

/// Re-establishes export authorization from the currently trusted policy
/// source. Persisted export rows are only job state; they are never an
/// authorization source for a worker.
pub trait ExportWorkerAuthorizer: Send + Sync {
    fn authorize_export(
        &self,
        principal_id: &PrincipalId,
        tenant_id: TenantId,
        subject_id: SubjectId,
        authorization_scope_sha256: &str,
    ) -> Result<PrincipalScope, ServiceError>;
}

#[derive(Clone, Debug)]
pub enum RetrievalPreparation {
    Replay(RetrievalMutationOutcome),
    Execute {
        embedding_profile: Option<EmbeddingProfile>,
    },
}

/// Bounded window for recent retrieval receipts (spec 015 R3). The cache is
/// advisory: an entry older than this TTL is a miss and the canonical read
/// decides. Receipts are append-only, so within the TTL a hit is airtight.
const HOT_CACHE_RECEIPT_TTL_SECONDS: u64 = 300;

/// Serve a retrieval receipt from the hot cache when the hit is valid.
///
/// The gate mirrors the canonical read (spec 015 R9): tenant, subject, and
/// retrieval id must match, the authorization scope digest must equal the
/// caller's freshly computed scope, and only the initial (cursor-less) read
/// is cached — paged reads always go canonical because the receipt's items
/// depend on the cursor position.
async fn cached_retrieval_receipt(
    cache: &dyn HotCache,
    tenant_id: TenantId,
    subject_id: SubjectId,
    retrieval_id: RetrievalId,
    cursor: Option<&str>,
    scope_sha256: &str,
) -> Option<RetrievalReceipt> {
    if cursor.is_some() {
        return None;
    }
    let bytes = cache
        .get(
            tenant_id,
            HotCacheKind::Receipt,
            retrieval_id.0.to_string().as_str(),
        )
        .await?;
    let receipt: RetrievalReceipt = serde_json::from_slice(&bytes).ok()?;
    let valid = receipt.tenant_id == tenant_id
        && receipt.subject_id == subject_id
        && receipt.retrieval_id == retrieval_id
        && receipt.authorization.scope_digest == scope_sha256;
    if valid { Some(receipt) } else { None }
}

#[derive(Clone, Debug)]
pub struct RetrievalQueryEmbedding {
    pub profile: EmbeddingProfile,
    pub output: EmbeddingOutput,
}

#[derive(Clone, Debug)]
pub struct EmbeddingRequest {
    pub profile: EmbeddingProfile,
    pub task: EmbeddingTask,
    pub inputs: Vec<EmbeddingInput>,
}

#[derive(Clone, Debug)]
pub struct EmbeddingResponse {
    pub profile_digest: String,
    pub outputs: Vec<EmbeddingOutput>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EmbeddingProviderError {
    #[error("embedding provider is unavailable: {code}")]
    Unavailable { code: String },
    #[error("embedding provider returned an invalid response: {code}")]
    InvalidResponse { code: String },
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> Result<EmbeddingResponse, EmbeddingProviderError>;
}

#[derive(Clone, Debug, Default)]
pub struct UnavailableEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for UnavailableEmbeddingProvider {
    async fn embed(
        &self,
        _request: EmbeddingRequest,
    ) -> Result<EmbeddingResponse, EmbeddingProviderError> {
        Err(EmbeddingProviderError::Unavailable {
            code: "provider_not_configured".to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct IdempotencyRequest {
    pub key: String,
    pub fingerprint: String,
}

#[derive(Debug)]
pub struct AppendOutcome {
    pub episode: Episode,
    pub replayed: bool,
}

#[derive(Debug)]
pub struct FactMutationOutcome {
    pub view: FactView,
    pub replayed: bool,
}

#[derive(Debug)]
pub struct CheckpointMutationOutcome {
    pub view: CheckpointView,
    pub replayed: bool,
}

#[derive(Clone, Debug)]
pub struct RetrievalMutationOutcome {
    pub receipt: RetrievalReceipt,
    pub replayed: bool,
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("resource not found")]
    NotFound,
    #[error("request conflicts with existing data")]
    Conflict,
    #[error("idempotency key was reused")]
    IdempotencyKeyReused,
    #[error("idempotent request is in progress")]
    IdempotencyInProgress,
    #[error("fact precondition failed")]
    PreconditionFailed,
    #[error("fact supersession conflicts with the current head")]
    SupersessionConflict,
    #[error("recorded-time coordinate is in the future")]
    FutureRecordedTime,
    #[error("invalid valid-time interval: {0}")]
    InvalidValidTime(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("unprocessable request: {0}")]
    Unprocessable(String),
    #[error("resource has expired")]
    Gone,
    #[error("export has expired")]
    ExportExpired,
    #[error("checkpoint precondition failed")]
    CheckpointPreconditionFailed,
    #[error("checkpoint parent conflicts with the current head")]
    CheckpointParentConflict,
    #[error("checkpoint case conflicts with the existing lineage")]
    CheckpointCaseConflict,
    #[error("checkpoint already exists")]
    CheckpointAlreadyExists,
    #[error("checkpoint has expired")]
    CheckpointExpired,
    #[error("effect key conflicts with an existing effect")]
    EffectKeyConflict,
    #[error("effect transition is invalid")]
    InvalidEffectTransition,
    #[error("retention policy rejected the checkpoint")]
    RetentionPolicyRejected,
    #[error("fact write policy is not registered")]
    WritePolicyRejected,
    #[error("checkpoint exceeds the supported size")]
    CheckpointTooLarge,
    #[error("retrieval request exceeds the supported size")]
    RetrievalTooLarge,
    #[error("deletion worker target failure and operation lease release both failed")]
    DeletionWorkerRecoveryFailed,
    #[error("export worker recovery failed")]
    ExportWorkerRecoveryFailed,
    #[error("service unavailable")]
    Unavailable,
}

#[derive(Debug)]
pub struct ContentLeasePermit {
    lease: SubjectContentLease,
}

impl ContentLeasePermit {
    pub fn expires_at(&self) -> time::OffsetDateTime {
        self.lease.expires_at
    }

    pub fn into_release(self) -> ContentLeaseRelease {
        ContentLeaseRelease { lease: self.lease }
    }
}

#[derive(Debug)]
pub struct ContentLeaseRelease {
    lease: SubjectContentLease,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FactAsOfCoordinates {
    pub valid_at: time::OffsetDateTime,
    pub recorded_at: time::OffsetDateTime,
}

#[derive(Clone)]
pub struct MemoryService {
    lifecycle: Arc<dyn SubjectLifecycleRepository>,
    exports: Option<Arc<dyn ExportRepository>>,
    export_store: Option<Arc<dyn ExportPackageStore>>,
    export_authorizer: Option<Arc<dyn ExportWorkerAuthorizer>>,
    episodes: Arc<dyn EpisodeRepository>,
    facts: Arc<dyn FactRepository>,
    checkpoints: Arc<dyn CheckpointRepository>,
    retrievals: Arc<dyn RetrievalRepository>,
    embeddings: Arc<dyn EmbeddingProvider>,
    consolidations: Option<Arc<dyn ConsolidationRepository>>,
    consolidation_interpreters: Option<Arc<InterpreterRegistry>>,
    surfaces: Option<Arc<dyn SurfaceRepository>>,
    cache: Arc<dyn HotCache>,
}

impl MemoryService {
    pub fn new(
        lifecycle: Arc<dyn SubjectLifecycleRepository>,
        episodes: Arc<dyn EpisodeRepository>,
        facts: Arc<dyn FactRepository>,
        checkpoints: Arc<dyn CheckpointRepository>,
        retrievals: Arc<dyn RetrievalRepository>,
    ) -> Self {
        Self {
            lifecycle,
            exports: None,
            export_store: None,
            export_authorizer: None,
            episodes,
            facts,
            checkpoints,
            retrievals,
            embeddings: Arc::new(UnavailableEmbeddingProvider),
            consolidations: None,
            consolidation_interpreters: None,
            surfaces: None,
            cache: Arc::new(NoopHotCache),
        }
    }

    /// Select the hot cache implementation (spec 015 R10). The default is the
    /// no-op cache: caching is off unless the caller configures it.
    pub fn with_hot_cache(mut self, cache: Arc<dyn HotCache>) -> Self {
        self.cache = cache;
        self
    }

    pub fn hot_cache(&self) -> Arc<dyn HotCache> {
        self.cache.clone()
    }

    pub fn with_surface_components(mut self, surfaces: Arc<dyn SurfaceRepository>) -> Self {
        self.surfaces = Some(surfaces);
        self
    }

    pub fn with_consolidation_components(
        mut self,
        consolidations: Arc<dyn ConsolidationRepository>,
        interpreters: Arc<InterpreterRegistry>,
    ) -> Self {
        self.consolidations = Some(consolidations);
        self.consolidation_interpreters = Some(interpreters);
        self
    }

    /// Registers an interpreter configuration for a tenant. The provider
    /// kind must resolve in the registry; otherwise the request fails.
    pub async fn register_consolidation_interpreter_config(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        request: NewConsolidationInterpreterConfig,
    ) -> Result<ConsolidationInterpreterConfigView, ServiceError> {
        authorize_tenant(principal, tenant_id)?;
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let Some(interpreters) = self.consolidation_interpreters.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        if interpreters.resolve(&request.provider_kind).is_err() {
            return Err(ServiceError::Unprocessable(format!(
                "interpreter provider is not registered: {}",
                request.provider_kind
            )));
        }
        consolidations
            .register_interpreter_config(tenant_id, request)
            .await
            .map_err(map_repository)
    }

    /// Registers a consolidation policy for a tenant and source kind.
    pub async fn register_consolidation_policy(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        request: NewConsolidationPolicy,
    ) -> Result<ConsolidationPolicyView, ServiceError> {
        authorize_tenant(principal, tenant_id)?;
        if !(0.0..=1.0).contains(&request.confidence_auto_promote_min) {
            return Err(ServiceError::Invalid(
                "confidence_auto_promote_min must be between 0.0 and 1.0".to_owned(),
            ));
        }
        WritePolicyId::try_from(request.write_policy_id.clone())
            .map_err(|_| ServiceError::Invalid("write_policy_id is invalid".to_owned()))?;
        WritePolicyVersion::try_from(request.write_policy_version.clone())
            .map_err(|_| ServiceError::Invalid("write_policy_version is invalid".to_owned()))?;
        RetentionPolicyId::try_from(request.retention_policy_id.clone())
            .map_err(|_| ServiceError::Invalid("retention_policy_id is invalid".to_owned()))?;
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        // The interpreter config must exist before a policy can reference it.
        let _ = consolidations
            .get_interpreter_config(tenant_id, request.interpreter_config_id)
            .await
            .map_err(map_repository)?;
        consolidations
            .register_policy(tenant_id, request)
            .await
            .map_err(map_repository)
    }

    /// Reads one consolidation policy for a tenant and source kind.
    pub async fn get_consolidation_policy(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        source_kind: &str,
        policy_id: &str,
    ) -> Result<ConsolidationPolicyView, ServiceError> {
        authorize_tenant(principal, tenant_id)?;
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        consolidations
            .get_policy(tenant_id, source_kind, policy_id)
            .await
            .map_err(map_repository)
    }

    /// Registers a surface policy for a tenant, host, and principal (D2).
    /// The registry follows the consolidation policy pattern: create or
    /// keep; the view always reflects the stored row.
    pub async fn register_surface_policy(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        request: NewSurfacePolicy,
    ) -> Result<SurfacePolicyView, ServiceError> {
        authorize_tenant(principal, tenant_id)?;
        validate_surface_identifier("host_id", &request.host_id)?;
        validate_surface_identifier("principal_id", &request.principal_id)?;
        if !(1..=SURFACE_MAX_ITEMS).contains(&request.max_items) {
            return Err(ServiceError::Invalid(format!(
                "max_items must be between 1 and {SURFACE_MAX_ITEMS}"
            )));
        }
        if request.max_context_tokens <= 0 {
            return Err(ServiceError::Invalid(
                "max_context_tokens must be positive".to_owned(),
            ));
        }
        if request.max_result_tokens <= 0 {
            return Err(ServiceError::Invalid(
                "max_result_tokens must be positive".to_owned(),
            ));
        }
        if let Some(ceiling) = &request.sensitivity_ceiling {
            Sensitivity::try_from(ceiling.clone())
                .map_err(|_| ServiceError::Invalid("sensitivity_ceiling is invalid".to_owned()))?;
        }
        if let (Some(from), Some(until)) = (request.window_from, request.window_until)
            && from >= until
        {
            return Err(ServiceError::Invalid(
                "window_from must precede window_until".to_owned(),
            ));
        }
        let Some(surfaces) = self.surfaces.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        surfaces
            .register_policy(tenant_id, request)
            .await
            .map_err(map_repository)
    }

    /// Reads one surface policy for a tenant, host, and principal.
    pub async fn get_surface_policy(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        host_id: &str,
        principal_id: &str,
    ) -> Result<SurfacePolicyView, ServiceError> {
        authorize_tenant(principal, tenant_id)?;
        let Some(surfaces) = self.surfaces.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        surfaces
            .get_policy(tenant_id, host_id, principal_id)
            .await
            .map_err(map_repository)
    }

    /// Evaluates a surface bundle (D1, D3). The request carries a bounded
    /// context digest. A missing or disabled policy yields an empty bundle
    /// (fail closed, R3). The response is stored for idempotent replay;
    /// a reused key with a different body fails with
    /// IdempotencyKeyReused (A8).
    pub async fn create_surface(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewSurfaceRequest,
        idempotency_key: String,
    ) -> Result<CreateSurfaceOutcome, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        validate_idempotency_key(&idempotency_key)?;
        validate_surface_identifier("host_id", &request.host_id)?;
        validate_surface_identifier("principal_id", &request.principal_id)?;
        if request.context_terms.len() > SURFACE_MAX_CONTEXT_TERMS {
            return Err(ServiceError::Invalid(format!(
                "context_terms must contain at most {SURFACE_MAX_CONTEXT_TERMS} terms"
            )));
        }
        for term in &request.context_terms {
            validate_surface_identifier("context term", term)?;
        }
        let Some(surfaces) = self.surfaces.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let fingerprint = surface_request_fingerprint(&request);
        let outcome = surfaces
            .create_surface(
                tenant_id,
                subject_id,
                &request,
                &principal.allowed_sensitivities,
                IdempotencyRequest {
                    key: idempotency_key,
                    fingerprint,
                },
            )
            .await
            .map_err(map_repository)?;
        Ok(outcome)
    }

    /// Reads one stored surface bundle by id (recall-receipt style).
    pub async fn get_surface(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        surface_id: Uuid,
    ) -> Result<SurfaceBundle, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        let Some(surfaces) = self.surfaces.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        surfaces
            .get_surface(tenant_id, subject_id, surface_id)
            .await
            .map_err(map_repository)
    }

    /// Creates a durable consolidation job for a subject. The request fails
    /// closed (R4) when the policy is missing, disabled, or unresolved.
    pub async fn create_consolidation_job(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: NewConsolidationJob,
        idempotency_key: String,
    ) -> Result<CreateConsolidationJobOutcome, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        validate_idempotency_key(&idempotency_key)?;
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let policy = consolidations
            .get_policy(tenant_id, &request.source_kind, &request.policy_id)
            .await
            .map_err(map_repository)?;
        if !policy.enabled {
            return Err(ServiceError::Unprocessable(format!(
                "consolidation policy is disabled: {}",
                request.policy_id
            )));
        }
        let Some(interpreters) = self.consolidation_interpreters.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let config = consolidations
            .get_interpreter_config(tenant_id, policy.interpreter_config_id)
            .await
            .map_err(map_repository)?;
        if interpreters.resolve(&config.provider_kind).is_err() {
            return Err(ServiceError::Unprocessable(format!(
                "interpreter provider is not registered: {}",
                config.provider_kind
            )));
        }
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&serde_json::json!({
                "source_kind": request.source_kind,
                "policy_id": request.policy_id,
                "window_from": request.window_from.to_string(),
                "window_until": request.window_until.to_string(),
            }))
            .expect("job fingerprint is serializable"),
        ));
        consolidations
            .create_job(
                tenant_id,
                subject_id,
                request,
                IdempotencyRequest {
                    key: idempotency_key,
                    fingerprint,
                },
            )
            .await
            .map_err(map_repository)
    }

    /// Reads one consolidation job for a subject.
    pub async fn poll_consolidation_job(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        job_id: Uuid,
    ) -> Result<ConsolidationJobView, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        consolidations
            .poll_job(tenant_id, subject_id, job_id)
            .await
            .map_err(map_repository)
    }

    /// One worker pass: claim at most one batch of jobs, interpret each
    /// window, materialize the claims through the governed fact path, and
    /// complete or fail the job. Crash-resumable: every claim has a lease,
    /// and the materialization is idempotent per claim (R3).
    pub async fn run_consolidation_worker_once(
        &self,
    ) -> Result<ConsolidationWorkerRunSummary, ServiceError> {
        let Some(consolidations) = self.consolidations.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let Some(interpreters) = self.consolidation_interpreters.as_ref() else {
            return Err(ServiceError::Unavailable);
        };
        let worker_id = Uuid::now_v7();
        let mut summary = ConsolidationWorkerRunSummary::default();
        while summary.jobs_processed < CONSOLIDATION_MAX_CLAIMS_PER_RUN {
            let Some(job) = consolidations
                .claim_next_job(worker_id, CONSOLIDATION_WORKER_LEASE_SECONDS)
                .await
                .map_err(map_repository)?
            else {
                break;
            };
            summary.jobs_processed += 1;
            let snapshot = match consolidations.worker_policy_snapshot(&job).await {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    let _ = consolidations
                        .fail_job(&job, "policy or interpreter config missing")
                        .await;
                    summary.failed_jobs += 1;
                    continue;
                }
            };
            let interpreter = match interpreters.resolve(&snapshot.provider_kind) {
                Ok(interpreter) => interpreter,
                Err(_) => {
                    let _ = consolidations
                        .fail_job(&job, "interpreter provider not registered")
                        .await;
                    summary.failed_jobs += 1;
                    continue;
                }
            };
            let episodes = consolidations
                .select_window_episodes(&job)
                .await
                .map_err(map_repository)?;
            if episodes.is_empty() {
                let _ = consolidations.complete_job(&job).await;
                continue;
            }
            let model_identity = format!("{}:{}", snapshot.provider_kind, snapshot.config_digest);
            let context = InterpreterContext {
                tenant_id: job.tenant_id,
                subject_id: job.subject_id,
                source_kind: &job.source_kind,
                policy_id: &job.policy_id,
                policy_version: &job.policy_version,
                model_identity: &model_identity,
                prompt_policy_version: &snapshot.prompt_policy_version,
            };
            let claims = match interpreter.interpret(&context, &episodes).await {
                Ok(claims) => claims,
                Err(error) => {
                    let _ = consolidations
                        .fail_job(&job, &sanitize_failure_reason(&error.to_string()))
                        .await;
                    summary.failed_jobs += 1;
                    continue;
                }
            };
            let pending: Vec<PendingConsolidationClaim> = claims
                .into_iter()
                .take(job.claim_cap as usize)
                .map(|claim| {
                    let claim_id = consolidation_claim_id(
                        job.tenant_id,
                        job.subject_id,
                        &job.source_kind,
                        &job.policy_id,
                        claim.episode_ids[0],
                    );
                    PendingConsolidationClaim {
                        claim_id,
                        case_id: claim.case_id,
                        episode_ids: claim.episode_ids,
                        content_hash: consolidation_content_hash(&claim.value),
                        confidence: claim.confidence,
                        sensitivity: claim.sensitivity,
                        observed_at: claim.observed_at,
                        valid_from: claim.valid_from,
                        valid_until: claim.valid_until,
                        model_identity: model_identity.clone(),
                        prompt_policy_version: snapshot.prompt_policy_version.clone(),
                        value: claim.value,
                    }
                })
                .collect();
            summary.claims_written += pending.len() as u32;
            consolidations
                .insert_claims(&job, &pending)
                .await
                .map_err(map_repository)?;
            while let Some(claim) = consolidations
                .claim_next_claim(&job, worker_id, CONSOLIDATION_WORKER_LEASE_SECONDS)
                .await
                .map_err(map_repository)?
            {
                if claim.confidence < snapshot.confidence_auto_promote_min {
                    if consolidations
                        .skip_claim(&claim, CONSOLIDATION_SKIP_REASON_LOW_CONFIDENCE)
                        .await
                        .map_err(map_repository)?
                    {
                        summary.claims_skipped += 1;
                    }
                    continue;
                }
                match self
                    .materialize_consolidation_claim(&job, &claim, &snapshot)
                    .await
                {
                    Ok((fact_id, revision_id)) => {
                        if consolidations
                            .complete_claim(&claim, fact_id, revision_id)
                            .await
                            .map_err(map_repository)?
                        {
                            summary.claims_done += 1;
                        }
                    }
                    Err(_error @ (ServiceError::Invalid(_) | ServiceError::Unprocessable(_))) => {
                        let _ = consolidations
                            .skip_claim(&claim, "materialization_failed")
                            .await;
                    }
                    Err(_error) => {
                        let _ = consolidations.release_claim(&claim).await;
                        break;
                    }
                }
            }
            if !consolidations
                .complete_job(&job)
                .await
                .map_err(map_repository)?
            {
                // Another pass may still hold leased claims whose leases
                // have not expired (the job lease expired mid-run and a
                // fresh pass claimed the job). That pass will complete the
                // claims and finish the job; failing it now would race the
                // pass. Only fail when no claim can still make progress.
                if consolidations
                    .has_in_flight_claims(&job)
                    .await
                    .map_err(map_repository)?
                {
                    continue;
                }
                let _ = consolidations.fail_job(&job, "claims incomplete").await;
                summary.failed_jobs += 1;
            }
        }
        Ok(summary)
    }

    async fn materialize_consolidation_claim(
        &self,
        job: &ClaimedConsolidationJob,
        claim: &ClaimedConsolidationClaim,
        snapshot: &WorkerPolicySnapshot,
    ) -> Result<(FactId, RevisionId), ServiceError> {
        let fact_id = FactId(Uuid::now_v7());
        let revision_id = RevisionId(Uuid::now_v7());
        let value_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&claim.value).expect("claim value is serializable"),
        ));
        let new_fact = NewFact {
            tenant_id: job.tenant_id,
            subject_id: job.subject_id,
            case_id: claim.case_id,
            fact_id,
            revision_id,
            namespace: FactNamespace::try_from(CONSOLIDATION_DERIVED_FACT_NAMESPACE.to_owned())
                .expect("derived namespace is valid"),
            key: FactKey::try_from(consolidation_fact_key(&job.source_kind, claim.claim_id))
                .expect("derived fact key is valid"),
            value: claim.value.clone(),
            observed_at: claim.observed_at,
            valid_time: ValidTime {
                from: claim.valid_from,
                until: claim.valid_until,
            },
            evidence_episode_ids: claim.episode_ids.clone(),
            write_policy: WritePolicy {
                id: WritePolicyId::try_from(snapshot.write_policy_id.clone())
                    .expect("registered policy id is valid"),
                version: WritePolicyVersion::try_from(snapshot.write_policy_version.clone())
                    .expect("registered policy version is valid"),
            },
            confidence: claim.confidence,
            sensitivity: Sensitivity::try_from(claim.sensitivity.clone())
                .expect("claim sensitivity is valid"),
            retention_policy_id: RetentionPolicyId::try_from(snapshot.retention_policy_id.clone())
                .expect("registered retention policy is valid"),
            writer_principal_id: PrincipalId(CONSOLIDATION_WRITER_PRINCIPAL_ID.to_owned()),
            schema_version: 1,
            value_sha256: value_sha256.clone(),
        };
        let idempotency = IdempotencyRequest {
            key: consolidation_claim_idempotency_key(
                job.tenant_id,
                job.subject_id,
                &job.source_kind,
                &job.policy_id,
                claim.claim_id,
            ),
            fingerprint: value_sha256,
        };
        let outcome = self
            .facts
            .create(new_fact, idempotency)
            .await
            .map_err(map_repository)?;
        Ok((outcome.view.fact_id, outcome.view.head_revision_id))
    }

    pub fn with_embedding_provider(mut self, embeddings: Arc<dyn EmbeddingProvider>) -> Self {
        self.embeddings = embeddings;
        self
    }

    pub fn with_export_components(
        mut self,
        exports: Arc<dyn ExportRepository>,
        export_store: Arc<dyn ExportPackageStore>,
    ) -> Self {
        self.exports = Some(exports);
        self.export_store = Some(export_store);
        self
    }

    pub fn with_export_worker_authorizer(
        mut self,
        authorizer: Arc<dyn ExportWorkerAuthorizer>,
    ) -> Self {
        self.export_authorizer = Some(authorizer);
        self
    }

    pub async fn acquire_subject_content_lease(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<ContentLeasePermit, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        let lease = self
            .lifecycle
            .acquire_content_lease(principal, tenant_id, subject_id)
            .await
            .map_err(map_repository)?;
        Ok(ContentLeasePermit { lease })
    }

    pub async fn release_subject_content_lease(
        &self,
        release: &ContentLeaseRelease,
    ) -> Result<(), ServiceError> {
        self.lifecycle
            .release_content_lease(&release.lease)
            .await
            .map_err(map_repository)
    }

    pub async fn fence_subject_for_deletion(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::SubjectDelete,
        )?;
        self.lifecycle
            .transition_to_deletion_pending(tenant_id, subject_id)
            .await
            .map_err(map_repository)
    }

    pub async fn mark_subject_deleted(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<SubjectLifecycle, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::SubjectDelete,
        )?;
        self.lifecycle
            .transition_to_deleted(tenant_id, subject_id)
            .await
            .map_err(map_repository)
    }

    pub async fn create_subject_deletion(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        idempotency_key: String,
    ) -> Result<CreateDeletionOutcome, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::SubjectDelete,
        )?;
        validate_idempotency_key(&idempotency_key)?;
        // The deletion policy is server-owned.  A caller cannot select an
        // unimplemented provider and make the operation claim that it was
        // purged.  Optional providers remain explicit `not_configured`
        // ledger rows; the PostgreSQL-backed canonical, projection, and
        // configured export targets are the only live capabilities enabled by
        // this service instance.
        let configured_targets = configured_deletion_targets(self.exports.is_some());
        let target_names = configured_targets
            .iter()
            .map(|target| target.as_str())
            .collect::<Vec<_>>();
        let fingerprint_input = json!({
            "operation_id": "createSubjectDeletion",
            "content_type": "application/json",
            "principal_id": principal.principal_id.0,
            "tenant_id": tenant_id.0,
            "subject_id": subject_id.0,
            "idempotency_key": idempotency_key,
            "targets": target_names,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        self.lifecycle
            .create_deletion_operation(CreateDeletionRequest {
                tenant_id,
                subject_id,
                principal_id: principal.principal_id.clone(),
                idempotency_key,
                request_fingerprint_sha256: fingerprint,
                configured_targets,
                retention_hours: DELETION_RETENTION_HOURS,
            })
            .await
            .map_err(map_repository)
    }

    pub async fn poll_subject_deletion(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
    ) -> Result<DeletionOperationView, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::SubjectDelete,
        )?;
        self.lifecycle
            .poll_deletion_operation(tenant_id, subject_id, operation_id)
            .await
            .map_err(map_repository)
    }

    pub async fn repair_subject_deletion(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        operation_id: DeletionOperationId,
        reason_code: String,
    ) -> Result<DeletionOperationView, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::SubjectDelete,
        )?;
        validate_deletion_repair_reason(&reason_code)?;
        self.lifecycle
            .repair_deletion_operation(tenant_id, subject_id, operation_id, &reason_code)
            .await
            .map_err(map_repository)?;
        self.poll_subject_deletion(principal, tenant_id, subject_id, operation_id)
            .await
    }

    /// Computes the effect receipt digest for a completed deletion target.
    /// The digest is part of the deletion evidence contract; the conformance
    /// suite uses this same function so production drift cannot hide.
    pub fn deletion_target_effect_receipt_sha256(
        operation_id: DeletionOperationId,
        target_key_digest: &str,
        attempts: u32,
    ) -> String {
        hex::encode(Sha256::digest(format!(
            "palimpsest.deletion-target/v1:{}:{}:{}",
            operation_id.0, target_key_digest, attempts
        )))
    }

    pub async fn run_deletion_worker_once(&self) -> Result<DeletionWorkerRunSummary, ServiceError> {
        let worker_id = Uuid::now_v7();
        let mut processed = 0u32;
        while processed < DELETION_WORKER_MAX_CLAIMS_PER_RUN {
            let Some(claimed) = self
                .lifecycle
                .claim_next_deletion_operation(worker_id, DELETION_WORKER_LEASE_SECONDS)
                .await
                .map_err(map_repository)?
            else {
                break;
            };
            processed += 1;

            // Operation state transitions and target effects are deliberately
            // separate. A target lease is committed before its effect runs,
            // so a crashed worker leaves a durable, reclaimable target row.
            for _ in 0..16 {
                self.lifecycle
                    .renew_deletion_operation_lease(
                        &claimed,
                        worker_id,
                        DELETION_WORKER_LEASE_SECONDS,
                    )
                    .await
                    .map_err(map_repository)?;
                let outcome = self
                    .lifecycle
                    .advance_deletion_operation(&claimed, worker_id, DELETION_MAX_ATTEMPTS)
                    .await
                    .map_err(map_repository)?;
                if matches!(
                    outcome.lifecycle_state,
                    DeletionOperationState::Completed
                        | DeletionOperationState::Failed
                        | DeletionOperationState::Expired
                ) {
                    break;
                }
                if outcome.lifecycle_state != DeletionOperationState::Purging {
                    if outcome.next_poll_seconds > 0 {
                        break;
                    }
                    continue;
                }

                let Some(target) = self
                    .lifecycle
                    .claim_next_deletion_target(&claimed, worker_id, DELETION_WORKER_LEASE_SECONDS)
                    .await
                    .map_err(map_repository)?
                else {
                    if outcome.next_poll_seconds > 0 {
                        break;
                    }
                    continue;
                };

                self.lifecycle
                    .renew_deletion_operation_lease(
                        &claimed,
                        worker_id,
                        DELETION_WORKER_LEASE_SECONDS,
                    )
                    .await
                    .map_err(map_repository)?;
                self.lifecycle
                    .renew_deletion_target_lease(&target, DELETION_WORKER_LEASE_SECONDS)
                    .await
                    .map_err(map_repository)?;

                let effect_result = async {
                    if target.target_name == DeletionTargetName::Exports {
                        let (Some(exports), Some(store)) =
                            (self.exports.as_ref(), self.export_store.as_ref())
                        else {
                            return Err(ServiceError::Unavailable);
                        };
                        let export_ids = exports
                            .list_export_ids_for_subject(claimed.tenant_id, claimed.subject_id)
                            .await
                            .map_err(map_repository)?;
                        for export_id in export_ids {
                            store
                                .discard_staging(export_id)
                                .await
                                .map_err(map_export_store_error)?;
                            store
                                .discard_published(export_id)
                                .await
                                .map_err(map_export_store_error)?;
                            if !store
                                .probe_absent(export_id)
                                .await
                                .map_err(map_export_store_error)?
                            {
                                return Err(ServiceError::Unavailable);
                            }
                        }
                    }

                    self.lifecycle
                        .apply_deletion_target(&target)
                        .await
                        .map_err(map_repository)
                }
                .await;
                if let Err(error) = effect_result {
                    self.lifecycle
                        .fail_deletion_target(
                            &target,
                            "target_effect_failed",
                            DELETION_MAX_ATTEMPTS,
                        )
                        .await
                        .map_err(map_repository)?;
                    let release_result = self
                        .lifecycle
                        .release_deletion_operation_lease(&claimed, worker_id)
                        .await;
                    return match release_result {
                        Ok(()) => Err(error),
                        Err(_release_error) => Err(ServiceError::DeletionWorkerRecoveryFailed),
                    };
                }
                let effect_receipt_sha256 = Self::deletion_target_effect_receipt_sha256(
                    target.operation_id,
                    &target.target_key_digest,
                    target.attempts,
                );
                self.lifecycle
                    .complete_deletion_target(&target, &effect_receipt_sha256)
                    .await
                    .map_err(map_repository)?;
            }
        }
        Ok(DeletionWorkerRunSummary { processed })
    }

    pub async fn create_export(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        idempotency_key: String,
        profile: &str,
    ) -> Result<ExportCreateOutcome, ServiceError> {
        if !is_supported_export_profile(profile) {
            return Err(ServiceError::Invalid(format!(
                "unsupported export profile: {profile}"
            )));
        }
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::CanonicalHistoryExport,
        )?;
        validate_idempotency_key(&idempotency_key)?;
        let exports = self.exports.as_ref().ok_or(ServiceError::Unavailable)?;
        let authorization_scope_sha256 =
            export_authorization_scope_sha256(principal, tenant_id, subject_id)?;
        let fingerprint_input = json!({
            "operation_id": "createExport",
            "content_type": "application/json",
            "principal_id": principal.principal_id.0,
            "tenant_id": tenant_id.0,
            "subject_id": subject_id.0,
            "profile": profile,
            "schema_version": 1,
            "authorization_scope_sha256": authorization_scope_sha256,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let mut allowed_sensitivities = principal
            .allowed_sensitivities
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>();
        allowed_sensitivities.sort_unstable();
        allowed_sensitivities.dedup();
        let operation = exports
            .create_export(NewExport {
                tenant_id,
                subject_id,
                export_id: ExportId(Uuid::now_v7()),
                principal_id: principal.principal_id.clone(),
                profile: profile.to_owned(),
                idempotency: IdempotencyRequest {
                    key: idempotency_key,
                    fingerprint,
                },
                authorization_scope_sha256,
                allowed_sensitivities,
                expires_at: time::OffsetDateTime::now_utc()
                    + time::Duration::hours(EXPORT_RETENTION_HOURS),
            })
            .await
            .map_err(map_repository)?;
        Ok(operation)
    }

    pub async fn get_export(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<ExportOperationView, ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::CanonicalHistoryExport,
        )?;
        let exports = self.exports.as_ref().ok_or(ServiceError::Unavailable)?;
        let operation = exports
            .get_export(tenant_id, subject_id, export_id)
            .await
            .map_err(map_repository)?;
        if operation.authorization_scope_sha256
            != export_authorization_scope_sha256(principal, tenant_id, subject_id)?
        {
            return Err(ServiceError::NotFound);
        }
        if operation.state == ExportOperationState::Expired
            && let Some(store) = self.export_store.as_ref()
        {
            let _ = store.discard_staging(export_id).await;
            let _ = store.discard_published(export_id).await;
        }
        Ok(operation)
    }

    pub fn spawn_export_materialization(
        &self,
        principal: PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            let _ = service
                .materialize_export(&principal, tenant_id, subject_id, export_id)
                .await;
        });
    }

    pub async fn run_export_worker_once(&self) -> Result<bool, ServiceError> {
        let exports = self.exports.as_ref().ok_or(ServiceError::Unavailable)?;
        let store = self
            .export_store
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let authorizer = self
            .export_authorizer
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let worker_lease_id = Uuid::now_v7();
        let Some(materialization) = exports
            .claim_next_export_for_materialization(worker_lease_id, 30)
            .await
            .map_err(map_repository)?
        else {
            let cleanup_lease_id = Uuid::now_v7();
            let Some(expired) = exports
                .claim_next_expired_export_for_cleanup(cleanup_lease_id, 30)
                .await
                .map_err(map_repository)?
            else {
                return Ok(false);
            };
            let staging_result = store.discard_staging(expired.export_id).await;
            let published_result = store.discard_published(expired.export_id).await;
            if let Err(error) = staging_result {
                let _ = published_result;
                return Err(map_export_store_error(error));
            }
            if let Err(error) = published_result {
                return Err(map_export_store_error(error));
            }
            exports
                .mark_export_cleanup_complete(
                    expired.tenant_id,
                    expired.subject_id,
                    expired.export_id,
                    cleanup_lease_id,
                )
                .await
                .map_err(map_repository)?;
            return Ok(true);
        };
        let tenant_id = materialization.operation.tenant_id;
        let subject_id = materialization.operation.subject_id;
        let principal = match authorizer.authorize_export(
            &materialization.operation.principal_id,
            tenant_id,
            subject_id,
            &materialization.operation.authorization_scope_sha256,
        ) {
            Ok(principal) => principal,
            Err(ServiceError::NotFound) => {
                exports
                    .mark_export_failed(
                        tenant_id,
                        subject_id,
                        materialization.operation.export_id,
                        worker_lease_id,
                        "authorization_revoked",
                    )
                    .await
                    .map_err(map_repository)?;
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = authorize_operation(
            &principal,
            tenant_id,
            subject_id,
            OperationGrant::CanonicalHistoryExport,
        ) {
            if !matches!(error, ServiceError::NotFound) {
                return Err(error);
            }
            exports
                .mark_export_failed(
                    tenant_id,
                    subject_id,
                    materialization.operation.export_id,
                    worker_lease_id,
                    "authorization_revoked",
                )
                .await
                .map_err(map_repository)?;
            return Ok(true);
        }
        if export_authorization_scope_sha256(&principal, tenant_id, subject_id)?
            != materialization.operation.authorization_scope_sha256
        {
            exports
                .mark_export_failed(
                    tenant_id,
                    subject_id,
                    materialization.operation.export_id,
                    worker_lease_id,
                    "authorization_revoked",
                )
                .await
                .map_err(map_repository)?;
            return Ok(true);
        }
        let permit = match self
            .acquire_subject_content_lease(&principal, tenant_id, subject_id)
            .await
        {
            Ok(permit) => permit,
            Err(ServiceError::NotFound) => {
                exports
                    .mark_export_failed(
                        tenant_id,
                        subject_id,
                        materialization.operation.export_id,
                        worker_lease_id,
                        "lifecycle_revoked",
                    )
                    .await
                    .map_err(map_repository)?;
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        let result = self
            .materialize_claimed_export_with_lease(
                &permit,
                &principal,
                exports.as_ref(),
                store.as_ref(),
                materialization,
            )
            .await;
        let release = permit.into_release();
        let release_result = self.release_subject_content_lease(&release).await;
        match (result, release_result) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(_), Err(_)) => Err(ServiceError::ExportWorkerRecoveryFailed),
        }
    }

    pub async fn materialize_export(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<(), ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::CanonicalHistoryExport,
        )?;
        let exports = self.exports.as_ref().ok_or(ServiceError::Unavailable)?;
        let store = self
            .export_store
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let principal = if let Some(authorizer) = self.export_authorizer.as_ref() {
            let operation = exports
                .get_export(tenant_id, subject_id, export_id)
                .await
                .map_err(map_repository)?;
            authorizer.authorize_export(
                &operation.principal_id,
                tenant_id,
                subject_id,
                &operation.authorization_scope_sha256,
            )?
        } else {
            principal.clone()
        };
        let permit = self
            .acquire_subject_content_lease(&principal, tenant_id, subject_id)
            .await?;
        let result = self
            .materialize_export_with_lease(
                &permit,
                &principal,
                exports.as_ref(),
                store.as_ref(),
                export_id,
            )
            .await;
        let release = permit.into_release();
        let release_result = self.release_subject_content_lease(&release).await;
        match (result, release_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(_), Err(_)) => Err(ServiceError::ExportWorkerRecoveryFailed),
        }
    }

    async fn materialize_export_with_lease(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        exports: &dyn ExportRepository,
        store: &dyn ExportPackageStore,
        export_id: ExportId,
    ) -> Result<(), ServiceError> {
        let tenant_id = permit.lease.tenant_id;
        let subject_id = permit.lease.subject_id;
        let materialization = await_content_operation(permit, async {
            exports
                .claim_export_for_materialization(tenant_id, subject_id, export_id)
                .await
                .map_err(map_repository)
        })
        .await?;
        let Some(materialization) = materialization else {
            return Ok(());
        };
        self.materialize_claimed_export_with_lease(
            permit,
            principal,
            exports,
            store,
            materialization,
        )
        .await
    }

    async fn materialize_claimed_export_with_lease(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        exports: &dyn ExportRepository,
        store: &dyn ExportPackageStore,
        materialization: ExportMaterialization,
    ) -> Result<(), ServiceError> {
        let tenant_id = materialization.operation.tenant_id;
        let subject_id = materialization.operation.subject_id;
        let export_id = materialization.operation.export_id;
        let worker_lease_id = materialization
            .operation
            .worker_lease_id
            .ok_or(ServiceError::Unavailable)?;
        let context = ExportProcessingContext {
            tenant_id,
            subject_id,
            export_id,
            snapshot_id: materialization.operation.export_id.0.to_string(),
            authorization_scope_sha256: materialization
                .operation
                .authorization_scope_sha256
                .clone(),
            generated_at: materialization.operation.created_at,
        };
        let profile = materialization.operation.profile.clone();
        let build_result: Result<Box<dyn ExportPackage>, &str> = match profile.as_str() {
            CANONICAL_HISTORY_EXPORT_PROFILE => {
                match CanonicalHistoryPackage::build(materialization.records, context) {
                    Ok(package) => Ok(Box::new(package) as Box<dyn ExportPackage>),
                    Err(_) => Err("package_build_failed"),
                }
            }
            WIKI_VAULT_EXPORT_PROFILE => {
                match WikiVaultPackage::build(materialization.records, context) {
                    Ok(package) => Ok(Box::new(package) as Box<dyn ExportPackage>),
                    Err(_) => Err("package_build_failed"),
                }
            }
            _ => Err("unsupported_export_profile"),
        };
        let package = match build_result {
            Ok(package) => package,
            Err(reason) => {
                let _ = exports
                    .mark_export_failed(tenant_id, subject_id, export_id, worker_lease_id, reason)
                    .await;
                return Err(ServiceError::Unavailable);
            }
        };
        let metadata = match await_content_operation(permit, async {
            store
                .stage(export_id, package)
                .await
                .map_err(map_export_store_error)
        })
        .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = store.discard_staging(export_id).await;
                let _ = exports
                    .mark_export_failed(
                        tenant_id,
                        subject_id,
                        export_id,
                        worker_lease_id,
                        "package_store_failed",
                    )
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = await_content_operation(permit, async {
            if let Some(authorizer) = self.export_authorizer.as_ref() {
                let current = authorizer.authorize_export(
                    &materialization.operation.principal_id,
                    tenant_id,
                    subject_id,
                    &materialization.operation.authorization_scope_sha256,
                )?;
                authorize_operation(
                    &current,
                    tenant_id,
                    subject_id,
                    OperationGrant::CanonicalHistoryExport,
                )?;
                if export_authorization_scope_sha256(&current, tenant_id, subject_id)?
                    != materialization.operation.authorization_scope_sha256
                {
                    return Err(ServiceError::NotFound);
                }
            } else {
                authorize_operation(
                    principal,
                    tenant_id,
                    subject_id,
                    OperationGrant::CanonicalHistoryExport,
                )?;
            }
            store
                .publish(export_id)
                .await
                .map_err(map_export_store_error)
        })
        .await
        {
            let _ = store.discard_staging(export_id).await;
            let _ = store.discard_published(export_id).await;
            let _ = exports
                .mark_export_failed(
                    tenant_id,
                    subject_id,
                    export_id,
                    worker_lease_id,
                    "package_publish_failed",
                )
                .await;
            return Err(error);
        }
        if let Err(error) = await_content_operation(permit, async {
            exports
                .mark_export_ready(tenant_id, subject_id, export_id, worker_lease_id, metadata)
                .await
                .map_err(map_repository)
        })
        .await
        {
            let _ = store.discard_published(export_id).await;
            let _ = exports
                .mark_export_failed(
                    tenant_id,
                    subject_id,
                    export_id,
                    worker_lease_id,
                    "publication_authorization_failed",
                )
                .await;
            return Err(error);
        }
        Ok(())
    }

    pub async fn get_export_content(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<(ExportOperationView, Vec<u8>), ServiceError> {
        authorize_operation(
            principal,
            tenant_id,
            subject_id,
            OperationGrant::CanonicalHistoryExport,
        )?;
        authorize_content(permit, principal, tenant_id, subject_id)?;
        let exports = self.exports.as_ref().ok_or(ServiceError::Unavailable)?;
        let store = self
            .export_store
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let operation = await_content_operation(permit, async {
            exports
                .get_export(tenant_id, subject_id, export_id)
                .await
                .map_err(map_repository)
        })
        .await?;
        if operation.authorization_scope_sha256
            != export_authorization_scope_sha256(principal, tenant_id, subject_id)?
        {
            return Err(ServiceError::NotFound);
        }
        if operation.state == ExportOperationState::Expired
            || operation.expires_at <= time::OffsetDateTime::now_utc()
        {
            let _ = store.discard_staging(export_id).await;
            let _ = store.discard_published(export_id).await;
            return Err(ServiceError::ExportExpired);
        }
        if operation.state != ExportOperationState::Ready {
            return Err(ServiceError::NotFound);
        }
        let bytes = await_content_operation(permit, async {
            store.read(export_id).await.map_err(map_export_store_error)
        })
        .await?;
        let expected = operation
            .content_sha256
            .as_deref()
            .ok_or(ServiceError::Unavailable)?;
        if hex::encode(Sha256::digest(&bytes)) != expected {
            return Err(ServiceError::Unavailable);
        }
        Ok((operation, bytes))
    }

    pub async fn save_checkpoint(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        idempotency_key: String,
        precondition: CheckpointPrecondition,
        command: SaveCheckpoint,
    ) -> Result<CheckpointMutationOutcome, ServiceError> {
        authorize_content(permit, principal, command.tenant_id, command.subject_id)?;
        validate_idempotency_key(&idempotency_key)?;
        validate_checkpoint(&command, precondition)?;

        let (precondition_kind, expected_head_revision_id) = match precondition {
            CheckpointPrecondition::Create => ("create", None),
            CheckpointPrecondition::Match(revision_id) => ("match", Some(revision_id.0)),
        };
        let fingerprint_input = json!({
            "operation_id": "saveCheckpoint",
            "content_type": "application/json",
            "principal_id": principal.principal_id.0,
            "tenant_id": command.tenant_id.0,
            "subject_id": command.subject_id.0,
            "agent_id": command.agent_id.0,
            "thread_id": command.thread_id.0,
            "case_id": command.case_id.0,
            "precondition": precondition_kind,
            "expected_head_revision_id": expected_head_revision_id,
            "parent_revision_id": command.parent_revision_id.map(|revision_id| revision_id.0),
            "state": command.state,
            "state_schema_version": command.state_schema_version,
            "effect_transitions": command.effect_transitions,
            "provenance": command.provenance,
            "sensitivity": command.sensitivity,
            "retention_policy_id": command.retention_policy_id,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let state_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&command.state)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let effect_transitions = command
            .effect_transitions
            .into_iter()
            .map(|transition| match transition {
                EffectTransition::Prepare(transition) => {
                    NewEffectTransition::Prepare(NewPreparedEffect {
                        effect_id: EffectId(Uuid::now_v7()),
                        effect_key: transition.effect_key,
                        kind: transition.kind,
                        recovery_mode: transition.recovery_mode,
                    })
                }
                EffectTransition::Complete(transition) => NewEffectTransition::Complete(transition),
            })
            .collect();

        await_content_operation(permit, async {
            self.checkpoints
                .save(
                    NewCheckpointRevision {
                        tenant_id: command.tenant_id,
                        subject_id: command.subject_id,
                        agent_id: command.agent_id,
                        thread_id: command.thread_id,
                        case_id: command.case_id,
                        checkpoint_id: palimpsest_domain::CheckpointId(Uuid::now_v7()),
                        revision_id: CheckpointRevisionId(Uuid::now_v7()),
                        parent_revision_id: command.parent_revision_id,
                        precondition,
                        state: command.state,
                        state_schema_version: command.state_schema_version,
                        effect_transitions,
                        provenance: command.provenance,
                        sensitivity: command.sensitivity,
                        retention_policy_id: command.retention_policy_id,
                        writer_principal_id: principal.principal_id.clone(),
                        schema_version: 1,
                        state_sha256,
                    },
                    IdempotencyRequest {
                        key: idempotency_key,
                        fingerprint,
                    },
                )
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn get_checkpoint(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        agent_id: AgentId,
        thread_id: ThreadId,
    ) -> Result<CheckpointView, ServiceError> {
        authorize_content(permit, principal, tenant_id, subject_id)?;
        await_content_operation(permit, async {
            self.checkpoints
                .get_current(tenant_id, subject_id, agent_id, thread_id)
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn append_episode(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: AppendEpisode,
    ) -> Result<AppendOutcome, ServiceError> {
        authorize_content(permit, principal, command.tenant_id, command.subject_id)?;
        validate_append(&command)?;

        let fingerprint_input = json!({
            "operation_id": "appendEpisode",
            "content_type": "application/json",
            "tenant_id": command.tenant_id.0,
            "subject_id": command.subject_id.0,
            "case_id": command.case_id.0,
            "kind": command.kind,
            "observed_at_unix_nanos": command.observed_at.unix_timestamp_nanos().to_string(),
            "provenance": command.provenance,
            "sensitivity": command.sensitivity,
            "retention_policy_id": command.retention_policy_id,
            "payload": command.payload,
        });
        let fingerprint_bytes = serde_json::to_vec(&fingerprint_input)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        let fingerprint = hex::encode(Sha256::digest(fingerprint_bytes));
        let payload_bytes = serde_json::to_vec(&command.payload)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        let payload_sha256 = hex::encode(Sha256::digest(payload_bytes));
        let episode = NewEpisode {
            tenant_id: command.tenant_id,
            subject_id: command.subject_id,
            case_id: command.case_id,
            episode_id: EpisodeId(Uuid::now_v7()),
            kind: command.kind,
            observed_at: command.observed_at,
            writer_principal_id: principal.principal_id.clone(),
            provenance: command.provenance,
            sensitivity: command.sensitivity,
            retention_policy_id: command.retention_policy_id,
            schema_version: 1,
            payload: command.payload,
            payload_sha256,
        };

        await_content_operation(permit, async {
            self.episodes
                .append(
                    episode,
                    IdempotencyRequest {
                        key: idempotency_key,
                        fingerprint,
                    },
                )
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn get_episode(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        episode_id: EpisodeId,
    ) -> Result<Episode, ServiceError> {
        authorize_content(permit, principal, tenant_id, subject_id)?;
        await_content_operation(permit, async {
            self.episodes
                .get(tenant_id, subject_id, episode_id)
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn create_fact(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: CreateFact,
    ) -> Result<FactMutationOutcome, ServiceError> {
        authorize_content(permit, principal, command.tenant_id, command.subject_id)?;
        validate_create_fact(&command)?;

        let fingerprint_input = json!({
            "operation_id": "createFact",
            "content_type": "application/json",
            "tenant_id": command.tenant_id.0,
            "subject_id": command.subject_id.0,
            "case_id": command.case_id.0,
            "namespace": command.namespace,
            "key": command.key,
            "value": command.value,
            "observed_at_unix_nanos": command.observed_at.unix_timestamp_nanos().to_string(),
            "valid_from_unix_nanos": command.valid_time.from.unix_timestamp_nanos().to_string(),
            "valid_until_unix_nanos": command.valid_time.until.map(|value| value.unix_timestamp_nanos().to_string()),
            "evidence_episode_ids": command.evidence_episode_ids,
            "write_policy": command.write_policy,
            "confidence": command.confidence,
            "sensitivity": command.sensitivity,
            "retention_policy_id": command.retention_policy_id,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let value_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&command.value)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let fact = NewFact {
            tenant_id: command.tenant_id,
            subject_id: command.subject_id,
            case_id: command.case_id,
            fact_id: FactId(Uuid::now_v7()),
            revision_id: palimpsest_domain::RevisionId(Uuid::now_v7()),
            namespace: command.namespace,
            key: command.key,
            value: command.value,
            observed_at: command.observed_at,
            valid_time: command.valid_time,
            evidence_episode_ids: command.evidence_episode_ids,
            write_policy: command.write_policy,
            confidence: command.confidence,
            sensitivity: command.sensitivity,
            retention_policy_id: command.retention_policy_id,
            writer_principal_id: principal.principal_id.clone(),
            schema_version: 1,
            value_sha256,
        };
        await_content_operation(permit, async {
            self.facts
                .create(
                    fact,
                    IdempotencyRequest {
                        key: idempotency_key,
                        fingerprint,
                    },
                )
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn get_current_fact(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
    ) -> Result<FactView, ServiceError> {
        authorize_content(permit, principal, tenant_id, subject_id)?;
        await_content_operation(permit, async {
            self.facts
                .get_current(tenant_id, subject_id, fact_id)
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn supersede_fact(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        idempotency_key: String,
        expected_head_revision_id: RevisionId,
        command: SupersedeFact,
    ) -> Result<FactMutationOutcome, ServiceError> {
        authorize_content(permit, principal, command.tenant_id, command.subject_id)?;
        validate_fact_revision(FactRevisionValidation {
            value: &command.value,
            valid_time: &command.valid_time,
            evidence_episode_ids: &command.evidence_episode_ids,
            write_policy_id: command.write_policy.id.as_str(),
            write_policy_version: command.write_policy.version.as_str(),
            confidence: command.confidence,
            sensitivity: command.sensitivity.as_str(),
            retention_policy_id: command.retention_policy_id.as_str(),
        })?;
        let fingerprint_input = json!({
            "operation_id": "supersedeFact",
            "content_type": "application/json",
            "tenant_id": command.tenant_id.0,
            "subject_id": command.subject_id.0,
            "fact_id": command.fact_id.0,
            "if_match": expected_head_revision_id.0,
            "supersedes_revision_id": command.supersedes_revision_id.0,
            "value": command.value,
            "observed_at_unix_nanos": command.observed_at.unix_timestamp_nanos().to_string(),
            "valid_from_unix_nanos": command.valid_time.from.unix_timestamp_nanos().to_string(),
            "valid_until_unix_nanos": command.valid_time.until.map(|value| value.unix_timestamp_nanos().to_string()),
            "evidence_episode_ids": command.evidence_episode_ids,
            "write_policy": command.write_policy,
            "confidence": command.confidence,
            "sensitivity": command.sensitivity,
            "retention_policy_id": command.retention_policy_id,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let value_sha256 = hex::encode(Sha256::digest(
            serde_json::to_vec(&command.value)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        await_content_operation(permit, async {
            self.facts
                .supersede(
                    NewFactRevision {
                        tenant_id: command.tenant_id,
                        subject_id: command.subject_id,
                        fact_id: command.fact_id,
                        revision_id: RevisionId(Uuid::now_v7()),
                        supersedes_revision_id: command.supersedes_revision_id,
                        expected_head_revision_id,
                        value: command.value,
                        observed_at: command.observed_at,
                        valid_time: command.valid_time,
                        evidence_episode_ids: command.evidence_episode_ids,
                        write_policy: command.write_policy,
                        confidence: command.confidence,
                        sensitivity: command.sensitivity,
                        retention_policy_id: command.retention_policy_id,
                        writer_principal_id: principal.principal_id.clone(),
                        schema_version: 1,
                        value_sha256,
                    },
                    IdempotencyRequest {
                        key: idempotency_key,
                        fingerprint,
                    },
                )
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn get_fact_as_of(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
        coordinates: FactAsOfCoordinates,
    ) -> Result<FactView, ServiceError> {
        authorize_content(permit, principal, tenant_id, subject_id)?;
        await_content_operation(permit, async {
            self.facts
                .get_as_of(
                    tenant_id,
                    subject_id,
                    fact_id,
                    coordinates.valid_at,
                    coordinates.recorded_at,
                )
                .await
                .map_err(map_repository)
        })
        .await
    }

    pub async fn create_retrieval(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: CreateRetrieval,
    ) -> Result<RetrievalMutationOutcome, ServiceError> {
        authorize_content(permit, principal, command.tenant_id, command.subject_id)?;
        validate_idempotency_key(&idempotency_key)?;
        if !(1..=50).contains(&command.page_size) {
            return Err(ServiceError::Unprocessable(
                "page_size must be between 1 and 50".to_owned(),
            ));
        }
        if command.query.as_str().len() > MAX_RETRIEVAL_QUERY_BYTES {
            return Err(ServiceError::RetrievalTooLarge);
        }
        let query = RetrievalQuery::try_from(command.query.as_str().trim().to_owned())
            .map_err(|error| ServiceError::Unprocessable(error.to_string()))?;
        let policy_id = command.policy_id.unwrap_or(
            RetrievalPolicyId::try_from("retrieval-lexical-v1".to_owned())
                .expect("the built-in retrieval policy ID is valid"),
        );
        if !matches!(
            policy_id.as_str(),
            "retrieval-lexical-v1"
                | "retrieval-exact-vector-v1"
                | "retrieval-hybrid-v1"
                | "retrieval-hybrid-temporal-v1"
        ) {
            return Err(ServiceError::Unprocessable(
                "policy_id is not supported".to_owned(),
            ));
        }
        let filters = normalize_retrieval_filters(command.filters, principal)?;
        let fingerprint_input = json!({
            "operation_id": "createRetrieval",
            "content_type": "application/json",
            "principal_id": principal.principal_id.0,
            "tenant_id": command.tenant_id.0,
            "subject_id": command.subject_id.0,
            "query": query.as_str(),
            "perspective": command.perspective,
            "page_size": command.page_size,
            "policy_id": policy_id,
            "filters": filters,
        });
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&fingerprint_input)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        ));
        let query_sha256 = hex::encode(Sha256::digest(query.as_str().as_bytes()));
        let mut allowed_sensitivities = principal.allowed_sensitivities.clone();
        allowed_sensitivities.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        allowed_sensitivities.dedup_by(|left, right| left.as_str() == right.as_str());
        let authorization_scope_sha256 = retrieval_authorization_scope_sha256(
            principal,
            command.tenant_id,
            command.subject_id,
            &allowed_sensitivities,
        )?;

        let retrieval = NewRetrieval {
            tenant_id: command.tenant_id,
            subject_id: command.subject_id,
            retrieval_id: RetrievalId(Uuid::now_v7()),
            query,
            query_sha256,
            authorization_scope_sha256,
            perspective: command.perspective,
            page_size: command.page_size,
            policy_id,
            filters,
            principal_id: principal.principal_id.clone(),
            allowed_sensitivities,
        };
        let idempotency = IdempotencyRequest {
            key: idempotency_key,
            fingerprint,
        };

        let preparation = await_content_operation(permit, async {
            self.retrievals
                .prepare_receipt(&retrieval, &idempotency)
                .await
                .map_err(map_repository)
        })
        .await?;
        let outcome = match preparation {
            RetrievalPreparation::Replay(outcome) => outcome,
            RetrievalPreparation::Execute { embedding_profile } => {
                let query_embedding = if let Some(profile) = embedding_profile {
                    let response = await_content_operation(permit, async {
                        self.embeddings
                            .embed(EmbeddingRequest {
                                profile: profile.clone(),
                                task: EmbeddingTask::Query,
                                inputs: vec![EmbeddingInput {
                                    input_sha256: retrieval.query_sha256.clone(),
                                    content: retrieval.query.as_str().to_owned(),
                                }],
                            })
                            .await
                            .map_err(|_| ServiceError::Unavailable)
                    })
                    .await?;
                    let mut outputs = validate_embedding_response(
                        &profile,
                        &[retrieval.query_sha256.as_str()],
                        response,
                    )
                    .map_err(|_| ServiceError::Unavailable)?;
                    Some(RetrievalQueryEmbedding {
                        profile,
                        output: outputs.remove(0),
                    })
                } else {
                    None
                };
                await_content_operation(permit, async {
                    self.retrievals
                        .create_receipt(retrieval, idempotency, query_embedding)
                        .await
                        .map_err(map_repository)
                })
                .await?
            }
        };
        self.cache_receipt(command.tenant_id, &outcome.receipt)
            .await;
        Ok(outcome)
    }

    /// Best-effort advisory write (spec 015). The cache never gates the
    /// canonical outcome; a failure here is not a retrieval failure.
    async fn cache_receipt(&self, tenant_id: TenantId, receipt: &RetrievalReceipt) {
        let Ok(bytes) = serde_json::to_vec(receipt) else {
            return;
        };
        let scope = receipt.retrieval_id.0.to_string();
        let _ = self
            .cache
            .put(
                tenant_id,
                HotCacheKind::Receipt,
                scope.as_str(),
                &bytes,
                HOT_CACHE_RECEIPT_TTL_SECONDS,
            )
            .await;
    }

    pub async fn get_retrieval(
        &self,
        permit: &ContentLeasePermit,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
    ) -> Result<RetrievalReceipt, ServiceError> {
        authorize_content(permit, principal, tenant_id, subject_id)?;
        if cursor
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 2048)
        {
            return Err(ServiceError::Unprocessable(
                "cursor must contain 1 to 2048 characters".to_owned(),
            ));
        }
        let mut allowed_sensitivities = principal.allowed_sensitivities.clone();
        allowed_sensitivities.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        allowed_sensitivities.dedup_by(|left, right| left.as_str() == right.as_str());
        let authorization_scope_sha256 = retrieval_authorization_scope_sha256(
            principal,
            tenant_id,
            subject_id,
            &allowed_sensitivities,
        )?;
        if let Some(receipt) = cached_retrieval_receipt(
            self.cache.as_ref(),
            tenant_id,
            subject_id,
            retrieval_id,
            cursor.as_deref(),
            &authorization_scope_sha256,
        )
        .await
        {
            return Ok(receipt);
        }
        let receipt = await_content_operation(permit, async {
            self.retrievals
                .get_receipt(
                    principal,
                    tenant_id,
                    subject_id,
                    retrieval_id,
                    cursor,
                    authorization_scope_sha256,
                )
                .await
                .map_err(map_repository)
        })
        .await?;
        self.cache_receipt(tenant_id, &receipt).await;
        Ok(receipt)
    }
}

fn retrieval_authorization_scope_sha256(
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
    allowed_sensitivities: &[Sensitivity],
) -> Result<String, ServiceError> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&json!({
            "principal_id": principal.principal_id.0,
            "tenant_id": tenant_id.0,
            "subject_id": subject_id.0,
            "allowed_sensitivities": allowed_sensitivities,
        }))
        .map_err(|error| ServiceError::Invalid(error.to_string()))?,
    )))
}

pub fn validate_embedding_response(
    profile: &EmbeddingProfile,
    expected_input_sha256: &[&str],
    response: EmbeddingResponse,
) -> Result<Vec<EmbeddingOutput>, EmbeddingProviderError> {
    if profile.dimensions == 0
        || profile.digest.is_empty()
        || profile.normalization != "unit_l2"
        || !profile.normalization_tolerance.is_finite()
        || profile.normalization_tolerance <= 0.0
        || profile.distance_metric != "cosine"
        || profile.scalar_type != "float32"
        || response.profile_digest != profile.digest
        || response.outputs.len() != expected_input_sha256.len()
    {
        return Err(EmbeddingProviderError::InvalidResponse {
            code: "profile_contract_mismatch".to_owned(),
        });
    }

    for (expected_input, output) in expected_input_sha256.iter().zip(&response.outputs) {
        if output.input_sha256 != *expected_input
            || output.values.len() != profile.dimensions
            || output.values.iter().any(|value| !value.is_finite())
        {
            return Err(EmbeddingProviderError::InvalidResponse {
                code: "embedding_shape_invalid".to_owned(),
            });
        }
        let squared_norm = output
            .values
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>();
        if squared_norm <= f64::EPSILON
            || (squared_norm.sqrt() - 1.0).abs() > profile.normalization_tolerance
        {
            return Err(EmbeddingProviderError::InvalidResponse {
                code: "embedding_normalization_invalid".to_owned(),
            });
        }
    }

    Ok(response.outputs)
}

fn normalize_retrieval_filters(
    mut filters: RetrievalFilters,
    principal: &PrincipalScope,
) -> Result<RetrievalFilters, ServiceError> {
    if [
        filters.case_ids.as_ref().map(Vec::len),
        filters.namespaces.as_ref().map(Vec::len),
        filters.keys.as_ref().map(Vec::len),
        filters.sensitivities.as_ref().map(Vec::len),
    ]
    .into_iter()
    .flatten()
    .any(|length| length > MAX_RETRIEVAL_FILTER_VALUES)
    {
        return Err(ServiceError::RetrievalTooLarge);
    }
    if filters.case_ids.as_ref().is_some_and(Vec::is_empty)
        || filters.namespaces.as_ref().is_some_and(Vec::is_empty)
        || filters.keys.as_ref().is_some_and(Vec::is_empty)
        || filters.sensitivities.as_ref().is_some_and(Vec::is_empty)
    {
        return Err(ServiceError::Unprocessable(
            "retrieval filter arrays must not be empty".to_owned(),
        ));
    }
    if let Some(case_ids) = &mut filters.case_ids {
        case_ids.sort_by_key(|value| value.0);
        case_ids.dedup();
    }
    if let Some(namespaces) = &mut filters.namespaces {
        namespaces.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        namespaces.dedup_by(|left, right| left.as_str() == right.as_str());
    }
    if let Some(keys) = &mut filters.keys {
        keys.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        keys.dedup_by(|left, right| left.as_str() == right.as_str());
    }
    if let Some(sensitivities) = &mut filters.sensitivities {
        sensitivities.retain(|value| principal.authorizes_sensitivity(value));
        sensitivities.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sensitivities.dedup_by(|left, right| left.as_str() == right.as_str());
    }
    Ok(filters)
}

fn authorize(
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), ServiceError> {
    if principal.authorizes(tenant_id, subject_id) {
        Ok(())
    } else {
        Err(ServiceError::NotFound)
    }
}

fn authorize_tenant(principal: &PrincipalScope, tenant_id: TenantId) -> Result<(), ServiceError> {
    if principal.tenant_id == tenant_id {
        Ok(())
    } else {
        Err(ServiceError::NotFound)
    }
}

fn sanitize_failure_reason(reason: &str) -> String {
    reason
        .chars()
        .take(200)
        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        .collect()
}

fn authorize_content(
    permit: &ContentLeasePermit,
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), ServiceError> {
    authorize(principal, tenant_id, subject_id)?;
    if permit.lease.tenant_id == tenant_id
        && permit.lease.subject_id == subject_id
        && permit.lease.principal_id == principal.principal_id
        && time::OffsetDateTime::now_utc() < permit.lease.expires_at
    {
        Ok(())
    } else {
        Err(ServiceError::NotFound)
    }
}

async fn await_content_operation<T>(
    permit: &ContentLeasePermit,
    future: impl std::future::Future<Output = Result<T, ServiceError>>,
) -> Result<T, ServiceError> {
    let remaining = permit.lease.expires_at - time::OffsetDateTime::now_utc();
    let remaining = if remaining <= time::Duration::ZERO {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::try_from(remaining).map_err(|_| ServiceError::Unavailable)?
    };
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| ServiceError::Unavailable)?
}

fn authorize_operation(
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
    operation: OperationGrant,
) -> Result<(), ServiceError> {
    if principal.authorizes(tenant_id, subject_id) && principal.authorizes_operation(operation) {
        Ok(())
    } else {
        Err(ServiceError::NotFound)
    }
}

fn validate_append(command: &AppendEpisode) -> Result<(), ServiceError> {
    for (name, value) in [
        ("kind", command.kind.as_str()),
        (
            "provenance.source_type",
            command.provenance.source_type.as_str(),
        ),
        ("sensitivity", command.sensitivity.as_str()),
        ("retention_policy_id", command.retention_policy_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::Invalid(format!("{name} must not be empty")));
        }
    }
    Ok(())
}

fn validate_idempotency_key(idempotency_key: &str) -> Result<(), ServiceError> {
    if idempotency_key.trim().is_empty() || idempotency_key.chars().count() > 255 {
        return Err(ServiceError::Invalid(
            "idempotency_key must contain 1 to 255 characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_surface_identifier(label: &str, value: &str) -> Result<(), ServiceError> {
    if value.trim().is_empty() || value.chars().count() > 255 {
        return Err(ServiceError::Invalid(format!(
            "{label} must contain 1 to 255 characters"
        )));
    }
    Ok(())
}

fn validate_deletion_repair_reason(reason_code: &str) -> Result<(), ServiceError> {
    if reason_code.is_empty()
        || reason_code.len() > 64
        || !reason_code.bytes().enumerate().all(|(index, byte)| {
            (index == 0 && byte.is_ascii_lowercase())
                || (index > 0
                    && (byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
        })
    {
        return Err(ServiceError::Invalid(
            "deletion repair reason must be a lowercase reason code".to_owned(),
        ));
    }
    Ok(())
}

fn validate_checkpoint(
    command: &SaveCheckpoint,
    precondition: CheckpointPrecondition,
) -> Result<(), ServiceError> {
    let state_bytes = serde_json::to_vec(&command.state)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    if state_bytes.len() > MAX_CHECKPOINT_STATE_BYTES
        || command.effect_transitions.len() > MAX_CHECKPOINT_EFFECT_TRANSITIONS
    {
        return Err(ServiceError::CheckpointTooLarge);
    }
    if command.state_schema_version == 0 {
        return Err(ServiceError::Invalid(
            "state_schema_version must be greater than zero".to_owned(),
        ));
    }
    match (precondition, command.parent_revision_id) {
        (CheckpointPrecondition::Create, None) => {}
        (CheckpointPrecondition::Match(expected), Some(parent)) if expected == parent => {}
        _ => return Err(ServiceError::CheckpointParentConflict),
    }

    let mut prepared_effect_keys = HashSet::new();
    let mut completed_effect_ids = HashSet::new();
    for transition in &command.effect_transitions {
        match transition {
            EffectTransition::Prepare(transition) => {
                if !prepared_effect_keys.insert(transition.effect_key.as_str()) {
                    return Err(ServiceError::EffectKeyConflict);
                }
            }
            EffectTransition::Complete(CompleteEffectTransition { effect_id, receipt }) => {
                if !completed_effect_ids.insert(effect_id.0) {
                    return Err(ServiceError::InvalidEffectTransition);
                }
                if receipt.outcome_sha256.len() != 64
                    || !receipt
                        .outcome_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(ServiceError::Invalid(
                        "effect receipt outcome_sha256 must be 64 lowercase hexadecimal characters"
                            .to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_create_fact(command: &CreateFact) -> Result<(), ServiceError> {
    for (name, value) in [
        ("namespace", command.namespace.as_str()),
        ("key", command.key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::Invalid(format!("{name} must not be empty")));
        }
    }
    validate_fact_revision(FactRevisionValidation {
        value: &command.value,
        valid_time: &command.valid_time,
        evidence_episode_ids: &command.evidence_episode_ids,
        write_policy_id: command.write_policy.id.as_str(),
        write_policy_version: command.write_policy.version.as_str(),
        confidence: command.confidence,
        sensitivity: command.sensitivity.as_str(),
        retention_policy_id: command.retention_policy_id.as_str(),
    })
}

struct FactRevisionValidation<'a> {
    value: &'a serde_json::Value,
    valid_time: &'a palimpsest_domain::ValidTime,
    evidence_episode_ids: &'a [EpisodeId],
    write_policy_id: &'a str,
    write_policy_version: &'a str,
    confidence: f64,
    sensitivity: &'a str,
    retention_policy_id: &'a str,
}

fn validate_fact_revision(input: FactRevisionValidation<'_>) -> Result<(), ServiceError> {
    for (name, value) in [
        ("write_policy.id", input.write_policy_id),
        ("write_policy.version", input.write_policy_version),
        ("sensitivity", input.sensitivity),
        ("retention_policy_id", input.retention_policy_id),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::Invalid(format!("{name} must not be empty")));
        }
    }
    if input.value.is_null() {
        return Err(ServiceError::Invalid("value must not be null".to_owned()));
    }
    if input.evidence_episode_ids.is_empty() {
        return Err(ServiceError::Invalid(
            "evidence_episode_ids must not be empty".to_owned(),
        ));
    }
    let mut evidence = input
        .evidence_episode_ids
        .iter()
        .map(|episode_id| episode_id.0)
        .collect::<Vec<_>>();
    evidence.sort_unstable();
    evidence.dedup();
    if evidence.len() != input.evidence_episode_ids.len() {
        return Err(ServiceError::Invalid(
            "evidence_episode_ids must be unique".to_owned(),
        ));
    }
    if input
        .valid_time
        .until
        .is_some_and(|until| until <= input.valid_time.from)
    {
        return Err(ServiceError::InvalidValidTime(
            "valid_time.until must be later than valid_time.from".to_owned(),
        ));
    }
    if !(0.0..=1.0).contains(&input.confidence) || !input.confidence.is_finite() {
        return Err(ServiceError::Invalid(
            "confidence must be between 0 and 1".to_owned(),
        ));
    }
    Ok(())
}

pub fn export_authorization_scope_sha256(
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<String, ServiceError> {
    let mut sensitivities = principal
        .allowed_sensitivities
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    sensitivities.sort_unstable();
    sensitivities.dedup();
    let input = json!({
        "principal_id": principal.principal_id.0,
        "tenant_id": tenant_id.0,
        "subject_id": subject_id.0,
        "allowed_sensitivities": sensitivities,
        "grant": "canonical_history_export",
        "schema_version": 1,
    });
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&input).map_err(|error| ServiceError::Invalid(error.to_string()))?,
    )))
}

fn configured_deletion_targets(exports_configured: bool) -> Vec<DeletionTargetName> {
    let mut targets = vec![
        DeletionTargetName::Canonical,
        DeletionTargetName::Projections,
    ];
    if exports_configured {
        targets.push(DeletionTargetName::Exports);
    }
    targets
}

fn map_export_store_error(error: ExportStoreError) -> ServiceError {
    match error {
        ExportStoreError::NotFound => ServiceError::NotFound,
        ExportStoreError::Conflict | ExportStoreError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_repository(error: RepositoryError) -> ServiceError {
    match error {
        RepositoryError::NotFound | RepositoryError::SubjectUnavailable => ServiceError::NotFound,
        RepositoryError::Conflict => ServiceError::Conflict,
        RepositoryError::Expired => ServiceError::ExportExpired,
        RepositoryError::IdempotencyKeyReused => ServiceError::IdempotencyKeyReused,
        RepositoryError::IdempotencyInProgress => ServiceError::IdempotencyInProgress,
        RepositoryError::PreconditionFailed => ServiceError::PreconditionFailed,
        RepositoryError::SupersessionConflict => ServiceError::SupersessionConflict,
        RepositoryError::FutureRecordedTime => ServiceError::FutureRecordedTime,
        RepositoryError::CheckpointPreconditionFailed => ServiceError::CheckpointPreconditionFailed,
        RepositoryError::CheckpointParentConflict => ServiceError::CheckpointParentConflict,
        RepositoryError::CheckpointCaseConflict => ServiceError::CheckpointCaseConflict,
        RepositoryError::CheckpointAlreadyExists => ServiceError::CheckpointAlreadyExists,
        RepositoryError::CheckpointExpired => ServiceError::CheckpointExpired,
        RepositoryError::EffectKeyConflict => ServiceError::EffectKeyConflict,
        RepositoryError::InvalidEffectTransition => ServiceError::InvalidEffectTransition,
        RepositoryError::RetentionPolicyRejected => ServiceError::RetentionPolicyRejected,
        RepositoryError::WritePolicyRejected => ServiceError::WritePolicyRejected,
        RepositoryError::SerializationRetry => ServiceError::Unavailable,
        RepositoryError::Unexpected(_) => ServiceError::Unavailable,
    }
}

#[cfg(test)]
mod content_lease_tests {
    use super::*;
    use palimpsest_domain::{ContentLeaseId, PrincipalId};

    #[test]
    fn expired_or_mismatched_permits_cannot_authorize_content_operations() {
        let tenant_id = TenantId(Uuid::now_v7());
        let subject_id = SubjectId(Uuid::now_v7());
        let principal = PrincipalScope {
            principal_id: PrincipalId("permit-test-principal".to_owned()),
            tenant_id,
            subject_ids: vec![subject_id],
            allowed_sensitivities: vec![],
            operation_grants: vec![],
        };
        let permit = ContentLeasePermit {
            lease: SubjectContentLease {
                tenant_id,
                subject_id,
                lease_id: ContentLeaseId(Uuid::now_v7()),
                principal_id: principal.principal_id.clone(),
                acquired_at: time::OffsetDateTime::now_utc() - time::Duration::SECOND,
                expires_at: time::OffsetDateTime::now_utc() - time::Duration::MILLISECOND,
            },
        };

        assert!(matches!(
            authorize_content(&permit, &principal, tenant_id, subject_id),
            Err(ServiceError::NotFound)
        ));
        assert!(matches!(
            authorize_content(&permit, &principal, tenant_id, SubjectId(Uuid::now_v7())),
            Err(ServiceError::NotFound)
        ));
    }
}

#[cfg(test)]
mod deletion_policy_tests {
    use super::*;

    #[test]
    fn deletion_policy_never_claims_unimplemented_targets() {
        assert_eq!(
            configured_deletion_targets(false),
            vec![
                DeletionTargetName::Canonical,
                DeletionTargetName::Projections
            ]
        );
        assert_eq!(
            configured_deletion_targets(true),
            vec![
                DeletionTargetName::Canonical,
                DeletionTargetName::Projections,
                DeletionTargetName::Exports,
            ]
        );
    }
}

#[cfg(test)]
mod hot_cache_receipt_tests {
    use super::*;
    use palimpsest_cache::MemoryHotCache;
    use palimpsest_domain::{RetrievalAuthorizationReceipt, RetrievalPolicy};

    fn sample_receipt(scope_digest: &str) -> RetrievalReceipt {
        let tenant_id = TenantId(Uuid::from_u128(1));
        let subject_id = SubjectId(Uuid::from_u128(2));
        let retrieval_id = RetrievalId(Uuid::from_u128(3));
        RetrievalReceipt {
            tenant_id,
            subject_id,
            retrieval_id,
            status: "authorized".to_owned(),
            evaluated_at: time::OffsetDateTime::now_utc(),
            valid_at: time::OffsetDateTime::now_utc(),
            recorded_at: time::OffsetDateTime::now_utc(),
            policy: RetrievalPolicy {
                id: RetrievalPolicyId::try_from("retrieval-lexical-v1".to_owned()).unwrap(),
                version: "1".to_owned(),
                digest: "test".to_owned(),
            },
            authorization: RetrievalAuthorizationReceipt {
                decision: "allowed".to_owned(),
                scope_digest: scope_digest.to_owned(),
            },
            document_schema_version: 1,
            query_embedding: None,
            items: vec![],
            next_cursor: None,
        }
    }

    async fn cached(cache: &MemoryHotCache, receipt: &RetrievalReceipt) {
        cache
            .put(
                receipt.tenant_id,
                HotCacheKind::Receipt,
                receipt.retrieval_id.0.to_string().as_str(),
                &serde_json::to_vec(receipt).unwrap(),
                300,
            )
            .await;
    }

    #[tokio::test]
    async fn matching_hit_is_served_only_without_a_cursor() {
        let cache = MemoryHotCache::new();
        let receipt = sample_receipt("scope-a");
        cached(&cache, &receipt).await;

        let hit = cached_retrieval_receipt(
            &cache,
            receipt.tenant_id,
            receipt.subject_id,
            receipt.retrieval_id,
            None,
            "scope-a",
        )
        .await;
        assert!(hit.is_some(), "a matching cursor-less read must hit");

        let paged = cached_retrieval_receipt(
            &cache,
            receipt.tenant_id,
            receipt.subject_id,
            receipt.retrieval_id,
            Some("page-2"),
            "scope-a",
        )
        .await;
        assert!(
            paged.is_none(),
            "a paged read must never come from the cache"
        );
    }

    #[tokio::test]
    async fn scope_mismatch_is_a_miss() {
        let cache = MemoryHotCache::new();
        let receipt = sample_receipt("scope-a");
        cached(&cache, &receipt).await;

        let hit = cached_retrieval_receipt(
            &cache,
            receipt.tenant_id,
            receipt.subject_id,
            receipt.retrieval_id,
            None,
            "scope-b",
        )
        .await;
        assert!(
            hit.is_none(),
            "a different authorization scope must not hit"
        );
    }

    #[tokio::test]
    async fn id_mismatch_is_a_miss() {
        let cache = MemoryHotCache::new();
        let receipt = sample_receipt("scope-a");
        cached(&cache, &receipt).await;

        let hit = cached_retrieval_receipt(
            &cache,
            receipt.tenant_id,
            receipt.subject_id,
            RetrievalId(Uuid::from_u128(99)),
            None,
            "scope-a",
        )
        .await;
        assert!(hit.is_none(), "a different retrieval id must not hit");
    }

    #[tokio::test]
    async fn corrupt_bytes_are_a_miss() {
        let cache = MemoryHotCache::new();
        cache
            .put(
                TenantId(Uuid::from_u128(1)),
                HotCacheKind::Receipt,
                "3",
                b"not-json",
                300,
            )
            .await;

        let hit = cached_retrieval_receipt(
            &cache,
            TenantId(Uuid::from_u128(1)),
            SubjectId(Uuid::from_u128(2)),
            RetrievalId(Uuid::from_u128(3)),
            None,
            "scope-a",
        )
        .await;
        assert!(
            hit.is_none(),
            "undecodable bytes must fall back to canonical"
        );
    }
}
