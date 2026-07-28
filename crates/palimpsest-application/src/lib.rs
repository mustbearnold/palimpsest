use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use palimpsest_domain::{
    AgentId, AppendEpisode, CheckpointPrecondition, CheckpointRevisionId, CheckpointView,
    CompleteEffectTransition, CreateFact, CreateRetrieval, EffectId, EffectTransition, Episode,
    EpisodeId, FactId, FactView, NewCheckpointRevision, NewEffectTransition, NewEpisode, NewFact,
    NewFactRevision, NewPreparedEffect, NewRetrieval, PrincipalScope, RetrievalFilters,
    RetrievalId, RetrievalPolicyId, RetrievalQuery, RetrievalReceipt, RevisionId, SaveCheckpoint,
    Sensitivity, SubjectId, SupersedeFact, TenantId, ThreadId,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MAX_CHECKPOINT_STATE_BYTES: usize = 1_048_576;
const MAX_CHECKPOINT_EFFECT_TRANSITIONS: usize = 100;
const MAX_RETRIEVAL_QUERY_BYTES: usize = 4096;
const MAX_RETRIEVAL_FILTER_VALUES: usize = 100;

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("record not found")]
    NotFound,
    #[error("record conflicts with existing data")]
    Conflict,
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
    async fn create_receipt(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
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

#[derive(Debug)]
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
    #[error("checkpoint exceeds the supported size")]
    CheckpointTooLarge,
    #[error("retrieval request exceeds the supported size")]
    RetrievalTooLarge,
    #[error("service unavailable")]
    Unavailable,
}

#[derive(Clone)]
pub struct MemoryService {
    episodes: Arc<dyn EpisodeRepository>,
    facts: Arc<dyn FactRepository>,
    checkpoints: Arc<dyn CheckpointRepository>,
    retrievals: Arc<dyn RetrievalRepository>,
}

impl MemoryService {
    pub fn new(
        episodes: Arc<dyn EpisodeRepository>,
        facts: Arc<dyn FactRepository>,
        checkpoints: Arc<dyn CheckpointRepository>,
        retrievals: Arc<dyn RetrievalRepository>,
    ) -> Self {
        Self {
            episodes,
            facts,
            checkpoints,
            retrievals,
        }
    }

    pub async fn save_checkpoint(
        &self,
        principal: &PrincipalScope,
        idempotency_key: String,
        precondition: CheckpointPrecondition,
        command: SaveCheckpoint,
    ) -> Result<CheckpointMutationOutcome, ServiceError> {
        authorize(principal, command.tenant_id, command.subject_id)?;
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
    }

    pub async fn get_checkpoint(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        agent_id: AgentId,
        thread_id: ThreadId,
    ) -> Result<CheckpointView, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        self.checkpoints
            .get_current(tenant_id, subject_id, agent_id, thread_id)
            .await
            .map_err(map_repository)
    }

    pub async fn append_episode(
        &self,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: AppendEpisode,
    ) -> Result<AppendOutcome, ServiceError> {
        authorize(principal, command.tenant_id, command.subject_id)?;
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
    }

    pub async fn get_episode(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        episode_id: EpisodeId,
    ) -> Result<Episode, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        self.episodes
            .get(tenant_id, subject_id, episode_id)
            .await
            .map_err(map_repository)
    }

    pub async fn create_fact(
        &self,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: CreateFact,
    ) -> Result<FactMutationOutcome, ServiceError> {
        authorize(principal, command.tenant_id, command.subject_id)?;
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
    }

    pub async fn get_current_fact(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
    ) -> Result<FactView, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        self.facts
            .get_current(tenant_id, subject_id, fact_id)
            .await
            .map_err(map_repository)
    }

    pub async fn supersede_fact(
        &self,
        principal: &PrincipalScope,
        idempotency_key: String,
        expected_head_revision_id: RevisionId,
        command: SupersedeFact,
    ) -> Result<FactMutationOutcome, ServiceError> {
        authorize(principal, command.tenant_id, command.subject_id)?;
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
    }

    pub async fn get_fact_as_of(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
        valid_at: time::OffsetDateTime,
        recorded_at: time::OffsetDateTime,
    ) -> Result<FactView, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
        self.facts
            .get_as_of(tenant_id, subject_id, fact_id, valid_at, recorded_at)
            .await
            .map_err(map_repository)
    }

    pub async fn create_retrieval(
        &self,
        principal: &PrincipalScope,
        idempotency_key: String,
        command: CreateRetrieval,
    ) -> Result<RetrievalMutationOutcome, ServiceError> {
        authorize(principal, command.tenant_id, command.subject_id)?;
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
        if policy_id.as_str() != "retrieval-lexical-v1" {
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

        self.retrievals
            .create_receipt(
                NewRetrieval {
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
                },
                IdempotencyRequest {
                    key: idempotency_key,
                    fingerprint,
                },
            )
            .await
            .map_err(map_repository)
    }

    pub async fn get_retrieval(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
    ) -> Result<RetrievalReceipt, ServiceError> {
        authorize(principal, tenant_id, subject_id)?;
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

fn map_repository(error: RepositoryError) -> ServiceError {
    match error {
        RepositoryError::NotFound => ServiceError::NotFound,
        RepositoryError::Conflict => ServiceError::Conflict,
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
        RepositoryError::SerializationRetry => ServiceError::Unavailable,
        RepositoryError::Unexpected(_) => ServiceError::Unavailable,
    }
}
