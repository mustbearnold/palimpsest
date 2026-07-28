use std::sync::Arc;

use async_trait::async_trait;
use palimpsest_domain::{
    AppendEpisode, CreateFact, Episode, EpisodeId, FactId, FactView, NewEpisode, NewFact,
    NewFactRevision, PrincipalScope, RevisionId, SubjectId, SupersedeFact, TenantId,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

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

#[derive(Debug)]
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
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("service unavailable")]
    Unavailable,
}

#[derive(Clone)]
pub struct MemoryService {
    episodes: Arc<dyn EpisodeRepository>,
    facts: Arc<dyn FactRepository>,
}

impl MemoryService {
    pub fn new(episodes: Arc<dyn EpisodeRepository>, facts: Arc<dyn FactRepository>) -> Self {
        Self { episodes, facts }
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
        return Err(ServiceError::Invalid(
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
        RepositoryError::Unexpected(_) => ServiceError::Unavailable,
    }
}
