use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    };
}

uuid_id!(TenantId);
uuid_id!(SubjectId);
uuid_id!(CaseId);
uuid_id!(AgentId);
uuid_id!(ThreadId);
uuid_id!(CheckpointId);
uuid_id!(CheckpointRevisionId);
uuid_id!(EffectId);
uuid_id!(EpisodeId);
uuid_id!(FactId);
uuid_id!(RevisionId);
uuid_id!(RetrievalId);
uuid_id!(ContentLeaseId);
uuid_id!(DeletionOperationId);
uuid_id!(ExportId);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextValueError {
    kind: &'static str,
    max_length: usize,
}

impl std::fmt::Display for TextValueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} must contain 1 to {} characters",
            self.kind, self.max_length
        )
    }
}

impl std::error::Error for TextValueError {}

macro_rules! text_value {
    ($name:ident, $max_length:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = TextValueError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.trim().is_empty() || value.chars().count() > $max_length {
                    Err(TextValueError {
                        kind: stringify!($name),
                        max_length: $max_length,
                    })
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::try_from(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

text_value!(EpisodeKind, 255);
text_value!(SourceType, 255);
text_value!(Sensitivity, 255);
text_value!(RetentionPolicyId, 255);
text_value!(FactNamespace, 255);
text_value!(FactKey, 512);
text_value!(WritePolicyId, 255);
text_value!(WritePolicyVersion, 255);
text_value!(EffectKey, 512);
text_value!(EffectKind, 255);
text_value!(ExternalEffectReference, 1024);
text_value!(RetrievalQuery, 4096);
text_value!(RetrievalPolicyId, 255);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingTask {
    Query,
    Document,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingProfile {
    pub id: String,
    pub version: String,
    pub provider: String,
    pub model: String,
    pub model_revision: String,
    pub dimensions: usize,
    pub normalization: String,
    pub normalization_tolerance: f64,
    pub distance_metric: String,
    pub scalar_type: String,
    pub input_serialization: String,
    pub query_task: String,
    pub document_task: String,
    pub provider_contract_schema_version: u32,
    pub digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EmbeddingInput {
    pub input_sha256: String,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingOutput {
    pub input_sha256: String,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PrincipalId(pub String);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationGrant {
    CanonicalHistoryExport,
    SubjectDelete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubjectContentLease {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub lease_id: ContentLeaseId,
    pub principal_id: PrincipalId,
    pub acquired_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectLifecycleState {
    Active,
    DeletionPending,
    Deleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubjectLifecycle {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub state: SubjectLifecycleState,
    pub state_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubjectLifecycleTransitionError {
    pub from: SubjectLifecycleState,
    pub to: SubjectLifecycleState,
}

impl std::fmt::Display for SubjectLifecycleTransitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "subject lifecycle cannot transition from {:?} to {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for SubjectLifecycleTransitionError {}

impl SubjectLifecycleState {
    pub fn transition_to(self, next: Self) -> Result<Self, SubjectLifecycleTransitionError> {
        if self == next
            || matches!(
                (self, next),
                (Self::Active, Self::DeletionPending) | (Self::DeletionPending, Self::Deleted)
            )
        {
            Ok(next)
        } else {
            Err(SubjectLifecycleTransitionError {
                from: self,
                to: next,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionOperationState {
    Draining,
    Fenced,
    Purging,
    RetryWait,
    Verifying,
    Completed,
    Failed,
    Expired,
}

impl DeletionOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::Fenced => "fenced",
            Self::Purging => "purging",
            Self::RetryWait => "retry_wait",
            Self::Verifying => "verifying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionTargetName {
    Canonical,
    Projections,
    Caches,
    Exports,
    Artifacts,
}

impl DeletionTargetName {
    pub const ALL: [Self; 5] = [
        Self::Canonical,
        Self::Projections,
        Self::Caches,
        Self::Exports,
        Self::Artifacts,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::Projections => "projections",
            Self::Caches => "caches",
            Self::Exports => "exports",
            Self::Artifacts => "artifacts",
        }
    }

    pub fn try_from_str(value: &str) -> Option<Self> {
        match value {
            "canonical" => Some(Self::Canonical),
            "projections" => Some(Self::Projections),
            "caches" => Some(Self::Caches),
            "exports" => Some(Self::Exports),
            "artifacts" => Some(Self::Artifacts),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionTargetCapability {
    Configured,
    NotConfigured,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionTargetState {
    Pending,
    Leased,
    Done,
    Failed,
    NotConfigured,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionTargetVerification {
    Pending,
    Verified,
    NotVerified,
    NotConfigured,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionLiveDisposition {
    PurgedAndVerified,
    FencedNotVerified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionBackupDisposition {
    IsolatedUntilExpiry,
    NotConfigured,
}

#[cfg(test)]
mod deletion_model_tests {
    use super::*;

    #[test]
    fn deletion_vocabulary_is_closed_and_stable() {
        assert_eq!(
            DeletionTargetName::ALL
                .iter()
                .map(|target| target.as_str())
                .collect::<Vec<_>>(),
            vec!["canonical", "projections", "caches", "exports", "artifacts"]
        );
        assert_eq!(DeletionOperationState::Completed.as_str(), "completed");
        assert!(serde_json::from_str::<DeletionOperationState>("\"purging\"").is_ok());
        assert!(serde_json::from_str::<DeletionTargetState>("\"leased\"").is_ok());
        assert!(serde_json::from_str::<DeletionTargetState>("\"done\"").is_ok());
        assert!(serde_json::from_str::<DeletionTargetState>("\"purging\"").is_err());
        assert!(serde_json::from_str::<DeletionTargetState>("\"verified\"").is_err());
        assert_eq!(
            serde_json::from_str::<DeletionTargetVerification>("\"verified\"")
                .expect("target verification vocabulary"),
            DeletionTargetVerification::Verified
        );
        assert_eq!(
            serde_json::from_str::<DeletionLiveDisposition>("\"purged_and_verified\"")
                .expect("live deletion disposition vocabulary"),
            DeletionLiveDisposition::PurgedAndVerified
        );
        assert_eq!(
            serde_json::from_str::<DeletionBackupDisposition>("\"not_configured\"")
                .expect("backup deletion disposition vocabulary"),
            DeletionBackupDisposition::NotConfigured
        );
        assert!(serde_json::from_str::<DeletionOperationState>("\"unknown\"").is_err());
        assert!(DeletionTargetName::try_from_str("unknown").is_none());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalScope {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub subject_ids: Vec<SubjectId>,
    pub allowed_sensitivities: Vec<Sensitivity>,
    pub operation_grants: Vec<OperationGrant>,
}

impl PrincipalScope {
    pub fn authorizes(&self, tenant_id: TenantId, subject_id: SubjectId) -> bool {
        self.tenant_id == tenant_id && self.subject_ids.contains(&subject_id)
    }

    pub fn authorizes_sensitivity(&self, sensitivity: &Sensitivity) -> bool {
        self.allowed_sensitivities.contains(sensitivity)
    }

    pub fn authorizes_operation(&self, operation: OperationGrant) -> bool {
        self.operation_grants.contains(&operation)
    }
}

#[cfg(test)]
mod principal_scope_tests {
    use super::*;

    #[test]
    fn operation_grants_are_closed_and_independent_from_subject_scope() {
        let scope = PrincipalScope {
            principal_id: PrincipalId("principal-a".to_owned()),
            tenant_id: TenantId(Uuid::nil()),
            subject_ids: vec![SubjectId(Uuid::nil())],
            allowed_sensitivities: vec![],
            operation_grants: vec![OperationGrant::CanonicalHistoryExport],
        };

        assert!(scope.authorizes_operation(OperationGrant::CanonicalHistoryExport));
        assert!(!scope.authorizes_operation(OperationGrant::SubjectDelete));
        assert!(serde_json::from_str::<OperationGrant>("\"subject_delete\"").is_ok());
        assert!(serde_json::from_str::<OperationGrant>("\"controller_override\"").is_err());
    }
}

#[cfg(test)]
mod subject_lifecycle_tests {
    use super::*;

    #[test]
    fn lifecycle_transitions_are_monotonic_and_never_reactivate() {
        assert_eq!(
            SubjectLifecycleState::Active.transition_to(SubjectLifecycleState::DeletionPending),
            Ok(SubjectLifecycleState::DeletionPending)
        );
        assert_eq!(
            SubjectLifecycleState::DeletionPending.transition_to(SubjectLifecycleState::Deleted),
            Ok(SubjectLifecycleState::Deleted)
        );
        assert_eq!(
            SubjectLifecycleState::DeletionPending
                .transition_to(SubjectLifecycleState::DeletionPending),
            Ok(SubjectLifecycleState::DeletionPending)
        );
        assert!(
            SubjectLifecycleState::Active
                .transition_to(SubjectLifecycleState::Deleted)
                .is_err()
        );
        assert!(
            SubjectLifecycleState::DeletionPending
                .transition_to(SubjectLifecycleState::Active)
                .is_err()
        );
        assert!(
            SubjectLifecycleState::Deleted
                .transition_to(SubjectLifecycleState::Active)
                .is_err()
        );
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectRecoveryMode {
    IdempotencyKey,
    Reconcile,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EffectReceipt {
    #[serde(
        serialize_with = "time::serde::rfc3339::serialize",
        deserialize_with = "deserialize_utc_microsecond_timestamp"
    )]
    pub observed_at: OffsetDateTime,
    pub external_reference: Option<ExternalEffectReference>,
    pub outcome_sha256: String,
}

pub fn parse_utc_microsecond_timestamp(value: &str) -> Result<OffsetDateTime, String> {
    if !is_utc_microsecond_timestamp(value) {
        return Err(
            "timestamp must be RFC 3339 UTC ending in Z with at most six fractional digits"
                .to_owned(),
        );
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|error| format!("timestamp must be RFC 3339: {error}"))
}

fn deserialize_utc_microsecond_timestamp<'de, D>(
    deserializer: D,
) -> Result<OffsetDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_utc_microsecond_timestamp(&value).map_err(serde::de::Error::custom)
}

fn is_utc_microsecond_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    let fixed_digits = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    let fixed_shape = bytes.len() >= 20
        && fixed_digits
            .into_iter()
            .all(|index| bytes[index].is_ascii_digit())
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':';
    if !fixed_shape {
        return false;
    }
    if bytes.len() == 20 {
        return bytes[19] == b'Z';
    }
    (22..=27).contains(&bytes.len())
        && bytes[19] == b'.'
        && bytes[bytes.len() - 1] == b'Z'
        && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PrepareEffectTransition {
    pub effect_key: EffectKey,
    pub kind: EffectKind,
    pub recovery_mode: EffectRecoveryMode,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CompleteEffectTransition {
    pub effect_id: EffectId,
    pub receipt: EffectReceipt,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectTransition {
    Prepare(PrepareEffectTransition),
    Complete(CompleteEffectTransition),
}

#[derive(Clone, Debug)]
pub struct NewPreparedEffect {
    pub effect_id: EffectId,
    pub effect_key: EffectKey,
    pub kind: EffectKind,
    pub recovery_mode: EffectRecoveryMode,
}

#[derive(Clone, Debug)]
pub enum NewEffectTransition {
    Prepare(NewPreparedEffect),
    Complete(CompleteEffectTransition),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Prepared,
    Completed,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CheckpointEffect {
    pub effect_id: EffectId,
    pub effect_key: EffectKey,
    pub kind: EffectKind,
    pub recovery_mode: EffectRecoveryMode,
    pub status: EffectStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub prepared_at: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    pub receipt: Option<EffectReceipt>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CheckpointSnapshot {
    pub state: Value,
    pub effects: Vec<CheckpointEffect>,
}

#[derive(Clone, Debug)]
pub struct SaveCheckpoint {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub agent_id: AgentId,
    pub thread_id: ThreadId,
    pub case_id: CaseId,
    pub parent_revision_id: Option<CheckpointRevisionId>,
    pub state: Value,
    pub state_schema_version: u32,
    pub effect_transitions: Vec<EffectTransition>,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointPrecondition {
    Create,
    Match(CheckpointRevisionId),
}

#[derive(Clone, Debug)]
pub struct NewCheckpointRevision {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub agent_id: AgentId,
    pub thread_id: ThreadId,
    pub case_id: CaseId,
    pub checkpoint_id: CheckpointId,
    pub revision_id: CheckpointRevisionId,
    pub parent_revision_id: Option<CheckpointRevisionId>,
    pub precondition: CheckpointPrecondition,
    pub state: Value,
    pub state_schema_version: u32,
    pub effect_transitions: Vec<NewEffectTransition>,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub writer_principal_id: PrincipalId,
    pub schema_version: u32,
    pub state_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CheckpointView {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub agent_id: AgentId,
    pub thread_id: ThreadId,
    pub case_id: CaseId,
    pub checkpoint_id: CheckpointId,
    pub checkpoint_revision_id: CheckpointRevisionId,
    pub parent_revision_id: Option<CheckpointRevisionId>,
    pub revision_number: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    #[serde(flatten)]
    pub snapshot: CheckpointSnapshot,
    pub state_schema_version: u32,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub writer_principal_id: PrincipalId,
    pub schema_version: u32,
    pub state_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Provenance {
    pub source_type: SourceType,
    pub source_uri: Option<String>,
    pub external_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AppendEpisode {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub kind: EpisodeKind,
    pub observed_at: OffsetDateTime,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub payload: Value,
}

#[derive(Clone, Debug)]
pub struct NewEpisode {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub episode_id: EpisodeId,
    pub kind: EpisodeKind,
    pub observed_at: OffsetDateTime,
    pub writer_principal_id: PrincipalId,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub schema_version: u32,
    pub payload: Value,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Episode {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub episode_id: EpisodeId,
    pub kind: EpisodeKind,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    pub writer_principal_id: PrincipalId,
    pub provenance: Provenance,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub schema_version: u32,
    pub payload: Value,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ValidTime {
    #[serde(with = "time::serde::rfc3339")]
    pub from: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub until: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WritePolicy {
    pub id: WritePolicyId,
    pub version: WritePolicyVersion,
}

#[derive(Clone, Debug)]
pub struct CreateFact {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

#[derive(Clone, Debug)]
pub struct NewFact {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub writer_principal_id: PrincipalId,
    pub schema_version: u32,
    pub value_sha256: String,
}

#[derive(Clone, Debug)]
pub struct SupersedeFact {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub fact_id: FactId,
    pub supersedes_revision_id: RevisionId,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

/// A vault page target for a wiki write-back annotation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteBackTarget {
    /// A fact page (`pages/facts/{fact_id}.md`).
    Fact { page_id: FactId },
    /// An episode page (`pages/episodes/{episode_id}.md`).
    Episode { page_id: EpisodeId },
}

/// An attributable annotation write through the wiki write-back API
/// (spec 017 R5, AC4). The annotation becomes a fact in the wiki
/// annotation namespace with a registered write policy.
#[derive(Clone, Debug)]
pub struct WriteBackAnnotation {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub target: WriteBackTarget,
    pub body: String,
    pub observed_at: OffsetDateTime,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

/// An attributable page edit through the wiki write-back API (spec 017 R5,
/// AC4). The edit supersedes the target fact with the edited value. The
/// evidence set is preserved from the current head revision.
#[derive(Clone, Debug)]
pub struct WriteBackPageEdit {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub fact_id: FactId,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

/// A filed agent answer through the wiki write-back API (spec 017 R5, AC5).
/// The answer becomes a fact in the derived namespace: the receipt records
/// the filing agent as writer and the provenance kind derived (011 R5).
#[derive(Clone, Debug)]
pub struct FileAnswer {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub question_fact_id: FactId,
    pub answer: Value,
    pub observed_at: OffsetDateTime,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

/// A governed canonical fact creation through the wiki write-back API
/// (spec 017 R5 closure, AC11). The fact keeps the caller's key and
/// namespace. The write records the authenticated principal as writer.
#[derive(Clone, Debug)]
pub struct WriteBackCreateFact {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
}

#[derive(Clone, Debug)]
pub struct NewFactRevision {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
    pub supersedes_revision_id: RevisionId,
    pub expected_head_revision_id: RevisionId,
    pub value: Value,
    pub observed_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub writer_principal_id: PrincipalId,
    pub schema_version: u32,
    pub value_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FactRevision {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
    pub revision_number: u64,
    pub supersedes_revision_id: Option<RevisionId>,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub value: Value,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    pub valid_time: ValidTime,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub write_policy: WritePolicy,
    pub confidence: f64,
    pub sensitivity: Sensitivity,
    pub retention_policy_id: RetentionPolicyId,
    pub writer_principal_id: PrincipalId,
    pub schema_version: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FactView {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub case_id: CaseId,
    pub fact_id: FactId,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub head_revision_id: RevisionId,
    #[serde(with = "time::serde::rfc3339")]
    pub evaluated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    pub revision: Option<FactRevision>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetrievalPerspective {
    Current,
    AsOf {
        #[serde(with = "time::serde::rfc3339")]
        valid_at: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        recorded_at: OffsetDateTime,
    },
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct RetrievalFilters {
    pub case_ids: Option<Vec<CaseId>>,
    pub namespaces: Option<Vec<FactNamespace>>,
    pub keys: Option<Vec<FactKey>>,
    pub sensitivities: Option<Vec<Sensitivity>>,
}

#[derive(Clone, Debug)]
pub struct CreateRetrieval {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub query: RetrievalQuery,
    pub perspective: RetrievalPerspective,
    pub page_size: u16,
    pub policy_id: Option<RetrievalPolicyId>,
    pub filters: RetrievalFilters,
}

#[derive(Clone, Debug)]
pub struct NewRetrieval {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub retrieval_id: RetrievalId,
    pub query: RetrievalQuery,
    pub query_sha256: String,
    pub authorization_scope_sha256: String,
    pub perspective: RetrievalPerspective,
    pub page_size: u16,
    pub policy_id: RetrievalPolicyId,
    pub filters: RetrievalFilters,
    pub principal_id: PrincipalId,
    pub allowed_sensitivities: Vec<Sensitivity>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievalPolicy {
    pub id: RetrievalPolicyId,
    pub version: String,
    pub digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievalAuthorizationReceipt {
    pub decision: String,
    pub scope_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievalScore {
    pub component: String,
    pub value: String,
}

pub const SCORE_SCALE: i128 = 1_000_000_000_000;
pub const DECAY_Q63_SCALE: u128 = 1_u128 << 63;
pub const Q63_EXP2_CONSTANTS_SHA256: &str =
    "769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa";
// At most 63 * (0.5 constant + 0.5 multiplication) Q63 units, plus less than
// one unit from truncating the fractional exponent after 63 bits.
pub const Q63_EXP2_MAX_ABSOLUTE_ERROR_UNITS: u128 = 64;

// Generated by scripts/generate-q63-exp2.py with GNU MPFR 4.2.2, GMP 6.3.0,
// 256-bit working precision, and MPFR_RNDN. Entry i is
// half_even(2^(-2^(-(i + 1))) * 2^63).
const Q63_EXP2_NEGATIVE_BINARY_POWERS: [u128; 63] = [
    6521908912666391106,
    7755900482342532474,
    8457869449776733335,
    8832331321595618838,
    9025734193507008925,
    9124017994966720698,
    9173560510430823462,
    9198432556164277331,
    9210893855724328809,
    9217130834664616070,
    9220250907674776491,
    9221811340221203999,
    9222591655524303666,
    9222981837935769002,
    9223176935331786073,
    9223274485577403901,
    9223323261087119913,
    9223347648938705290,
    9223359842888679895,
    9223365939869712687,
    9223368988361740456,
    9223370512608132184,
    9223371274731422509,
    9223371655793091287,
    9223371846323931579,
    9223371941589353202,
    9223371989222064382,
    9223372013038420064,
    9223372024946597928,
    9223372030900686866,
    9223372033877731337,
    9223372035366253572,
    9223372036110514690,
    9223372036482645249,
    9223372036668710529,
    9223372036761743168,
    9223372036808259488,
    9223372036831517648,
    9223372036843146728,
    9223372036848961268,
    9223372036851868538,
    9223372036853322173,
    9223372036854048991,
    9223372036854412399,
    9223372036854594104,
    9223372036854684956,
    9223372036854730382,
    9223372036854753095,
    9223372036854764451,
    9223372036854770130,
    9223372036854772969,
    9223372036854774388,
    9223372036854775098,
    9223372036854775453,
    9223372036854775631,
    9223372036854775719,
    9223372036854775764,
    9223372036854775786,
    9223372036854775797,
    9223372036854775802,
    9223372036854775805,
    9223372036854775807,
    9223372036854775807,
];

const ACTIVE_CASE_HALF_LIFE_US: u128 = 2_592_000_000_000;
const ACTIVE_CASE_FLOOR_Q63: u128 = DECAY_Q63_SCALE / 8;
const IMPORTANCE_BASE_UNITS: i128 = SCORE_SCALE / 2;
const NAMESPACE_KEY_BONUS_UNITS: i128 = 16_393_442_623;
const KEY_ONLY_BONUS_UNITS: i128 = 8_196_721_311;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScoreUnits(i128);

impl ScoreUnits {
    pub const fn from_raw(units: i128) -> Self {
        Self(units)
    }

    pub const fn raw_units(self) -> i128 {
        self.0
    }

    pub fn from_ratio(numerator: i128, denominator: i128) -> Result<Self, ScoreMathError> {
        let scaled = numerator
            .checked_mul(SCORE_SCALE)
            .ok_or(ScoreMathError::Overflow)?;
        round_signed_ratio(scaled, denominator).map(Self)
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, ScoreMathError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(ScoreMathError::Overflow)
    }

    pub fn checked_mul_factor(self, factor: Self) -> Result<Self, ScoreMathError> {
        let product = self
            .0
            .checked_mul(factor.0)
            .ok_or(ScoreMathError::Overflow)?;
        round_signed_ratio(product, SCORE_SCALE).map(Self)
    }
}

impl std::fmt::Display for ScoreUnits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let magnitude = self.0.unsigned_abs();
        let scale = SCORE_SCALE as u128;
        let whole = magnitude / scale;
        let fractional = magnitude % scale;
        if self.0 < 0 {
            write!(formatter, "-{whole}.{fractional:012}")
        } else {
            write!(formatter, "{whole}.{fractional:012}")
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScoreMathError {
    DivisionByZero,
    Overflow,
    InvalidFactor,
    InvalidRank,
}

fn round_signed_ratio(numerator: i128, denominator: i128) -> Result<i128, ScoreMathError> {
    if denominator == 0 {
        return Err(ScoreMathError::DivisionByZero);
    }

    let negative = (numerator < 0) != (denominator < 0);
    let rounded = round_unsigned_ratio(numerator.unsigned_abs(), denominator.unsigned_abs())?;
    if negative {
        if rounded == 1_u128 << 127 {
            Ok(i128::MIN)
        } else {
            let magnitude = i128::try_from(rounded).map_err(|_| ScoreMathError::Overflow)?;
            magnitude.checked_neg().ok_or(ScoreMathError::Overflow)
        }
    } else {
        i128::try_from(rounded).map_err(|_| ScoreMathError::Overflow)
    }
}

fn round_unsigned_ratio(numerator: u128, denominator: u128) -> Result<u128, ScoreMathError> {
    if denominator == 0 {
        return Err(ScoreMathError::DivisionByZero);
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let complement = denominator - remainder;
    if remainder > complement || (remainder == complement && quotient % 2 == 1) {
        quotient.checked_add(1).ok_or(ScoreMathError::Overflow)
    } else {
        Ok(quotient)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecayQ63(u128);

impl DecayQ63 {
    pub const fn raw_units(self) -> u128 {
        self.0
    }

    pub fn to_score_units(self) -> Result<ScoreUnits, ScoreMathError> {
        let numerator = self
            .0
            .checked_mul(SCORE_SCALE as u128)
            .ok_or(ScoreMathError::Overflow)?;
        let units = round_unsigned_ratio(numerator, DECAY_Q63_SCALE)?;
        i128::try_from(units)
            .map(ScoreUnits)
            .map_err(|_| ScoreMathError::Overflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecencyProfile {
    StableV1,
    ActiveCase30dV1,
}

pub fn temporal_factor_q63(
    profile: RecencyProfile,
    valid_at_us: i128,
    recency_anchor_at_us: i128,
) -> Result<DecayQ63, ScoreMathError> {
    if profile == RecencyProfile::StableV1 || valid_at_us <= recency_anchor_at_us {
        return Ok(DecayQ63(DECAY_Q63_SCALE));
    }

    let age_us = valid_at_us
        .checked_sub(recency_anchor_at_us)
        .ok_or(ScoreMathError::Overflow)? as u128;
    let floor_age_us = ACTIVE_CASE_HALF_LIFE_US
        .checked_mul(3)
        .ok_or(ScoreMathError::Overflow)?;
    if age_us >= floor_age_us {
        return Ok(DecayQ63(ACTIVE_CASE_FLOOR_Q63));
    }

    let whole_half_lives = age_us / ACTIVE_CASE_HALF_LIFE_US;
    let mut remainder = age_us % ACTIVE_CASE_HALF_LIFE_US;
    let mut factor = DECAY_Q63_SCALE >> whole_half_lives;

    for constant in Q63_EXP2_NEGATIVE_BINARY_POWERS {
        remainder = remainder.checked_mul(2).ok_or(ScoreMathError::Overflow)?;
        if remainder >= ACTIVE_CASE_HALF_LIFE_US {
            remainder -= ACTIVE_CASE_HALF_LIFE_US;
            let product = factor
                .checked_mul(constant)
                .ok_or(ScoreMathError::Overflow)?;
            factor = round_unsigned_ratio(product, DECAY_Q63_SCALE)?;
        }
    }

    Ok(DecayQ63(factor.max(ACTIVE_CASE_FLOOR_Q63)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactIdentityTier {
    NamespaceAndKey,
    KeyOnly,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalScoreInput {
    pub exact_rank: Option<u32>,
    pub lexical_rank: Option<u32>,
    pub vector_rank: Option<u32>,
    pub recency_profile: RecencyProfile,
    pub valid_at_us: i128,
    pub recency_anchor_at_us: i128,
    pub confidence_factor: ScoreUnits,
    pub importance: ScoreUnits,
    pub exact_identity: ExactIdentityTier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalScoreBreakdown {
    pub exact_rrf: ScoreUnits,
    pub lexical_rrf: ScoreUnits,
    pub vector_rrf: ScoreUnits,
    pub fused_score: ScoreUnits,
    pub recency_factor: ScoreUnits,
    pub temporal_adjustment: ScoreUnits,
    pub confidence_factor: ScoreUnits,
    pub confidence_adjustment: ScoreUnits,
    pub importance_factor: ScoreUnits,
    pub importance_adjustment: ScoreUnits,
    pub exact_identity_bonus: ScoreUnits,
    pub final_score: ScoreUnits,
}

pub fn score_temporal_retrieval(
    input: TemporalScoreInput,
) -> Result<TemporalScoreBreakdown, ScoreMathError> {
    validate_factor(input.confidence_factor, SCORE_SCALE)?;
    validate_factor(input.importance, SCORE_SCALE)?;

    let exact_rrf = rrf_score(input.exact_rank)?;
    let lexical_rrf = rrf_score(input.lexical_rank)?;
    let vector_rrf = rrf_score(input.vector_rank)?;
    let fused_score = exact_rrf
        .checked_add(lexical_rrf)?
        .checked_add(vector_rrf)?;
    let recency_factor = temporal_factor_q63(
        input.recency_profile,
        input.valid_at_us,
        input.recency_anchor_at_us,
    )?
    .to_score_units()?;

    let after_temporal = fused_score.checked_mul_factor(recency_factor)?;
    let temporal_adjustment = checked_difference(after_temporal, fused_score)?;
    let after_confidence = after_temporal.checked_mul_factor(input.confidence_factor)?;
    let confidence_adjustment = checked_difference(after_confidence, after_temporal)?;
    let importance_factor = ScoreUnits(IMPORTANCE_BASE_UNITS).checked_add(input.importance)?;
    let after_importance = after_confidence.checked_mul_factor(importance_factor)?;
    let importance_adjustment = checked_difference(after_importance, after_confidence)?;
    let exact_identity_bonus = ScoreUnits(match input.exact_identity {
        ExactIdentityTier::NamespaceAndKey => NAMESPACE_KEY_BONUS_UNITS,
        ExactIdentityTier::KeyOnly => KEY_ONLY_BONUS_UNITS,
        ExactIdentityTier::None => 0,
    });
    let final_score = after_importance.checked_add(exact_identity_bonus)?;

    Ok(TemporalScoreBreakdown {
        exact_rrf,
        lexical_rrf,
        vector_rrf,
        fused_score,
        recency_factor,
        temporal_adjustment,
        confidence_factor: input.confidence_factor,
        confidence_adjustment,
        importance_factor,
        importance_adjustment,
        exact_identity_bonus,
        final_score,
    })
}

fn validate_factor(factor: ScoreUnits, maximum: i128) -> Result<(), ScoreMathError> {
    if (0..=maximum).contains(&factor.0) {
        Ok(())
    } else {
        Err(ScoreMathError::InvalidFactor)
    }
}

fn rrf_score(rank: Option<u32>) -> Result<ScoreUnits, ScoreMathError> {
    let Some(rank) = rank else {
        return Ok(ScoreUnits(0));
    };
    if rank == 0 {
        return Err(ScoreMathError::InvalidRank);
    }
    let denominator = 60_i128
        .checked_add(i128::from(rank))
        .ok_or(ScoreMathError::Overflow)?;
    ScoreUnits::from_ratio(1, denominator)
}

fn checked_difference(left: ScoreUnits, right: ScoreUnits) -> Result<ScoreUnits, ScoreMathError> {
    left.0
        .checked_sub(right.0)
        .map(ScoreUnits)
        .ok_or(ScoreMathError::Overflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalOrderKey {
    pub exact_identity_rank: Option<u32>,
    pub final_score: ScoreUnits,
    pub exact_rank: Option<u32>,
    pub lexical_rank: Option<u32>,
    pub vector_rank: Option<u32>,
    pub case_id: CaseId,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
}

impl Ord for TemporalOrderKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_optional_rank(self.exact_identity_rank, other.exact_identity_rank)
            .then_with(|| other.final_score.0.cmp(&self.final_score.0))
            .then_with(|| compare_optional_rank(self.exact_rank, other.exact_rank))
            .then_with(|| compare_optional_rank(self.lexical_rank, other.lexical_rank))
            .then_with(|| compare_optional_rank(self.vector_rank, other.vector_rank))
            .then_with(|| self.case_id.0.cmp(&other.case_id.0))
            .then_with(|| self.fact_id.0.cmp(&other.fact_id.0))
            .then_with(|| self.revision_id.0.cmp(&other.revision_id.0))
    }
}

impl PartialOrd for TemporalOrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn compare_optional_rank(left: Option<u32>, right: Option<u32>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetrievalEmbeddingLineage {
    pub profile_id: String,
    pub profile_version: String,
    pub profile_digest: String,
    pub projection_sha256: String,
    pub input_sha256: String,
    pub vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetrievalQueryEmbeddingLineage {
    pub profile_id: String,
    pub profile_version: String,
    pub profile_digest: String,
    pub projection_profile_id: String,
    pub projection_profile_version: String,
    pub projection_profile_digest: String,
    pub input_sha256: String,
    pub vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievalItem {
    pub memory_kind: String,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
    pub namespace: FactNamespace,
    pub key: FactKey,
    pub value: Value,
    pub evidence_episode_ids: Vec<EpisodeId>,
    pub scores: Vec<RetrievalScore>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<RetrievalEmbeddingLineage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RetrievalReceipt {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub retrieval_id: RetrievalId,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub evaluated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
    pub policy: RetrievalPolicy,
    pub authorization: RetrievalAuthorizationReceipt,
    pub document_schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_embedding: Option<RetrievalQueryEmbeddingLineage>,
    pub items: Vec<RetrievalItem>,
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod temporal_score_tests {
    use super::*;

    const DAY_US: i128 = 86_400_000_000;

    fn sha256_hex(input: &[u8]) -> String {
        const ROUND_CONSTANTS: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];

        let bit_length = u64::try_from(input.len())
            .expect("the Q63 canonical payload length fits u64")
            .checked_mul(8)
            .expect("the Q63 canonical payload bit length fits u64");
        let mut message = input.to_vec();
        message.push(0x80);
        while message.len() % 64 != 56 {
            message.push(0);
        }
        message.extend_from_slice(&bit_length.to_be_bytes());

        let mut state = [
            0x6a09e667_u32,
            0xbb67ae85,
            0x3c6ef372,
            0xa54ff53a,
            0x510e527f,
            0x9b05688c,
            0x1f83d9ab,
            0x5be0cd19,
        ];
        for chunk in message.chunks_exact(64) {
            let mut schedule = [0_u32; 64];
            for (index, word) in chunk.chunks_exact(4).enumerate() {
                schedule[index] = u32::from_be_bytes(
                    word.try_into()
                        .expect("a SHA-256 message word contains four bytes"),
                );
            }
            for index in 16..64 {
                let sigma0 = schedule[index - 15].rotate_right(7)
                    ^ schedule[index - 15].rotate_right(18)
                    ^ (schedule[index - 15] >> 3);
                let sigma1 = schedule[index - 2].rotate_right(17)
                    ^ schedule[index - 2].rotate_right(19)
                    ^ (schedule[index - 2] >> 10);
                schedule[index] = schedule[index - 16]
                    .wrapping_add(sigma0)
                    .wrapping_add(schedule[index - 7])
                    .wrapping_add(sigma1);
            }

            let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
            for index in 0..64 {
                let sum1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let choice = (e & f) ^ (!e & g);
                let temporary1 = h
                    .wrapping_add(sum1)
                    .wrapping_add(choice)
                    .wrapping_add(ROUND_CONSTANTS[index])
                    .wrapping_add(schedule[index]);
                let sum0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let majority = (a & b) ^ (a & c) ^ (b & c);
                let temporary2 = sum0.wrapping_add(majority);
                h = g;
                g = f;
                f = e;
                e = d.wrapping_add(temporary1);
                d = c;
                c = b;
                b = a;
                a = temporary1.wrapping_add(temporary2);
            }
            for (word, compressed) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
                *word = word.wrapping_add(compressed);
            }
        }

        state.iter().map(|word| format!("{word:08x}")).collect()
    }

    #[test]
    fn q63_generator_digest_and_error_bound_are_frozen() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "the dependency-free SHA-256 test oracle is invalid"
        );
        assert_eq!(Q63_EXP2_NEGATIVE_BINARY_POWERS.len(), 63);
        assert_eq!(
            Q63_EXP2_NEGATIVE_BINARY_POWERS[0],
            6_521_908_912_666_391_106
        );
        assert_eq!(
            Q63_EXP2_NEGATIVE_BINARY_POWERS[62],
            9_223_372_036_854_775_807
        );
        assert_eq!(
            Q63_EXP2_CONSTANTS_SHA256,
            "769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa"
        );
        assert_eq!(Q63_EXP2_MAX_ABSOLUTE_ERROR_UNITS, 64);
        let canonical_payload = format!(
            "q63-exp2-v1\n{}",
            Q63_EXP2_NEGATIVE_BINARY_POWERS
                .iter()
                .enumerate()
                .map(|(index, value)| format!("{}={value}\n", index + 1))
                .collect::<String>()
        );
        assert_eq!(
            sha256_hex(canonical_payload.as_bytes()),
            Q63_EXP2_CONSTANTS_SHA256,
            "the full committed Q63 table must match the generator digest"
        );
        assert!(
            Q63_EXP2_MAX_ABSOLUTE_ERROR_UNITS * 2 * (SCORE_SCALE as u128) < DECAY_Q63_SCALE,
            "Q63 approximation bound must stay below half one public score unit"
        );
    }

    #[test]
    fn score_units_half_even_exact_rational_values() {
        let midpoint_denominator = 2 * SCORE_SCALE * SCORE_SCALE;
        let cases = [
            (1, 61, 16_393_442_623),
            (1, 122, 8_196_721_311),
            (5 * SCORE_SCALE - 1, midpoint_denominator, 2),
            (5 * SCORE_SCALE, midpoint_denominator, 2),
            (5 * SCORE_SCALE + 1, midpoint_denominator, 3),
            (7 * SCORE_SCALE - 1, midpoint_denominator, 3),
            (7 * SCORE_SCALE, midpoint_denominator, 4),
            (7 * SCORE_SCALE + 1, midpoint_denominator, 4),
            (-5 * SCORE_SCALE - 1, midpoint_denominator, -3),
            (-5 * SCORE_SCALE, midpoint_denominator, -2),
            (-5 * SCORE_SCALE + 1, midpoint_denominator, -2),
            (-7 * SCORE_SCALE - 1, midpoint_denominator, -4),
            (-7 * SCORE_SCALE, midpoint_denominator, -4),
            (-7 * SCORE_SCALE + 1, midpoint_denominator, -3),
        ];

        for (numerator, denominator, expected_units) in cases {
            assert_eq!(
                ScoreUnits::from_ratio(numerator, denominator),
                Ok(ScoreUnits::from_raw(expected_units)),
                "unexpected half-even result for {numerator}/{denominator}"
            );
        }
        assert_eq!(
            ScoreUnits::from_ratio(1, 0),
            Err(ScoreMathError::DivisionByZero)
        );
    }

    #[test]
    fn half_even_differs_from_postgres_half_away_at_even_midpoints() {
        let midpoint_denominator = 2 * SCORE_SCALE * SCORE_SCALE;
        for (numerator, expected_half_even, postgres_half_away) in
            [(5 * SCORE_SCALE, 2, 3), (-5 * SCORE_SCALE, -2, -3)]
        {
            let actual = ScoreUnits::from_ratio(numerator, midpoint_denominator)
                .expect("the locked midpoint ratio should fit score units")
                .raw_units();
            assert_eq!(actual, expected_half_even);
            assert_ne!(actual, postgres_half_away);
        }
    }

    #[test]
    fn score_units_are_canonical_and_overflow_is_never_silent() {
        let canonical = [
            (0, "0.000000000000"),
            (1, "0.000000000001"),
            (-1, "-0.000000000001"),
            (1_234_567_890_123, "1.234567890123"),
            (-1_234_567_890_123, "-1.234567890123"),
        ];

        for (units, expected) in canonical {
            assert_eq!(ScoreUnits::from_raw(units).to_string(), expected);
        }
        assert_eq!(
            ScoreUnits::from_raw(i128::MAX).checked_add(ScoreUnits::from_raw(1)),
            Err(ScoreMathError::Overflow)
        );
        assert_eq!(
            ScoreUnits::from_ratio(i128::MAX, 1),
            Err(ScoreMathError::Overflow)
        );
    }

    #[test]
    fn q63_recency_profiles_lock_all_temporal_boundaries() {
        let active_cases = [
            (-1, 0, DECAY_Q63_SCALE, "1.000000000000"),
            (0, 0, DECAY_Q63_SCALE, "1.000000000000"),
            (1, 0, 9_223_372_036_852_309_314, "1.000000000000"),
            (15 * DAY_US, 0, 6_521_908_912_666_391_106, "0.707106781187"),
            (30 * DAY_US, 0, 4_611_686_018_427_387_904, "0.500000000000"),
            (60 * DAY_US, 0, 2_305_843_009_213_693_952, "0.250000000000"),
            (90 * DAY_US, 0, 1_152_921_504_606_846_976, "0.125000000000"),
        ];

        for (valid_at_us, anchor_at_us, expected_q63, expected_decimal) in active_cases {
            let factor =
                temporal_factor_q63(RecencyProfile::ActiveCase30dV1, valid_at_us, anchor_at_us)
                    .expect("locked active recency profile should evaluate");
            assert_eq!(factor.raw_units(), expected_q63);
            assert_eq!(
                factor
                    .to_score_units()
                    .expect("Q63 factor should fit score units")
                    .to_string(),
                expected_decimal
            );
        }

        let stable = temporal_factor_q63(RecencyProfile::StableV1, 90 * DAY_US, 0)
            .expect("stable profile should evaluate");
        assert_eq!(stable.raw_units(), DECAY_Q63_SCALE);
        assert_eq!(
            stable
                .to_score_units()
                .expect("stable factor should fit score units")
                .to_string(),
            "1.000000000000"
        );
    }

    #[test]
    fn active_recency_floor_is_exactly_clamped_at_ninety_elapsed_days() {
        let floor_age_us = 90 * DAY_US;
        let cases = [
            (floor_age_us - 1, 1_152_921_504_607_155_285_u128),
            (floor_age_us, ACTIVE_CASE_FLOOR_Q63),
            (floor_age_us + 1, ACTIVE_CASE_FLOOR_Q63),
        ];

        for (age_us, expected_q63) in cases {
            let factor = temporal_factor_q63(RecencyProfile::ActiveCase30dV1, age_us, 0)
                .expect("the locked 90-day boundary should evaluate");
            assert_eq!(
                factor.raw_units(),
                expected_q63,
                "unexpected factor at {age_us}us"
            );
        }
        let mpfr_before_floor_q63 = 1_152_921_504_607_155_288_u128;
        assert!(
            mpfr_before_floor_q63.abs_diff(cases[0].1) <= Q63_EXP2_MAX_ABSOLUTE_ERROR_UNITS,
            "the committed pre-floor approximation exceeded its MPFR error bound"
        );
    }

    #[test]
    fn recency_handles_the_full_finite_postgres_timestamp_span() {
        // PostgreSQL stores finite timestamptz values in this half-open internal
        // microsecond range relative to 2000-01-01. END_TIMESTAMP itself is not
        // a valid timestamp, so the latest supported instant is one unit below it.
        const POSTGRES_MIN_TIMESTAMP_US: i128 = -211_813_488_000_000_000;
        const POSTGRES_END_TIMESTAMP_US: i128 = 9_223_371_331_200_000_000;
        const MAX_POSTGRES_TIMESTAMP_US: i128 = POSTGRES_END_TIMESTAMP_US - 1;
        const MAX_POSTGRES_AGE_US: i128 = 9_435_184_819_199_999_999;

        assert_eq!(
            MAX_POSTGRES_TIMESTAMP_US - POSTGRES_MIN_TIMESTAMP_US,
            MAX_POSTGRES_AGE_US
        );
        let maximum_age = temporal_factor_q63(
            RecencyProfile::ActiveCase30dV1,
            MAX_POSTGRES_TIMESTAMP_US,
            POSTGRES_MIN_TIMESTAMP_US,
        )
        .expect("the maximum PostgreSQL timestamp age should evaluate");
        assert_eq!(maximum_age.raw_units(), ACTIVE_CASE_FLOOR_Q63);

        let maximum_negative_age = temporal_factor_q63(
            RecencyProfile::ActiveCase30dV1,
            POSTGRES_MIN_TIMESTAMP_US,
            MAX_POSTGRES_TIMESTAMP_US,
        )
        .expect("the maximum negative PostgreSQL age should clamp");
        assert_eq!(maximum_negative_age.raw_units(), DECAY_Q63_SCALE);

        let stable_maximum_age = temporal_factor_q63(
            RecencyProfile::StableV1,
            MAX_POSTGRES_TIMESTAMP_US,
            POSTGRES_MIN_TIMESTAMP_US,
        )
        .expect("the stable profile should cover the PostgreSQL timestamp span");
        assert_eq!(stable_maximum_age.raw_units(), DECAY_Q63_SCALE);
    }

    #[test]
    fn temporal_scoring_rounds_each_named_boundary_in_locked_order() {
        let score = score_temporal_retrieval(TemporalScoreInput {
            exact_rank: Some(1),
            lexical_rank: Some(2),
            vector_rank: Some(3),
            recency_profile: RecencyProfile::ActiveCase30dV1,
            valid_at_us: 30 * DAY_US,
            recency_anchor_at_us: 0,
            confidence_factor: ScoreUnits::from_raw(800_000_000_000),
            importance: ScoreUnits::from_raw(750_000_000_000),
            exact_identity: ExactIdentityTier::NamespaceAndKey,
        })
        .expect("locked temporal score should fit checked units");

        assert_eq!(score.exact_rrf.to_string(), "0.016393442623");
        assert_eq!(score.lexical_rrf.to_string(), "0.016129032258");
        assert_eq!(score.vector_rrf.to_string(), "0.015873015873");
        assert_eq!(score.fused_score.to_string(), "0.048395490754");
        assert_eq!(score.recency_factor.to_string(), "0.500000000000");
        assert_eq!(score.temporal_adjustment.to_string(), "-0.024197745377");
        assert_eq!(score.confidence_factor.to_string(), "0.800000000000");
        assert_eq!(score.confidence_adjustment.to_string(), "-0.004839549075");
        assert_eq!(score.importance_factor.to_string(), "1.250000000000");
        assert_eq!(score.importance_adjustment.to_string(), "0.004839549076");
        assert_eq!(score.exact_identity_bonus.to_string(), "0.016393442623");
        assert_eq!(score.final_score.to_string(), "0.040591188001");

        let recomposed_final = score
            .fused_score
            .checked_add(score.temporal_adjustment)
            .and_then(|value| value.checked_add(score.confidence_adjustment))
            .and_then(|value| value.checked_add(score.importance_adjustment))
            .and_then(|value| value.checked_add(score.exact_identity_bonus))
            .expect("named additive components should fit checked score units");
        assert_eq!(recomposed_final, score.final_score);
    }

    #[test]
    fn temporal_order_key_uses_every_locked_tie_break_in_sequence() {
        let base = order_key(
            Some(2),
            500_000_000_000,
            (Some(3), Some(4), Some(5)),
            (20, 20, 20),
        );

        let precedence_pairs = [
            (
                order_key(Some(2), 1, (None, None, None), (99, 99, 99)),
                order_key(
                    None,
                    900_000_000_000,
                    (Some(1), Some(1), Some(1)),
                    (1, 1, 1),
                ),
            ),
            (
                order_key(Some(1), 1, (None, None, None), (99, 99, 99)),
                base,
            ),
            (
                order_key(Some(2), 600_000_000_000, (None, None, None), (99, 99, 99)),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(2), None, None),
                    (99, 99, 99),
                ),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(3), Some(3), None),
                    (99, 99, 99),
                ),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(3), Some(4), Some(4)),
                    (99, 99, 99),
                ),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(3), Some(4), Some(5)),
                    (19, 99, 99),
                ),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(3), Some(4), Some(5)),
                    (20, 19, 99),
                ),
                base,
            ),
            (
                order_key(
                    Some(2),
                    500_000_000_000,
                    (Some(3), Some(4), Some(5)),
                    (20, 20, 19),
                ),
                base,
            ),
        ];

        for (earlier, later) in precedence_pairs {
            assert!(earlier < later, "locked ordering criterion was not applied");
        }
    }

    fn order_key(
        exact_identity_rank: Option<u32>,
        final_score_units: i128,
        channel_ranks: (Option<u32>, Option<u32>, Option<u32>),
        ids: (u128, u128, u128),
    ) -> TemporalOrderKey {
        TemporalOrderKey {
            exact_identity_rank,
            final_score: ScoreUnits::from_raw(final_score_units),
            exact_rank: channel_ranks.0,
            lexical_rank: channel_ranks.1,
            vector_rank: channel_ranks.2,
            case_id: CaseId(Uuid::from_u128(ids.0)),
            fact_id: FactId(Uuid::from_u128(ids.1)),
            revision_id: RevisionId(Uuid::from_u128(ids.2)),
        }
    }
}

// ---------------------------------------------------------------------------
// Optional hot cache contract (spec 015).
// ---------------------------------------------------------------------------

/// Kind of cache entry. Mirrors the spec 015 key schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HotCacheKind {
    Checkpoint,
    Lock,
    Receipt,
}

/// Optional hot cache for checkpoints, locks, and recent retrieval receipts.
///
/// The cache is never a source of truth. A miss must always fall back to the
/// canonical path. Implementations must be safe to lose: eviction, restart,
/// or a total wipe must leave retrieval correct.
#[async_trait::async_trait]
pub trait HotCache: Send + Sync {
    /// Read a value for the tenant, kind, and scope.
    /// Returns None on a miss.
    async fn get(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) -> Option<Vec<u8>>;

    /// Write a value with a TTL in seconds.
    /// The caller embeds any coverage-version marker in the value.
    async fn put(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        value: &[u8],
        ttl_seconds: u64,
    );

    /// Delete a value for the tenant, kind, and scope.
    async fn delete(&self, tenant: TenantId, kind: HotCacheKind, scope: &str);
}

/// The default cache implementation. Every operation is a no-op.
/// All reads are misses, so all paths fall back to the canonical store.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopHotCache;

#[async_trait::async_trait]
impl HotCache for NoopHotCache {
    async fn get(&self, _tenant: TenantId, _kind: HotCacheKind, _scope: &str) -> Option<Vec<u8>> {
        None
    }

    async fn put(
        &self,
        _tenant: TenantId,
        _kind: HotCacheKind,
        _scope: &str,
        _value: &[u8],
        _ttl_seconds: u64,
    ) {
    }

    async fn delete(&self, _tenant: TenantId, _kind: HotCacheKind, _scope: &str) {}
}

#[cfg(test)]
mod hot_cache_tests {
    use super::*;

    #[tokio::test]
    async fn noop_cache_is_always_a_miss() {
        let cache = NoopHotCache;
        let tenant = TenantId::from(Uuid::from_u128(1));
        assert!(
            cache
                .get(tenant, HotCacheKind::Checkpoint, "scope")
                .await
                .is_none()
        );
        assert!(
            cache
                .get(tenant, HotCacheKind::Lock, "scope")
                .await
                .is_none()
        );
        assert!(
            cache
                .get(tenant, HotCacheKind::Receipt, "scope")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn noop_cache_accepts_writes_and_deletes() {
        let cache = NoopHotCache;
        let tenant = TenantId::from(Uuid::from_u128(2));
        cache
            .put(tenant, HotCacheKind::Checkpoint, "scope", b"value", 60)
            .await;
        cache
            .delete(tenant, HotCacheKind::Checkpoint, "scope")
            .await;
        assert!(
            cache
                .get(tenant, HotCacheKind::Checkpoint, "scope")
                .await
                .is_none()
        );
    }

    #[test]
    fn hot_cache_kinds_are_distinct() {
        assert_ne!(HotCacheKind::Checkpoint, HotCacheKind::Lock);
        assert_ne!(HotCacheKind::Lock, HotCacheKind::Receipt);
    }
}

/// Versioned cache wrapper (spec 015 R7).
///
/// Values are stored as an 8-byte little-endian coverage version followed by
/// the payload. A get for a version that differs from the stored version is a
/// miss. A cache entry written under an older coverage marker therefore fails
/// validation and is lazily refreshed from the canonical path.
#[derive(Clone, Debug)]
pub struct VersionedHotCache<C> {
    inner: C,
}

impl<C: HotCache> VersionedHotCache<C> {
    pub fn new(inner: C) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    pub async fn get(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        coverage_version: u64,
    ) -> Option<Vec<u8>> {
        let raw = self.inner.get(tenant, kind, scope).await?;
        if raw.len() < 8 {
            return None;
        }
        let stored = u64::from_le_bytes(raw[..8].try_into().ok()?);
        if stored != coverage_version {
            return None;
        }
        Some(raw[8..].to_vec())
    }

    pub async fn put(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        coverage_version: u64,
        payload: &[u8],
        ttl_seconds: u64,
    ) {
        let mut raw = Vec::with_capacity(8 + payload.len());
        raw.extend_from_slice(&coverage_version.to_le_bytes());
        raw.extend_from_slice(payload);
        self.inner.put(tenant, kind, scope, &raw, ttl_seconds).await;
    }
}
