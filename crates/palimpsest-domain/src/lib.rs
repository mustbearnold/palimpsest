use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
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
uuid_id!(EpisodeId);
uuid_id!(FactId);
uuid_id!(RevisionId);

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PrincipalId(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalScope {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub subject_ids: Vec<SubjectId>,
}

impl PrincipalScope {
    pub fn authorizes(&self, tenant_id: TenantId, subject_id: SubjectId) -> bool {
        self.tenant_id == tenant_id && self.subject_ids.contains(&subject_id)
    }
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
