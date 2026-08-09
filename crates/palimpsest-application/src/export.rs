use std::{
    collections::BTreeMap,
    io::{self, Write},
};

use crate::{IdempotencyRequest, RepositoryError};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use palimpsest_domain::{ExportId, PrincipalId, SubjectId, TenantId};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;
use uuid::Uuid;

pub const CANONICAL_HISTORY_EXPORT_PROFILE: &str = "palimpsest-canonical-history-v1";
pub const WIKI_VAULT_EXPORT_PROFILE: &str = "palimpsest-wiki-vault-v1";
pub const EXPORT_RETENTION_HOURS: i64 = 24;
const MAX_EXPORT_RECORDS: usize = 100_000;
const MAX_EXPORT_PACKAGE_BYTES: usize = 256 * 1024 * 1024;

/// Builds an export package for one profile.
pub type ExportPackageBuilder = fn(
    Vec<ExportRecord>,
    ExportProcessingContext,
) -> Result<Box<dyn ExportPackage>, ExportPackageError>;

/// One registered export profile: its name and the package builder behind it.
pub struct ExportProfileDef {
    pub name: &'static str,
    pub build: ExportPackageBuilder,
}

fn canonical_history_build(
    records: Vec<ExportRecord>,
    context: ExportProcessingContext,
) -> Result<Box<dyn ExportPackage>, ExportPackageError> {
    Ok(Box::new(CanonicalHistoryPackage::build(records, context)?))
}

fn wiki_vault_build(
    records: Vec<ExportRecord>,
    context: ExportProcessingContext,
) -> Result<Box<dyn ExportPackage>, ExportPackageError> {
    Ok(Box::new(WikiVaultPackage::build(records, context)?))
}

/// The registered export profiles, in stable order.
pub static EXPORT_PROFILE_REGISTRY: [ExportProfileDef; 2] = [
    ExportProfileDef {
        name: CANONICAL_HISTORY_EXPORT_PROFILE,
        build: canonical_history_build,
    },
    ExportProfileDef {
        name: WIKI_VAULT_EXPORT_PROFILE,
        build: wiki_vault_build,
    },
];

/// Returns the registered profile definition for `name`, if any.
pub fn export_profile(name: &str) -> Option<&'static ExportProfileDef> {
    EXPORT_PROFILE_REGISTRY.iter().find(|def| def.name == name)
}

/// Returns true when the profile is a registered export profile.
pub fn is_supported_export_profile(profile: &str) -> bool {
    export_profile(profile).is_some()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportOperationState {
    Queued,
    Materializing,
    Ready,
    Failed,
    Revoked,
    Expired,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ExportOperationView {
    pub export_id: ExportId,
    pub profile: String,
    pub state: ExportOperationState,
    pub status_version: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub content_sha256: Option<String>,
    pub size_bytes: Option<u64>,
    pub record_count: Option<u64>,
    pub failure_code: Option<String>,
    #[serde(skip)]
    pub tenant_id: TenantId,
    #[serde(skip)]
    pub subject_id: SubjectId,
    #[serde(skip)]
    pub principal_id: PrincipalId,
    #[serde(skip)]
    pub allowed_sensitivities: Vec<String>,
    #[serde(skip)]
    pub authorization_scope_sha256: String,
    #[serde(skip)]
    pub worker_lease_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct NewExport {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub export_id: ExportId,
    pub principal_id: PrincipalId,
    pub profile: String,
    pub idempotency: IdempotencyRequest,
    pub authorization_scope_sha256: String,
    pub allowed_sensitivities: Vec<String>,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct ExportCreateOutcome {
    pub operation: ExportOperationView,
    pub replayed: bool,
}

#[derive(Clone, Debug)]
pub struct ExportMaterialization {
    pub operation: ExportOperationView,
    pub records: Vec<ExportRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportPackageMetadata {
    pub content_sha256: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

#[async_trait]
pub trait ExportRepository: Send + Sync {
    async fn create_export(
        &self,
        request: NewExport,
    ) -> Result<ExportCreateOutcome, RepositoryError>;

    async fn get_export(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<ExportOperationView, RepositoryError>;

    async fn list_export_ids_for_subject(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
    ) -> Result<Vec<ExportId>, RepositoryError>;

    async fn claim_export_for_materialization(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
    ) -> Result<Option<ExportMaterialization>, RepositoryError>;

    async fn claim_next_export_for_materialization(
        &self,
        worker_lease_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ExportMaterialization>, RepositoryError>;

    async fn claim_next_expired_export_for_cleanup(
        &self,
        worker_lease_id: Uuid,
        lease_seconds: u32,
    ) -> Result<Option<ExportOperationView>, RepositoryError>;

    async fn mark_export_cleanup_complete(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: Uuid,
    ) -> Result<(), RepositoryError>;

    async fn mark_export_ready(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: Uuid,
        metadata: ExportPackageMetadata,
    ) -> Result<(), RepositoryError>;

    async fn mark_export_failed(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        export_id: ExportId,
        worker_lease_id: Uuid,
        failure_code: &str,
    ) -> Result<(), RepositoryError>;
}

pub fn canonical_uri(url: &Url) -> String {
    let path = url.path();
    if path.is_empty() {
        "/".to_owned()
    } else {
        path.to_owned()
    }
}

pub fn host_header(url: &Url) -> String {
    let mut host = url.host_str().unwrap_or_default().to_owned();
    if let Some(port) = url.port() {
        host.push(':');
        host.push_str(&port.to_string());
    }
    host
}

pub fn canonical_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn aws_timestamp(timestamp: OffsetDateTime) -> String {
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        timestamp.year(),
        timestamp.month() as u8,
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
}

pub fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts every key length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

#[derive(Debug, Error)]
pub enum ExportStoreError {
    #[error("export package is not available")]
    NotFound,
    #[error("export package store rejected the operation")]
    Conflict,
    #[error("export package store is unavailable")]
    Unavailable,
}

/// A materialized export package, independent of its profile.
pub trait ExportPackage: Send + Sync {
    fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError>;

    fn record_count(&self) -> usize;
}

#[async_trait]
pub trait ExportPackageStore: Send + Sync {
    async fn stage(
        &self,
        export_id: ExportId,
        package: Box<dyn ExportPackage>,
    ) -> Result<ExportPackageMetadata, ExportStoreError>;

    async fn publish(&self, export_id: ExportId) -> Result<(), ExportStoreError>;

    async fn read(&self, export_id: ExportId) -> Result<Vec<u8>, ExportStoreError>;

    async fn discard_staging(&self, export_id: ExportId) -> Result<(), ExportStoreError>;

    async fn discard_published(&self, export_id: ExportId) -> Result<(), ExportStoreError>;

    /// Performs an independent negative probe after revocation. A successful
    /// delete is not itself evidence that a stale published object cannot be
    /// read.
    async fn probe_absent(&self, export_id: ExportId) -> Result<bool, ExportStoreError> {
        match self.read(export_id).await {
            Ok(_) => Ok(false),
            Err(ExportStoreError::NotFound) => Ok(true),
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExportRecordKind {
    Episode,
    Checkpoint,
    FactRevision,
    Procedure,
    ArtifactReference,
}

impl ExportRecordKind {
    fn file_name(self) -> &'static str {
        match self {
            Self::Episode => "records/episodes.ndjson",
            Self::Checkpoint => "records/checkpoints.ndjson",
            Self::FactRevision => "records/fact-revisions.ndjson",
            Self::Procedure => "records/procedures.ndjson",
            Self::ArtifactReference => "records/artifact-references.ndjson",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Episode => "episode",
            Self::Checkpoint => "checkpoint",
            Self::FactRevision => "fact_revision",
            Self::Procedure => "procedure",
            Self::ArtifactReference => "artifact_reference",
        }
    }

    const ALL: [Self; 5] = [
        Self::Episode,
        Self::Checkpoint,
        Self::FactRevision,
        Self::Procedure,
        Self::ArtifactReference,
    ];
}

#[derive(Clone, Debug)]
pub struct ExportRecord {
    pub kind: ExportRecordKind,
    pub id: Uuid,
    pub recorded_at: OffsetDateTime,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportProcessingContext {
    pub tenant_id: TenantId,
    pub subject_id: SubjectId,
    pub export_id: ExportId,
    pub snapshot_id: String,
    pub authorization_scope_sha256: String,
    pub generated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalHistoryPackage {
    files: Vec<(String, Vec<u8>)>,
    record_count: usize,
}

impl CanonicalHistoryPackage {
    pub fn build(
        records: Vec<ExportRecord>,
        context: ExportProcessingContext,
    ) -> Result<Self, ExportPackageError> {
        if records.len() > MAX_EXPORT_RECORDS {
            return Err(ExportPackageError::TooLarge);
        }
        let mut grouped = BTreeMap::<ExportRecordKind, Vec<ExportRecord>>::new();
        for record in records {
            grouped.entry(record.kind).or_default().push(record);
        }

        let mut record_count = 0;
        let mut record_class_counts = BTreeMap::<ExportRecordKind, usize>::new();
        let mut files = Vec::new();
        for kind in ExportRecordKind::ALL {
            let mut records = grouped.remove(&kind).unwrap_or_default();
            records.sort_by(|left, right| {
                left.recorded_at
                    .cmp(&right.recorded_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
            for pair in records.windows(2) {
                if pair[0].id == pair[1].id && pair[0].recorded_at == pair[1].recorded_at {
                    return Err(ExportPackageError::DuplicateRecord {
                        kind: kind.as_str(),
                        id: pair[0].id,
                    });
                }
            }
            record_count += records.len();
            record_class_counts.insert(kind, records.len());
            let mut content = Vec::new();
            for record in records {
                let recorded_at = record
                    .recorded_at
                    .format(&Rfc3339)
                    .map_err(|_| ExportPackageError::InvalidTimestamp)?;
                let line =
                    canonical_record(record.kind, record.id, recorded_at, record.value, &context)?;
                serde_json::to_writer(&mut content, &line)?;
                content.push(b'\n');
            }
            if content.len() > MAX_EXPORT_PACKAGE_BYTES {
                return Err(ExportPackageError::TooLarge);
            }
            files.push((kind.file_name().to_owned(), content));
        }

        let generated_at = context
            .generated_at
            .format(&Rfc3339)
            .map_err(|_| ExportPackageError::InvalidTimestamp)?;
        let processing_context_value = canonical_json(&json!({
            "profile": CANONICAL_HISTORY_EXPORT_PROFILE,
            "export_id": context.export_id,
            "scope": {
                "tenant_id": context.tenant_id,
                "subject_id": context.subject_id,
            },
            "snapshot_id": &context.snapshot_id,
            "generated_at": &generated_at,
            "authorization_scope_sha256": &context.authorization_scope_sha256,
        }))?;
        let processing_context = serde_json::to_vec(&processing_context_value)?;
        files.push(("processing-context.json".to_owned(), processing_context));
        files.insert(
            0,
            (
                "schema/palimpsest-canonical-history-v1.schema.json".to_owned(),
                schema_bytes(),
            ),
        );
        files.push((
            "README.txt".to_owned(),
            b"Palimpsest canonical history export.\n".to_vec(),
        ));

        let mut manifest_files = Vec::new();
        for (path, content) in &files {
            manifest_files.push(json!({
                "path": path,
                "size_bytes": content.len(),
                "sha256": sha256_hex(content),
            }));
        }
        let record_classes = ExportRecordKind::ALL
            .into_iter()
            .map(|kind| {
                let count = record_class_counts.get(&kind).copied().unwrap_or(0);
                let status = match kind {
                    ExportRecordKind::Episode
                    | ExportRecordKind::Checkpoint
                    | ExportRecordKind::FactRevision => {
                        if count == 0 {
                            "supported_empty"
                        } else {
                            "supported"
                        }
                    }
                    ExportRecordKind::Procedure | ExportRecordKind::ArtifactReference => {
                        "unsupported"
                    }
                };
                json!({
                    "record_kind": kind.as_str(),
                    "status": status,
                    "record_count": count,
                })
            })
            .collect::<Vec<_>>();
        let manifest_value = canonical_json(&json!({
            "format": CANONICAL_HISTORY_EXPORT_PROFILE,
            "profile": CANONICAL_HISTORY_EXPORT_PROFILE,
            "schema_version": 1,
            "export_id": context.export_id,
            "scope": {
                "tenant_id": context.tenant_id,
                "subject_id": context.subject_id,
            },
            "snapshot": {
                "snapshot_id": &context.snapshot_id,
                "generated_at": &generated_at,
            },
            "policy_versions": {
                "export": CANONICAL_HISTORY_EXPORT_PROFILE,
                "record_schema": 1,
            },
            "authorization_scope_sha256": &context.authorization_scope_sha256,
            "record_count": record_count,
            "record_classes": record_classes,
            "policy_omissions": [],
            "files": manifest_files,
        }))?;
        let manifest = serde_json::to_vec(&manifest_value)?;
        files.insert(0, ("manifest.json".to_owned(), manifest));

        Ok(Self {
            files,
            record_count,
        })
    }

    pub fn write_to<W: Write>(
        &self,
        writer: &mut W,
    ) -> Result<ExportPackageMetadata, ExportPackageError> {
        write_zip(&self.files, writer, self.record_count)
    }

    pub fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }
}

impl ExportPackage for CanonicalHistoryPackage {
    fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError> {
        CanonicalHistoryPackage::as_bytes(self)
    }

    fn record_count(&self) -> usize {
        self.record_count
    }
}

/// A derived markdown projection of the canonical semantic layer.
///
/// The vault renders one page per episode and one page per fact. The
/// pages derive from canonical records; they are not a dump of the
/// record envelopes. The renderer is a pure function of its records, so
/// the same canonical state rebuilds the same pages byte for byte.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WikiVaultPackage {
    files: Vec<(String, Vec<u8>)>,
    record_count: usize,
}

impl WikiVaultPackage {
    pub fn build(
        records: Vec<ExportRecord>,
        context: ExportProcessingContext,
    ) -> Result<Self, ExportPackageError> {
        if records.len() > MAX_EXPORT_RECORDS {
            return Err(ExportPackageError::TooLarge);
        }
        let mut episodes = Vec::new();
        let mut fact_revisions = Vec::new();
        for record in records {
            match record.kind {
                ExportRecordKind::Episode => episodes.push(record),
                ExportRecordKind::FactRevision => fact_revisions.push(record),
                // Checkpoints, procedures, and artifact references render
                // in a later phase (spec 017 P3/P4).
                _ => {}
            }
        }
        episodes.sort_by(|left, right| {
            left.recorded_at
                .cmp(&right.recorded_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        fact_revisions.sort_by(|left, right| {
            left.recorded_at
                .cmp(&right.recorded_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        for (kind, records) in [("episode", &episodes), ("fact_revision", &fact_revisions)] {
            for pair in records.windows(2) {
                if pair[0].id == pair[1].id && pair[0].recorded_at == pair[1].recorded_at {
                    return Err(ExportPackageError::DuplicateRecord {
                        kind,
                        id: pair[0].id,
                    });
                }
            }
        }

        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        let mut page_count = 0usize;
        for episode in &episodes {
            let page = render_episode_page(episode)?;
            page_count += 1;
            files.push((format!("pages/episodes/{}.md", episode.id), page));
        }
        let mut facts: BTreeMap<Uuid, Vec<&ExportRecord>> = BTreeMap::new();
        for revision in &fact_revisions {
            if let Some(fact_id) = record_scope_uuid(revision, "fact_id") {
                facts.entry(fact_id).or_default().push(revision);
            }
        }
        for (fact_id, revisions) in &facts {
            let page = render_fact_page(*fact_id, revisions)?;
            page_count += 1;
            files.push((format!("pages/facts/{fact_id}.md"), page));
        }
        files.push((
            "README.md".to_owned(),
            vault_readme(facts.len(), episodes.len()),
        ));
        files.push((
            "schema/palimpsest-wiki-vault-v1.md".to_owned(),
            VAULT_SCHEMA_DESCRIPTION.to_vec(),
        ));

        let generated_at = context
            .generated_at
            .format(&Rfc3339)
            .map_err(|_| ExportPackageError::InvalidTimestamp)?;
        let processing_context_value = canonical_json(&json!({
            "profile": WIKI_VAULT_EXPORT_PROFILE,
            "export_id": context.export_id,
            "scope": {
                "tenant_id": context.tenant_id,
                "subject_id": context.subject_id,
            },
            "snapshot_id": &context.snapshot_id,
            "generated_at": &generated_at,
            "authorization_scope_sha256": &context.authorization_scope_sha256,
        }))?;
        files.push((
            "processing-context.json".to_owned(),
            serde_json::to_vec(&processing_context_value)?,
        ));

        let mut manifest_files = Vec::new();
        for (path, content) in &files {
            manifest_files.push(json!({
                "path": path,
                "size_bytes": content.len(),
                "sha256": sha256_hex(content),
            }));
        }
        let manifest_value = canonical_json(&json!({
            "format": WIKI_VAULT_EXPORT_PROFILE,
            "profile": WIKI_VAULT_EXPORT_PROFILE,
            "schema_version": 1,
            "export_id": context.export_id,
            "scope": {
                "tenant_id": context.tenant_id,
                "subject_id": context.subject_id,
            },
            "snapshot": {
                "snapshot_id": &context.snapshot_id,
                "generated_at": &generated_at,
            },
            "policy_versions": {
                "export": WIKI_VAULT_EXPORT_PROFILE,
                "record_schema": 1,
            },
            "authorization_scope_sha256": &context.authorization_scope_sha256,
            "record_count": page_count,
            "page_count": page_count,
            "record_classes": [
                {
                    "record_kind": "episode",
                    "status": if episodes.is_empty() { "supported_empty" } else { "supported" },
                    "record_count": episodes.len(),
                },
                {
                    "record_kind": "fact_revision",
                    "status": if fact_revisions.is_empty() { "supported_empty" } else { "supported" },
                    "record_count": fact_revisions.len(),
                },
                {"record_kind": "checkpoint", "status": "unsupported", "record_count": 0},
                {"record_kind": "procedure", "status": "unsupported", "record_count": 0},
                {"record_kind": "artifact_reference", "status": "unsupported", "record_count": 0},
            ],
            "policy_omissions": [],
            "files": manifest_files,
        }))?;
        let manifest = serde_json::to_vec(&manifest_value)?;
        files.insert(0, ("manifest.json".to_owned(), manifest));

        Ok(Self {
            files,
            record_count: page_count,
        })
    }

    pub fn write_to<W: Write>(
        &self,
        writer: &mut W,
    ) -> Result<ExportPackageMetadata, ExportPackageError> {
        write_zip(&self.files, writer, self.record_count)
    }

    pub fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }
}

impl ExportPackage for WikiVaultPackage {
    fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError> {
        WikiVaultPackage::as_bytes(self)
    }

    fn record_count(&self) -> usize {
        self.record_count
    }
}

fn record_envelope(record: &ExportRecord) -> &Map<String, Value> {
    record
        .value
        .as_object()
        .expect("export records are JSON objects")
}

fn record_scope(record: &ExportRecord) -> &Map<String, Value> {
    record_envelope(record)
        .get("scope")
        .and_then(Value::as_object)
        .expect("export records carry a scope object")
}

fn record_temporal(record: &ExportRecord) -> &Map<String, Value> {
    record_envelope(record)
        .get("temporal")
        .and_then(Value::as_object)
        .expect("export records carry a temporal object")
}

fn record_provenance(record: &ExportRecord) -> &Map<String, Value> {
    record_envelope(record)
        .get("provenance")
        .and_then(Value::as_object)
        .expect("export records carry a provenance object")
}

fn record_governance(record: &ExportRecord) -> &Map<String, Value> {
    record_envelope(record)
        .get("governance")
        .and_then(Value::as_object)
        .expect("export records carry a governance object")
}

fn record_payload(record: &ExportRecord) -> &Value {
    record_envelope(record)
        .get("payload")
        .expect("export records carry a payload")
}

fn record_scope_uuid(record: &ExportRecord, key: &str) -> Option<Uuid> {
    record_scope(record)
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn record_timestamp(record: &ExportRecord, key: &str) -> Option<String> {
    record_temporal(record)
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn frontmatter_value(value: &str) -> String {
    serde_json::to_string(value).expect("strings serialize to JSON")
}

fn markdown_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' | '`' | '#' | '*' | '_' | '[' | ']' | '(' | ')' | '<' | '>' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn pretty_payload(value: &Value) -> Result<String, ExportPackageError> {
    let canonical = canonical_json(value)?;
    Ok(serde_json::to_string_pretty(&canonical)?)
}

fn render_episode_page(record: &ExportRecord) -> Result<Vec<u8>, ExportPackageError> {
    let scope = record_scope(record);
    let temporal = record_temporal(record);
    let provenance = record_provenance(record);
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str("palimpsest_kind: episode\n");
    out.push_str(&format!("palimpsest_id: {}\n", record.id));
    if let Some(case_id) = scope.get("case_id").and_then(Value::as_str) {
        out.push_str(&format!("case_id: {case_id}\n"));
    }
    out.push_str(&format!(
        "last_touched: {}\n",
        record_timestamp(record, "recorded_at").unwrap_or_default()
    ));
    out.push_str("---\n\n");
    out.push_str(&format!(
        "# Episode: {}\n\n",
        markdown_escape(&record.id.to_string())
    ));
    if let Some(observed_at) = temporal.get("observed_at").and_then(Value::as_str) {
        out.push_str(&format!("- observed_at: {observed_at}\n"));
    }
    out.push_str(&format!(
        "- recorded_at: {}\n",
        record_timestamp(record, "recorded_at").unwrap_or_default()
    ));
    if let Some(writer) = provenance
        .get("writer_principal_id")
        .and_then(Value::as_str)
    {
        out.push_str(&format!("- writer: {}\n", markdown_escape(writer)));
    }
    if let Some(source_type) = provenance.get("source_type").and_then(Value::as_str) {
        out.push_str(&format!(
            "- source_type: {}\n",
            markdown_escape(source_type)
        ));
    }
    if let Some(source_uri) = provenance.get("source_uri").and_then(Value::as_str) {
        out.push_str(&format!("- source_uri: {}\n", markdown_escape(source_uri)));
    }
    out.push_str("\n## Payload\n\n```json\n");
    out.push_str(&pretty_payload(record_payload(record))?);
    out.push_str("\n```\n");
    Ok(out.into_bytes())
}

fn render_fact_page(
    fact_id: Uuid,
    revisions: &[&ExportRecord],
) -> Result<Vec<u8>, ExportPackageError> {
    // Revisions arrive sorted by (recorded_at, id); the head is the last.
    let head = revisions
        .last()
        .expect("a fact page has at least one revision");
    let scope = record_scope(head);
    let namespace = scope
        .get("namespace")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let key = scope
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    // A revision is superseded when a later revision names it as its
    // supersedes_id. The head is current unless its lifecycle says so.
    let mut superseded_by: BTreeMap<Uuid, Uuid> = BTreeMap::new();
    for revision in revisions.iter().skip(1) {
        if let Some(supersedes) = record_relations_supersedes(revision) {
            superseded_by.insert(supersedes, revision.id);
        }
    }
    let head_lifecycle = record_governance(head)
        .get("lifecycle_state")
        .and_then(Value::as_str)
        .unwrap_or("active");
    let status = if superseded_by.contains_key(&head.id) {
        "superseded"
    } else {
        head_lifecycle
    };

    let mut out = String::new();
    out.push_str("---\n");
    out.push_str("palimpsest_kind: fact\n");
    out.push_str(&format!("palimpsest_id: {fact_id}\n"));
    out.push_str(&format!("namespace: {}\n", frontmatter_value(&namespace)));
    out.push_str(&format!("key: {}\n", frontmatter_value(&key)));
    out.push_str(&format!("status: {status}\n"));
    out.push_str(&format!(
        "last_touched: {}\n",
        record_timestamp(head, "recorded_at").unwrap_or_default()
    ));
    out.push_str(&format!(
        "created_at: {}\n",
        record_timestamp(
            revisions.first().expect("non-empty revisions"),
            "recorded_at"
        )
        .unwrap_or_default()
    ));
    out.push_str(&format!("revisions: {}\n", revisions.len()));
    out.push_str("---\n\n");
    out.push_str(&format!(
        "# Fact: {}/{}\n\n",
        markdown_escape(&namespace),
        markdown_escape(&key)
    ));
    out.push_str(&format!(
        "- status: {status}\n- last_touched: {}\n",
        record_timestamp(head, "recorded_at").unwrap_or_default()
    ));

    out.push_str("\n## Current revision\n\n");
    out.push_str(&format!(
        "- recorded_at: {}\n",
        record_timestamp(head, "recorded_at").unwrap_or_default()
    ));
    if let Some(writer) = record_provenance(head)
        .get("writer_principal_id")
        .and_then(Value::as_str)
    {
        out.push_str(&format!("- writer: {}\n", markdown_escape(writer)));
    }
    if let Some(policy) = record_provenance(head)
        .get("write_policy_id")
        .and_then(Value::as_str)
    {
        out.push_str(&format!("- write_policy: {}\n", markdown_escape(policy)));
    }
    append_evidence(&mut out, head);
    out.push_str("\n```json\n");
    out.push_str(&pretty_payload(record_payload(head))?);
    out.push_str("\n```\n");

    if revisions.len() > 1 {
        out.push_str("\n## Revision history\n\n");
        for revision in revisions.iter().rev().skip(1) {
            out.push_str(&format!(
                "### {} (revision {})\n\n",
                record_timestamp(revision, "recorded_at").unwrap_or_default(),
                markdown_escape(&revision.id.to_string())
            ));
            if let Some(superseded_by_id) = superseded_by.get(&revision.id) {
                out.push_str(&format!(
                    "- superseded by: {}\n",
                    markdown_escape(&superseded_by_id.to_string())
                ));
            }
            if let Some(writer) = record_provenance(revision)
                .get("writer_principal_id")
                .and_then(Value::as_str)
            {
                out.push_str(&format!("- writer: {}\n", markdown_escape(writer)));
            }
            append_evidence(&mut out, revision);
            out.push_str("\n```json\n");
            out.push_str(&pretty_payload(record_payload(revision))?);
            out.push_str("\n```\n");
        }
    }
    Ok(out.into_bytes())
}

fn record_relations_supersedes(record: &ExportRecord) -> Option<Uuid> {
    record_envelope(record)
        .get("relations")
        .and_then(Value::as_object)
        .and_then(|relations| relations.get("supersedes_id"))
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn append_evidence(out: &mut String, record: &ExportRecord) {
    let evidence = record_provenance(record)
        .get("evidence")
        .and_then(Value::as_array);
    if let Some(evidence) = evidence
        && !evidence.is_empty()
    {
        out.push_str("- evidence:\n");
        for item in evidence {
            if let Some(episode_id) = item.get("episode_id").and_then(Value::as_str) {
                out.push_str(&format!(
                    "  - [Episode {episode_id}](../episodes/{episode_id}.md) ({})\n",
                    item.get("role").and_then(Value::as_str).unwrap_or_default()
                ));
            }
        }
    }
}

fn vault_readme(fact_count: usize, episode_count: usize) -> Vec<u8> {
    format!(
        "# Palimpsest wiki vault\n\n\
         A derived markdown projection of Palimpsest canonical memory.\n\n\
         - profile: {WIKI_VAULT_EXPORT_PROFILE}\n\
         - facts: {fact_count}\n\
         - episodes: {episode_count}\n\
         - rebuildable: yes. The sync script rebuilds every page from\n\
           canonical records.\n\
         - write-back: none in this projection. The vault is read-only.\n\
         - page format: see schema/palimpsest-wiki-vault-v1.md\n"
    )
    .into_bytes()
}

const VAULT_SCHEMA_DESCRIPTION: &[u8] = br#"# Wiki vault page format

The vault renders one markdown page per fact and per episode.

## Frontmatter

Every page carries a YAML frontmatter block with these fields:

- palimpsest_kind: fact or episode
- palimpsest_id: the canonical identifier (UUID)
- last_touched: the newest recorded_at from canonical metadata

Fact pages also carry namespace, key, status, created_at, and revisions.
Episode pages carry case_id.

## Fact pages

A fact page renders the fact head (current revision), its provenance and
evidence links, and the full revision history. Older revisions render
"superseded by" links when a later revision supersedes them.

## Episode pages

An episode page renders the episode metadata (observed_at, recorded_at,
writer, source) and its payload.

## Determinism

Pages are a pure function of the canonical records. The same records
rebuild the same bytes. No page embeds a run timestamp.
"#;

#[derive(Debug, Error)]
pub enum ExportPackageError {
    #[error("export contains duplicate {kind} record {id}")]
    DuplicateRecord { kind: &'static str, id: Uuid },
    #[error("export record timestamp is not RFC 3339 encodable")]
    InvalidTimestamp,
    #[error("export package contains an item too large for ZIP32")]
    TooLarge,
    #[error("export JSON could not be encoded: {0}")]
    Json(#[from] serde_json::Error),
    #[error("export package could not be written: {0}")]
    Io(#[from] io::Error),
}

fn canonical_json(value: &Value) -> Result<Value, ExportPackageError> {
    Ok(match value {
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let mut result = Map::new();
            for (key, value) in entries {
                result.insert(key.clone(), canonical_json(value)?);
            }
            Value::Object(result)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<_, _>>()?,
        ),
        value => value.clone(),
    })
}

fn canonical_record(
    kind: ExportRecordKind,
    id: Uuid,
    recorded_at: String,
    value: Value,
    context: &ExportProcessingContext,
) -> Result<Value, ExportPackageError> {
    let mut envelope = match value {
        Value::Object(object)
            if [
                "schema_version",
                "record_kind",
                "origin_class",
                "scope",
                "temporal",
                "governance",
                "provenance",
                "relations",
                "payload",
            ]
            .iter()
            .all(|key| object.contains_key(*key)) =>
        {
            Value::Object(object)
        }
        value => json!({
            "schema_version": 1,
            "record_kind": kind.as_str(),
            "origin_class": match kind {
                ExportRecordKind::Episode => "observed",
                ExportRecordKind::Checkpoint => "provided",
                ExportRecordKind::FactRevision => "derived",
                ExportRecordKind::Procedure => "derived",
                ExportRecordKind::ArtifactReference => "provided",
            },
            "id": id,
            "scope": {
                "tenant_id": context.tenant_id,
                "subject_id": context.subject_id,
            },
            "temporal": {"recorded_at": recorded_at.clone()},
            "governance": {"schema_version": 1},
            "provenance": {},
            "relations": {},
            "payload": value,
        }),
    };
    if let Value::Object(object) = &mut envelope {
        object.insert("id".to_owned(), json!(id));
        if let Some(Value::Object(temporal)) = object.get_mut("temporal") {
            temporal.insert("recorded_at".to_owned(), Value::String(recorded_at));
        }
    }
    canonical_json(&envelope)
}

fn schema_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://palimpsest.dev/schemas/palimpsest-canonical-history-v1.schema.json",
        "type": "object",
        "required": [
            "schema_version", "record_kind", "origin_class", "id", "scope",
            "temporal", "governance", "provenance", "relations", "payload"
        ],
        "properties": {
            "schema_version": {"const": 1},
            "record_kind": {"type": "string"},
            "origin_class": {"enum": ["provided", "observed", "derived", "system"]},
            "id": {"type": "string", "format": "uuid"},
            "scope": {"type": "object"},
            "temporal": {"type": "object"},
            "governance": {"type": "object"},
            "provenance": {"type": "object"},
            "relations": {"type": "object"},
            "payload": {}
        }
    }))
    .expect("the static package schema is valid JSON")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_zip<W: Write>(
    files: &[(String, Vec<u8>)],
    writer: &mut W,
    record_count: usize,
) -> Result<ExportPackageMetadata, ExportPackageError> {
    let mut output = HashingWriter::new(writer);
    let mut entries = Vec::with_capacity(files.len());
    for (path, content) in files {
        let name = path.as_bytes();
        let size = u32::try_from(content.len()).map_err(|_| ExportPackageError::TooLarge)?;
        let offset =
            u32::try_from(output.bytes_written).map_err(|_| ExportPackageError::TooLarge)?;
        let name_len = u16::try_from(name.len()).map_err(|_| ExportPackageError::TooLarge)?;
        let crc = crc32(content);
        write_zip_bytes(&mut output, &0x0403_4b50_u32.to_le_bytes())?;
        write_zip_bytes(&mut output, &20_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &crc.to_le_bytes())?;
        write_zip_bytes(&mut output, &size.to_le_bytes())?;
        write_zip_bytes(&mut output, &size.to_le_bytes())?;
        write_zip_bytes(&mut output, &name_len.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, name)?;
        write_zip_bytes(&mut output, content)?;
        entries.push((name.to_vec(), crc, size, offset));
    }
    let central_offset =
        u32::try_from(output.bytes_written).map_err(|_| ExportPackageError::TooLarge)?;
    for (name, crc, size, offset) in &entries {
        let name_len = u16::try_from(name.len()).map_err(|_| ExportPackageError::TooLarge)?;
        write_zip_bytes(&mut output, &0x0201_4b50_u32.to_le_bytes())?;
        write_zip_bytes(&mut output, &20_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &20_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &crc.to_le_bytes())?;
        write_zip_bytes(&mut output, &size.to_le_bytes())?;
        write_zip_bytes(&mut output, &size.to_le_bytes())?;
        write_zip_bytes(&mut output, &name_len.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
        write_zip_bytes(&mut output, &0_u32.to_le_bytes())?;
        write_zip_bytes(&mut output, &offset.to_le_bytes())?;
        write_zip_bytes(&mut output, name)?;
    }
    let central_size = u32::try_from(output.bytes_written)
        .map_err(|_| ExportPackageError::TooLarge)?
        .saturating_sub(central_offset);
    let entry_count = u16::try_from(entries.len()).map_err(|_| ExportPackageError::TooLarge)?;
    write_zip_bytes(&mut output, &0x0605_4b50_u32.to_le_bytes())?;
    write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
    write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
    write_zip_bytes(&mut output, &entry_count.to_le_bytes())?;
    write_zip_bytes(&mut output, &entry_count.to_le_bytes())?;
    write_zip_bytes(&mut output, &central_size.to_le_bytes())?;
    write_zip_bytes(&mut output, &central_offset.to_le_bytes())?;
    write_zip_bytes(&mut output, &0_u16.to_le_bytes())?;
    if output.bytes_written > MAX_EXPORT_PACKAGE_BYTES as u64 {
        return Err(ExportPackageError::TooLarge);
    }
    Ok(ExportPackageMetadata {
        content_sha256: hex::encode(output.hasher.finalize()),
        size_bytes: output.bytes_written,
        record_count: u64::try_from(record_count).map_err(|_| ExportPackageError::TooLarge)?,
    })
}

fn write_zip_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<(), ExportPackageError> {
    writer.write_all(bytes).map_err(ExportPackageError::Io)
}

struct HashingWriter<'a, W> {
    writer: &'a mut W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<'a, W> HashingWriter<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(written).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "ZIP byte count overflow")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "ZIP byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).unwrap()
    }

    fn record(kind: ExportRecordKind, id: u128, recorded_at: i64, value: Value) -> ExportRecord {
        ExportRecord {
            kind,
            id: Uuid::from_u128(id),
            recorded_at: timestamp(recorded_at),
            value,
        }
    }

    fn context() -> ExportProcessingContext {
        ExportProcessingContext {
            tenant_id: TenantId(Uuid::from_u128(10)),
            subject_id: SubjectId(Uuid::from_u128(11)),
            export_id: ExportId(Uuid::from_u128(12)),
            snapshot_id: "snapshot-1".to_owned(),
            authorization_scope_sha256: "a".repeat(64),
            generated_at: timestamp(30),
        }
    }

    #[test]
    fn package_bytes_are_deterministic_and_records_are_canonically_ordered() {
        let first = record(
            ExportRecordKind::Episode,
            2,
            20,
            json!({"z": 1, "a": {"d": 2, "c": 1}}),
        );
        let second = record(
            ExportRecordKind::Episode,
            1,
            10,
            json!({"payload": {"b": 2, "a": 1}}),
        );
        let fact = record(ExportRecordKind::FactRevision, 3, 5, json!({"fact": true}));
        let package = CanonicalHistoryPackage::build(
            vec![first.clone(), fact.clone(), second.clone()],
            context(),
        )
        .unwrap();
        let replay = CanonicalHistoryPackage::build(vec![second, first, fact], context()).unwrap();
        let package_bytes = package.as_bytes().unwrap();
        let replay_bytes = replay.as_bytes().unwrap();

        assert_eq!(package_bytes, replay_bytes);
        assert_eq!(sha256_hex(&package_bytes), sha256_hex(&replay_bytes));
        assert_eq!(package.record_count(), 3);
        let first_id = Uuid::from_u128(1).to_string();
        let second_id = Uuid::from_u128(2).to_string();
        assert!(
            package_bytes
                .windows(first_id.len())
                .position(|window| window == first_id.as_bytes())
                .unwrap()
                < package_bytes
                    .windows(second_id.len())
                    .position(|window| window == second_id.as_bytes())
                    .unwrap()
        );
    }

    #[test]
    fn package_contains_only_the_versioned_canonical_file_set() {
        let package = CanonicalHistoryPackage::build(vec![], context()).unwrap();
        let bytes = package.as_bytes().unwrap();
        let names = zip_local_file_names(&bytes);
        assert_eq!(
            names,
            vec![
                "manifest.json",
                "schema/palimpsest-canonical-history-v1.schema.json",
                "records/episodes.ndjson",
                "records/checkpoints.ndjson",
                "records/fact-revisions.ndjson",
                "records/procedures.ndjson",
                "records/artifact-references.ndjson",
                "processing-context.json",
                "README.txt",
            ]
        );
        assert!(!String::from_utf8_lossy(&bytes).contains("embedding"));
        assert!(!String::from_utf8_lossy(&bytes).contains("cache"));
    }

    #[test]
    fn duplicate_membership_is_rejected() {
        let item = record(ExportRecordKind::Episode, 1, 10, json!({"a": 1}));
        let duplicate = item.clone();
        assert!(matches!(
            CanonicalHistoryPackage::build(vec![item, duplicate], context()),
            Err(ExportPackageError::DuplicateRecord { .. })
        ));
    }

    fn zip_local_file_names(bytes: &[u8]) -> Vec<String> {
        let mut names = Vec::new();
        let mut offset = 0;
        while offset + 30 <= bytes.len()
            && bytes[offset..offset + 4] == 0x0403_4b50_u32.to_le_bytes()
        {
            let name_len = u16::from_le_bytes([bytes[offset + 26], bytes[offset + 27]]) as usize;
            let extra_len = u16::from_le_bytes([bytes[offset + 28], bytes[offset + 29]]) as usize;
            let name_start = offset + 30;
            let name_end = name_start + name_len;
            names.push(String::from_utf8(bytes[name_start..name_end].to_vec()).unwrap());
            let size = u32::from_le_bytes([
                bytes[offset + 18],
                bytes[offset + 19],
                bytes[offset + 20],
                bytes[offset + 21],
            ]) as usize;
            offset = name_end + extra_len + size;
        }
        names
    }

    fn vault_fact_revision(
        id: u128,
        fact_id: u128,
        recorded_at: i64,
        supersedes_id: Option<u128>,
        key: &str,
    ) -> ExportRecord {
        let supersedes = supersedes_id
            .map(|id| json!({"supersedes_id": Uuid::from_u128(id).to_string()}))
            .unwrap_or_else(|| json!({}));
        record(
            ExportRecordKind::FactRevision,
            id,
            recorded_at,
            json!({
                "schema_version": 1,
                "record_kind": "fact_revision",
                "origin_class": "assigned",
                "id": Uuid::from_u128(id).to_string(),
                "scope": {
                    "tenant_id": "t", "subject_id": "s", "case_id": "c",
                    "fact_id": Uuid::from_u128(fact_id).to_string(),
                    "namespace": "scratch", "key": key,
                },
                "temporal": {
                    "observed_at": "2026-01-01T00:00:00Z",
                    "recorded_at": format!("2026-01-0{}T00:00:00Z", (recorded_at % 10) + 1),
                    "valid_from": "2026-01-01T00:00:00Z", "valid_to": null,
                },
                "governance": {"lifecycle_state": "active", "importance": "normal"},
                "provenance": {
                    "writer_principal_id": "agent-1",
                    "write_policy_id": "policy-1",
                    "evidence": [
                        {"episode_id": Uuid::from_u128(1).to_string(), "role": "supporting"}
                    ],
                },
                "relations": supersedes,
                "payload": {"summary": format!("fact revision {}", id)},
            }),
        )
    }

    fn vault_episode() -> ExportRecord {
        record(
            ExportRecordKind::Episode,
            1,
            10,
            json!({
                "schema_version": 1,
                "record_kind": "episode",
                "origin_class": "observed",
                "id": Uuid::from_u128(1).to_string(),
                "scope": {"tenant_id": "t", "subject_id": "s", "case_id": "c"},
                "temporal": {
                    "observed_at": "2026-01-01T00:00:00Z",
                    "recorded_at": "2026-01-01T00:00:00Z",
                },
                "governance": {"sensitivity": "internal", "retention_policy_id": "r", "schema_version": 1},
                "provenance": {
                    "writer_principal_id": "agent-1", "source_type": "chat",
                    "source_uri": "urn:test:1", "external_id": null,
                },
                "relations": {},
                "payload": {"summary": "the episode"},
            }),
        )
    }

    #[test]
    fn export_profile_registry_resolves_builtin_profiles_only() {
        assert!(export_profile(CANONICAL_HISTORY_EXPORT_PROFILE).is_some());
        assert!(export_profile(WIKI_VAULT_EXPORT_PROFILE).is_some());
        assert!(export_profile("palimpsest-no-such-v9").is_none());
        assert!(is_supported_export_profile(WIKI_VAULT_EXPORT_PROFILE));
        assert!(!is_supported_export_profile("palimpsest-no-such-v9"));
    }

    #[test]
    fn vault_package_rebuilds_byte_for_byte_and_groups_revisions() {
        let records = vec![
            vault_episode(),
            vault_fact_revision(2, 100, 20, None, "temperature"),
            vault_fact_revision(3, 100, 30, Some(2), "temperature"),
            vault_fact_revision(4, 101, 25, None, "weather#now"),
        ];
        let first = WikiVaultPackage::build(records.clone(), context()).unwrap();
        let second = WikiVaultPackage::build(records, context()).unwrap();
        assert_eq!(
            first.files, second.files,
            "the vault must rebuild byte for byte"
        );
        let names: Vec<&str> = first.files.iter().map(|(path, _)| path.as_str()).collect();
        assert!(names.contains(&"manifest.json"));
        assert!(names.contains(&"README.md"));
        assert!(names.contains(&"schema/palimpsest-wiki-vault-v1.md"));
        assert!(names.contains(&"processing-context.json"));
        assert!(names.contains(&"pages/episodes/00000000-0000-0000-0000-000000000001.md"));
        assert!(names.contains(&"pages/facts/00000000-0000-0000-0000-000000000064.md"));
        assert!(names.contains(&"pages/facts/00000000-0000-0000-0000-000000000065.md"));
        assert_eq!(first.record_count(), 3, "three pages for three entities");
        let fact_page = first
            .files
            .iter()
            .find(|(path, _)| path == "pages/facts/00000000-0000-0000-0000-000000000064.md")
            .unwrap()
            .1
            .clone();
        let fact_page = String::from_utf8(fact_page).unwrap();
        assert!(fact_page.contains("status: active"), "head is current");
        assert!(
            fact_page.contains("superseded by: 00000000-0000-0000-0000-000000000003"),
            "older revision shows its successor"
        );
        assert!(fact_page.contains("[Episode 00000000-0000-0000-0000-000000000001](../episodes/00000000-0000-0000-0000-000000000001.md)"),
            "evidence renders as a relative page link");
        assert!(
            fact_page.contains("last_touched:"),
            "frontmatter carries last_touched"
        );
    }

    #[test]
    fn vault_package_escapes_markdown_in_headings() {
        let records = vec![vault_fact_revision(4, 101, 25, None, "weather#now")];
        let package = WikiVaultPackage::build(records, context()).unwrap();
        let page = package
            .files
            .iter()
            .find(|(path, _)| path == "pages/facts/00000000-0000-0000-0000-000000000065.md")
            .unwrap()
            .1
            .clone();
        let page = String::from_utf8(page).unwrap();
        assert!(
            page.contains("# Fact: scratch/weather\\#now"),
            "heading hash is escaped"
        );
        assert!(
            page.contains("key: \"weather#now\""),
            "frontmatter keeps the raw key"
        );
    }

    #[test]
    fn vault_package_rejects_duplicate_records() {
        let records = vec![vault_episode(), vault_episode()];
        let error = WikiVaultPackage::build(records, context()).unwrap_err();
        assert!(matches!(
            error,
            ExportPackageError::DuplicateRecord {
                kind: "episode",
                ..
            }
        ));
    }
}
