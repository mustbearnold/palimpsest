use std::{
    collections::{BTreeMap, HashMap},
    fs::OpenOptions,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use crate::{IdempotencyRequest, RepositoryError};
use async_trait::async_trait;
use palimpsest_domain::{ExportId, PrincipalId, SubjectId, TenantId};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub const CANONICAL_HISTORY_EXPORT_PROFILE: &str = "palimpsest-canonical-history-v1";
pub const EXPORT_RETENTION_HOURS: i64 = 24;
const MAX_EXPORT_RECORDS: usize = 100_000;
const MAX_EXPORT_PACKAGE_BYTES: usize = 256 * 1024 * 1024;

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

#[derive(Debug, Error)]
pub enum ExportStoreError {
    #[error("export package is not available")]
    NotFound,
    #[error("export package store rejected the operation")]
    Conflict,
    #[error("export package store is unavailable")]
    Unavailable,
}

#[async_trait]
pub trait ExportPackageStore: Send + Sync {
    async fn stage(
        &self,
        export_id: ExportId,
        package: &CanonicalHistoryPackage,
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

#[derive(Clone, Default)]
pub struct InMemoryExportPackageStore {
    inner: Arc<Mutex<InMemoryExportPackageStoreState>>,
}

#[derive(Default)]
struct InMemoryExportPackageStoreState {
    staging: HashMap<ExportId, Vec<u8>>,
    published: HashMap<ExportId, Vec<u8>>,
}

#[async_trait]
impl ExportPackageStore for InMemoryExportPackageStore {
    async fn stage(
        &self,
        export_id: ExportId,
        package: &CanonicalHistoryPackage,
    ) -> Result<ExportPackageMetadata, ExportStoreError> {
        let mut bytes = Vec::new();
        let metadata = package
            .write_to(&mut bytes)
            .map_err(map_package_write_error)?;
        self.inner
            .lock()
            .map_err(|_| ExportStoreError::Unavailable)?
            .staging
            .insert(export_id, bytes);
        Ok(metadata)
    }

    async fn publish(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| ExportStoreError::Unavailable)?;
        if state.published.contains_key(&export_id) {
            state.staging.remove(&export_id);
            return Ok(());
        }
        let Some(package) = state.staging.remove(&export_id) else {
            return Err(ExportStoreError::NotFound);
        };
        state.published.insert(export_id, package);
        Ok(())
    }

    async fn read(&self, export_id: ExportId) -> Result<Vec<u8>, ExportStoreError> {
        self.inner
            .lock()
            .map_err(|_| ExportStoreError::Unavailable)?
            .published
            .get(&export_id)
            .cloned()
            .ok_or(ExportStoreError::NotFound)
    }

    async fn discard_staging(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        self.inner
            .lock()
            .map_err(|_| ExportStoreError::Unavailable)?
            .staging
            .remove(&export_id);
        Ok(())
    }

    async fn discard_published(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        self.inner
            .lock()
            .map_err(|_| ExportStoreError::Unavailable)?
            .published
            .remove(&export_id);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FileExportPackageStore {
    root: Arc<PathBuf>,
}

impl FileExportPackageStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    fn path(&self, export_id: ExportId, suffix: &str) -> PathBuf {
        self.root.join(format!("{}.{}", export_id.0, suffix))
    }
}

#[async_trait]
impl ExportPackageStore for FileExportPackageStore {
    async fn stage(
        &self,
        export_id: ExportId,
        package: &CanonicalHistoryPackage,
    ) -> Result<ExportPackageMetadata, ExportStoreError> {
        let root = self.root.clone();
        let staging = self.path(export_id, "staging");
        let temporary = self.path(export_id, "staging.tmp");
        let package = package.clone();
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                std::fs::create_dir_all(root.as_ref())
                    .map_err(|_| ExportStoreError::Unavailable)?;
                #[cfg(unix)]
                std::fs::set_permissions(root.as_ref(), std::fs::Permissions::from_mode(0o700))
                    .map_err(|_| ExportStoreError::Unavailable)?;
                let mut options = OpenOptions::new();
                options.create(true).write(true).truncate(true);
                #[cfg(unix)]
                options.mode(0o600);
                let mut file = options
                    .open(&temporary)
                    .map_err(|_| ExportStoreError::Unavailable)?;
                let metadata = package
                    .write_to(&mut file)
                    .map_err(map_package_write_error)?;
                file.sync_all().map_err(|_| ExportStoreError::Unavailable)?;
                std::fs::rename(&temporary, staging).map_err(|_| ExportStoreError::Unavailable)?;
                Ok(metadata)
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            result
        })
        .await
        .map_err(|_| ExportStoreError::Unavailable)?
    }

    async fn publish(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        let staging = self.path(export_id, "staging");
        let published = self.path(export_id, "zip");
        tokio::task::spawn_blocking(move || {
            if published.exists() {
                let _ = std::fs::remove_file(staging);
                return Ok(());
            }
            std::fs::rename(staging, published).map_err(|_| ExportStoreError::NotFound)
        })
        .await
        .map_err(|_| ExportStoreError::Unavailable)?
    }

    async fn read(&self, export_id: ExportId) -> Result<Vec<u8>, ExportStoreError> {
        let published = self.path(export_id, "zip");
        tokio::task::spawn_blocking(move || {
            std::fs::read(published).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    ExportStoreError::NotFound
                } else {
                    ExportStoreError::Unavailable
                }
            })
        })
        .await
        .map_err(|_| ExportStoreError::Unavailable)?
    }

    async fn discard_staging(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        let staging = self.path(export_id, "staging");
        let temporary = self.path(export_id, "staging.tmp");
        tokio::task::spawn_blocking(move || {
            for path in [staging, temporary] {
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(ExportStoreError::Unavailable),
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| ExportStoreError::Unavailable)?
    }

    async fn discard_published(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        let published = self.path(export_id, "zip");
        tokio::task::spawn_blocking(move || match std::fs::remove_file(published) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(ExportStoreError::Unavailable),
        })
        .await
        .map_err(|_| ExportStoreError::Unavailable)?
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

fn sha256_hex(bytes: &[u8]) -> String {
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

fn map_package_write_error(error: ExportPackageError) -> ExportStoreError {
    match error {
        ExportPackageError::TooLarge
        | ExportPackageError::DuplicateRecord { .. }
        | ExportPackageError::InvalidTimestamp
        | ExportPackageError::Json(_) => ExportStoreError::Conflict,
        ExportPackageError::Io(_) => ExportStoreError::Unavailable,
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

    #[tokio::test]
    async fn in_memory_package_store_publishes_only_after_staging() {
        let package = CanonicalHistoryPackage::build(vec![], context()).unwrap();
        let export_id = ExportId(Uuid::now_v7());
        let store = InMemoryExportPackageStore::default();

        assert!(matches!(
            store.read(export_id).await,
            Err(ExportStoreError::NotFound)
        ));
        let metadata = store.stage(export_id, &package).await.unwrap();
        assert_eq!(metadata.record_count, 0);
        let bytes = package.as_bytes().unwrap();
        assert!(matches!(
            store.read(export_id).await,
            Err(ExportStoreError::NotFound)
        ));
        store.publish(export_id).await.unwrap();
        assert_eq!(store.read(export_id).await.unwrap(), bytes);
        store.discard_published(export_id).await.unwrap();
        assert!(matches!(
            store.read(export_id).await,
            Err(ExportStoreError::NotFound)
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
}
