use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub const CANONICAL_HISTORY_EXPORT_PROFILE: &str = "palimpsest-canonical-history-v1";

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
    pub snapshot_id: String,
    pub authorization_scope_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalHistoryPackage {
    bytes: Vec<u8>,
    content_sha256: String,
    record_count: usize,
}

impl CanonicalHistoryPackage {
    pub fn build(
        records: Vec<ExportRecord>,
        context: ExportProcessingContext,
    ) -> Result<Self, ExportPackageError> {
        let mut grouped = BTreeMap::<ExportRecordKind, Vec<ExportRecord>>::new();
        for record in records {
            grouped.entry(record.kind).or_default().push(record);
        }

        let mut record_count = 0;
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
            let mut content = Vec::new();
            for record in records {
                let recorded_at = record
                    .recorded_at
                    .format(&Rfc3339)
                    .map_err(|_| ExportPackageError::InvalidTimestamp)?;
                let record_value = canonical_json(&record.value)?;
                let line = canonical_json(&json!({
                    "id": record.id,
                    "recorded_at": recorded_at,
                    "record": record_value,
                }))?;
                serde_json::to_writer(&mut content, &line)?;
                content.push(b'\n');
            }
            files.push((kind.file_name().to_owned(), content));
        }

        let processing_context_value = canonical_json(&json!({
            "profile": CANONICAL_HISTORY_EXPORT_PROFILE,
            "snapshot_id": context.snapshot_id,
            "authorization_scope_sha256": context.authorization_scope_sha256,
        }))?;
        let processing_context = serde_json::to_vec(&processing_context_value)?;
        files.push(("processing-context.json".to_owned(), processing_context));

        let mut manifest_files = Vec::new();
        for (path, content) in &files {
            manifest_files.push(json!({
                "path": path,
                "size_bytes": content.len(),
                "sha256": sha256_hex(content),
            }));
        }
        let manifest_value = canonical_json(&json!({
            "profile": CANONICAL_HISTORY_EXPORT_PROFILE,
            "manifest_schema_version": 1,
            "record_count": record_count,
            "files": manifest_files,
        }))?;
        let manifest = serde_json::to_vec(&manifest_value)?;
        files.insert(0, ("manifest.json".to_owned(), manifest));
        files.insert(
            1,
            (
                "schema/palimpsest-canonical-history-v1.schema.json".to_owned(),
                schema_bytes(),
            ),
        );
        files.push((
            "README.txt".to_owned(),
            b"Palimpsest canonical history export.\n".to_vec(),
        ));

        let bytes = write_zip(&files)?;
        let content_sha256 = sha256_hex(&bytes);
        Ok(Self {
            bytes,
            content_sha256,
            record_count,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    pub fn size_bytes(&self) -> usize {
        self.bytes.len()
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

fn schema_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://palimpsest.dev/schemas/palimpsest-canonical-history-v1.schema.json",
        "type": "object",
        "required": ["id", "recorded_at", "record"],
        "properties": {
            "id": {"type": "string", "format": "uuid"},
            "recorded_at": {"type": "string", "format": "date-time"},
            "record": {"type": "object"}
        }
    }))
    .expect("the static package schema is valid JSON")
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_zip(files: &[(String, Vec<u8>)]) -> Result<Vec<u8>, ExportPackageError> {
    let mut output = Vec::new();
    let mut entries = Vec::with_capacity(files.len());
    for (path, content) in files {
        let name = path.as_bytes();
        let size = u32::try_from(content.len()).map_err(|_| ExportPackageError::TooLarge)?;
        let offset = u32::try_from(output.len()).map_err(|_| ExportPackageError::TooLarge)?;
        let name_len = u16::try_from(name.len()).map_err(|_| ExportPackageError::TooLarge)?;
        let crc = crc32(content);
        output.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
        output.extend_from_slice(&20_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&crc.to_le_bytes());
        output.extend_from_slice(&size.to_le_bytes());
        output.extend_from_slice(&size.to_le_bytes());
        output.extend_from_slice(&name_len.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(name);
        output.extend_from_slice(content);
        entries.push((name.to_vec(), crc, size, offset));
    }
    let central_offset = u32::try_from(output.len()).map_err(|_| ExportPackageError::TooLarge)?;
    for (name, crc, size, offset) in &entries {
        let name_len = u16::try_from(name.len()).map_err(|_| ExportPackageError::TooLarge)?;
        output.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
        output.extend_from_slice(&20_u16.to_le_bytes());
        output.extend_from_slice(&20_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&crc.to_le_bytes());
        output.extend_from_slice(&size.to_le_bytes());
        output.extend_from_slice(&size.to_le_bytes());
        output.extend_from_slice(&name_len.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u16.to_le_bytes());
        output.extend_from_slice(&0_u32.to_le_bytes());
        output.extend_from_slice(&offset.to_le_bytes());
        output.extend_from_slice(name);
    }
    let central_size = u32::try_from(output.len())
        .map_err(|_| ExportPackageError::TooLarge)?
        .saturating_sub(central_offset);
    let entry_count = u16::try_from(entries.len()).map_err(|_| ExportPackageError::TooLarge)?;
    output.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&entry_count.to_le_bytes());
    output.extend_from_slice(&entry_count.to_le_bytes());
    output.extend_from_slice(&central_size.to_le_bytes());
    output.extend_from_slice(&central_offset.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    Ok(output)
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
            snapshot_id: "snapshot-1".to_owned(),
            authorization_scope_sha256: "a".repeat(64),
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

        assert_eq!(package.as_bytes(), replay.as_bytes());
        assert_eq!(package.content_sha256(), replay.content_sha256());
        assert_eq!(package.record_count(), 3);
        let first_id = Uuid::from_u128(1).to_string();
        let second_id = Uuid::from_u128(2).to_string();
        assert!(
            package
                .as_bytes()
                .windows(first_id.len())
                .position(|window| window == first_id.as_bytes())
                .unwrap()
                < package
                    .as_bytes()
                    .windows(second_id.len())
                    .position(|window| window == second_id.as_bytes())
                    .unwrap()
        );
    }

    #[test]
    fn package_contains_only_the_versioned_canonical_file_set() {
        let package = CanonicalHistoryPackage::build(vec![], context()).unwrap();
        let names = zip_local_file_names(package.as_bytes());
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
        assert!(!String::from_utf8_lossy(package.as_bytes()).contains("embedding"));
        assert!(!String::from_utf8_lossy(package.as_bytes()).contains("cache"));
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
}
