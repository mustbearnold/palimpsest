use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const RESTORE_FENCE_LEDGER_PROFILE: &str = "palimpsest-deletion-fence-ledger-v1";
pub const RESTORE_FENCE_LEDGER_SCHEMA_VERSION: u32 = 1;
const SCOPE_DIGEST_HEX_LENGTH: usize = 64;
const MAX_RESTORE_FENCE_LEDGER_BYTES: usize = 64 * 1024 * 1024;
const MAX_RESTORE_FENCE_ENTRIES: usize = 100_000;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreFenceEntry {
    pub scope_digest: String,
    pub state_version: u64,
    pub deletion_watermark: String,
    pub expires_at: String,
}

impl RestoreFenceEntry {
    pub fn new(
        scope_digest: impl Into<String>,
        state_version: u64,
        deletion_watermark: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<Self, RestoreFenceLedgerError> {
        let entry = Self {
            scope_digest: scope_digest.into(),
            state_version,
            deletion_watermark: format_timestamp(deletion_watermark)?,
            expires_at: format_timestamp(expires_at)?,
        };
        validate_entry(&entry, None)?;
        Ok(entry)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreFenceLedger {
    pub profile: String,
    pub schema_version: u32,
    pub generated_at: String,
    pub entries: Vec<RestoreFenceEntry>,
    pub ledger_sha256: String,
}

impl RestoreFenceLedger {
    pub fn build(
        generated_at: OffsetDateTime,
        mut entries: Vec<RestoreFenceEntry>,
    ) -> Result<Self, RestoreFenceLedgerError> {
        entries.sort_by(|left, right| left.scope_digest.cmp(&right.scope_digest));
        let ledger = Self {
            profile: RESTORE_FENCE_LEDGER_PROFILE.to_owned(),
            schema_version: RESTORE_FENCE_LEDGER_SCHEMA_VERSION,
            generated_at: format_timestamp(generated_at)?,
            entries,
            ledger_sha256: String::new(),
        };
        validate_ledger_shape(&ledger, None)?;
        let ledger_sha256 = digest_unsigned(&ledger)?;
        Ok(Self {
            ledger_sha256,
            ..ledger
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, RestoreFenceLedgerError> {
        validate_ledger_shape(self, None)?;
        if self.ledger_sha256 != digest_unsigned(self)? {
            return Err(RestoreFenceLedgerError::DigestMismatch);
        }
        canonical_json_bytes(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RestoreFenceLedgerError {
    #[error("restore fence ledger is missing")]
    Missing,
    #[error("restore fence ledger encoding is invalid")]
    InvalidEncoding,
    #[error("restore fence ledger profile is unsupported")]
    UnsupportedProfile,
    #[error("restore fence ledger schema version is unsupported")]
    UnsupportedSchemaVersion,
    #[error("restore fence ledger contains invalid metadata")]
    InvalidMetadata,
    #[error("restore fence ledger entries are not canonically ordered")]
    UnorderedEntries,
    #[error("restore fence ledger contains a duplicate scope")]
    DuplicateScope,
    #[error("restore fence ledger contains an expired fence")]
    Stale,
    #[error("restore fence ledger contains a future timestamp")]
    FutureTimestamp,
    #[error("restore fence ledger digest is invalid")]
    InvalidDigest,
    #[error("restore fence ledger digest does not match its contents")]
    DigestMismatch,
    #[error("restore fence ledger is not canonical")]
    NonCanonical,
}

pub fn verify_restore_fence_ledger(
    bytes: Option<&[u8]>,
    expected_sha256: &str,
    now: OffsetDateTime,
) -> Result<RestoreFenceLedger, RestoreFenceLedgerError> {
    let bytes = bytes.ok_or(RestoreFenceLedgerError::Missing)?;
    if bytes.len() > MAX_RESTORE_FENCE_LEDGER_BYTES {
        return Err(RestoreFenceLedgerError::InvalidMetadata);
    }
    if !is_lower_hex_digest(expected_sha256) {
        return Err(RestoreFenceLedgerError::InvalidDigest);
    }
    let ledger: RestoreFenceLedger =
        serde_json::from_slice(bytes).map_err(|_| RestoreFenceLedgerError::InvalidEncoding)?;
    validate_ledger_shape(&ledger, Some(now))?;
    if ledger.ledger_sha256 != expected_sha256 || ledger.ledger_sha256 != digest_unsigned(&ledger)?
    {
        return Err(RestoreFenceLedgerError::DigestMismatch);
    }
    if ledger.to_bytes()? != bytes {
        return Err(RestoreFenceLedgerError::NonCanonical);
    }
    Ok(ledger)
}

#[derive(serde::Serialize)]
struct UnsignedLedger<'a> {
    profile: &'a str,
    schema_version: u32,
    generated_at: &'a str,
    entries: &'a [RestoreFenceEntry],
}

fn digest_unsigned(ledger: &RestoreFenceLedger) -> Result<String, RestoreFenceLedgerError> {
    let unsigned = UnsignedLedger {
        profile: &ledger.profile,
        schema_version: ledger.schema_version,
        generated_at: &ledger.generated_at,
        entries: &ledger.entries,
    };
    let bytes = canonical_json_bytes(&unsigned)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn canonical_json_bytes<T: serde::Serialize>(
    value: &T,
) -> Result<Vec<u8>, RestoreFenceLedgerError> {
    let value =
        serde_json::to_value(value).map_err(|_| RestoreFenceLedgerError::InvalidEncoding)?;
    let value = canonical_json_value(value)?;
    serde_json::to_vec(&value).map_err(|_| RestoreFenceLedgerError::InvalidEncoding)
}

fn canonical_json_value(
    value: serde_json::Value,
) -> Result<serde_json::Value, RestoreFenceLedgerError> {
    match value {
        serde_json::Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut canonical = serde_json::Map::new();
            for key in keys {
                let child = object
                    .get(key)
                    .cloned()
                    .ok_or(RestoreFenceLedgerError::InvalidEncoding)?;
                canonical.insert(key.clone(), canonical_json_value(child)?);
            }
            Ok(serde_json::Value::Object(canonical))
        }
        serde_json::Value::Array(array) => array
            .into_iter()
            .map(canonical_json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        scalar => Ok(scalar),
    }
}

fn validate_ledger_shape(
    ledger: &RestoreFenceLedger,
    now: Option<OffsetDateTime>,
) -> Result<(), RestoreFenceLedgerError> {
    if ledger.profile != RESTORE_FENCE_LEDGER_PROFILE {
        return Err(RestoreFenceLedgerError::UnsupportedProfile);
    }
    if ledger.schema_version != RESTORE_FENCE_LEDGER_SCHEMA_VERSION {
        return Err(RestoreFenceLedgerError::UnsupportedSchemaVersion);
    }
    let generated_at = parse_timestamp(&ledger.generated_at)?;
    if let Some(now) = now
        && generated_at > now
    {
        return Err(RestoreFenceLedgerError::FutureTimestamp);
    }
    if ledger.entries.len() > MAX_RESTORE_FENCE_ENTRIES {
        return Err(RestoreFenceLedgerError::InvalidMetadata);
    }
    if !is_lower_hex_digest(&ledger.ledger_sha256) && !ledger.ledger_sha256.is_empty() {
        return Err(RestoreFenceLedgerError::InvalidDigest);
    }
    let mut previous_scope: Option<&str> = None;
    for entry in &ledger.entries {
        validate_entry(entry, now)?;
        if let Some(previous_scope) = previous_scope {
            match previous_scope.cmp(&entry.scope_digest) {
                std::cmp::Ordering::Greater => {
                    return Err(RestoreFenceLedgerError::UnorderedEntries);
                }
                std::cmp::Ordering::Equal => {
                    return Err(RestoreFenceLedgerError::DuplicateScope);
                }
                std::cmp::Ordering::Less => {}
            }
        }
        previous_scope = Some(entry.scope_digest.as_str());
    }
    Ok(())
}

fn validate_entry(
    entry: &RestoreFenceEntry,
    now: Option<OffsetDateTime>,
) -> Result<(), RestoreFenceLedgerError> {
    if !is_scope_digest(&entry.scope_digest) || entry.state_version == 0 {
        return Err(RestoreFenceLedgerError::InvalidMetadata);
    }
    let deletion_watermark = parse_timestamp(&entry.deletion_watermark)?;
    let expires_at = parse_timestamp(&entry.expires_at)?;
    if expires_at <= deletion_watermark {
        return Err(RestoreFenceLedgerError::InvalidMetadata);
    }
    if let Some(now) = now {
        if deletion_watermark > now {
            return Err(RestoreFenceLedgerError::FutureTimestamp);
        }
        if expires_at <= now {
            return Err(RestoreFenceLedgerError::Stale);
        }
    }
    Ok(())
}

fn format_timestamp(timestamp: OffsetDateTime) -> Result<String, RestoreFenceLedgerError> {
    timestamp
        .format(&Rfc3339)
        .map_err(|_| RestoreFenceLedgerError::InvalidMetadata)
}

fn parse_timestamp(value: &str) -> Result<OffsetDateTime, RestoreFenceLedgerError> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| RestoreFenceLedgerError::InvalidMetadata)
}

fn is_scope_digest(value: &str) -> bool {
    let Some((version, digest)) = value.split_once(':') else {
        return false;
    };
    version.len() >= 2
        && version.starts_with('v')
        && version[1..].bytes().all(|byte| byte.is_ascii_digit())
        && digest.len() == SCOPE_DIGEST_HEX_LENGTH
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == SCOPE_DIGEST_HEX_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp should be valid")
    }

    fn entry(number: u64) -> RestoreFenceEntry {
        RestoreFenceEntry::new(
            format!("v1:{number:064x}"),
            number,
            at(1_000 + number as i64),
            at(10_000 + number as i64),
        )
        .expect("test entry should be valid")
    }

    #[test]
    fn ledger_bytes_are_deterministic_and_verify() {
        let first = RestoreFenceLedger::build(at(2_000), vec![entry(2), entry(1)])
            .expect("ledger should build");
        let second = RestoreFenceLedger::build(at(2_000), vec![entry(1), entry(2)])
            .expect("ledger should build");
        let first_bytes = first.to_bytes().expect("ledger should encode");
        let second_bytes = second.to_bytes().expect("ledger should encode");

        assert_eq!(first, second);
        assert_eq!(first_bytes, second_bytes);
        assert!(
            String::from_utf8(first_bytes.clone())
                .expect("canonical JSON should be UTF-8")
                .starts_with("{\"entries\"")
        );
        assert_eq!(
            verify_restore_fence_ledger(Some(&first_bytes), &first.ledger_sha256, at(3_000))
                .expect("ledger should verify"),
            first
        );
    }

    #[test]
    fn missing_or_malformed_input_fails_closed_without_echoing_content() {
        assert_eq!(
            verify_restore_fence_ledger(None, &"0".repeat(64), at(3_000)),
            Err(RestoreFenceLedgerError::Missing)
        );
        let error = verify_restore_fence_ledger(
            Some(br#"{"scope_digest":"secret"}"#),
            &"0".repeat(64),
            at(3_000),
        )
        .expect_err("malformed ledger must fail");
        assert_eq!(error, RestoreFenceLedgerError::InvalidEncoding);
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn tampered_payload_or_expected_digest_is_rejected() {
        let ledger =
            RestoreFenceLedger::build(at(2_000), vec![entry(1)]).expect("ledger should build");
        let mut tampered = ledger.clone();
        tampered.entries[0].state_version = 2;
        let tampered_bytes = serde_json::to_vec(&tampered).expect("test JSON should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&tampered_bytes), &ledger.ledger_sha256, at(3_000)),
            Err(RestoreFenceLedgerError::DigestMismatch)
        );
        let bytes = ledger.to_bytes().expect("ledger should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&bytes), &"f".repeat(64), at(3_000)),
            Err(RestoreFenceLedgerError::DigestMismatch)
        );
    }

    #[test]
    fn stale_or_future_fences_are_rejected() {
        let ledger =
            RestoreFenceLedger::build(at(2_000), vec![entry(1)]).expect("ledger should build");
        let bytes = ledger.to_bytes().expect("ledger should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&bytes), &ledger.ledger_sha256, at(20_000)),
            Err(RestoreFenceLedgerError::Stale)
        );

        let future =
            RestoreFenceLedger::build(at(20_000), vec![entry(1)]).expect("ledger should build");
        let future_bytes = future.to_bytes().expect("ledger should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&future_bytes), &future.ledger_sha256, at(3_000)),
            Err(RestoreFenceLedgerError::FutureTimestamp)
        );
    }

    #[test]
    fn unsupported_profile_schema_and_malformed_fences_are_rejected() {
        let ledger =
            RestoreFenceLedger::build(at(2_000), vec![entry(1)]).expect("ledger should build");

        let mut unsupported_profile = ledger.clone();
        unsupported_profile.profile = "other-profile".to_owned();
        let profile_bytes =
            serde_json::to_vec(&unsupported_profile).expect("test JSON should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&profile_bytes), &ledger.ledger_sha256, at(3_000)),
            Err(RestoreFenceLedgerError::UnsupportedProfile)
        );

        let mut unsupported_schema = ledger.clone();
        unsupported_schema.schema_version = 99;
        let schema_bytes =
            serde_json::to_vec(&unsupported_schema).expect("test JSON should encode");
        assert_eq!(
            verify_restore_fence_ledger(Some(&schema_bytes), &ledger.ledger_sha256, at(3_000)),
            Err(RestoreFenceLedgerError::UnsupportedSchemaVersion)
        );

        let mut malformed_entry = ledger;
        malformed_entry.entries[0].scope_digest = "not-a-scope".to_owned();
        let malformed_bytes =
            serde_json::to_vec(&malformed_entry).expect("test JSON should encode");
        assert_eq!(
            verify_restore_fence_ledger(
                Some(&malformed_bytes),
                &malformed_entry.ledger_sha256,
                at(3_000)
            ),
            Err(RestoreFenceLedgerError::InvalidMetadata)
        );
    }

    #[test]
    fn duplicate_and_noncanonical_entries_are_rejected() {
        let duplicate = RestoreFenceLedger::build(at(2_000), vec![entry(1), entry(1)])
            .expect_err("duplicate fences must not build");
        assert_eq!(duplicate, RestoreFenceLedgerError::DuplicateScope);

        let ledger = RestoreFenceLedger::build(at(2_000), vec![entry(1), entry(2)])
            .expect("ledger should build");
        let mut noncanonical = ledger.to_bytes().expect("ledger should encode");
        noncanonical.extend_from_slice(b"\n");
        assert_eq!(
            verify_restore_fence_ledger(Some(&noncanonical), &ledger.ledger_sha256, at(3_000)),
            Err(RestoreFenceLedgerError::NonCanonical)
        );
    }
}
