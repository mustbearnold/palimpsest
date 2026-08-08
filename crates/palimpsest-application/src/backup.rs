//! S3-compatible backup object store (spec 016).
//!
//! The store holds base backups, WAL segments, and the backup index. It
//! follows the spec 004 export-store patterns: path-style URLs, AWS
//! Signature Version 4 signing, content-free errors, and a local HTTP
//! object-shaped fixture for contract tests. The store is provider-neutral.
//! It proves the Palimpsest object port and failure semantics, not the
//! availability or durability of a particular provider.

use std::{collections::BTreeMap, env, sync::Arc};

use crate::export::{
    aws_timestamp, canonical_header_value, canonical_uri, hmac_sha256, host_header, sha256_hex,
};
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::Url;

/// Object key of the backup index document.
pub const BACKUP_INDEX_OBJECT: &str = "index/palimpsest-backup-index-v1.json";

/// Object key prefix for base backup archives.
pub const BASE_OBJECT_PREFIX: &str = "base";

/// Object key prefix for WAL segments.
pub const WAL_OBJECT_PREFIX: &str = "wal";

/// Configuration error for the S3-compatible backup object store.
#[derive(Debug, thiserror::Error)]
pub enum S3BackupObjectStoreConfigError {
    #[error("PALIMPSEST_BACKUP_S3_* configuration is incomplete: {0}")]
    MissingEnvironment(String),
    #[error("PALIMPSEST_BACKUP_S3_ENDPOINT is not a valid HTTP(S) URL")]
    InvalidEndpoint,
    #[error("PALIMPSEST_BACKUP_S3_PREFIX must not start with '/' or contain '..'")]
    InvalidPrefix,
    #[error("PALIMPSEST_BACKUP_S3_{0} must not be empty")]
    EmptyField(String),
}

/// Configuration for the S3-compatible backup object store.
#[derive(Clone, Debug)]
pub struct S3BackupObjectStoreConfig {
    pub endpoint: Url,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl S3BackupObjectStoreConfig {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        prefix: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<String>,
    ) -> Result<Self, S3BackupObjectStoreConfigError> {
        let endpoint = Url::parse(endpoint).map_err(|_| S3BackupObjectStoreConfigError::InvalidEndpoint)?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(S3BackupObjectStoreConfigError::InvalidEndpoint);
        }
        if prefix.starts_with('/') || prefix.split('/').any(|segment| segment == "..") {
            return Err(S3BackupObjectStoreConfigError::InvalidPrefix);
        }
        let non_empty = |value: &str, name: &str| {
            if value.trim().is_empty() {
                Err(S3BackupObjectStoreConfigError::EmptyField(name.to_owned()))
            } else {
                Ok(value.to_owned())
            }
        };
        Ok(Self {
            endpoint,
            bucket: non_empty(bucket, "BUCKET")?,
            prefix: prefix.to_owned(),
            region: non_empty(region, "REGION")?,
            access_key_id: non_empty(access_key_id, "ACCESS_KEY_ID")?,
            secret_access_key: non_empty(secret_access_key, "SECRET_ACCESS_KEY")?,
            session_token,
        })
    }

    /// Read the configuration from `PALIMPSEST_BACKUP_S3_*` variables.
    ///
    /// Returns `Ok(None)` when no endpoint variable is present. A partial
    /// configuration fails rather than silently reverting to another store.
    pub fn from_environment() -> Result<Option<Self>, S3BackupObjectStoreConfigError> {
        let endpoint = env::var("PALIMPSEST_BACKUP_S3_ENDPOINT").ok();
        let bucket = env::var("PALIMPSEST_BACKUP_S3_BUCKET").ok();
        let prefix = env::var("PALIMPSEST_BACKUP_S3_PREFIX").ok();
        let region = env::var("PALIMPSEST_BACKUP_S3_REGION").ok();
        let access_key_id = env::var("PALIMPSEST_BACKUP_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = env::var("PALIMPSEST_BACKUP_S3_SECRET_ACCESS_KEY").ok();
        let session_token = env::var("PALIMPSEST_BACKUP_S3_SESSION_TOKEN").ok();
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };
        let required = |value: Option<String>, name: &str| {
            value.ok_or_else(|| {
                S3BackupObjectStoreConfigError::MissingEnvironment(format!(
                    "PALIMPSEST_BACKUP_S3_{name}"
                ))
            })
        };
        let bucket = required(bucket, "BUCKET")?;
        let region = required(region, "REGION")?;
        let access_key_id = required(access_key_id, "ACCESS_KEY_ID")?;
        let secret_access_key = required(secret_access_key, "SECRET_ACCESS_KEY")?;
        Self::new(
            &endpoint,
            &bucket,
            prefix.as_deref().unwrap_or(""),
            &region,
            &access_key_id,
            &secret_access_key,
            session_token,
        )
        .map(Some)
    }
}

/// A content-free error from the backup object store.
#[derive(Debug, thiserror::Error)]
pub enum S3BackupStoreError {
    #[error("backup object store configuration is invalid")]
    Config(#[from] S3BackupObjectStoreConfigError),
    #[error("backup object store is unavailable")]
    Unavailable,
    #[error("backup object was not found")]
    NotFound,
    #[error("backup index is not valid JSON")]
    InvalidIndex,
}

/// S3-compatible backup object store.
#[derive(Clone)]
pub struct S3BackupObjectStore {
    client: Client,
    config: Arc<S3BackupObjectStoreConfig>,
}

impl S3BackupObjectStore {
    pub fn from_config(config: S3BackupObjectStoreConfig) -> Self {
        Self {
            client: Client::new(),
            config: Arc::new(config),
        }
    }

    pub fn from_environment() -> Result<Option<Self>, S3BackupObjectStoreConfigError> {
        S3BackupObjectStoreConfig::from_environment()
            .map(|config| config.map(Self::from_config))
    }

    fn object_url(&self, key: &str) -> Url {
        let mut url = self.config.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .expect("validated S3 endpoint must support path segments");
            segments.push(&self.config.bucket);
            if !self.config.prefix.is_empty() {
                for segment in self.config.prefix.split('/') {
                    segments.push(segment);
                }
            }
            for segment in key.split('/') {
                segments.push(segment);
            }
        }
        url
    }

    fn signed_request(
        &self,
        method: Method,
        url: &Url,
        body: &[u8],
        now: OffsetDateTime,
    ) -> RequestBuilder {
        let payload_hash = sha256_hex(body);
        let amz_date = aws_timestamp(now);
        let date = &amz_date[..8];
        let host = host_header(url);
        let mut headers = BTreeMap::from([
            ("host".to_owned(), host.clone()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ]);
        if let Some(token) = self.config.session_token.as_ref() {
            headers.insert("x-amz-security-token".to_owned(), token.clone());
        }
        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{}:{}\n", name, canonical_header_value(value)))
            .collect::<String>();
        let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method,
            canonical_uri(url),
            canonical_headers,
            signed_headers,
            payload_hash,
        );
        let scope = format!("{date}/{}/{}/aws4_request", self.config.region, "s3");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let date_key = hmac_sha256(
            format!("AWS4{}", self.config.secret_access_key).as_bytes(),
            date.as_bytes(),
        );
        let region_key = hmac_sha256(&date_key, self.config.region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"s3");
        let signing_key = hmac_sha256(&service_key, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.access_key_id, scope, signed_headers, signature
        );

        let mut request = self
            .client
            .request(method.clone(), url.clone())
            .header(reqwest::header::HOST, host)
            .header(
                "x-amz-content-sha256",
                headers["x-amz-content-sha256"].as_str(),
            )
            .header("x-amz-date", headers["x-amz-date"].as_str())
            .header(reqwest::header::AUTHORIZATION, authorization);
        if let Some(token) = self.config.session_token.as_ref() {
            request = request.header("x-amz-security-token", token);
        }
        if method == Method::PUT {
            request = request.body(body.to_owned());
        }
        request
    }

    /// Write an object. The write replaces any existing object with the key.
    pub async fn put_object(&self, key: &str, bytes: &[u8]) -> Result<(), S3BackupStoreError> {
        let url = self.object_url(key);
        let response = self
            .signed_request(Method::PUT, &url, bytes, OffsetDateTime::now_utc())
            .send()
            .await
            .map_err(|_| S3BackupStoreError::Unavailable)?;
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(()),
            _ => Err(S3BackupStoreError::Unavailable),
        }
    }

    /// Read an object. A missing object maps to `NotFound`.
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, S3BackupStoreError> {
        let url = self.object_url(key);
        let response = self
            .signed_request(Method::GET, &url, &[], OffsetDateTime::now_utc())
            .send()
            .await
            .map_err(|_| S3BackupStoreError::Unavailable)?;
        match response.status() {
            StatusCode::OK => response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|_| S3BackupStoreError::Unavailable),
            StatusCode::NOT_FOUND => Err(S3BackupStoreError::NotFound),
            _ => Err(S3BackupStoreError::Unavailable),
        }
    }

    /// Delete an object. An already-absent object is success.
    pub async fn delete_object(&self, key: &str) -> Result<(), S3BackupStoreError> {
        let url = self.object_url(key);
        let response = self
            .signed_request(Method::DELETE, &url, &[], OffsetDateTime::now_utc())
            .send()
            .await
            .map_err(|_| S3BackupStoreError::Unavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK | StatusCode::NOT_FOUND => Ok(()),
            _ => Err(S3BackupStoreError::Unavailable),
        }
    }

    /// Read the backup index. A missing index reads as an empty index.
    pub async fn read_index(&self) -> Result<BackupIndex, S3BackupStoreError> {
        match self.get_object(BACKUP_INDEX_OBJECT).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|_| S3BackupStoreError::InvalidIndex),
            Err(S3BackupStoreError::NotFound) => Ok(BackupIndex::default()),
            Err(error) => Err(error),
        }
    }

    /// Write the backup index. The write replaces the existing index.
    pub async fn write_index(&self, index: &BackupIndex) -> Result<(), S3BackupStoreError> {
        let bytes = serde_json::to_vec(index).map_err(|_| S3BackupStoreError::InvalidIndex)?;
        self.put_object(BACKUP_INDEX_OBJECT, &bytes).await
    }
}

/// Deterministic backup index (spec 016 R3). The index is the v1 discovery
/// mechanism for backups. It follows a single-writer assumption: the
/// orchestration script is the only writer.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupIndex {
    pub entries: Vec<BackupIndexEntry>,
}

impl BackupIndex {
    /// Insert an entry in deterministic order (by backup id).
    pub fn insert(&mut self, entry: BackupIndexEntry) {
        self.entries.push(entry);
        self.entries.sort_by(|left, right| left.backup_id.cmp(&right.backup_id));
    }

    /// Remove an entry by backup id. Returns the removed entry.
    pub fn remove(&mut self, backup_id: &str) -> Option<BackupIndexEntry> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.backup_id == backup_id)?;
        Some(self.entries.remove(position))
    }
}

/// One base backup in the index.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupIndexEntry {
    /// Versioned backup id (v7 uuid string).
    pub backup_id: String,
    /// Named retention policy id declared at backup time.
    pub retention_policy_id: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// Object key of the base archive.
    pub base_object: String,
    /// SHA-256 of the base archive bytes.
    pub base_sha256: String,
    /// Size of the base archive in bytes.
    pub base_size_bytes: u64,
    /// First WAL segment required to replay past the base backup.
    pub wal_from: String,
    /// Last WAL segment archived at backup time.
    pub wal_to: String,
}

/// Build the object key for a base backup archive.
pub fn base_object_key(backup_id: &str) -> String {
    format!("{BASE_OBJECT_PREFIX}/{backup_id}.tar.gz")
}

/// Build the object key for a WAL segment. The timeline id is the first
/// eight characters of the segment name (PostgreSQL WAL naming).
pub fn wal_object_key(wal_name: &str) -> String {
    let timeline = &wal_name[..8.min(wal_name.len())];
    format!("{WAL_OBJECT_PREFIX}/{timeline}/{wal_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).unwrap()
    }

    fn sample_entry(backup_id: &str) -> BackupIndexEntry {
        BackupIndexEntry {
            backup_id: backup_id.to_owned(),
            retention_policy_id: "pitr-v1".to_owned(),
            created_at: "2026-08-08T00:00:00Z".to_owned(),
            base_object: base_object_key(backup_id),
            base_sha256: "a".repeat(64),
            base_size_bytes: 42,
            wal_from: "000000010000000000000001".to_owned(),
            wal_to: "000000010000000000000003".to_owned(),
        }
    }

    #[test]
    fn index_entries_are_always_sorted_by_backup_id() {
        let mut index = BackupIndex::default();
        index.insert(sample_entry("backup-b"));
        index.insert(sample_entry("backup-a"));
        index.insert(sample_entry("backup-c"));
        let ids = index
            .entries
            .iter()
            .map(|entry| entry.backup_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["backup-a", "backup-b", "backup-c"]);
        let removed = index.remove("backup-b").expect("entry exists");
        assert_eq!(removed.backup_id, "backup-b");
        assert_eq!(index.entries.len(), 2);
        assert!(index.remove("backup-b").is_none());
    }

    #[test]
    fn object_keys_are_versioned_and_partitioned() {
        assert_eq!(
            base_object_key("019be000-0000-7000-8000-000000000401"),
            "base/019be000-0000-7000-8000-000000000401.tar.gz"
        );
        assert_eq!(
            wal_object_key("000000010000000000000002"),
            "wal/00000001/000000010000000000000002"
        );
    }

    #[test]
    fn s3_configuration_rejects_unsafe_endpoint_and_prefix() {
        assert!(matches!(
            S3BackupObjectStoreConfig::new(
                "file:///tmp/objects",
                "bucket",
                "",
                "us-east-1",
                "key",
                "secret",
                None,
            ),
            Err(S3BackupObjectStoreConfigError::InvalidEndpoint)
        ));
        assert!(matches!(
            S3BackupObjectStoreConfig::new(
                "https://s3.example.test",
                "bucket",
                "/leading-slash",
                "us-east-1",
                "key",
                "secret",
                None,
            ),
            Err(S3BackupObjectStoreConfigError::InvalidPrefix)
        ));
        assert!(matches!(
            S3BackupObjectStoreConfig::new(
                "https://s3.example.test",
                "bucket",
                "a/../b",
                "us-east-1",
                "key",
                "secret",
                None,
            ),
            Err(S3BackupObjectStoreConfigError::InvalidPrefix)
        ));
    }

    #[test]
    fn s3_sigv4_authorization_is_deterministic() {
        let config = S3BackupObjectStoreConfig::new(
            "https://s3.example.test",
            "bucket",
            "backups",
            "us-east-1",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None,
        )
        .unwrap();
        let store = S3BackupObjectStore::from_config(config);
        let url = store.object_url("base/backup-a.tar.gz");
        let now = timestamp(1_784_649_600);
        let request = store
            .signed_request(Method::GET, &url, &[], now)
            .build()
            .unwrap();
        let authorization = request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            authorization.starts_with(
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260721/us-east-1/s3/aws4_request"
            ),
            "authorization must carry the deterministic scope: {authorization}"
        );
    }

    struct FakeS3State {
        objects: Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }

    async fn fake_s3_handler(
        axum::extract::State(state): axum::extract::State<std::sync::Arc<FakeS3State>>,
        request: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::{body::to_bytes, response::IntoResponse};

        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let body = to_bytes(request.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec();
        let mut objects = state.objects.lock().unwrap();
        match method {
            axum::http::Method::PUT => {
                objects.insert(path, body);
                axum::http::StatusCode::OK.into_response()
            }
            axum::http::Method::GET => objects
                .get(&path)
                .cloned()
                .map(|bytes| (axum::http::StatusCode::OK, bytes).into_response())
                .unwrap_or_else(|| axum::http::StatusCode::NOT_FOUND.into_response()),
            axum::http::Method::DELETE => {
                objects.remove(&path);
                axum::http::StatusCode::NO_CONTENT.into_response()
            }
            _ => axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    #[tokio::test]
    async fn backup_store_round_trips_objects_and_index() {
        use axum::{Router, routing::any};

        let state = std::sync::Arc::new(FakeS3State {
            objects: Mutex::new(std::collections::HashMap::new()),
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(fake_s3_handler))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = S3BackupObjectStoreConfig::new(
            &format!("http://{address}"),
            "bucket",
            "",
            "us-east-1",
            "key",
            "secret",
            None,
        )
        .unwrap();
        let store = S3BackupObjectStore::from_config(config);

        assert!(matches!(
            store.get_object("base/missing.tar.gz").await,
            Err(S3BackupStoreError::NotFound)
        ));
        let index = store.read_index().await.unwrap();
        assert!(index.entries.is_empty(), "missing index reads as empty");

        store
            .put_object("base/backup-a.tar.gz", b"base-bytes")
            .await
            .unwrap();
        assert_eq!(
            store.get_object("base/backup-a.tar.gz").await.unwrap(),
            b"base-bytes"
        );

        let mut index = BackupIndex::default();
        index.insert(sample_entry("backup-a"));
        store.write_index(&index).await.unwrap();
        let reread = store.read_index().await.unwrap();
        assert_eq!(reread, index, "index round-trips deterministically");

        store.delete_object("base/backup-a.tar.gz").await.unwrap();
        assert!(matches!(
            store.get_object("base/backup-a.tar.gz").await,
            Err(S3BackupStoreError::NotFound)
        ));
        store.delete_object("base/absent.tar.gz").await.unwrap();

        server.abort();
    }
}
