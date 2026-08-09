//! Export-package storage adapters for Palimpsest.
//!
//! The `ExportPackageStore` interface and the export package types live in
//! `palimpsest-application`. This crate owns the concrete adapters behind
//! that seam: an S3-compatible object store (path-style, SigV4-signed), the
//! local filesystem, and an in-memory store for tests and embedded mode.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use async_trait::async_trait;
use palimpsest_application::{
    aws_timestamp, canonical_header_value, canonical_uri, hmac_sha256, host_header, sha256_hex,
    ExportPackage, ExportPackageError, ExportPackageMetadata, ExportPackageStore, ExportStoreError,
};
use palimpsest_domain::ExportId;
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use thiserror::Error;
use time::OffsetDateTime;
use url::Url;

pub const S3_EXPORT_PACKAGE_STORE_PROFILE: &str = "s3-compatible-path-style-v1";

fn map_package_write_error(error: ExportPackageError) -> ExportStoreError {
    match error {
        ExportPackageError::TooLarge
        | ExportPackageError::DuplicateRecord { .. }
        | ExportPackageError::InvalidTimestamp
        | ExportPackageError::Json(_) => ExportStoreError::Conflict,
        ExportPackageError::Io(_) => ExportStoreError::Unavailable,
    }
}

#[derive(Debug, Error)]
pub enum S3ExportPackageStoreConfigError {
    #[error("S3 export endpoint must be an HTTP(S) URL with a host and no query or fragment")]
    InvalidEndpoint,
    #[error("S3 export configuration field is empty: {0}")]
    EmptyField(&'static str),
    #[error("S3 export configuration is missing a required environment variable: {0}")]
    MissingEnvironment(&'static str),
    #[error("S3 export prefix contains an unsafe path segment")]
    InvalidPrefix,
}

#[derive(Clone)]
pub struct S3ExportPackageStoreConfig {
    endpoint: Url,
    bucket: String,
    prefix: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl S3ExportPackageStoreConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, S3ExportPackageStoreConfigError> {
        let endpoint = Url::parse(&endpoint.into())
            .map_err(|_| S3ExportPackageStoreConfigError::InvalidEndpoint)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || endpoint.cannot_be_a_base()
            || endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(S3ExportPackageStoreConfigError::InvalidEndpoint);
        }

        let bucket = required_config_value(bucket.into(), "bucket")?;
        let region = required_config_value(region.into(), "region")?;
        let access_key_id = required_config_value(access_key_id.into(), "access_key_id")?;
        let secret_access_key =
            required_config_value(secret_access_key.into(), "secret_access_key")?;
        let prefix = prefix.into().trim_matches('/').to_owned();
        if prefix
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        {
            return Err(S3ExportPackageStoreConfigError::InvalidPrefix);
        }
        let session_token = session_token
            .filter(|token| !token.trim().is_empty())
            .map(|token| token.trim().to_owned());

        Ok(Self {
            endpoint,
            bucket,
            prefix,
            region,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    pub fn from_environment() -> Result<Option<Self>, S3ExportPackageStoreConfigError> {
        let endpoint = env::var("PALIMPSEST_EXPORT_S3_ENDPOINT").ok();
        let bucket = env::var("PALIMPSEST_EXPORT_S3_BUCKET").ok();
        let prefix = env::var("PALIMPSEST_EXPORT_S3_PREFIX").ok();
        let region = env::var("PALIMPSEST_EXPORT_S3_REGION").ok();
        let access_key_id = env::var("PALIMPSEST_EXPORT_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = env::var("PALIMPSEST_EXPORT_S3_SECRET_ACCESS_KEY").ok();
        let session_token = env::var("PALIMPSEST_EXPORT_S3_SESSION_TOKEN").ok();

        if endpoint.is_none()
            && bucket.is_none()
            && prefix.is_none()
            && region.is_none()
            && access_key_id.is_none()
            && secret_access_key.is_none()
            && session_token.is_none()
        {
            return Ok(None);
        }

        let endpoint = endpoint.ok_or(S3ExportPackageStoreConfigError::MissingEnvironment(
            "PALIMPSEST_EXPORT_S3_ENDPOINT",
        ))?;
        let bucket = bucket.ok_or(S3ExportPackageStoreConfigError::MissingEnvironment(
            "PALIMPSEST_EXPORT_S3_BUCKET",
        ))?;
        let region = region.ok_or(S3ExportPackageStoreConfigError::MissingEnvironment(
            "PALIMPSEST_EXPORT_S3_REGION",
        ))?;
        let access_key_id =
            access_key_id.ok_or(S3ExportPackageStoreConfigError::MissingEnvironment(
                "PALIMPSEST_EXPORT_S3_ACCESS_KEY_ID",
            ))?;
        let secret_access_key =
            secret_access_key.ok_or(S3ExportPackageStoreConfigError::MissingEnvironment(
                "PALIMPSEST_EXPORT_S3_SECRET_ACCESS_KEY",
            ))?;

        Self::new(
            endpoint,
            bucket,
            prefix.unwrap_or_default(),
            region,
            access_key_id,
            secret_access_key,
            session_token,
        )
        .map(Some)
    }
}

fn required_config_value(
    value: String,
    name: &'static str,
) -> Result<String, S3ExportPackageStoreConfigError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        Err(S3ExportPackageStoreConfigError::EmptyField(name))
    } else {
        Ok(value)
    }
}

#[derive(Clone)]
pub struct S3ExportPackageStore {
    client: Client,
    config: Arc<S3ExportPackageStoreConfig>,
}

impl S3ExportPackageStore {
    pub fn from_config(config: S3ExportPackageStoreConfig) -> Self {
        Self {
            client: Client::new(),
            config: Arc::new(config),
        }
    }

    pub fn from_environment() -> Result<Option<Self>, S3ExportPackageStoreConfigError> {
        S3ExportPackageStoreConfig::from_environment().map(|config| config.map(Self::from_config))
    }

    fn object_url(&self, export_id: ExportId, suffix: &str) -> Url {
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
            segments.push(&format!("{}.{}", export_id.0, suffix));
        }
        url
    }

    fn signed_request(
        &self,
        method: Method,
        url: &Url,
        body: &[u8],
        if_none_match: Option<&str>,
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
        if let Some(value) = if_none_match {
            headers.insert("if-none-match".to_owned(), value.to_owned());
        }
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
        if let Some(value) = if_none_match {
            request = request.header(reqwest::header::IF_NONE_MATCH, value);
        }
        if let Some(token) = self.config.session_token.as_ref() {
            request = request.header("x-amz-security-token", token);
        }
        if method == Method::PUT {
            request = request.body(body.to_owned());
        }
        request
    }

    async fn read_object(&self, url: Url) -> Result<Vec<u8>, ExportStoreError> {
        let response = self
            .signed_request(Method::GET, &url, &[], None, OffsetDateTime::now_utc())
            .send()
            .await
            .map_err(|_| ExportStoreError::Unavailable)?;
        match response.status() {
            StatusCode::OK => response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|_| ExportStoreError::Unavailable),
            StatusCode::NOT_FOUND => Err(ExportStoreError::NotFound),
            _ => Err(ExportStoreError::Unavailable),
        }
    }

    async fn put_if_absent(&self, url: Url, bytes: &[u8]) -> Result<(), ExportStoreError> {
        let response = self
            .signed_request(
                Method::PUT,
                &url,
                bytes,
                Some("*"),
                OffsetDateTime::now_utc(),
            )
            .send()
            .await
            .map_err(|_| ExportStoreError::Unavailable)?;
        if response.status().is_success() {
            Ok(())
        } else if response.status() == StatusCode::PRECONDITION_FAILED {
            Err(ExportStoreError::Conflict)
        } else {
            Err(ExportStoreError::Unavailable)
        }
    }

    async fn delete_object(&self, url: Url) -> Result<(), ExportStoreError> {
        let response = self
            .signed_request(Method::DELETE, &url, &[], None, OffsetDateTime::now_utc())
            .send()
            .await
            .map_err(|_| ExportStoreError::Unavailable)?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(ExportStoreError::Unavailable)
        }
    }
}

#[async_trait]
impl ExportPackageStore for S3ExportPackageStore {
    async fn stage(
        &self,
        export_id: ExportId,
        package: Box<dyn ExportPackage>,
    ) -> Result<ExportPackageMetadata, ExportStoreError> {
        let bytes = package.as_bytes().map_err(map_package_write_error)?;
        let metadata = ExportPackageMetadata {
            content_sha256: sha256_hex(&bytes),
            size_bytes: bytes.len() as u64,
            record_count: u64::try_from(package.record_count())
                .map_err(|_| ExportPackageError::TooLarge)
                .map_err(map_package_write_error)?,
        };
        let url = self.object_url(export_id, "staging");
        match self.read_object(url.clone()).await {
            Ok(existing) => {
                if existing == bytes {
                    Ok(metadata)
                } else {
                    Err(ExportStoreError::Conflict)
                }
            }
            Err(ExportStoreError::NotFound) => {
                match self.put_if_absent(url.clone(), &bytes).await {
                    Ok(()) => Ok(metadata),
                    Err(ExportStoreError::Conflict) => {
                        let existing = self.read_object(url).await?;
                        if existing == bytes {
                            Ok(metadata)
                        } else {
                            Err(ExportStoreError::Conflict)
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn publish(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        let staging_url = self.object_url(export_id, "staging");
        let published_url = self.object_url(export_id, "zip");
        let staged = self.read_object(staging_url.clone()).await?;
        match self.read_object(published_url.clone()).await {
            Ok(published) => {
                if published != staged {
                    return Err(ExportStoreError::Conflict);
                }
            }
            Err(ExportStoreError::NotFound) => {
                match self.put_if_absent(published_url.clone(), &staged).await {
                    Ok(()) => {}
                    Err(ExportStoreError::Conflict) => {
                        let published = self.read_object(published_url).await?;
                        if published != staged {
                            return Err(ExportStoreError::Conflict);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        self.delete_object(staging_url).await
    }

    async fn read(&self, export_id: ExportId) -> Result<Vec<u8>, ExportStoreError> {
        self.read_object(self.object_url(export_id, "zip")).await
    }

    async fn discard_staging(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        self.delete_object(self.object_url(export_id, "staging"))
            .await
    }

    async fn discard_published(&self, export_id: ExportId) -> Result<(), ExportStoreError> {
        self.delete_object(self.object_url(export_id, "zip")).await
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
        package: Box<dyn ExportPackage>,
    ) -> Result<ExportPackageMetadata, ExportStoreError> {
        let bytes = package.as_bytes().map_err(map_package_write_error)?;
        let metadata = ExportPackageMetadata {
            content_sha256: sha256_hex(&bytes),
            size_bytes: bytes.len() as u64,
            record_count: u64::try_from(package.record_count())
                .map_err(|_| ExportPackageError::TooLarge)
                .map_err(map_package_write_error)?,
        };
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
        if let Some(published) = state.published.get(&export_id) {
            let Some(staged) = state.staging.get(&export_id) else {
                return Err(ExportStoreError::NotFound);
            };
            if published != staged {
                return Err(ExportStoreError::Conflict);
            }
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
        package: Box<dyn ExportPackage>,
    ) -> Result<ExportPackageMetadata, ExportStoreError> {
        let root = self.root.clone();
        let staging = self.path(export_id, "staging");
        let temporary = self.path(export_id, "staging.tmp");
        let bytes = package.as_bytes().map_err(map_package_write_error)?;
        let metadata = ExportPackageMetadata {
            content_sha256: sha256_hex(&bytes),
            size_bytes: bytes.len() as u64,
            record_count: u64::try_from(package.record_count())
                .map_err(|_| ExportPackageError::TooLarge)
                .map_err(map_package_write_error)?,
        };
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
                file.write_all(&bytes)
                    .map_err(|_| ExportStoreError::Unavailable)?;
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
                let staged_bytes = std::fs::read(&staging).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        ExportStoreError::NotFound
                    } else {
                        ExportStoreError::Unavailable
                    }
                })?;
                let published_bytes =
                    std::fs::read(&published).map_err(|_| ExportStoreError::Unavailable)?;
                if staged_bytes != published_bytes {
                    return Err(ExportStoreError::Conflict);
                }
                std::fs::remove_file(staging).map_err(|_| ExportStoreError::Unavailable)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[derive(Clone)]
    struct TestPackage {
        bytes: Vec<u8>,
        record_count: usize,
    }

    impl TestPackage {
        fn new(record_count: usize) -> Self {
            Self {
                bytes: format!("test-package-{record_count}-{}", Uuid::now_v7()).into_bytes(),
                record_count,
            }
        }
    }

    impl ExportPackage for TestPackage {
        fn as_bytes(&self) -> Result<Vec<u8>, ExportPackageError> {
            Ok(self.bytes.clone())
        }

        fn record_count(&self) -> usize {
            self.record_count
        }
    }

    #[tokio::test]
    async fn in_memory_package_store_publishes_only_after_staging() {
        let package = TestPackage::new(0);
        let export_id = ExportId(Uuid::now_v7());
        let store = InMemoryExportPackageStore::default();

        assert!(matches!(
            store.read(export_id).await,
            Err(ExportStoreError::NotFound)
        ));
        let metadata = store
            .stage(export_id, Box::new(package.clone()))
            .await
            .unwrap();
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

    #[tokio::test]
    async fn in_memory_package_store_rejects_a_different_published_object() {
        let first = TestPackage::new(1);
        let second = TestPackage::new(2);
        let export_id = ExportId(Uuid::now_v7());
        let store = InMemoryExportPackageStore::default();

        store
            .stage(export_id, Box::new(first.clone()))
            .await
            .unwrap();
        store.publish(export_id).await.unwrap();
        store
            .stage(export_id, Box::new(second.clone()))
            .await
            .unwrap();

        assert!(matches!(
            store.publish(export_id).await,
            Err(ExportStoreError::Conflict)
        ));
        assert_eq!(
            store.read(export_id).await.unwrap(),
            first.as_bytes().unwrap()
        );
    }

    #[tokio::test]
    async fn file_package_store_rejects_a_different_published_object() {
        let first = TestPackage::new(3);
        let second = TestPackage::new(4);
        let root = std::env::temp_dir().join(format!("palimpsest-export-{}", Uuid::now_v7()));
        let export_id = ExportId(Uuid::now_v7());
        let store = FileExportPackageStore::new(&root);

        store
            .stage(export_id, Box::new(first.clone()))
            .await
            .unwrap();
        store.publish(export_id).await.unwrap();
        store
            .stage(export_id, Box::new(second.clone()))
            .await
            .unwrap();

        assert!(matches!(
            store.publish(export_id).await,
            Err(ExportStoreError::Conflict)
        ));
        assert_eq!(
            store.read(export_id).await.unwrap(),
            first.as_bytes().unwrap()
        );

        store.discard_staging(export_id).await.unwrap();
        store.discard_published(export_id).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[derive(Clone, Default)]
    struct FakeS3State {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        signed_requests: Arc<Mutex<Vec<String>>>,
    }

    async fn fake_s3_handler(
        axum::extract::State(state): axum::extract::State<FakeS3State>,
        request: axum::extract::Request,
    ) -> axum::response::Response {
        use axum::{body::to_bytes, response::IntoResponse};

        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let if_none_match = request
            .headers()
            .get(reqwest::header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let signed = request
            .headers()
            .contains_key(reqwest::header::AUTHORIZATION)
            && request.headers().contains_key("x-amz-content-sha256")
            && request.headers().contains_key("x-amz-date");
        state
            .signed_requests
            .lock()
            .unwrap()
            .push(format!("{} {} signed={signed}", method, path));
        let body = to_bytes(request.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec();
        let mut objects = state.objects.lock().unwrap();
        match method {
            axum::http::Method::PUT => {
                if if_none_match.as_deref() == Some("*") && objects.contains_key(&path) {
                    return axum::http::StatusCode::PRECONDITION_FAILED.into_response();
                }
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
    async fn s3_package_store_signs_requests_and_recovers_idempotently() {
        use axum::{routing::any, Router};

        let state = FakeS3State::default();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(fake_s3_handler))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = S3ExportPackageStoreConfig::new(
            format!("http://{address}"),
            "palimpsest-test",
            "exports",
            "us-east-1",
            "access-key",
            "secret-key",
            None,
        )
        .unwrap();
        let store = S3ExportPackageStore::from_config(config);
        let package = TestPackage::new(5);
        let export_id = ExportId(Uuid::now_v7());

        store
            .stage(export_id, Box::new(package.clone()))
            .await
            .unwrap();
        store.publish(export_id).await.unwrap();
        assert_eq!(
            store.read(export_id).await.unwrap(),
            package.as_bytes().unwrap()
        );

        // A retry after a crash between publication and staging cleanup sees
        // the same published bytes and safely removes the duplicate staging object.
        store
            .stage(export_id, Box::new(package.clone()))
            .await
            .unwrap();
        store.publish(export_id).await.unwrap();
        assert_eq!(
            store.read(export_id).await.unwrap(),
            package.as_bytes().unwrap()
        );

        // A second publish of a different object is rejected by the store,
        // and a stale staged object does not overwrite the published one.
        let second = TestPackage::new(6);
        store
            .stage(export_id, Box::new(second.clone()))
            .await
            .unwrap();
        assert!(matches!(
            store.publish(export_id).await,
            Err(ExportStoreError::Conflict)
        ));
        assert_eq!(
            store.read(export_id).await.unwrap(),
            package.as_bytes().unwrap()
        );

        // Every request carried the SigV4 authorization headers.
        store.discard_published(export_id).await.unwrap();
        let signed = state.signed_requests.lock().unwrap();
        assert!(!signed.is_empty());
        assert!(signed.iter().all(|entry| entry.ends_with(" signed=true")));
        drop(signed);

        server.abort();
    }

    #[test]
    fn s3_configuration_rejects_unsafe_endpoint_and_prefix() {
        assert!(matches!(
            S3ExportPackageStoreConfig::new(
                "https://access:secret@example.invalid",
                "bucket",
                "exports",
                "us-east-1",
                "access",
                "secret",
                None,
            ),
            Err(S3ExportPackageStoreConfigError::InvalidEndpoint)
        ));
        assert!(matches!(
            S3ExportPackageStoreConfig::new(
                "https://example.invalid",
                "bucket",
                "exports/../private",
                "us-east-1",
                "access",
                "secret",
                None,
            ),
            Err(S3ExportPackageStoreConfigError::InvalidPrefix)
        ));
    }

    #[test]
    fn s3_sigv4_authorization_is_deterministic() {
        use time::format_description::well_known::Rfc3339;

        let config = S3ExportPackageStoreConfig::new(
            "https://s3.example.test",
            "bucket",
            "exports",
            "us-east-1",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            None,
        )
        .unwrap();
        let store = S3ExportPackageStore::from_config(config);
        let url = store.object_url(ExportId(Uuid::from_u128(12)), "zip");
        let timestamp = OffsetDateTime::parse("2026-08-03T00:00:00Z", &Rfc3339).unwrap();
        let request = store
            .signed_request(Method::GET, &url, &[], None, timestamp)
            .build()
            .unwrap();
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260803/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0a34a84c0f19666c79fbd4af9cbbc0fc95eb1fcd9c48e1271c18498f73f0575f"
        );
    }
}
