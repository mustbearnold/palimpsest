//! fixtures — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use std::{
    collections::HashSet,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use palimpsest_application::{
    EmbeddingProvider, EmbeddingProviderError, EmbeddingRequest, EmbeddingResponse,
    ExportWorkerAuthorizer, ServiceError,
};
use palimpsest_domain::{
    EmbeddingOutput, EmbeddingTask, PrincipalId, PrincipalScope, SubjectId, TenantId,
};
use palimpsest_http::{Authenticator, StaticAuthenticator};

use tokio::sync::Notify;
use uuid::Uuid;

pub(crate) static PROVIDER_APPLICATIONS: AtomicUsize = AtomicUsize::new(0);

pub(crate) static PROVIDER_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

pub(crate) static PROVIDER_EFFECTS: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Clone, Copy, Debug)]
pub(crate) struct RestoreFixture {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) episode_id: Uuid,
}

pub(crate) struct StaticExportWorkerAuthorizer {
    pub(crate) authenticator: Arc<StaticAuthenticator>,
}

pub(crate) struct DenyingExportWorkerAuthorizer;

impl ExportWorkerAuthorizer for DenyingExportWorkerAuthorizer {
    fn authorize_export(
        &self,
        _principal_id: &PrincipalId,
        _tenant_id: TenantId,
        _subject_id: SubjectId,
        _authorization_scope_sha256: &str,
    ) -> std::result::Result<PrincipalScope, ServiceError> {
        Err(ServiceError::NotFound)
    }
}

impl ExportWorkerAuthorizer for StaticExportWorkerAuthorizer {
    fn authorize_export(
        &self,
        principal_id: &PrincipalId,
        tenant_id: TenantId,
        subject_id: SubjectId,
        authorization_scope_sha256: &str,
    ) -> std::result::Result<PrincipalScope, ServiceError> {
        self.authenticator
            .authorize_export_worker(
                principal_id,
                tenant_id,
                subject_id,
                authorization_scope_sha256,
            )
            .ok_or(ServiceError::NotFound)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum EmbeddingFixtureMode {
    Valid = 0,
    Unavailable = 1,
    MissingOutput = 2,
    WrongProfileDigest = 3,
    WrongInputDigest = 4,
    NonFinite = 5,
    WrongDimensions = 6,
    ZeroNorm = 7,
    OutsideNormalizationTolerance = 8,
}

impl EmbeddingFixtureMode {
    fn from_usize(value: usize) -> Self {
        match value {
            0 => Self::Valid,
            1 => Self::Unavailable,
            2 => Self::MissingOutput,
            3 => Self::WrongProfileDigest,
            4 => Self::WrongInputDigest,
            5 => Self::NonFinite,
            6 => Self::WrongDimensions,
            7 => Self::ZeroNorm,
            8 => Self::OutsideNormalizationTolerance,
            _ => panic!("unknown embedding fixture mode {value}"),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct DeterministicEmbeddingProvider {
    mode: AtomicUsize,
    calls: AtomicUsize,
}

#[derive(Debug, Default)]
pub(crate) struct BlockingEmbeddingProvider {
    pub(crate) calls: AtomicUsize,
    pub(crate) release: Notify,
}

#[async_trait]
impl EmbeddingProvider for BlockingEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> std::result::Result<EmbeddingResponse, EmbeddingProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(EmbeddingResponse {
            profile_digest: request.profile.digest,
            outputs: request
                .inputs
                .iter()
                .map(|input| EmbeddingOutput {
                    input_sha256: input.input_sha256.clone(),
                    values: fixture_embedding(&request.task, &input.content),
                })
                .collect(),
        })
    }
}

impl DeterministicEmbeddingProvider {
    pub(crate) fn set_mode(&self, mode: EmbeddingFixtureMode) {
        self.mode.store(mode as usize, Ordering::SeqCst);
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EmbeddingProvider for DeterministicEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> std::result::Result<EmbeddingResponse, EmbeddingProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = EmbeddingFixtureMode::from_usize(self.mode.load(Ordering::SeqCst));
        if matches!(mode, EmbeddingFixtureMode::Unavailable) {
            return Err(EmbeddingProviderError::Unavailable {
                code: "fixture-provider-outage-private-vector-[1,0,0,0]".to_owned(),
            });
        }

        assert_eq!(request.profile.id, "embedding-conformance-4d-v1");
        assert_eq!(request.profile.version, "1");
        assert_eq!(request.profile.provider, "palimpsest-conformance");
        assert_eq!(request.profile.model, "deterministic-fixture");
        assert_eq!(request.profile.model_revision, "fixture-4d-2026-07-29");
        assert_eq!(request.profile.dimensions, 4);
        assert_eq!(request.profile.normalization, "unit_l2");
        assert!((request.profile.normalization_tolerance - 0.000001).abs() < f64::EPSILON);
        assert_eq!(request.profile.distance_metric, "cosine");
        assert_eq!(request.profile.scalar_type, "float32");
        assert_eq!(request.profile.input_serialization, "utf8");
        assert_eq!(request.profile.query_task, "query");
        assert_eq!(request.profile.document_task, "document");
        assert_eq!(request.profile.provider_contract_schema_version, 1);
        assert_eq!(request.profile.digest.len(), 64);

        let mut outputs = request
            .inputs
            .iter()
            .map(|input| EmbeddingOutput {
                input_sha256: input.input_sha256.clone(),
                values: fixture_embedding(&request.task, &input.content),
            })
            .collect::<Vec<_>>();
        match mode {
            EmbeddingFixtureMode::Valid | EmbeddingFixtureMode::Unavailable => {}
            EmbeddingFixtureMode::MissingOutput => {
                outputs.pop();
            }
            EmbeddingFixtureMode::WrongProfileDigest => {}
            EmbeddingFixtureMode::WrongInputDigest => {
                if let Some(output) = outputs.first_mut() {
                    output.input_sha256 = "0".repeat(64);
                }
            }
            EmbeddingFixtureMode::NonFinite => {
                if let Some(output) = outputs.first_mut() {
                    output.values[0] = f32::NAN;
                }
            }
            EmbeddingFixtureMode::WrongDimensions => {
                if let Some(output) = outputs.first_mut() {
                    output.values.pop();
                }
            }
            EmbeddingFixtureMode::ZeroNorm => {
                if let Some(output) = outputs.first_mut() {
                    output.values = vec![0.0; 4];
                }
            }
            EmbeddingFixtureMode::OutsideNormalizationTolerance => {
                if let Some(output) = outputs.first_mut() {
                    output.values = vec![1.00001, 0.0, 0.0, 0.0];
                }
            }
        }
        Ok(EmbeddingResponse {
            profile_digest: if matches!(mode, EmbeddingFixtureMode::WrongProfileDigest) {
                "0".repeat(64)
            } else {
                request.profile.digest
            },
            outputs,
        })
    }
}

pub(crate) fn fixture_embedding(task: &EmbeddingTask, content: &str) -> Vec<f32> {
    if matches!(task, EmbeddingTask::Query) {
        if content.contains("palimpsest-") {
            return vec![1.0, 0.0, 0.0, 0.0];
        }
        assert!(
            matches!(
                content,
                "case.retrieval:fusiontoken" | "case.temporal:chronotoken"
            ),
            "unexpected conformance query embedding input"
        );
        return vec![1.0, 0.0, 0.0, 0.0];
    }
    if content.contains("embedding_fixture_role") {
        if content.contains("\"relevant\"") || content.contains("\"trap\"") {
            return vec![1.0, 0.0, 0.0, 0.0];
        }
        if content.contains("\"near\"") {
            return vec![0.8, 0.6, 0.0, 0.0];
        }
        if content.contains("\"distractor\"") {
            return vec![0.0, 1.0, 0.0, 0.0];
        }
    }
    for (marker, vector) in [
        ("vector_fixture_forbidden_4d", [1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_exact_4d", [-1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_alpha_4d", [-0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_beta_4d", [0.8, 0.6, 0.0, 0.0]),
        ("vector_fixture_gamma_4d", [0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_delta_4d", [0.0, 1.0, 0.0, 0.0]),
        ("temporal_vector_fixture_exact_4d", [-1.0, 0.0, 0.0, 0.0]),
        ("temporal_vector_fixture_alpha_4d", [-0.6, 0.8, 0.0, 0.0]),
        ("temporal_vector_fixture_beta_4d", [0.8, 0.6, 0.0, 0.0]),
        ("temporal_vector_fixture_gamma_4d", [0.6, 0.8, 0.0, 0.0]),
        ("temporal_vector_fixture_delta_4d", [0.0, 1.0, 0.0, 0.0]),
    ] {
        if content.contains(marker) {
            return vector.to_vec();
        }
    }
    vec![0.0, 0.0, 0.0, 1.0]
}
