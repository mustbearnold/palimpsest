//! Proactive surfacing (spec 012): the surface seam types and the
//! repository contract. The seam is a synchronous read operation (D1).
//! A surface request returns a bounded, explained bundle. The service
//! stores the response for idempotent replay, exactly like the recall
//! contract (spec 002).

use async_trait::async_trait;
use palimpsest_domain::{FactId, PrincipalId, RevisionId, Sensitivity, SubjectId, TenantId};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

pub const SURFACE_DEFAULT_MAX_ITEMS: i16 = 20;
pub const SURFACE_DEFAULT_MAX_CONTEXT_TOKENS: i32 = 4096;
pub const SURFACE_DEFAULT_MAX_RESULT_TOKENS: i32 = 2048;
pub const SURFACE_MAX_CONTEXT_TERMS: usize = 32;
pub const SURFACE_MAX_TERM_LENGTH: usize = 512;
pub const SURFACE_MAX_ITEMS: i16 = 50;

/// The digest of a surface request body. The repository stores it beside
/// the idempotency key. A reused key with a different digest is a 409
/// IdempotencyKeyReused (A8).
pub fn surface_request_fingerprint(request: &NewSurfaceRequest) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(&json!({
            "host_id": request.host_id,
            "principal_id": request.principal_id,
            "context_terms": request.context_terms,
        }))
        .expect("surface request fingerprint is serializable"),
    ))
}

#[derive(Debug, Clone, Serialize)]
pub struct SurfacePolicyView {
    pub tenant_id: TenantId,
    pub host_id: String,
    pub principal_id: String,
    pub enabled: bool,
    pub max_items: i16,
    pub max_context_tokens: i32,
    pub max_result_tokens: i32,
    pub sensitivity_ceiling: Option<String>,
    pub window_from: Option<OffsetDateTime>,
    pub window_until: Option<OffsetDateTime>,
    pub schema_version: i32,
}

#[derive(Debug, Clone)]
pub struct NewSurfacePolicy {
    pub host_id: String,
    pub principal_id: String,
    pub enabled: bool,
    pub max_items: i16,
    pub max_context_tokens: i32,
    pub max_result_tokens: i32,
    pub sensitivity_ceiling: Option<String>,
    pub window_from: Option<OffsetDateTime>,
    pub window_until: Option<OffsetDateTime>,
    pub created_by_principal_id: PrincipalId,
}

#[derive(Debug, Clone)]
pub struct NewSurfaceRequest {
    pub host_id: String,
    pub principal_id: String,
    pub context_terms: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SurfaceBundle {
    pub surface_id: Uuid,
    pub subject_id: SubjectId,
    pub host_id: String,
    pub principal_id: String,
    pub evaluated_at: OffsetDateTime,
    pub policy_sha256: Option<String>,
    pub item_count: i16,
    pub truncated: bool,
    pub context_terms_used: Vec<String>,
    pub items: Vec<SurfaceBundleItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SurfaceBundleItem {
    pub ordinal: i16,
    pub case_id: Uuid,
    pub fact_id: FactId,
    pub revision_id: RevisionId,
    pub namespace: String,
    pub fact_key: String,
    pub value: serde_json::Value,
    pub confidence: f64,
    pub sensitivity: String,
    pub lexical_score: f64,
    pub content_sha256: String,
    pub item_sha256: String,
}

impl SurfaceBundle {
    /// The canonical digest of the bundle. The repository stores it for
    /// integrity; a replay returns the stored bundle verbatim (A8).
    pub fn bundle_sha256(&self) -> String {
        hex::encode(Sha256::digest(
            serde_json::to_vec(self).expect("surface bundle is serializable"),
        ))
    }
}

#[derive(Debug)]
pub struct CreateSurfaceOutcome {
    pub bundle: SurfaceBundle,
    pub replayed: bool,
}

#[async_trait]
pub trait SurfaceRepository: Send + Sync {
    async fn register_policy(
        &self,
        tenant_id: TenantId,
        request: NewSurfacePolicy,
    ) -> Result<SurfacePolicyView, crate::RepositoryError>;

    async fn get_policy(
        &self,
        tenant_id: TenantId,
        host_id: &str,
        principal_id: &str,
    ) -> Result<SurfacePolicyView, crate::RepositoryError>;

    async fn create_surface(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: &NewSurfaceRequest,
        allowed_sensitivities: &[Sensitivity],
        idempotency: crate::IdempotencyRequest,
    ) -> Result<CreateSurfaceOutcome, crate::RepositoryError>;

    async fn get_surface(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        surface_id: Uuid,
    ) -> Result<SurfaceBundle, crate::RepositoryError>;
}
