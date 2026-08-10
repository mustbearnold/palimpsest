//! schema_config — tenant-owned versioned wiki schema configuration
//! (spec 017 P4, AC10).
//!
//! The schema configuration is tenant-owned and versioned (R11). A schema
//! amendment is a governed write: it carries a registered write policy
//! (001 R9) and records the amending principal. Old versions stay
//! retrievable. The registry follows the surface-policy pattern: RLS
//! FORCE, scope GUCs, register + get routes.

use palimpsest_domain::{TenantId, WritePolicy};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewSchemaConfig {
    pub config: serde_json::Value,
    pub write_policy: WritePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfigView {
    pub tenant_id: TenantId,
    pub schema_version: i32,
    pub config: serde_json::Value,
    pub amended_by_principal_id: String,
    pub supersedes_version: Option<i32>,
    pub created_at: OffsetDateTime,
}

#[async_trait::async_trait]
pub trait SchemaConfigRepository: Send + Sync {
    /// Amends the tenant schema configuration: creates the next version
    /// (or version 1 when none exists). The amendment is governed: an
    /// unregistered write policy fails closed (WritePolicyRejected).
    async fn amend(
        &self,
        tenant_id: TenantId,
        request: NewSchemaConfig,
        idempotency: crate::IdempotencyRequest,
        amended_by_principal_id: String,
    ) -> Result<SchemaConfigView, crate::RepositoryError>;

    /// Reads one stored schema version.
    async fn get(
        &self,
        tenant_id: TenantId,
        schema_version: i32,
    ) -> Result<SchemaConfigView, crate::RepositoryError>;

    /// Reads the latest schema version for the tenant.
    async fn get_current(
        &self,
        tenant_id: TenantId,
    ) -> Result<SchemaConfigView, crate::RepositoryError>;
}

/// Deterministic idempotency key for a schema amendment.
pub fn schema_amendment_idempotency_key(tenant_id: TenantId, idempotency_key: &str) -> String {
    format!("wiki-schema:{}:{}", tenant_id.0, idempotency_key)
}

/// A fresh schema config view for a tenant with no amendments yet.
pub fn default_schema_config_view(tenant_id: TenantId) -> SchemaConfigView {
    SchemaConfigView {
        tenant_id,
        schema_version: 1,
        config: serde_json::json!({
            "page_format": "palimpsest-wiki-vault-v1",
            "frontmatter": ["title", "namespace", "last-touched"],
        }),
        amended_by_principal_id: "palimpsest-schema-baseline".to_owned(),
        supersedes_version: None,
        created_at: OffsetDateTime::now_utc(),
    }
}

/// Placeholder id for the schema config view type (kept for symmetry with
/// the other view types; the config table has no uuid key).
pub const SCHEMA_CONFIG_SCOPE_ID: Uuid = Uuid::from_u128(0x736368656d615f636f6e6669675f7630);
