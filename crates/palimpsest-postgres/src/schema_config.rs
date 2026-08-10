//! schema_config — tenant-owned versioned wiki schema configuration
//! persistence (spec 017 P4, AC10). The amendment is a governed write:
//! the write policy must be registered (001 R9), and the amending
//! principal is recorded. Old versions stay retrievable.

use async_trait::async_trait;
use palimpsest_application::{
    NewSchemaConfig, RepositoryError, SchemaConfigRepository, SchemaConfigView,
};
use sqlx::Row;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, unexpected};
use sha2::{Digest, Sha256};

fn map_schema_config_sqlx(error: sqlx::Error) -> RepositoryError {
    match error {
        sqlx::Error::RowNotFound => RepositoryError::NotFound,
        other => RepositoryError::Unexpected(other.to_string()),
    }
}

fn schema_config_view_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<SchemaConfigView, RepositoryError> {
    Ok(SchemaConfigView {
        tenant_id: palimpsest_domain::TenantId(row.try_get("tenant_id").map_err(unexpected)?),
        schema_version: row.try_get("schema_version").map_err(unexpected)?,
        config: row.try_get("config").map_err(unexpected)?,
        amended_by_principal_id: row.try_get("amended_by_principal_id").map_err(unexpected)?,
        supersedes_version: row.try_get("supersedes_version").map_err(unexpected)?,
        created_at: row.try_get("created_at").map_err(unexpected)?,
    })
}

#[async_trait]
impl SchemaConfigRepository for PostgresMemoryRepository {
    async fn amend(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        request: NewSchemaConfig,
        idempotency: palimpsest_application::IdempotencyRequest,
        amended_by_principal_id: String,
    ) -> Result<SchemaConfigView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(
            &mut transaction,
            tenant_id,
            palimpsest_domain::SubjectId(uuid::Uuid::nil()),
        )
        .await?;

        // The amendment is a governed write: the write policy must be
        // registered (001 R9). An unregistered policy fails closed.
        let policy_known: Option<(String, String)> = sqlx::query_as(
            r#"
            SELECT policy_id, policy_version
            FROM memory.fact_retrieval_metadata_policies
            WHERE policy_id = $1 AND policy_version = $2
            "#,
        )
        .bind(request.write_policy.id.as_str())
        .bind(request.write_policy.version.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?;
        if policy_known.is_none() {
            return Err(RepositoryError::WritePolicyRejected);
        }

        let key_digest = hex::encode(Sha256::digest(idempotency.key.as_bytes()));
        let current: i32 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(schema_version), 0)
            FROM memory.wiki_schema_configs
            WHERE tenant_id = $1
            "#,
        )
        .bind(tenant_id.0)
        .fetch_one(&mut *transaction)
        .await
        .map_err(unexpected)?;
        let next_version = current + 1;

        let row = sqlx::query(
            r#"
            INSERT INTO memory.wiki_schema_configs (
                tenant_id, schema_version, config, amended_by_principal_id,
                supersedes_version, write_policy_id, write_policy_version,
                idempotency_key_digest, request_fingerprint
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, schema_version) DO NOTHING
            RETURNING tenant_id, schema_version, config, amended_by_principal_id,
                supersedes_version, created_at
            "#,
        )
        .bind(tenant_id.0)
        .bind(next_version)
        .bind(&request.config)
        .bind(&amended_by_principal_id)
        .bind((current > 0).then_some(current))
        .bind(request.write_policy.id.as_str())
        .bind(request.write_policy.version.as_str())
        .bind(&key_digest)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_schema_config_sqlx)?;

        let view = if let Some(row) = row {
            schema_config_view_from_row(&row)?
        } else {
            // Idempotent replay: the amendment for this key already landed.
            let existing = sqlx::query(
                r#"
                SELECT tenant_id, schema_version, config, amended_by_principal_id,
                    supersedes_version, created_at
                FROM memory.wiki_schema_configs
                WHERE tenant_id = $1
                  AND idempotency_key_digest = $2
                ORDER BY schema_version DESC
                LIMIT 1
                "#,
            )
            .bind(tenant_id.0)
            .bind(&key_digest)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(map_schema_config_sqlx)?
            .ok_or(RepositoryError::Conflict)?;
            schema_config_view_from_row(&existing)?
        };
        transaction.commit().await.map_err(unexpected)?;
        Ok(view)
    }

    async fn get(
        &self,
        tenant_id: palimpsest_domain::TenantId,
        schema_version: i32,
    ) -> Result<SchemaConfigView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(
            &mut transaction,
            tenant_id,
            palimpsest_domain::SubjectId(uuid::Uuid::nil()),
        )
        .await?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, schema_version, config, amended_by_principal_id,
                supersedes_version, created_at
            FROM memory.wiki_schema_configs
            WHERE tenant_id = $1 AND schema_version = $2
            "#,
        )
        .bind(tenant_id.0)
        .bind(schema_version)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_schema_config_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        schema_config_view_from_row(&row)
    }

    async fn get_current(
        &self,
        tenant_id: palimpsest_domain::TenantId,
    ) -> Result<SchemaConfigView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(
            &mut transaction,
            tenant_id,
            palimpsest_domain::SubjectId(uuid::Uuid::nil()),
        )
        .await?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, schema_version, config, amended_by_principal_id,
                supersedes_version, created_at
            FROM memory.wiki_schema_configs
            WHERE tenant_id = $1
            ORDER BY schema_version DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_schema_config_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        schema_config_view_from_row(&row)
    }
}
