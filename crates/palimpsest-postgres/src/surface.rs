//! surface — proactive surfacing (spec 012). The seam is a synchronous
//! read over the authorized current projection, ranked lexically with the
//! same machinery as retrieval (0006), capped by the surface policy,
//! and stored for idempotent replay.

use async_trait::async_trait;
use palimpsest_application::{
    CreateSurfaceOutcome, IdempotencyRequest, NewSurfacePolicy, NewSurfaceRequest, RepositoryError,
    SurfaceBundle, SurfaceBundleItem, SurfacePolicyView, SurfaceRepository,
};
use palimpsest_domain::{FactId, RevisionId, SubjectId, TenantId};
use serde_json::json;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::retrieval::set_scope;
use super::{PostgresMemoryRepository, unexpected};

fn map_surface_sqlx(error: sqlx::Error) -> RepositoryError {
    match error {
        sqlx::Error::RowNotFound => RepositoryError::NotFound,
        other => RepositoryError::Unexpected(other.to_string()),
    }
}

fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

#[allow(clippy::too_many_arguments)]
fn surface_policy_sha256(
    host_id: &str,
    principal_id: &str,
    max_items: i16,
    max_context_tokens: i32,
    max_result_tokens: i32,
    sensitivity_ceiling: &Option<String>,
    window_from: Option<&time::OffsetDateTime>,
    window_until: Option<&time::OffsetDateTime>,
) -> String {
    let canonical = json!({
        "host_id": host_id,
        "principal_id": principal_id,
        "max_items": max_items,
        "max_context_tokens": max_context_tokens,
        "max_result_tokens": max_result_tokens,
        "sensitivity_ceiling": sensitivity_ceiling,
        "window_from": window_from.map(|ts| ts.unix_timestamp_nanos()),
        "window_until": window_until.map(|ts| ts.unix_timestamp_nanos()),
    });
    hex::encode(sha256_bytes(
        serde_json::to_vec(&canonical)
            .expect("canonical surface policy always serializes")
            .as_slice(),
    ))
}

/// Deterministic token estimate (spec 012: caps bound the bundle).
/// One token per four characters, rounded up.
fn estimated_tokens(value: &str) -> usize {
    value.chars().count().div_ceil(4)
}

/// A bounded, deterministic summary of a fact value for the wiki index
/// (spec 017 P4, AC9). The summary is a plain-text projection of the
/// value, capped at WIKI_INDEX_SUMMARY_CHARS; it never embeds raw JSON.
fn summarize_value(value: &serde_json::Value) -> String {
    use palimpsest_application::WIKI_INDEX_SUMMARY_CHARS;
    let text = match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Object(fields) => fields
            .get("summary")
            .or_else(|| fields.get("title"))
            .or_else(|| fields.get("body"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned(),
        _ => String::new(),
    };
    let mut summary = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if summary.chars().count() > WIKI_INDEX_SUMMARY_CHARS {
        summary = summary
            .chars()
            .take(WIKI_INDEX_SUMMARY_CHARS)
            .collect::<String>();
        summary.push('…');
    }
    summary
}

fn surface_policy_view_from_row(row: &PgRow) -> Result<SurfacePolicyView, RepositoryError> {
    Ok(SurfacePolicyView {
        tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
        host_id: row.try_get("host_id").map_err(unexpected)?,
        principal_id: row.try_get("principal_id").map_err(unexpected)?,
        enabled: row.try_get("enabled").map_err(unexpected)?,
        max_items: row.try_get("max_items").map_err(unexpected)?,
        max_context_tokens: row.try_get("max_context_tokens").map_err(unexpected)?,
        max_result_tokens: row.try_get("max_result_tokens").map_err(unexpected)?,
        sensitivity_ceiling: row.try_get("sensitivity_ceiling").map_err(unexpected)?,
        window_from: row.try_get("window_from").map_err(unexpected)?,
        window_until: row.try_get("window_until").map_err(unexpected)?,
        schema_version: row.try_get("schema_version").map_err(unexpected)?,
    })
}

fn surface_item_from_row(row: &PgRow, ordinal: i16) -> Result<SurfaceBundleItem, RepositoryError> {
    let lexical_score: String = row.try_get("lexical_score").map_err(unexpected)?;
    let lexical_score = lexical_score.parse::<f64>().map_err(|error| {
        RepositoryError::Unexpected(format!("surface lexical score is not a number: {error}"))
    })?;
    let confidence: String = row.try_get("confidence").map_err(unexpected)?;
    let confidence = confidence.parse::<f64>().map_err(|error| {
        RepositoryError::Unexpected(format!("surface confidence is not a number: {error}"))
    })?;
    Ok(SurfaceBundleItem {
        ordinal,
        case_id: row.try_get("case_id").map_err(unexpected)?,
        fact_id: FactId(row.try_get("fact_id").map_err(unexpected)?),
        revision_id: RevisionId(row.try_get("revision_id").map_err(unexpected)?),
        namespace: row.try_get("namespace").map_err(unexpected)?,
        fact_key: row.try_get("fact_key").map_err(unexpected)?,
        value: row.try_get("value").map_err(unexpected)?,
        confidence,
        sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
        lexical_score,
        content_sha256: row.try_get("content_sha256").map_err(unexpected)?,
        item_sha256: String::new(),
    })
}

#[async_trait]
impl SurfaceRepository for PostgresMemoryRepository {
    async fn register_policy(
        &self,
        tenant_id: TenantId,
        request: NewSurfacePolicy,
    ) -> Result<SurfacePolicyView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let row = sqlx::query(
            r#"
            INSERT INTO memory.surface_policies (
                tenant_id, host_id, principal_id, enabled,
                max_items, max_context_tokens, max_result_tokens,
                sensitivity_ceiling, window_from, window_until,
                created_by_principal_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id, host_id, principal_id) DO NOTHING
            RETURNING tenant_id, host_id, principal_id, enabled,
                max_items, max_context_tokens, max_result_tokens,
                sensitivity_ceiling, window_from, window_until,
                created_by_principal_id, created_at, updated_at,
                schema_version
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.host_id)
        .bind(&request.principal_id)
        .bind(request.enabled)
        .bind(request.max_items)
        .bind(request.max_context_tokens)
        .bind(request.max_result_tokens)
        .bind(&request.sensitivity_ceiling)
        .bind(request.window_from)
        .bind(request.window_until)
        .bind(request.created_by_principal_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?;
        let row = match row {
            Some(row) => row,
            None => sqlx::query(
                r#"
                    SELECT tenant_id, host_id, principal_id, enabled,
                        max_items, max_context_tokens, max_result_tokens,
                        sensitivity_ceiling, window_from, window_until,
                        created_by_principal_id, created_at, updated_at,
                        schema_version
                    FROM memory.surface_policies
                    WHERE tenant_id = $1 AND host_id = $2 AND principal_id = $3
                    "#,
            )
            .bind(tenant_id.0)
            .bind(&request.host_id)
            .bind(&request.principal_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_surface_sqlx)?,
        };
        transaction.commit().await.map_err(unexpected)?;
        surface_policy_view_from_row(&row)
    }

    async fn get_policy(
        &self,
        tenant_id: TenantId,
        host_id: &str,
        principal_id: &str,
    ) -> Result<SurfacePolicyView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, SubjectId(Uuid::nil())).await?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, host_id, principal_id, enabled,
                max_items, max_context_tokens, max_result_tokens,
                sensitivity_ceiling, window_from, window_until,
                created_by_principal_id, created_at, updated_at,
                schema_version
            FROM memory.surface_policies
            WHERE tenant_id = $1 AND host_id = $2 AND principal_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(host_id)
        .bind(principal_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        surface_policy_view_from_row(&row)
    }

    async fn create_surface(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: &NewSurfaceRequest,
        allowed_sensitivities: &[palimpsest_domain::Sensitivity],
        idempotency: IdempotencyRequest,
    ) -> Result<CreateSurfaceOutcome, RepositoryError> {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => return Err(unexpected(error)),
        };
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let allowed: Vec<String> = allowed_sensitivities
            .iter()
            .map(|sensitivity| sensitivity.as_str().to_owned())
            .collect();

        let policy = sqlx::query(
            r#"
            SELECT host_id, principal_id, enabled,
                max_items, max_context_tokens, max_result_tokens,
                sensitivity_ceiling, window_from, window_until
            FROM memory.surface_policies
            WHERE tenant_id = $1 AND host_id = $2 AND principal_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.host_id)
        .bind(&request.principal_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?;

        let (bundle, policy_sha256) = match policy {
            Some(policy)
                if {
                    let enabled: bool = policy.try_get("enabled").map_err(unexpected)?;
                    enabled
                } =>
            {
                let max_items: i16 = policy.try_get("max_items").map_err(unexpected)?;
                let max_context_tokens: i32 =
                    policy.try_get("max_context_tokens").map_err(unexpected)?;
                let max_result_tokens: i32 =
                    policy.try_get("max_result_tokens").map_err(unexpected)?;
                let sensitivity_ceiling: Option<String> =
                    policy.try_get("sensitivity_ceiling").map_err(unexpected)?;
                let window_from: Option<time::OffsetDateTime> =
                    policy.try_get("window_from").map_err(unexpected)?;
                let window_until: Option<time::OffsetDateTime> =
                    policy.try_get("window_until").map_err(unexpected)?;
                let policy_sha256 = surface_policy_sha256(
                    &request.host_id,
                    &request.principal_id,
                    max_items,
                    max_context_tokens,
                    max_result_tokens,
                    &sensitivity_ceiling,
                    window_from.as_ref(),
                    window_until.as_ref(),
                );
                let bundle = self
                    .evaluate_surface(
                        &mut transaction,
                        tenant_id,
                        subject_id,
                        &request.host_id,
                        &request.principal_id,
                        request,
                        &allowed,
                        &sensitivity_ceiling,
                        window_from,
                        window_until,
                        max_items,
                        max_context_tokens,
                        max_result_tokens,
                    )
                    .await?;
                (bundle, Some(policy_sha256))
            }
            _ => (
                SurfaceBundle {
                    surface_id: Uuid::now_v7(),
                    subject_id,
                    host_id: request.host_id.clone(),
                    principal_id: request.principal_id.clone(),
                    evaluated_at: time::OffsetDateTime::now_utc(),
                    policy_sha256: None,
                    item_count: 0,
                    truncated: false,
                    context_terms_used: request.context_terms.clone(),
                    items: Vec::new(),
                },
                None,
            ),
        };

        let bundle_sha256 = bundle.bundle_sha256();
        let key_digest = hex::encode(sha256_bytes(idempotency.key.as_bytes()));
        let row = sqlx::query(
            r#"
            INSERT INTO memory.surface_responses (
                tenant_id, subject_id, surface_id, host_id, principal_id,
                idempotency_key_digest, request_fingerprint, policy_sha256,
                bundle_sha256, item_count, truncated, context_terms,
                evaluated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id, host_id, principal_id, idempotency_key_digest)
                DO NOTHING
            RETURNING surface_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(bundle.surface_id)
        .bind(&bundle.host_id)
        .bind(&bundle.principal_id)
        .bind(&key_digest)
        .bind(&idempotency.fingerprint)
        .bind(&policy_sha256)
        .bind(&bundle_sha256)
        .bind(i16::try_from(bundle.items.len()).map_err(unexpected)?)
        .bind(bundle.truncated)
        .bind(&bundle.context_terms_used)
        .bind(bundle.evaluated_at)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?;

        if row.is_some() {
            for item in &bundle.items {
                sqlx::query(
                    r#"
                    INSERT INTO memory.surface_response_items (
                        tenant_id, subject_id, surface_id, ordinal,
                        case_id, fact_id, revision_id, namespace, fact_key,
                        value, confidence, sensitivity, lexical_score,
                        content_sha256, item_sha256
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    "#,
                )
                .bind(tenant_id.0)
                .bind(subject_id.0)
                .bind(bundle.surface_id)
                .bind(item.ordinal)
                .bind(item.case_id)
                .bind(item.fact_id.0)
                .bind(item.revision_id.0)
                .bind(&item.namespace)
                .bind(&item.fact_key)
                .bind(&item.value)
                .bind(item.confidence)
                .bind(&item.sensitivity)
                .bind(item.lexical_score)
                .bind(&item.content_sha256)
                .bind(&item.item_sha256)
                .execute(&mut *transaction)
                .await
                .map_err(map_surface_sqlx)?;
            }
            transaction.commit().await.map_err(unexpected)?;
            return Ok(CreateSurfaceOutcome {
                bundle,
                replayed: false,
            });
        }

        let existing = sqlx::query(
            r#"
            SELECT request_fingerprint, surface_id
            FROM memory.surface_responses
            WHERE tenant_id = $1
              AND host_id = $2
              AND principal_id = $3
              AND idempotency_key_digest = $4
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.host_id)
        .bind(&request.principal_id)
        .bind(&key_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?
        .ok_or(RepositoryError::Unexpected(
            "surface idempotency replay lost its reservation".to_owned(),
        ))?;
        let stored_fingerprint: String = existing
            .try_get("request_fingerprint")
            .map_err(unexpected)?;
        if stored_fingerprint != idempotency.fingerprint {
            return Err(RepositoryError::IdempotencyKeyReused);
        }
        let stored_surface_id: Uuid = existing.try_get("surface_id").map_err(unexpected)?;
        let replayed = self
            .read_stored_surface(&mut transaction, tenant_id, subject_id, stored_surface_id)
            .await?
            .ok_or(RepositoryError::Unexpected(
                "surface idempotency replay lost its stored bundle".to_owned(),
            ))?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(CreateSurfaceOutcome {
            bundle: replayed,
            replayed: true,
        })
    }

    async fn get_surface(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        surface_id: Uuid,
    ) -> Result<SurfaceBundle, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let bundle = self
            .read_stored_surface(&mut transaction, tenant_id, subject_id, surface_id)
            .await?
            .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(bundle)
    }

    async fn create_index_surface(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        request: &palimpsest_application::NewIndexSurfaceRequest,
        allowed_sensitivities: &[palimpsest_domain::Sensitivity],
        idempotency: IdempotencyRequest,
    ) -> Result<CreateSurfaceOutcome, RepositoryError> {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => return Err(unexpected(error)),
        };
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let allowed: Vec<String> = allowed_sensitivities
            .iter()
            .map(|sensitivity| sensitivity.as_str().to_owned())
            .collect();

        let policy = sqlx::query(
            r#"
            SELECT host_id, principal_id, enabled,
                max_items, max_context_tokens, max_result_tokens,
                sensitivity_ceiling, window_from, window_until
            FROM memory.surface_policies
            WHERE tenant_id = $1 AND host_id = $2 AND principal_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.host_id)
        .bind(&request.principal_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?;

        let (bundle, policy_sha256) = match policy {
            Some(policy)
                if {
                    let enabled: bool = policy.try_get("enabled").map_err(unexpected)?;
                    enabled
                } =>
            {
                let max_items: i16 = policy.try_get("max_items").map_err(unexpected)?;
                let max_context_tokens: i32 =
                    policy.try_get("max_context_tokens").map_err(unexpected)?;
                let max_result_tokens: i32 =
                    policy.try_get("max_result_tokens").map_err(unexpected)?;
                let sensitivity_ceiling: Option<String> =
                    policy.try_get("sensitivity_ceiling").map_err(unexpected)?;
                let window_from: Option<time::OffsetDateTime> =
                    policy.try_get("window_from").map_err(unexpected)?;
                let window_until: Option<time::OffsetDateTime> =
                    policy.try_get("window_until").map_err(unexpected)?;
                let policy_sha256 = surface_policy_sha256(
                    &request.host_id,
                    &request.principal_id,
                    max_items,
                    max_context_tokens,
                    max_result_tokens,
                    &sensitivity_ceiling,
                    window_from.as_ref(),
                    window_until.as_ref(),
                );
                let bundle = self
                    .evaluate_index_surface(
                        &mut transaction,
                        tenant_id,
                        subject_id,
                        &request.host_id,
                        &request.principal_id,
                        &allowed,
                        &sensitivity_ceiling,
                        window_from,
                        window_until,
                        max_items,
                        max_result_tokens,
                    )
                    .await?;
                (bundle, Some(policy_sha256))
            }
            _ => (
                SurfaceBundle {
                    surface_id: Uuid::now_v7(),
                    subject_id,
                    host_id: request.host_id.clone(),
                    principal_id: request.principal_id.clone(),
                    evaluated_at: time::OffsetDateTime::now_utc(),
                    policy_sha256: None,
                    item_count: 0,
                    truncated: false,
                    context_terms_used: Vec::new(),
                    items: Vec::new(),
                },
                None,
            ),
        };

        let bundle_sha256 = bundle.bundle_sha256();
        let key_digest = hex::encode(sha256_bytes(idempotency.key.as_bytes()));
        let row = sqlx::query(
            r#"
            INSERT INTO memory.surface_responses (
                tenant_id, subject_id, surface_id, host_id, principal_id,
                idempotency_key_digest, request_fingerprint, policy_sha256,
                bundle_sha256, item_count, truncated, context_terms,
                evaluated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id, host_id, principal_id, idempotency_key_digest)
                DO NOTHING
            RETURNING surface_id
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(bundle.surface_id)
        .bind(&bundle.host_id)
        .bind(&bundle.principal_id)
        .bind(&key_digest)
        .bind(&idempotency.fingerprint)
        .bind(&policy_sha256)
        .bind(&bundle_sha256)
        .bind(i16::try_from(bundle.items.len()).map_err(unexpected)?)
        .bind(bundle.truncated)
        .bind(&bundle.context_terms_used)
        .bind(bundle.evaluated_at)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?;

        if row.is_some() {
            for item in &bundle.items {
                sqlx::query(
                    r#"
                    INSERT INTO memory.surface_response_items (
                        tenant_id, subject_id, surface_id, ordinal,
                        case_id, fact_id, revision_id, namespace, fact_key,
                        value, confidence, sensitivity, lexical_score,
                        content_sha256, item_sha256
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    "#,
                )
                .bind(tenant_id.0)
                .bind(subject_id.0)
                .bind(bundle.surface_id)
                .bind(item.ordinal)
                .bind(item.case_id)
                .bind(item.fact_id.0)
                .bind(item.revision_id.0)
                .bind(&item.namespace)
                .bind(&item.fact_key)
                .bind(&item.value)
                .bind(item.confidence)
                .bind(&item.sensitivity)
                .bind(item.lexical_score)
                .bind(&item.content_sha256)
                .bind(&item.item_sha256)
                .execute(&mut *transaction)
                .await
                .map_err(map_surface_sqlx)?;
            }
            transaction.commit().await.map_err(unexpected)?;
            return Ok(CreateSurfaceOutcome {
                bundle,
                replayed: false,
            });
        }

        let existing = sqlx::query(
            r#"
            SELECT request_fingerprint, surface_id
            FROM memory.surface_responses
            WHERE tenant_id = $1
              AND host_id = $2
              AND principal_id = $3
              AND idempotency_key_digest = $4
            "#,
        )
        .bind(tenant_id.0)
        .bind(&request.host_id)
        .bind(&request.principal_id)
        .bind(&key_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_surface_sqlx)?
        .ok_or(RepositoryError::Unexpected(
            "surface idempotency replay lost its reservation".to_owned(),
        ))?;
        let stored_fingerprint: String = existing
            .try_get("request_fingerprint")
            .map_err(unexpected)?;
        if stored_fingerprint != idempotency.fingerprint {
            return Err(RepositoryError::IdempotencyKeyReused);
        }
        let stored_surface_id: Uuid = existing.try_get("surface_id").map_err(unexpected)?;
        let replayed = self
            .read_stored_surface(&mut transaction, tenant_id, subject_id, stored_surface_id)
            .await?
            .ok_or(RepositoryError::Unexpected(
                "surface idempotency replay lost its stored bundle".to_owned(),
            ))?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(CreateSurfaceOutcome {
            bundle: replayed,
            replayed: true,
        })
    }
}

impl PostgresMemoryRepository {
    #[allow(clippy::too_many_arguments)]
    async fn evaluate_index_surface(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: TenantId,
        subject_id: SubjectId,
        host_id: &str,
        principal_id: &str,
        allowed_sensitivities: &[String],
        sensitivity_ceiling: &Option<String>,
        window_from: Option<time::OffsetDateTime>,
        window_until: Option<time::OffsetDateTime>,
        max_items: i16,
        max_result_tokens: i32,
    ) -> Result<SurfaceBundle, RepositoryError> {
        let surface_id = Uuid::now_v7();
        let evaluated_at = time::OffsetDateTime::now_utc();
        // The catalog lists every current page, ordered by namespace and
        // key (hierarchical index, R10). It is bounded by the policy caps:
        // item cap and result-token cap (012 R6).
        let rows = sqlx::query(
            r#"
            SELECT p.case_id, p.fact_id, p.revision_id, p.namespace,
                p.fact_key, p.value,
                p.confidence::text AS confidence,
                p.sensitivity,
                p.content_sha256,
                '0'::text AS lexical_score
            FROM memory.authorized_current_projection p
            WHERE p.tenant_id = $1
              AND p.subject_id = $2
              AND p.sensitivity = ANY($3::text[])
              AND ($4::text IS NULL OR p.sensitivity = $4)
              AND p.valid_during && tstzrange(
                  COALESCE($5::timestamptz, '-infinity'::timestamptz),
                  COALESCE($6::timestamptz, 'infinity'::timestamptz),
                  '[)'
              )
            ORDER BY p.namespace ASC, p.fact_key ASC, p.fact_id ASC
            LIMIT $7
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(allowed_sensitivities)
        .bind(sensitivity_ceiling)
        .bind(window_from)
        .bind(window_until)
        .bind(i64::from(max_items) + 1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(map_surface_sqlx)?;

        let mut items = Vec::new();
        let mut truncated = rows.len() > max_items as usize;
        let mut result_tokens: usize = 0;
        for (ordinal, row) in (0_i16..).zip(rows.iter().take(max_items as usize)) {
            let namespace: String = row.try_get("namespace").map_err(unexpected)?;
            let fact_key: String = row.try_get("fact_key").map_err(unexpected)?;
            let value: serde_json::Value = row.try_get("value").map_err(unexpected)?;
            let mut item = surface_item_from_row(row, ordinal)?;
            // The index entry is a link and a summary, not the raw fact.
            item.value = serde_json::json!({
                "link": format!("pages/facts/{}.md", item.fact_id.0),
                "namespace": namespace,
                "key": fact_key,
                "summary": summarize_value(&value),
            });
            item.item_sha256 = item.item_sha256();
            let item_tokens = estimated_tokens(&item.value.to_string());
            if result_tokens + item_tokens > max_result_tokens as usize {
                truncated = true;
                break;
            }
            result_tokens += item_tokens;
            items.push(item);
        }
        Ok(SurfaceBundle {
            surface_id,
            subject_id,
            host_id: host_id.to_owned(),
            principal_id: principal_id.to_owned(),
            evaluated_at,
            policy_sha256: None,
            item_count: i16::try_from(items.len()).map_err(unexpected)?,
            truncated,
            context_terms_used: Vec::new(),
            items,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn evaluate_surface(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: TenantId,
        subject_id: SubjectId,
        host_id: &str,
        principal_id: &str,
        request: &NewSurfaceRequest,
        allowed_sensitivities: &[String],
        sensitivity_ceiling: &Option<String>,
        window_from: Option<time::OffsetDateTime>,
        window_until: Option<time::OffsetDateTime>,
        max_items: i16,
        max_context_tokens: i32,
        max_result_tokens: i32,
    ) -> Result<SurfaceBundle, RepositoryError> {
        let surface_id = Uuid::now_v7();
        let evaluated_at = time::OffsetDateTime::now_utc();
        let trimmed_terms: Vec<&str> = request
            .context_terms
            .iter()
            .map(|term| term.trim())
            .filter(|term| !term.is_empty())
            .collect();
        // The context cap bounds the terms that reach the query. Terms are
        // dropped from the end until the joined digest fits (A2).
        let mut terms: Vec<&str> = Vec::new();
        for term in trimmed_terms {
            let candidate = format!("{} {}", terms.join(" "), term);
            let candidate = candidate.trim();
            if estimated_tokens(candidate) > max_context_tokens as usize && !terms.is_empty() {
                break;
            }
            terms.push(term);
        }
        let mut items = Vec::new();
        let mut truncated = false;
        let mut result_tokens: usize = 0;
        if !terms.is_empty() {
            let query_terms = terms.join(" ");
            let rows = sqlx::query(
                r#"
                SELECT p.case_id, p.fact_id, p.revision_id, p.namespace,
                    p.fact_key, p.value,
                    p.confidence::text AS confidence,
                    p.sensitivity,
                    p.content_sha256,
                    ts_rank_cd(doc.search_vector, websearch_to_tsquery('pg_catalog.simple', $3))::text AS lexical_score
                FROM memory.authorized_current_projection p
                JOIN memory.fact_revision_search_documents doc
                  ON doc.tenant_id = p.tenant_id
                 AND doc.subject_id = p.subject_id
                 AND doc.case_id = p.case_id
                 AND doc.fact_id = p.fact_id
                 AND doc.revision_id = p.revision_id
                WHERE p.tenant_id = $1
                  AND p.subject_id = $2
                  AND p.sensitivity = ANY($4::text[])
                  AND ($5::text IS NULL OR p.sensitivity = $5)
                  AND p.valid_during && tstzrange(
                      COALESCE($6::timestamptz, '-infinity'::timestamptz),
                      COALESCE($7::timestamptz, 'infinity'::timestamptz),
                      '[)'
                  )
                  AND doc.search_vector @@ websearch_to_tsquery('pg_catalog.simple', $3)
                ORDER BY lexical_score DESC, p.fact_id ASC
                LIMIT $8
                "#,
            )
            .bind(tenant_id.0)
            .bind(subject_id.0)
            .bind(&query_terms)
            .bind(allowed_sensitivities)
            .bind(sensitivity_ceiling)
            .bind(window_from)
            .bind(window_until)
            .bind(i64::from(max_items) + 1)
            .fetch_all(&mut **transaction)
            .await
            .map_err(map_surface_sqlx)?;
            for (ordinal, row) in (0_i16..).zip(rows.iter().take(max_items as usize)) {
                let mut item = surface_item_from_row(row, ordinal)?;
                item.item_sha256 = item.item_sha256();
                // The result cap bounds the surfaced payload. Items are
                // dropped after the cap, in rank order (A2). A cap smaller
                // than the smallest item yields an empty bundle (fail closed).
                let item_tokens = estimated_tokens(&item.value.to_string());
                if result_tokens + item_tokens > max_result_tokens as usize {
                    truncated = true;
                    break;
                }
                result_tokens += item_tokens;
                items.push(item);
            }
            truncated = truncated || rows.len() > max_items as usize;
        }
        let context_terms_used = terms
            .iter()
            .map(|term| (*term).to_owned())
            .collect::<Vec<_>>();
        Ok(SurfaceBundle {
            surface_id,
            subject_id,
            host_id: host_id.to_owned(),
            principal_id: principal_id.to_owned(),
            evaluated_at,
            policy_sha256: None,
            item_count: i16::try_from(items.len()).map_err(unexpected)?,
            truncated,
            context_terms_used,
            items,
        })
    }

    async fn read_stored_surface(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: TenantId,
        subject_id: SubjectId,
        surface_id: Uuid,
    ) -> Result<Option<SurfaceBundle>, RepositoryError> {
        let response = sqlx::query(
            r#"
            SELECT surface_id, host_id, principal_id, policy_sha256,
                item_count, truncated, context_terms, evaluated_at
            FROM memory.surface_responses
            WHERE tenant_id = $1 AND subject_id = $2 AND surface_id = $3
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(surface_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(map_surface_sqlx)?;
        let Some(response) = response else {
            return Ok(None);
        };
        let items = sqlx::query(
            r#"
            SELECT ordinal, case_id, fact_id, revision_id, namespace,
                fact_key, value,
                confidence::text AS confidence,
                sensitivity,
                lexical_score::text AS lexical_score,
                content_sha256, item_sha256
            FROM memory.surface_response_items
            WHERE tenant_id = $1 AND subject_id = $2 AND surface_id = $3
            ORDER BY ordinal ASC
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(surface_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(map_surface_sqlx)?;
        let mut bundle_items = Vec::with_capacity(items.len());
        for row in &items {
            let mut item = surface_item_from_row(row, row.try_get("ordinal").map_err(unexpected)?)?;
            item.item_sha256 = row.try_get("item_sha256").map_err(unexpected)?;
            bundle_items.push(item);
        }
        Ok(Some(SurfaceBundle {
            surface_id: response.try_get("surface_id").map_err(unexpected)?,
            subject_id,
            host_id: response.try_get("host_id").map_err(unexpected)?,
            principal_id: response.try_get("principal_id").map_err(unexpected)?,
            evaluated_at: response.try_get("evaluated_at").map_err(unexpected)?,
            policy_sha256: response.try_get("policy_sha256").map_err(unexpected)?,
            item_count: response.try_get("item_count").map_err(unexpected)?,
            truncated: response.try_get("truncated").map_err(unexpected)?,
            context_terms_used: response.try_get("context_terms").map_err(unexpected)?,
            items: bundle_items,
        }))
    }
}
