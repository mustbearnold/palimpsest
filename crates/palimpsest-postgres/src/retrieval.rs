//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    IdempotencyRequest, RepositoryError, RetrievalMutationOutcome, RetrievalPreparation,
    RetrievalQueryEmbedding, RetrievalRepository,
};
use palimpsest_domain::{
    CaseId, EmbeddingProfile, EpisodeId, ExactIdentityTier, FactId, FactKey, FactNamespace,
    NewRetrieval, PrincipalId, PrincipalScope, RecencyProfile, RetrievalAuthorizationReceipt,
    RetrievalEmbeddingLineage, RetrievalId, RetrievalItem, RetrievalPolicy, RetrievalPolicyId,
    RetrievalQueryEmbeddingLineage, RetrievalReceipt, RetrievalScore, RevisionId, ScoreUnits,
    Sensitivity, SubjectId, TemporalOrderKey, TemporalScoreInput, TenantId,
    score_temporal_retrieval,
};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

use super::{PostgresMemoryRepository, required_column, text_value_from_row, unexpected};

#[derive(Debug)]
pub(crate) struct LexicalCandidate {
    pub(crate) case_id: uuid::Uuid,
    pub(crate) fact_id: uuid::Uuid,
    pub(crate) revision_id: uuid::Uuid,
    pub(crate) exact_identity_rank: Option<i16>,
    pub(crate) lexical_rank: Option<i64>,
    pub(crate) lexical_score: String,
    pub(crate) final_score: String,
    pub(crate) source_content_sha256: String,
    pub(crate) projection_sha256: String,
    pub(crate) item_sha256: String,
}

#[derive(Clone, Debug)]
pub(crate) struct HybridPolicyPlan {
    pub(crate) policy_version: String,
    pub(crate) policy_sha256: String,
    pub(crate) exact_candidate_limit: i32,
    pub(crate) lexical_candidate_limit: i32,
    pub(crate) vector_candidate_limit: i32,
    pub(crate) manifest_limit: i32,
    pub(crate) fts_rank_normalization: i32,
    pub(crate) score_scale: i32,
    pub(crate) rrf_k: i32,
    pub(crate) temporal_scoring: bool,
    pub(crate) profile: EmbeddingProfile,
    pub(crate) projection_profile_id: String,
    pub(crate) projection_profile_version: String,
    pub(crate) projection_profile_sha256: String,
}

#[derive(Clone, Debug)]
pub(crate) struct HybridCandidate {
    pub(crate) case_id: uuid::Uuid,
    pub(crate) fact_id: uuid::Uuid,
    pub(crate) revision_id: uuid::Uuid,
    pub(crate) exact_identity_rank: Option<i16>,
    pub(crate) exact_rank: Option<i64>,
    pub(crate) lexical_rank: Option<i64>,
    pub(crate) lexical_score: Option<String>,
    pub(crate) vector_rank: Option<i64>,
    pub(crate) vector_distance: Option<String>,
    pub(crate) vector_similarity: Option<String>,
    pub(crate) exact_rrf: String,
    pub(crate) lexical_rrf: String,
    pub(crate) vector_rrf: String,
    pub(crate) fused_score: String,
    pub(crate) source_content_sha256: String,
    pub(crate) projection_sha256: String,
    pub(crate) embedding_input_sha256: String,
    pub(crate) embedding_vector_sha256: String,
    pub(crate) temporal: Option<TemporalCandidate>,
    pub(crate) item_sha256: String,
}

#[derive(Clone, Debug)]
pub(crate) struct TemporalCandidate {
    pub(crate) recency_profile_id: String,
    pub(crate) recency_profile_version: String,
    pub(crate) recency_profile_sha256: String,
    pub(crate) recency_anchor_at: OffsetDateTime,
    pub(crate) recency_age_us: String,
    pub(crate) recency_factor: String,
    pub(crate) confidence_factor: String,
    pub(crate) importance_factor: String,
    pub(crate) temporal_adjustment: String,
    pub(crate) confidence_adjustment: String,
    pub(crate) importance_adjustment: String,
    pub(crate) exact_identity_bonus: String,
    pub(crate) final_score: String,
    pub(crate) order_key: TemporalOrderKey,
}

#[async_trait]
impl RetrievalRepository for PostgresMemoryRepository {
    async fn prepare_receipt(
        &self,
        retrieval: &NewRetrieval,
        idempotency: &IdempotencyRequest,
    ) -> Result<RetrievalPreparation, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        set_retrieval_scope(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            &retrieval.principal_id,
            &retrieval.allowed_sensitivities,
        )
        .await?;

        let reservation = sqlx::query(
            r#"
            SELECT subject_id, retrieval_id, request_fingerprint
            FROM memory.retrieval_idempotency_reservations
            WHERE tenant_id = $1
              AND principal_id = $2
              AND idempotency_key = $3
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;
        if let Some(reservation) = reservation {
            let stored_subject_id: uuid::Uuid =
                reservation.try_get("subject_id").map_err(unexpected)?;
            let stored_fingerprint: String = reservation
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if stored_subject_id != retrieval.subject_id.0
                || stored_fingerprint != idempotency.fingerprint
            {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let retrieval_id =
                RetrievalId(reservation.try_get("retrieval_id").map_err(unexpected)?);
            let receipt = select_retrieval_receipt(
                &mut transaction,
                retrieval.tenant_id,
                retrieval.subject_id,
                retrieval_id,
                None,
                &retrieval.authorization_scope_sha256,
            )
            .await?
            .ok_or(RepositoryError::IdempotencyInProgress)?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalPreparation::Replay(RetrievalMutationOutcome {
                receipt,
                replayed: true,
            }));
        }

        let policy = sqlx::query(
            r#"
            SELECT policy.retrieval_mode,
                profile.profile_id,
                profile.profile_version,
                profile.provider,
                profile.model,
                profile.model_revision,
                profile.dimensions,
                profile.normalization,
                profile.normalization_tolerance::double precision
                    AS normalization_tolerance,
                profile.distance_metric,
                profile.scalar_type,
                profile.input_serialization,
                profile.query_task_mode,
                profile.document_task_mode,
                profile.provider_contract_schema_version,
                profile.profile_sha256
            FROM memory.retrieval_policies AS policy
            LEFT JOIN memory.embedding_profiles AS profile
              ON profile.profile_id = policy.embedding_profile_id
             AND profile.profile_version = policy.embedding_profile_version
             AND profile.profile_sha256 = policy.embedding_profile_sha256
            WHERE policy.policy_id = $1
              AND policy.policy_version = '1'
            "#,
        )
        .bind(retrieval.policy_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is unavailable".to_owned()))?;
        let retrieval_mode: String = policy.try_get("retrieval_mode").map_err(unexpected)?;
        let embedding_profile = match retrieval_mode.as_str() {
            "lexical" => None,
            "hybrid" => Some(EmbeddingProfile {
                id: required_column(&policy, "profile_id")?,
                version: required_column(&policy, "profile_version")?,
                provider: required_column(&policy, "provider")?,
                model: required_column(&policy, "model")?,
                model_revision: required_column(&policy, "model_revision")?,
                dimensions: usize::try_from(required_column::<i32>(&policy, "dimensions")?)
                    .map_err(unexpected)?,
                normalization: required_column(&policy, "normalization")?,
                normalization_tolerance: required_column(&policy, "normalization_tolerance")?,
                distance_metric: required_column(&policy, "distance_metric")?,
                scalar_type: required_column(&policy, "scalar_type")?,
                input_serialization: required_column(&policy, "input_serialization")?,
                query_task: required_column(&policy, "query_task_mode")?,
                document_task: required_column(&policy, "document_task_mode")?,
                provider_contract_schema_version: u32::try_from(required_column::<i32>(
                    &policy,
                    "provider_contract_schema_version",
                )?)
                .map_err(unexpected)?,
                digest: required_column(&policy, "profile_sha256")?,
            }),
            _ => {
                return Err(RepositoryError::Unexpected(
                    "retrieval policy mode is invalid".to_owned(),
                ));
            }
        };
        transaction.commit().await.map_err(map_retrieval_sqlx)?;
        Ok(RetrievalPreparation::Execute { embedding_profile })
    }

    async fn create_receipt(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
        query_embedding: Option<RetrievalQueryEmbedding>,
    ) -> Result<RetrievalMutationOutcome, RepositoryError> {
        const MAX_SERIALIZATION_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_SERIALIZATION_ATTEMPTS {
            match self
                .create_receipt_once(
                    retrieval.clone(),
                    idempotency.clone(),
                    query_embedding.clone(),
                )
                .await
            {
                Err(RepositoryError::SerializationRetry)
                    if attempt < MAX_SERIALIZATION_ATTEMPTS => {}
                outcome => return outcome,
            }
        }
        unreachable!("the bounded serialization retry loop always returns")
    }

    async fn get_receipt(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
        authorization_scope_sha256: String,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        self.get_receipt_once(
            principal,
            tenant_id,
            subject_id,
            retrieval_id,
            cursor,
            &authorization_scope_sha256,
        )
        .await
    }
}

pub(crate) async fn set_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), RepositoryError> {
    set_scope_context(transaction, tenant_id, subject_id).await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock_shared(\
            hashtextextended($1::text || ':' || $2::text, 0)\
        )",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let lifecycle_state = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lifecycle_state
        FROM memory.subject_lifecycles
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    if lifecycle_state.is_some_and(|state| state != "active") {
        return Err(RepositoryError::SubjectUnavailable);
    }
    Ok(())
}

pub(crate) async fn set_scope_context(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

pub(crate) async fn set_retrieval_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    principal_id: &PrincipalId,
    allowed_sensitivities: &[Sensitivity],
) -> Result<(), RepositoryError> {
    set_scope(transaction, tenant_id, subject_id).await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
        .bind(&principal_id.0)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    let allowed_sensitivities = serde_json::to_string(
        &allowed_sensitivities
            .iter()
            .map(Sensitivity::as_str)
            .collect::<Vec<_>>(),
    )
    .map_err(unexpected)?;
    sqlx::query("SELECT set_config('palimpsest.allowed_sensitivities', $1, true)")
        .bind(allowed_sensitivities)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

pub(crate) fn lexical_candidate_from_row(row: &PgRow) -> Result<LexicalCandidate, RepositoryError> {
    let exact_identity_rank: Option<i16> =
        row.try_get("exact_identity_rank").map_err(unexpected)?;
    let lexical_rank: Option<i64> = row.try_get("lexical_rank").map_err(unexpected)?;
    let lexical_score: String = row.try_get("lexical_score").map_err(unexpected)?;
    let final_score = lexical_score.clone();
    let case_id: uuid::Uuid = row.try_get("case_id").map_err(unexpected)?;
    let fact_id: uuid::Uuid = row.try_get("fact_id").map_err(unexpected)?;
    let revision_id: uuid::Uuid = row.try_get("revision_id").map_err(unexpected)?;
    let source_content_sha256: String = row.try_get("content_sha256").map_err(unexpected)?;
    let projection_sha256: String = row.try_get("projection_sha256").map_err(unexpected)?;
    let item_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&serde_json::json!({
            "case_id": case_id,
            "fact_id": fact_id,
            "revision_id": revision_id,
            "exact_identity_rank": exact_identity_rank,
            "lexical_rank": lexical_rank,
            "lexical_score": lexical_score,
            "final_score": final_score,
            "source_content_sha256": source_content_sha256,
            "projection_sha256": projection_sha256,
        }))
        .map_err(unexpected)?,
    ));
    Ok(LexicalCandidate {
        case_id,
        fact_id,
        revision_id,
        exact_identity_rank,
        lexical_rank,
        lexical_score,
        final_score,
        source_content_sha256,
        projection_sha256,
        item_sha256,
    })
}

pub(crate) fn hybrid_candidate_from_row(
    row: &PgRow,
    temporal_scoring: bool,
) -> Result<HybridCandidate, RepositoryError> {
    let case_id = row.try_get("case_id").map_err(unexpected)?;
    let fact_id = row.try_get("fact_id").map_err(unexpected)?;
    let revision_id = row.try_get("revision_id").map_err(unexpected)?;
    let exact_identity_rank: Option<i16> =
        row.try_get("exact_identity_rank").map_err(unexpected)?;
    let exact_rank: Option<i64> = row.try_get("exact_rank").map_err(unexpected)?;
    let lexical_rank: Option<i64> = row.try_get("lexical_rank").map_err(unexpected)?;
    let lexical_score = row.try_get("lexical_score").map_err(unexpected)?;
    let vector_rank: Option<i64> = row.try_get("vector_rank").map_err(unexpected)?;
    let vector_distance = row.try_get("vector_distance").map_err(unexpected)?;
    let vector_similarity = row.try_get("vector_similarity").map_err(unexpected)?;
    let mut exact_rrf: String = row.try_get("exact_rrf").map_err(unexpected)?;
    let mut lexical_rrf: String = row.try_get("lexical_rrf").map_err(unexpected)?;
    let mut vector_rrf: String = row.try_get("vector_rrf").map_err(unexpected)?;
    let mut fused_score: String = row.try_get("fused_score").map_err(unexpected)?;
    let source_content_sha256 = row.try_get("content_sha256").map_err(unexpected)?;
    let projection_sha256 = row.try_get("projection_sha256").map_err(unexpected)?;
    let embedding_input_sha256 = row.try_get("embedding_input_sha256").map_err(unexpected)?;
    let embedding_vector_sha256 = row.try_get("embedding_vector_sha256").map_err(unexpected)?;
    let temporal = if temporal_scoring {
        let recency_profile_id: String = row.try_get("recency_profile_id").map_err(unexpected)?;
        let recency_profile_version: String =
            row.try_get("recency_profile_version").map_err(unexpected)?;
        let recency_profile_sha256: String =
            row.try_get("recency_profile_sha256").map_err(unexpected)?;
        let recency_profile = match (
            recency_profile_id.as_str(),
            recency_profile_version.as_str(),
        ) {
            ("stable-v1", "1") => RecencyProfile::StableV1,
            ("active-case-30d-v1", "1") => RecencyProfile::ActiveCase30dV1,
            _ => {
                return Err(RepositoryError::Unexpected(
                    "temporal retrieval recency profile is unsupported".to_owned(),
                ));
            }
        };
        let recency_anchor_at = row.try_get("recency_anchor_at").map_err(unexpected)?;
        let recency_age_us: String = row.try_get("recency_age_us").map_err(unexpected)?;
        let age_us = recency_age_us.parse::<i128>().map_err(unexpected)?;
        let confidence_basis_points: i64 =
            row.try_get("confidence_basis_points").map_err(unexpected)?;
        let importance_basis_points: i64 =
            row.try_get("importance_basis_points").map_err(unexpected)?;
        let confidence_factor = ScoreUnits::from_ratio(i128::from(confidence_basis_points), 10_000)
            .map_err(score_math_unexpected)?;
        let importance = ScoreUnits::from_ratio(i128::from(importance_basis_points), 10_000)
            .map_err(score_math_unexpected)?;
        let exact_identity = match exact_identity_rank {
            Some(1) => ExactIdentityTier::NamespaceAndKey,
            Some(2) => ExactIdentityTier::KeyOnly,
            None => ExactIdentityTier::None,
            Some(_) => {
                return Err(RepositoryError::Unexpected(
                    "temporal retrieval exact identity rank is invalid".to_owned(),
                ));
            }
        };
        let score = score_temporal_retrieval(TemporalScoreInput {
            exact_rank: temporal_rank(exact_rank)?,
            lexical_rank: temporal_rank(lexical_rank)?,
            vector_rank: temporal_rank(vector_rank)?,
            recency_profile,
            valid_at_us: age_us,
            recency_anchor_at_us: 0,
            confidence_factor,
            importance,
            exact_identity,
        })
        .map_err(score_math_unexpected)?;
        exact_rrf = score.exact_rrf.to_string();
        lexical_rrf = score.lexical_rrf.to_string();
        vector_rrf = score.vector_rrf.to_string();
        fused_score = score.fused_score.to_string();
        Some(TemporalCandidate {
            recency_profile_id,
            recency_profile_version,
            recency_profile_sha256,
            recency_anchor_at,
            recency_age_us,
            recency_factor: score.recency_factor.to_string(),
            confidence_factor: score.confidence_factor.to_string(),
            importance_factor: score.importance_factor.to_string(),
            temporal_adjustment: score.temporal_adjustment.to_string(),
            confidence_adjustment: score.confidence_adjustment.to_string(),
            importance_adjustment: score.importance_adjustment.to_string(),
            exact_identity_bonus: score.exact_identity_bonus.to_string(),
            final_score: score.final_score.to_string(),
            order_key: TemporalOrderKey {
                exact_identity_rank: exact_identity_rank
                    .map(u32::try_from)
                    .transpose()
                    .map_err(unexpected)?,
                final_score: score.final_score,
                exact_rank: temporal_rank(exact_rank)?,
                lexical_rank: temporal_rank(lexical_rank)?,
                vector_rank: temporal_rank(vector_rank)?,
                case_id: CaseId(case_id),
                fact_id: FactId(fact_id),
                revision_id: RevisionId(revision_id),
            },
        })
    } else {
        None
    };
    let mut item_document = serde_json::json!({
        "case_id": case_id,
        "fact_id": fact_id,
        "revision_id": revision_id,
        "exact_identity_rank": exact_identity_rank,
        "exact_rank": exact_rank,
        "lexical_rank": lexical_rank,
        "lexical_score": lexical_score,
        "vector_rank": vector_rank,
        "vector_distance": vector_distance,
        "vector_similarity": vector_similarity,
        "exact_rrf": exact_rrf,
        "lexical_rrf": lexical_rrf,
        "vector_rrf": vector_rrf,
        "fused_score": fused_score,
        "source_content_sha256": source_content_sha256,
        "projection_sha256": projection_sha256,
        "embedding_input_sha256": embedding_input_sha256,
        "embedding_vector_sha256": embedding_vector_sha256,
    });
    if let Some(temporal) = &temporal {
        let object = item_document.as_object_mut().ok_or_else(|| {
            RepositoryError::Unexpected("temporal item document is invalid".to_owned())
        })?;
        object.insert(
            "recency_profile_id".to_owned(),
            serde_json::json!(temporal.recency_profile_id),
        );
        object.insert(
            "recency_profile_version".to_owned(),
            serde_json::json!(temporal.recency_profile_version),
        );
        object.insert(
            "recency_profile_sha256".to_owned(),
            serde_json::json!(temporal.recency_profile_sha256),
        );
        object.insert(
            "recency_anchor_at_unix_nanos".to_owned(),
            serde_json::json!(
                temporal
                    .recency_anchor_at
                    .unix_timestamp_nanos()
                    .to_string()
            ),
        );
        object.insert(
            "recency_age_us".to_owned(),
            serde_json::json!(temporal.recency_age_us),
        );
        for (name, value) in [
            ("recency_factor", &temporal.recency_factor),
            ("confidence_factor", &temporal.confidence_factor),
            ("importance_factor", &temporal.importance_factor),
            ("temporal_adjustment", &temporal.temporal_adjustment),
            ("confidence_adjustment", &temporal.confidence_adjustment),
            ("importance_adjustment", &temporal.importance_adjustment),
            ("exact_identity_bonus", &temporal.exact_identity_bonus),
            ("final_score", &temporal.final_score),
        ] {
            object.insert(name.to_owned(), serde_json::json!(value));
        }
    }
    let item_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&item_document).map_err(unexpected)?,
    ));
    Ok(HybridCandidate {
        case_id,
        fact_id,
        revision_id,
        exact_identity_rank,
        exact_rank,
        lexical_rank,
        lexical_score,
        vector_rank,
        vector_distance,
        vector_similarity,
        exact_rrf,
        lexical_rrf,
        vector_rrf,
        fused_score,
        source_content_sha256,
        projection_sha256,
        embedding_input_sha256,
        embedding_vector_sha256,
        temporal,
        item_sha256,
    })
}

pub(crate) fn temporal_rank(rank: Option<i64>) -> Result<Option<u32>, RepositoryError> {
    rank.map(|rank| u32::try_from(rank).map_err(unexpected))
        .transpose()
}

pub(crate) fn score_math_unexpected(error: palimpsest_domain::ScoreMathError) -> RepositoryError {
    RepositoryError::Unexpected(format!("temporal retrieval score is invalid: {error:?}"))
}

pub(crate) async fn select_retrieval_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    retrieval_id: RetrievalId,
    cursor: Option<&str>,
    authorization_scope_sha256: &str,
) -> Result<Option<RetrievalReceipt>, RepositoryError> {
    let receipt = sqlx::query(
        r#"
        SELECT evaluated_at, valid_at, recorded_at, policy_id, policy_version,
            policy_sha256, projection_schema_version,
            authorization_scope_sha256, page_size,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            query_input_sha256, query_vector_sha256
        FROM memory.retrieval_receipts
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
          AND principal_id = NULLIF(
              current_setting('palimpsest.principal_id', true),
              ''
          )
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(retrieval_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    let page_size: i16 = receipt.try_get("page_size").map_err(unexpected)?;
    let after_ordinal = if let Some(cursor) = cursor {
        let Ok(cursor) = uuid::Uuid::parse_str(cursor) else {
            return Ok(None);
        };
        let cursor_row = sqlx::query(
            r#"
            SELECT ordinal
            FROM memory.retrieval_manifest_items
            WHERE tenant_id = $1
              AND subject_id = $2
              AND retrieval_id = $3
              AND principal_id = NULLIF(
                  current_setting('palimpsest.principal_id', true),
                  ''
              )
              AND cursor_token = $4
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .bind(retrieval_id.0)
        .bind(cursor)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(unexpected)?;
        let Some(cursor_row) = cursor_row else {
            return Ok(None);
        };
        cursor_row
            .try_get::<i16, _>("ordinal")
            .map_err(unexpected)?
    } else {
        0
    };
    let rows = sqlx::query(
        r#"
        SELECT manifest.ordinal, manifest.cursor_token, manifest.fact_id,
            manifest.revision_id, manifest.exact_identity_rank,
            manifest.lexical_rank, manifest.lexical_score::text AS lexical_score,
            manifest.final_rank, manifest.final_score::text AS final_score,
            manifest.exact_rank, manifest.vector_rank,
            manifest.vector_distance::text AS vector_distance,
            manifest.vector_similarity::text AS vector_similarity,
            manifest.exact_rrf_contribution::text AS exact_rrf_contribution,
            manifest.lexical_rrf_contribution::text AS lexical_rrf_contribution,
            manifest.vector_rrf_contribution::text AS vector_rrf_contribution,
            manifest.fused_score::text AS fused_score,
            manifest.recency_profile_id,
            manifest.recency_profile_version,
            manifest.recency_profile_sha256,
            manifest.recency_anchor_at,
            manifest.recency_age_us::text AS recency_age_us,
            manifest.recency_factor::text AS recency_factor,
            manifest.confidence_factor::text AS confidence_factor,
            manifest.importance_factor::text AS importance_factor,
            manifest.temporal_adjustment::text AS temporal_adjustment,
            manifest.confidence_adjustment::text AS confidence_adjustment,
            manifest.importance_adjustment::text AS importance_adjustment,
            manifest.exact_identity_bonus::text AS exact_identity_bonus,
            manifest.embedding_input_sha256,
            manifest.embedding_vector_sha256,
            receipt.embedding_profile_id,
            receipt.embedding_profile_version,
            receipt.embedding_profile_sha256,
            receipt.embedding_projection_profile_sha256,
            fact.namespace, fact.fact_key, revision.value,
            ARRAY(
                SELECT evidence.episode_id
                FROM memory.fact_revision_evidence AS evidence
                WHERE evidence.tenant_id = manifest.tenant_id
                  AND evidence.subject_id = manifest.subject_id
                  AND evidence.case_id = manifest.case_id
                  AND evidence.fact_id = manifest.fact_id
                  AND evidence.revision_id = manifest.revision_id
                ORDER BY evidence.episode_id
            ) AS evidence_episode_ids
        FROM memory.authorized_retrieval_manifest AS manifest
        JOIN memory.retrieval_receipts AS receipt
          ON receipt.tenant_id = manifest.tenant_id
         AND receipt.subject_id = manifest.subject_id
         AND receipt.retrieval_id = manifest.retrieval_id
         AND receipt.principal_id = manifest.principal_id
        JOIN memory.facts AS fact
          ON fact.tenant_id = manifest.tenant_id
         AND fact.subject_id = manifest.subject_id
         AND fact.case_id = manifest.case_id
         AND fact.fact_id = manifest.fact_id
        JOIN memory.fact_revisions AS revision
          ON revision.tenant_id = manifest.tenant_id
         AND revision.subject_id = manifest.subject_id
         AND revision.case_id = manifest.case_id
         AND revision.fact_id = manifest.fact_id
         AND revision.revision_id = manifest.revision_id
        WHERE manifest.tenant_id = $1
          AND manifest.subject_id = $2
          AND manifest.retrieval_id = $3
          AND manifest.ordinal > $4
        ORDER BY manifest.ordinal
        LIMIT $5
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(retrieval_id.0)
    .bind(after_ordinal)
    .bind(i32::from(page_size) + 1)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let has_more = rows.len() > usize::try_from(page_size).map_err(unexpected)?;
    let visible_rows = rows
        .iter()
        .take(usize::try_from(page_size).map_err(unexpected)?)
        .collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| visible_rows.last())
        .flatten()
        .map(|row| row.try_get::<uuid::Uuid, _>("cursor_token"))
        .transpose()
        .map_err(unexpected)?
        .map(|cursor| cursor.to_string());
    let items = visible_rows
        .into_iter()
        .map(retrieval_item_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let policy_id = RetrievalPolicyId::try_from(
        receipt
            .try_get::<String, _>("policy_id")
            .map_err(unexpected)?,
    )
    .map_err(unexpected)?;
    let projection_schema_version: i32 = receipt
        .try_get("projection_schema_version")
        .map_err(unexpected)?;
    let query_embedding = receipt
        .try_get::<Option<String>, _>("embedding_profile_id")
        .map_err(unexpected)?
        .map(|profile_id| {
            Ok(RetrievalQueryEmbeddingLineage {
                profile_id,
                profile_version: required_column(&receipt, "embedding_profile_version")?,
                profile_digest: required_column(&receipt, "embedding_profile_sha256")?,
                projection_profile_id: required_column(
                    &receipt,
                    "embedding_projection_profile_id",
                )?,
                projection_profile_version: required_column(
                    &receipt,
                    "embedding_projection_profile_version",
                )?,
                projection_profile_digest: required_column(
                    &receipt,
                    "embedding_projection_profile_sha256",
                )?,
                input_sha256: required_column(&receipt, "query_input_sha256")?,
                vector_sha256: required_column(&receipt, "query_vector_sha256")?,
            })
        })
        .transpose()?;
    Ok(Some(RetrievalReceipt {
        tenant_id,
        subject_id,
        retrieval_id,
        status: if items.is_empty() {
            "abstained".to_owned()
        } else {
            "results".to_owned()
        },
        evaluated_at: receipt.try_get("evaluated_at").map_err(unexpected)?,
        valid_at: receipt.try_get("valid_at").map_err(unexpected)?,
        recorded_at: receipt.try_get("recorded_at").map_err(unexpected)?,
        policy: RetrievalPolicy {
            id: policy_id,
            version: receipt.try_get("policy_version").map_err(unexpected)?,
            digest: receipt.try_get("policy_sha256").map_err(unexpected)?,
        },
        authorization: RetrievalAuthorizationReceipt {
            decision: "authorized".to_owned(),
            scope_digest: authorization_scope_sha256.to_owned(),
        },
        document_schema_version: u32::try_from(projection_schema_version).map_err(unexpected)?,
        query_embedding,
        items,
        next_cursor,
    }))
}

pub(crate) fn retrieval_item_from_row(row: &PgRow) -> Result<RetrievalItem, RepositoryError> {
    let mut scores = Vec::new();
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("exact_identity_rank")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "exact_identity_rank".to_owned(),
            value: rank.to_string(),
        });
    }
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("exact_rank")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "exact_rank".to_owned(),
            value: rank.to_string(),
        });
        scores.push(RetrievalScore {
            component: "exact_rrf".to_owned(),
            value: row.try_get("exact_rrf_contribution").map_err(unexpected)?,
        });
    }
    let lexical_rank = row
        .try_get::<Option<i64>, _>("lexical_rank")
        .map_err(unexpected)?;
    if let Some(rank) = lexical_rank {
        scores.push(RetrievalScore {
            component: "lexical_rank".to_owned(),
            value: rank.to_string(),
        });
        scores.push(RetrievalScore {
            component: "lexical_score".to_owned(),
            value: row.try_get("lexical_score").map_err(unexpected)?,
        });
        if row
            .try_get::<Option<String>, _>("fused_score")
            .map_err(unexpected)?
            .is_some()
        {
            scores.push(RetrievalScore {
                component: "lexical_rrf".to_owned(),
                value: row
                    .try_get("lexical_rrf_contribution")
                    .map_err(unexpected)?,
            });
        }
    }
    if let Some(rank) = row
        .try_get::<Option<i16>, _>("vector_rank")
        .map_err(unexpected)?
    {
        scores.extend([
            RetrievalScore {
                component: "vector_rank".to_owned(),
                value: rank.to_string(),
            },
            RetrievalScore {
                component: "vector_distance".to_owned(),
                value: row.try_get("vector_distance").map_err(unexpected)?,
            },
            RetrievalScore {
                component: "vector_similarity".to_owned(),
                value: row.try_get("vector_similarity").map_err(unexpected)?,
            },
            RetrievalScore {
                component: "vector_rrf".to_owned(),
                value: row.try_get("vector_rrf_contribution").map_err(unexpected)?,
            },
        ]);
    }
    if let Some(fused_score) = row
        .try_get::<Option<String>, _>("fused_score")
        .map_err(unexpected)?
    {
        scores.push(RetrievalScore {
            component: "fused_score".to_owned(),
            value: fused_score,
        });
    }
    if row
        .try_get::<Option<String>, _>("recency_profile_id")
        .map_err(unexpected)?
        .is_some()
    {
        for (component, column) in [
            ("recency_factor", "recency_factor"),
            ("confidence_factor", "confidence_factor"),
            ("importance_factor", "importance_factor"),
            ("temporal_adjustment", "temporal_adjustment"),
            ("confidence_adjustment", "confidence_adjustment"),
            ("importance_adjustment", "importance_adjustment"),
            ("exact_identity_bonus", "exact_identity_bonus"),
        ] {
            scores.push(RetrievalScore {
                component: component.to_owned(),
                value: required_column(row, column)?,
            });
        }
    }
    scores.extend([
        RetrievalScore {
            component: "final_rank".to_owned(),
            value: row
                .try_get::<i16, _>("final_rank")
                .map_err(unexpected)?
                .to_string(),
        },
        RetrievalScore {
            component: "final_score".to_owned(),
            value: row.try_get("final_score").map_err(unexpected)?,
        },
    ]);
    let embedding = row
        .try_get::<Option<String>, _>("embedding_profile_id")
        .map_err(unexpected)?
        .map(|profile_id| {
            Ok(RetrievalEmbeddingLineage {
                profile_id,
                profile_version: required_column(row, "embedding_profile_version")?,
                profile_digest: required_column(row, "embedding_profile_sha256")?,
                projection_sha256: required_column(row, "embedding_projection_profile_sha256")?,
                input_sha256: required_column(row, "embedding_input_sha256")?,
                vector_sha256: required_column(row, "embedding_vector_sha256")?,
            })
        })
        .transpose()?;
    let evidence_episode_ids: Vec<uuid::Uuid> =
        row.try_get("evidence_episode_ids").map_err(unexpected)?;
    Ok(RetrievalItem {
        memory_kind: "fact_revision".to_owned(),
        fact_id: FactId(row.try_get("fact_id").map_err(unexpected)?),
        revision_id: RevisionId(row.try_get("revision_id").map_err(unexpected)?),
        namespace: text_value_from_row::<FactNamespace>(row, "namespace")?,
        key: text_value_from_row::<FactKey>(row, "fact_key")?,
        value: row.try_get("value").map_err(unexpected)?,
        evidence_episode_ids: evidence_episode_ids.into_iter().map(EpisodeId).collect(),
        scores,
        embedding,
    })
}

pub(crate) fn map_retrieval_sqlx(error: sqlx::Error) -> RepositoryError {
    if error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "40001")
    {
        RepositoryError::SerializationRetry
    } else {
        unexpected(error)
    }
}
