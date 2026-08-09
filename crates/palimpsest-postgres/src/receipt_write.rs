//! write_path — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use palimpsest_application::{
    IdempotencyRequest, RepositoryError, RetrievalMutationOutcome, RetrievalQueryEmbedding,
};
use palimpsest_domain::{
    EmbeddingProfile, NewRetrieval, Q63_EXP2_CONSTANTS_SHA256, RetrievalId, RetrievalPerspective,
};
use sha2::{Digest, Sha256};
use sqlx::{Row, postgres::PgRow};
use time::OffsetDateTime;

use super::retrieval::{
    HybridPolicyPlan, lexical_candidate_from_row, map_retrieval_sqlx, select_retrieval_receipt,
    set_retrieval_scope,
};
use super::{PostgresMemoryRepository, required_column, unexpected};

pub(crate) fn hybrid_policy_plan(
    row: &PgRow,
    policy_version: String,
    policy_sha256: String,
    document: &serde_json::Value,
) -> Result<HybridPolicyPlan, RepositoryError> {
    fn integer(document: &serde_json::Value, pointer: &str) -> Result<i32, RepositoryError> {
        document
            .pointer(pointer)
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| {
                RepositoryError::Unexpected("hybrid retrieval policy is incomplete".to_owned())
            })
    }

    let policy_id: String = required_column(row, "policy_id")?;
    let scoring_mode: String = required_column(row, "scoring_mode")?;
    let temporal_scoring = match scoring_mode.as_str() {
        "channel-only" => false,
        "temporal-v1" => true,
        _ => {
            return Err(RepositoryError::Unexpected(
                "hybrid retrieval scoring mode is unsupported".to_owned(),
            ));
        }
    };
    let expected_rounding = if temporal_scoring {
        "half-even"
    } else {
        "half-away-from-zero"
    };

    if document
        .pointer("/fusion/method")
        .and_then(|value| value.as_str())
        != Some("reciprocal-rank")
        || document
            .pointer("/fts_configuration")
            .and_then(|value| value.as_str())
            != Some("pg_catalog.simple")
        || document
            .pointer("/fts_rank")
            .and_then(|value| value.as_str())
            != Some("ts_rank_cd")
        || document
            .pointer("/distance_metric")
            .and_then(|value| value.as_str())
            != Some("cosine")
        || document
            .pointer("/fallback")
            .and_then(|value| value.as_str())
            != Some("none")
        || document
            .pointer("/rounding")
            .and_then(|value| value.as_str())
            != Some(expected_rounding)
        || integer(document, "/fusion/weights/exact")? != 1
        || integer(document, "/fusion/weights/lexical")? != 1
        || integer(document, "/fusion/weights/vector")? != 1
    {
        return Err(RepositoryError::Unexpected(
            "hybrid retrieval policy is unsupported".to_owned(),
        ));
    }

    let plan = HybridPolicyPlan {
        policy_version,
        policy_sha256,
        exact_candidate_limit: integer(document, "/candidate_limits/exact")?,
        lexical_candidate_limit: integer(document, "/candidate_limits/lexical")?,
        vector_candidate_limit: integer(document, "/candidate_limits/vector")?,
        manifest_limit: integer(document, "/manifest_limit")?,
        fts_rank_normalization: integer(document, "/fts_rank_normalization")?,
        score_scale: integer(document, "/score_scale")?,
        rrf_k: integer(document, "/fusion/k")?,
        temporal_scoring,
        profile: EmbeddingProfile {
            id: required_column(row, "profile_id")?,
            version: required_column(row, "profile_version")?,
            provider: required_column(row, "provider")?,
            model: required_column(row, "model")?,
            model_revision: required_column(row, "model_revision")?,
            dimensions: usize::try_from(required_column::<i32>(row, "dimensions")?)
                .map_err(unexpected)?,
            normalization: required_column(row, "normalization")?,
            normalization_tolerance: required_column(row, "normalization_tolerance")?,
            distance_metric: required_column(row, "distance_metric")?,
            scalar_type: required_column(row, "scalar_type")?,
            input_serialization: required_column(row, "input_serialization")?,
            query_task: required_column(row, "query_task_mode")?,
            document_task: required_column(row, "document_task_mode")?,
            provider_contract_schema_version: u32::try_from(required_column::<i32>(
                row,
                "provider_contract_schema_version",
            )?)
            .map_err(unexpected)?,
            digest: required_column(row, "profile_sha256")?,
        },
        projection_profile_id: required_column(row, "embedding_projection_profile_id")?,
        projection_profile_version: required_column(row, "embedding_projection_profile_version")?,
        projection_profile_sha256: required_column(row, "embedding_projection_profile_sha256")?,
    };
    let expected_tie_break = if temporal_scoring {
        serde_json::json!([
            "exact_identity_rank_asc_nulls_last",
            "final_score_units_desc",
            "exact_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "vector_rank_asc_nulls_last",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ])
    } else {
        serde_json::json!([
            "fused_score_desc",
            "exact_identity_rank_asc_nulls_last",
            "exact_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "vector_rank_asc_nulls_last",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ])
    };
    let expected_channel_tie_breaks = serde_json::json!({
        "exact": [
            "exact_identity_rank_asc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ],
        "lexical": [
            "lexical_score_desc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ],
        "vector": [
            "vector_distance_asc",
            "case_id_asc",
            "fact_id_asc",
            "revision_id_asc"
        ]
    });
    let temporal_policy_supported = if temporal_scoring {
        let stable_profile_sha256: String = required_column(row, "stable_recency_profile_sha256")?;
        let active_profile_sha256: String = required_column(row, "active_recency_profile_sha256")?;
        temporal_policy_is_supported(document, &stable_profile_sha256, &active_profile_sha256)
    } else {
        true
    };
    let lexical_limit_supported = if policy_id == "retrieval-exact-vector-v1" {
        !temporal_scoring && plan.lexical_candidate_limit == 0
    } else {
        (1..=50).contains(&plan.lexical_candidate_limit)
    };
    if !(1..=50).contains(&plan.exact_candidate_limit)
        || !lexical_limit_supported
        || !(1..=50).contains(&plan.vector_candidate_limit)
        || !(1..=50).contains(&plan.manifest_limit)
        || plan.fts_rank_normalization != 32
        || plan.score_scale != 12
        || plan.rrf_k != 60
        || document
            .pointer("/exact_identity_precedence")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || document.pointer("/tie_break") != Some(&expected_tie_break)
        || document.pointer("/channel_tie_breaks") != Some(&expected_channel_tie_breaks)
        || document
            .pointer("/embedding_profile/id")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.id.as_str())
        || document
            .pointer("/embedding_profile/version")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.version.as_str())
        || document
            .pointer("/embedding_profile/digest")
            .and_then(serde_json::Value::as_str)
            != Some(plan.profile.digest.as_str())
        || document
            .pointer("/projection_profile/id")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_id.as_str())
        || document
            .pointer("/projection_profile/version")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_version.as_str())
        || document
            .pointer("/projection_profile/digest")
            .and_then(serde_json::Value::as_str)
            != Some(plan.projection_profile_sha256.as_str())
        || !temporal_policy_supported
    {
        return Err(RepositoryError::Unexpected(
            "hybrid retrieval policy is unsupported".to_owned(),
        ));
    }
    Ok(plan)
}

pub(crate) fn temporal_policy_is_supported(
    document: &serde_json::Value,
    stable_profile_sha256: &str,
    active_profile_sha256: &str,
) -> bool {
    let value = |pointer: &str| document.pointer(pointer).and_then(|value| value.as_str());
    let integer = |pointer: &str| document.pointer(pointer).and_then(|value| value.as_i64());
    let expected_operation_order = serde_json::json!([
        "rrf-channel-half-even",
        "fused-exact-sum",
        "recency-half-even",
        "confidence-half-even",
        "importance-half-even",
        "exact-identity-bonus"
    ]);
    let expected_profile_lineage = serde_json::json!({
        "active-case-30d-v1": {
            "version": "1",
            "digest": active_profile_sha256
        },
        "stable-v1": {
            "version": "1",
            "digest": stable_profile_sha256
        }
    });
    value("/arithmetic/id") == Some("score-units-q63-v1")
        && integer("/arithmetic/score_scale") == Some(12)
        && value("/arithmetic/rounding") == Some("half-even")
        && value("/arithmetic/overflow") == Some("reject")
        && document.pointer("/arithmetic/operation_order") == Some(&expected_operation_order)
        && value("/temporal/axis") == Some("request.valid_at")
        && value("/temporal/anchor") == Some("fact_revision_governance.recency_anchor_at")
        && value("/temporal/age_unit") == Some("microsecond")
        && value("/temporal/negative_age") == Some("clamp_zero")
        && document.pointer("/temporal/profile_lineage") == Some(&expected_profile_lineage)
        && value("/temporal/profiles/stable-v1/kind") == Some("constant")
        && value("/temporal/profiles/stable-v1/factor_units") == Some("1000000000000")
        && value("/temporal/profiles/active-case-30d-v1/kind") == Some("continuous-half-life")
        && value("/temporal/profiles/active-case-30d-v1/half_life_us") == Some("2592000000000")
        && value("/temporal/profiles/active-case-30d-v1/floor_units") == Some("125000000000")
        && value("/temporal/profiles/active-case-30d-v1/arithmetic") == Some("q63-exp2-v1")
        && value("/temporal/profiles/active-case-30d-v1/constants_sha256")
            == Some(Q63_EXP2_CONSTANTS_SHA256)
        && value("/quality_factors/confidence/source") == Some("fact_revisions.confidence")
        && value("/quality_factors/confidence/formula") == Some("identity")
        && value("/quality_factors/confidence/minimum_units") == Some("0")
        && value("/quality_factors/confidence/maximum_units") == Some("1000000000000")
        && value("/quality_factors/importance/source")
            == Some("fact_revision_governance.importance")
        && value("/quality_factors/importance/formula") == Some("offset-plus-value")
        && value("/quality_factors/importance/offset_units") == Some("500000000000")
        && value("/quality_factors/importance/minimum_units") == Some("500000000000")
        && value("/quality_factors/importance/maximum_units") == Some("1500000000000")
        && value("/exact_identity_bonus_units/namespace_key") == Some("16393442623")
        && value("/exact_identity_bonus_units/key") == Some("8196721311")
        && value("/exact_identity_bonus_units/none") == Some("0")
}

/// Fast-path candidate query over the precomputed authorized-current
/// structure (ADR-0032, migration 0021). Only used when the durable
/// scope-local coverage marker is complete for the retrieval policy's
/// projection schema; otherwise the canonical query below serves retrieval.
/// The per-query full-set pipeline (authorized-set materialization,
/// governance join, per-row projection verification) is replaced by a single
/// filtered scan: lifecycle, retention, sensitivity, validity, and document
/// readiness were applied at write time and are re-checked by cheap
/// per-row predicates.
const AUTHORIZED_CURRENT_PROJECTION_CANDIDATE_SQL: &str = r#"
            WITH scored AS (
                SELECT projection.tenant_id,
                    projection.subject_id,
                    projection.case_id,
                    projection.fact_id,
                    projection.revision_id,
                    projection.namespace,
                    projection.fact_key,
                    projection.value,
                    projection.sensitivity,
                    projection.content_sha256,
                    projection.projection_sha256,
                    projection.search_vector,
                    CASE
                        WHEN lower(projection.namespace || ':' || projection.fact_key)
                            = lower(btrim($13)) THEN 1::smallint
                        WHEN lower(projection.fact_key) = lower(btrim($13)) THEN 2::smallint
                        ELSE NULL::smallint
                    END AS exact_identity_rank,
                    projection.search_vector
                        @@ websearch_to_tsquery('pg_catalog.simple', $13)
                        AS lexical_match,
                    ts_rank_cd(
                        projection.search_vector,
                        websearch_to_tsquery('pg_catalog.simple', $13),
                        $14
                    )::double precision AS lexical_score
                FROM memory.authorized_current_projection AS projection
                WHERE projection.tenant_id = $1
                  AND projection.subject_id = $2
                  AND projection.recorded_at <= $3
                  AND projection.valid_during @> $4::timestamptz
                  AND ($5::uuid[] IS NULL OR projection.case_id = ANY($5))
                  AND ($6::text[] IS NULL OR projection.namespace = ANY($6))
                  AND ($7::text[] IS NULL OR projection.fact_key = ANY($7))
                  AND projection.lifecycle_state = 'active'
                  AND (
                      projection.retention_expires_at IS NULL
                      OR projection.retention_expires_at > $8
                  )
                  AND projection.sensitivity = ANY($9::text[])
                  AND ($10::text[] IS NULL OR projection.sensitivity = ANY($10))
                  AND projection.projection_ready
                  AND projection.projection_schema_version = $11
                  AND projection.projection_schema_sha256 = $12
            ),
            ranked AS (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.exact_identity_rank, scored.lexical_match,
                    CASE WHEN scored.lexical_match THEN
                        row_number() OVER (
                            PARTITION BY scored.lexical_match
                            ORDER BY scored.lexical_score DESC,
                                scored.fact_id, scored.revision_id
                        )
                    END AS lexical_rank,
                    scored.lexical_score,
                    scored.content_sha256, scored.projection_sha256
                FROM scored
                WHERE scored.exact_identity_rank IS NOT NULL OR scored.lexical_match
            ),
            limited AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id, exact_identity_rank,
                    lexical_rank,
                    round(lexical_score::numeric, $15)::text AS lexical_score,
                    content_sha256,
                    projection_sha256
                FROM ranked
                ORDER BY exact_identity_rank ASC NULLS LAST,
                    lexical_rank ASC NULLS LAST, fact_id, revision_id
                LIMIT $16
            )
            SELECT candidate.case_id, candidate.fact_id, candidate.revision_id,
                candidate.exact_identity_rank, candidate.lexical_rank,
                candidate.lexical_score, candidate.content_sha256,
                candidate.projection_sha256,
                true AS candidate_present
            FROM limited AS candidate
            ORDER BY candidate.exact_identity_rank ASC NULLS LAST,
                candidate.lexical_rank ASC NULLS LAST,
                candidate.fact_id, candidate.revision_id
            "#;

impl PostgresMemoryRepository {
    pub(crate) async fn create_receipt_once(
        &self,
        retrieval: NewRetrieval,
        idempotency: IdempotencyRequest,
        query_embedding: Option<RetrievalQueryEmbedding>,
    ) -> Result<RetrievalMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
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

        let reserved = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_idempotency_reservations (
                tenant_id, subject_id, principal_id, idempotency_key,
                request_fingerprint, retrieval_id
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, principal_id, idempotency_key) DO NOTHING
            RETURNING retrieval_id
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .bind(retrieval.retrieval_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;
        if reserved.is_none() {
            let existing = sqlx::query(
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
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_retrieval_sqlx)?;
            let stored_subject_id: uuid::Uuid =
                existing.try_get("subject_id").map_err(unexpected)?;
            let stored_fingerprint: String = existing
                .try_get("request_fingerprint")
                .map_err(unexpected)?;
            if stored_subject_id != retrieval.subject_id.0
                || stored_fingerprint != idempotency.fingerprint
            {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let retrieval_id = RetrievalId(existing.try_get("retrieval_id").map_err(unexpected)?);
            let receipt = select_retrieval_receipt(
                &mut transaction,
                retrieval.tenant_id,
                retrieval.subject_id,
                retrieval_id,
                None,
                &retrieval.authorization_scope_sha256,
            )
            .await?
            .ok_or_else(|| {
                RepositoryError::Unexpected(
                    "completed retrieval receipt could not be reauthorized".to_owned(),
                )
            })?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalMutationOutcome {
                receipt,
                replayed: true,
            });
        }

        let evaluated_at: OffsetDateTime = sqlx::query("SELECT CURRENT_TIMESTAMP AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?
            .try_get("evaluated_at")
            .map_err(unexpected)?;
        let (perspective, valid_at, recorded_at) = match &retrieval.perspective {
            RetrievalPerspective::Current => ("current", evaluated_at, evaluated_at),
            RetrievalPerspective::AsOf {
                valid_at,
                recorded_at,
            } => {
                if *recorded_at > evaluated_at {
                    return Err(RepositoryError::FutureRecordedTime);
                }
                ("as_of", *valid_at, *recorded_at)
            }
        };
        let current_projection_coverage = Self::current_projection_coverage_state(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            perspective,
            evaluated_at,
        )
        .await?;

        let policy = sqlx::query(
            r#"
            SELECT policy.policy_id, policy.policy_version, policy.policy_sha256,
                policy.retrieval_mode, policy.scoring_mode, policy.policy_document,
                (policy_document ->> 'candidate_limit')::integer AS candidate_limit,
                (policy_document ->> 'fts_rank_normalization')::integer
                    AS fts_rank_normalization,
                (policy_document ->> 'score_scale')::integer AS score_scale,
                profile.profile_id, profile.profile_version, profile.provider,
                profile.model, profile.model_revision, profile.dimensions,
                profile.normalization,
                profile.normalization_tolerance::double precision
                    AS normalization_tolerance,
                profile.distance_metric, profile.scalar_type,
                profile.input_serialization,
                profile.query_task_mode, profile.document_task_mode,
                profile.provider_contract_schema_version, profile.profile_sha256,
                policy.embedding_projection_profile_id,
                policy.embedding_projection_profile_version,
                policy.embedding_projection_profile_sha256,
                stable_recency.profile_sha256
                    AS stable_recency_profile_sha256,
                active_recency.profile_sha256
                    AS active_recency_profile_sha256
            FROM memory.retrieval_policies AS policy
            LEFT JOIN memory.embedding_profiles AS profile
              ON profile.profile_id = policy.embedding_profile_id
             AND profile.profile_version = policy.embedding_profile_version
             AND profile.profile_sha256 = policy.embedding_profile_sha256
            LEFT JOIN memory.recency_profiles AS stable_recency
              ON stable_recency.profile_id = 'stable-v1'
             AND stable_recency.profile_version = '1'
            LEFT JOIN memory.recency_profiles AS active_recency
              ON active_recency.profile_id = 'active-case-30d-v1'
             AND active_recency.profile_version = '1'
            WHERE policy.policy_id = $1 AND policy.policy_version = '1'
            "#,
        )
        .bind(retrieval.policy_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is unavailable".to_owned()))?;
        let policy_version: String = policy.try_get("policy_version").map_err(unexpected)?;
        let policy_sha256: String = policy.try_get("policy_sha256").map_err(unexpected)?;
        let retrieval_mode: String = policy.try_get("retrieval_mode").map_err(unexpected)?;
        let projection = sqlx::query(
            r#"
            SELECT projection_schema_version, projection_sha256
            FROM memory.search_projection_schemas
            WHERE projection_schema_version = 1
            "#,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or_else(|| {
            RepositoryError::Unexpected("search projection schema is unavailable".to_owned())
        })?;
        let projection_schema_version: i32 = projection
            .try_get("projection_schema_version")
            .map_err(unexpected)?;
        let projection_schema_sha256: String = projection
            .try_get("projection_sha256")
            .map_err(unexpected)?;

        if retrieval_mode == "hybrid" {
            let policy_document: serde_json::Value =
                policy.try_get("policy_document").map_err(unexpected)?;
            let plan =
                hybrid_policy_plan(&policy, policy_version, policy_sha256, &policy_document)?;
            let receipt = self
                .create_hybrid_receipt_in_transaction(
                    &mut transaction,
                    &retrieval,
                    &idempotency,
                    query_embedding.as_ref(),
                    perspective,
                    &current_projection_coverage,
                    valid_at,
                    recorded_at,
                    evaluated_at,
                    projection_schema_version,
                    &projection_schema_sha256,
                    &plan,
                )
                .await?;
            transaction.commit().await.map_err(map_retrieval_sqlx)?;
            return Ok(RetrievalMutationOutcome {
                receipt,
                replayed: false,
            });
        }
        if retrieval_mode != "lexical" || query_embedding.is_some() {
            return Err(RepositoryError::Unexpected(
                "retrieval policy execution plan is invalid".to_owned(),
            ));
        }
        let candidate_limit: i32 = policy.try_get("candidate_limit").map_err(unexpected)?;
        let fts_rank_normalization: i32 = policy
            .try_get("fts_rank_normalization")
            .map_err(unexpected)?;
        let score_scale: i32 = policy.try_get("score_scale").map_err(unexpected)?;
        let authorized_current_coverage = Self::authorized_current_projection_coverage_state(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            perspective,
            evaluated_at,
            projection_schema_version,
            &projection_schema_sha256,
        )
        .await?;
        let use_authorized_current = authorized_current_coverage == "complete";

        let case_ids = retrieval
            .filters
            .case_ids
            .as_ref()
            .map(|values| values.iter().map(|value| value.0).collect::<Vec<_>>());
        let namespaces = retrieval.filters.namespaces.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let keys = retrieval.filters.keys.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let requested_sensitivities = retrieval.filters.sensitivities.as_ref().map(|values| {
            values
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let allowed_sensitivities = retrieval
            .allowed_sensitivities
            .iter()
            .map(|value| value.as_str().to_owned())
            .collect::<Vec<_>>();
        let candidate_started = std::time::Instant::now();
        let rows = if use_authorized_current {
            sqlx::query(AUTHORIZED_CURRENT_PROJECTION_CANDIDATE_SQL)
                .bind(retrieval.tenant_id.0)
                .bind(retrieval.subject_id.0)
                .bind(recorded_at)
                .bind(valid_at)
                .bind(case_ids.clone())
                .bind(namespaces.clone())
                .bind(keys.clone())
                .bind(evaluated_at)
                .bind(allowed_sensitivities.clone())
                .bind(requested_sensitivities.clone())
                .bind(projection_schema_version)
                .bind(&projection_schema_sha256)
                .bind(retrieval.query.as_str())
                .bind(fts_rank_normalization)
                .bind(score_scale)
                .bind(candidate_limit)
                .fetch_all(&mut *transaction)
                .await
                .map_err(unexpected)?
        } else {
            sqlx::query(
                r#"
            WITH current_projection AS MATERIALIZED (
                SELECT projection.tenant_id,
                    projection.subject_id,
                    projection.case_id,
                    projection.fact_id,
                    projection.revision_id,
                    projection.value,
                    projection.namespace,
                    projection.fact_key,
                    projection.sensitivity,
                    projection.content_sha256
                FROM memory.fact_revision_current AS projection
                WHERE $17::text = 'current'
                  AND projection.tenant_id = $1
                  AND projection.subject_id = $2
                  AND projection.recorded_at <= $3
                  AND projection.valid_during @> $4::timestamptz
                  AND ($5::uuid[] IS NULL OR projection.case_id = ANY($5))
                  AND ($6::text[] IS NULL OR projection.namespace = ANY($6))
                  AND ($7::text[] IS NULL OR projection.fact_key = ANY($7))
            ),
            missing_facts AS MATERIALIZED (
                SELECT fact.tenant_id,
                    fact.subject_id,
                    fact.case_id,
                    fact.fact_id,
                    fact.namespace,
                    fact.fact_key
                FROM memory.facts AS fact
                WHERE fact.tenant_id = $1
                  AND fact.subject_id = $2
                  AND ($5::uuid[] IS NULL OR fact.case_id = ANY($5))
                  AND ($6::text[] IS NULL OR fact.namespace = ANY($6))
                  AND ($7::text[] IS NULL OR fact.fact_key = ANY($7))
                  AND (
                      $17::text <> 'current'
                      OR (
                          $18::text <> 'complete'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM current_projection AS current_row
                              WHERE current_row.tenant_id = fact.tenant_id
                                AND current_row.subject_id = fact.subject_id
                                AND current_row.case_id = fact.case_id
                                AND current_row.fact_id = fact.fact_id
                          )
                      )
                  )
            ),
            fallback AS MATERIALIZED (
                SELECT revision.tenant_id,
                    revision.subject_id,
                    revision.case_id,
                    revision.fact_id,
                    revision.revision_id,
                    revision.value,
                    missing.namespace,
                    missing.fact_key,
                    revision.sensitivity,
                    revision.content_sha256
                FROM missing_facts AS missing
                CROSS JOIN LATERAL (
                    SELECT revision.tenant_id,
                        revision.subject_id,
                        revision.case_id,
                        revision.fact_id,
                        revision.revision_id,
                        revision.value,
                        revision.sensitivity,
                        revision.content_sha256
                    FROM memory.fact_revisions AS revision
                    WHERE revision.tenant_id = missing.tenant_id
                      AND revision.subject_id = missing.subject_id
                      AND revision.case_id = missing.case_id
                      AND revision.fact_id = missing.fact_id
                      AND revision.recorded_at <= $3
                      AND revision.valid_during @> $4::timestamptz
                    ORDER BY revision.revision_no DESC, revision.revision_id
                    LIMIT 1
                ) AS revision
            ),
            effective AS MATERIALIZED (
                SELECT * FROM current_projection
                UNION ALL
                SELECT * FROM fallback
            ),
            authorized AS MATERIALIZED (
                SELECT effective.*
                FROM effective
                JOIN memory.fact_revision_governance AS governance
                  ON governance.tenant_id = effective.tenant_id
                 AND governance.subject_id = effective.subject_id
                 AND governance.case_id = effective.case_id
                 AND governance.fact_id = effective.fact_id
                 AND governance.revision_id = effective.revision_id
                WHERE governance.lifecycle_state = 'active'
                  AND (
                      governance.retention_expires_at IS NULL
                      OR governance.retention_expires_at > $8
                  )
                  AND effective.sensitivity = ANY($9::text[])
                  AND (
                      $10::text[] IS NULL
                      OR effective.sensitivity = ANY($10)
                  )
            ),
            projected AS MATERIALIZED (
                SELECT authorized.*, document.search_vector,
                    document.projection_sha256,
                    (
                        document.revision_id IS NOT NULL
                        AND document.projection_schema_sha256 = $12
                        AND document.source_content_sha256 = authorized.content_sha256
                        AND document.projection_sha256 =
                            memory.fact_projection_sha256_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                        AND document.search_vector = memory.fact_search_vector_v1(
                            authorized.namespace,
                            authorized.fact_key,
                            authorized.value
                        )
                    ) AS projection_ready
                FROM authorized
                LEFT JOIN memory.fact_revision_search_documents AS document
                  ON document.tenant_id = authorized.tenant_id
                 AND document.subject_id = authorized.subject_id
                 AND document.case_id = authorized.case_id
                 AND document.fact_id = authorized.fact_id
                 AND document.revision_id = authorized.revision_id
                 AND document.projection_schema_version = $11
            ),
            coverage AS (
                SELECT COALESCE(bool_or(NOT projection_ready), false)
                    AS coverage_missing
                FROM projected
            ),
            eligible AS MATERIALIZED (
                SELECT *
                FROM projected
                WHERE projection_ready
            ),
            scored AS (
                SELECT eligible.*,
                    CASE
                        WHEN lower(eligible.namespace || ':' || eligible.fact_key)
                            = lower(btrim($13)) THEN 1::smallint
                        WHEN lower(eligible.fact_key) = lower(btrim($13)) THEN 2::smallint
                        ELSE NULL::smallint
                    END AS exact_identity_rank,
                    eligible.search_vector
                        @@ websearch_to_tsquery('pg_catalog.simple', $13)
                        AS lexical_match,
                    ts_rank_cd(
                        eligible.search_vector,
                        websearch_to_tsquery('pg_catalog.simple', $13),
                        $14
                    )::double precision AS lexical_score
                FROM eligible
            ),
            ranked AS (
                SELECT scored.*,
                    CASE WHEN lexical_match THEN
                        row_number() OVER (
                            PARTITION BY lexical_match
                            ORDER BY lexical_score DESC, fact_id, revision_id
                        )
                    END AS lexical_rank
                FROM scored
                WHERE exact_identity_rank IS NOT NULL OR lexical_match
            ),
            limited AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id, exact_identity_rank,
                    lexical_rank,
                    round(lexical_score::numeric, $15)::text AS lexical_score,
                    content_sha256,
                    projection_sha256
                FROM ranked
                ORDER BY exact_identity_rank ASC NULLS LAST,
                    lexical_rank ASC NULLS LAST, fact_id, revision_id
                LIMIT $16
            )
            SELECT coverage.coverage_missing,
                candidate.fact_id IS NOT NULL AS candidate_present,
                candidate.case_id, candidate.fact_id, candidate.revision_id,
                candidate.exact_identity_rank, candidate.lexical_rank,
                candidate.lexical_score, candidate.content_sha256,
                candidate.projection_sha256
            FROM coverage
            LEFT JOIN limited AS candidate
              ON NOT coverage.coverage_missing
            ORDER BY candidate.exact_identity_rank ASC NULLS LAST,
                candidate.lexical_rank ASC NULLS LAST,
                candidate.fact_id, candidate.revision_id
            "#,
            )
            .bind(retrieval.tenant_id.0)
            .bind(retrieval.subject_id.0)
            .bind(recorded_at)
            .bind(valid_at)
            .bind(case_ids)
            .bind(namespaces)
            .bind(keys)
            .bind(evaluated_at)
            .bind(allowed_sensitivities)
            .bind(requested_sensitivities)
            .bind(projection_schema_version)
            .bind(&projection_schema_sha256)
            .bind(retrieval.query.as_str())
            .bind(fts_rank_normalization)
            .bind(score_scale)
            .bind(candidate_limit)
            .bind(perspective)
            .bind(&current_projection_coverage)
            .fetch_all(&mut *transaction)
            .await
            .map_err(unexpected)?
        };
        let coverage_missing = if use_authorized_current {
            false
        } else {
            rows.first()
                .ok_or_else(|| {
                    RepositoryError::Unexpected("retrieval query returned no rows".to_owned())
                })?
                .try_get::<bool, _>("coverage_missing")
                .map_err(unexpected)?
        };
        if coverage_missing {
            return Err(RepositoryError::Unexpected(
                "retrieval index is not ready".to_owned(),
            ));
        }
        let mut candidates = Vec::new();
        for row in &rows {
            if row
                .try_get::<bool, _>("candidate_present")
                .map_err(unexpected)?
            {
                candidates.push(lexical_candidate_from_row(row)?);
            }
        }
        let manifest_sha256 = hex::encode(Sha256::digest(
            candidates
                .iter()
                .map(|candidate| candidate.item_sha256.as_str())
                .collect::<String>()
                .as_bytes(),
        ));
        let outcome = if candidates.is_empty() {
            "abstention"
        } else {
            "results"
        };
        let abstention_reason = candidates.is_empty().then_some("no_authorized_match");
        let stage_timings_ms = serde_json::json!({
            "candidate_generation": candidate_started.elapsed().as_secs_f64() * 1000.0
        });
        let _inserted = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_receipts (
                tenant_id, subject_id, retrieval_id, principal_id,
                idempotency_key, request_fingerprint, query_sha256,
                perspective, valid_at, recorded_at, evaluated_at,
                policy_id, policy_version, policy_sha256,
                projection_schema_version, projection_schema_sha256,
                authorization_scope_sha256, authorization_policy_version,
                outcome, abstention_reason, stage_timings_ms, manifest_sha256,
                page_size, schema_version
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, 'principal-scope-v1',
                $18, $19, $20, $21, $22, 1
            )
            RETURNING retrieval_id
            "#,
        )
        .bind(retrieval.tenant_id.0)
        .bind(retrieval.subject_id.0)
        .bind(retrieval.retrieval_id.0)
        .bind(&retrieval.principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .bind(&retrieval.query_sha256)
        .bind(perspective)
        .bind(valid_at)
        .bind(recorded_at)
        .bind(evaluated_at)
        .bind(retrieval.policy_id.as_str())
        .bind(&policy_version)
        .bind(&policy_sha256)
        .bind(projection_schema_version)
        .bind(&projection_schema_sha256)
        .bind(&retrieval.authorization_scope_sha256)
        .bind(outcome)
        .bind(abstention_reason)
        .bind(&stage_timings_ms)
        .bind(&manifest_sha256)
        .bind(i16::try_from(retrieval.page_size).map_err(unexpected)?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_retrieval_sqlx)?;

        for (index, candidate) in candidates.iter().enumerate() {
            let ordinal = i16::try_from(index + 1).map_err(unexpected)?;
            sqlx::query(
                r#"
                INSERT INTO memory.retrieval_manifest_items (
                    tenant_id, subject_id, retrieval_id, principal_id,
                    ordinal, case_id, fact_id, revision_id,
                    exact_identity_rank, lexical_rank, lexical_score,
                    final_rank, final_score, source_content_sha256,
                    projection_sha256, item_sha256, schema_version
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11::numeric, $5, $12::numeric, $13, $14, $15, 1
                )
                "#,
            )
            .bind(retrieval.tenant_id.0)
            .bind(retrieval.subject_id.0)
            .bind(retrieval.retrieval_id.0)
            .bind(&retrieval.principal_id.0)
            .bind(ordinal)
            .bind(candidate.case_id)
            .bind(candidate.fact_id)
            .bind(candidate.revision_id)
            .bind(candidate.exact_identity_rank)
            .bind(candidate.lexical_rank)
            .bind(&candidate.lexical_score)
            .bind(&candidate.final_score)
            .bind(&candidate.source_content_sha256)
            .bind(&candidate.projection_sha256)
            .bind(&candidate.item_sha256)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        }

        let receipt = select_retrieval_receipt(
            &mut transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            retrieval.retrieval_id,
            None,
            &retrieval.authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(map_retrieval_sqlx)?;
        Ok(RetrievalMutationOutcome {
            receipt,
            replayed: false,
        })
    }
}
