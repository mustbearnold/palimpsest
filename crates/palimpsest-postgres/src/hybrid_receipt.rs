//! write_path — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use palimpsest_application::{IdempotencyRequest, RepositoryError, RetrievalQueryEmbedding};
use palimpsest_domain::{NewRetrieval, RetrievalReceipt};
use pgvector::Vector;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;

use super::retrieval::{
    HybridPolicyPlan, hybrid_candidate_from_row, map_retrieval_sqlx, select_retrieval_receipt,
};
use super::{PostgresMemoryRepository, embedding_vector_sha256, unexpected};

impl PostgresMemoryRepository {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_hybrid_receipt_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        retrieval: &NewRetrieval,
        idempotency: &IdempotencyRequest,
        query_embedding: Option<&RetrievalQueryEmbedding>,
        perspective: &str,
        current_projection_coverage: &str,
        valid_at: OffsetDateTime,
        recorded_at: OffsetDateTime,
        evaluated_at: OffsetDateTime,
        projection_schema_version: i32,
        projection_schema_sha256: &str,
        plan: &HybridPolicyPlan,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        let query_embedding = query_embedding.ok_or_else(|| {
            RepositoryError::Unexpected("query embedding is unavailable".to_owned())
        })?;
        if query_embedding.profile != plan.profile
            || query_embedding.output.input_sha256 != retrieval.query_sha256
            || query_embedding.output.values.len() != plan.profile.dimensions
        {
            return Err(RepositoryError::Unexpected(
                "query embedding does not match the retrieval plan".to_owned(),
            ));
        }
        let query_vector_sha256 = embedding_vector_sha256(&query_embedding.output.values);
        let query_vector = Vector::from(query_embedding.output.values.clone());
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
        let rows = sqlx::query(
            r#"
            WITH current_projection AS MATERIALIZED (
                SELECT projection.tenant_id,
                    projection.subject_id,
                    projection.case_id,
                    projection.fact_id,
                    projection.revision_id,
                    projection.namespace,
                    projection.fact_key,
                    projection.value,
                    projection.observed_at,
                    projection.confidence,
                    projection.sensitivity,
                    projection.content_sha256
                FROM memory.fact_revision_current AS projection
                WHERE $30::text = 'current'
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
                      $30::text <> 'current'
                      OR (
                          $31::text <> 'complete'
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
                    missing.namespace,
                    missing.fact_key,
                    revision.value,
                    revision.observed_at,
                    revision.confidence,
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
                        revision.observed_at,
                        revision.confidence,
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
                SELECT effective.*,
                    governance.recency_profile_id,
                    governance.recency_profile_version,
                    governance.recency_profile_sha256,
                    governance.recency_anchor_at,
                    governance.importance,
                    greatest(
                        0::numeric,
                        extract(epoch FROM (
                            $4::timestamptz - governance.recency_anchor_at
                        )) * 1000000
                    )::numeric(30, 0) AS recency_age_us
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
                SELECT authorized.*,
                    document.search_vector,
                    document.projection_sha256,
                    embedding.embedding,
                    embedding.embedding_profile_sha256,
                    embedding.embedding_projection_profile_sha256,
                    embedding.input_sha256 AS embedding_input_sha256,
                    embedding.vector_sha256 AS embedding_vector_sha256,
                    (
                        document.revision_id IS NOT NULL
                        AND document.projection_schema_sha256 = $12
                        AND document.source_content_sha256
                            = authorized.content_sha256
                        AND document.projection_sha256 =
                            memory.fact_projection_sha256_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                        AND document.search_vector =
                            memory.fact_search_vector_v1(
                                authorized.namespace,
                                authorized.fact_key,
                                authorized.value
                            )
                    ) AS lexical_ready,
                    (
                        embedding.revision_id IS NOT NULL
                        AND embedding.embedding_profile_id = $16
                        AND embedding.embedding_profile_version = $17
                        AND embedding.embedding_profile_sha256 = $18
                        AND embedding.embedding_dimensions = $19
                        AND embedding.embedding_projection_profile_id = $20
                        AND embedding.embedding_projection_profile_version = $21
                        AND embedding.embedding_projection_profile_sha256 = $22
                        AND embedding.source_content_sha256
                            = authorized.content_sha256
                        AND embedding.source_projection_sha256
                            = document.projection_sha256
                    ) AS embedding_ready
                FROM authorized
                LEFT JOIN memory.fact_revision_search_documents AS document
                  ON document.tenant_id = authorized.tenant_id
                 AND document.subject_id = authorized.subject_id
                 AND document.case_id = authorized.case_id
                 AND document.fact_id = authorized.fact_id
                 AND document.revision_id = authorized.revision_id
                 AND document.projection_schema_version = $11
                LEFT JOIN memory.retrieval_ready_fact_revision_embeddings AS embedding
                  ON embedding.tenant_id = authorized.tenant_id
                 AND embedding.subject_id = authorized.subject_id
                 AND embedding.case_id = authorized.case_id
                 AND embedding.fact_id = authorized.fact_id
                 AND embedding.revision_id = authorized.revision_id
                 AND embedding.embedding_profile_id = $16
                 AND embedding.embedding_profile_version = $17
                 AND embedding.embedding_projection_profile_id = $20
                 AND embedding.embedding_projection_profile_version = $21
            ),
            coverage AS (
                SELECT COALESCE(
                    bool_or(NOT lexical_ready OR NOT embedding_ready),
                    false
                ) AS coverage_missing
                FROM projected
            ),
            eligible AS MATERIALIZED (
                SELECT *
                FROM projected
                WHERE lexical_ready AND embedding_ready
            ),
            scored AS MATERIALIZED (
                SELECT eligible.*,
                    CASE
                        WHEN lower(eligible.namespace || ':' || eligible.fact_key)
                            = lower(btrim($13)) THEN 1::smallint
                        WHEN lower(eligible.fact_key) = lower(btrim($13))
                            THEN 2::smallint
                        ELSE NULL::smallint
                    END AS exact_identity_rank,
                    eligible.search_vector
                        @@ websearch_to_tsquery('pg_catalog.simple', $13)
                        AS lexical_match,
                    ts_rank_cd(
                        eligible.search_vector,
                        websearch_to_tsquery('pg_catalog.simple', $13),
                        $14
                    )::double precision AS lexical_score,
                    eligible.embedding <=> $15 AS vector_distance
                FROM eligible
            ),
            exact_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.exact_identity_rank,
                    row_number() OVER (
                        ORDER BY scored.exact_identity_rank,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS exact_rank
                FROM scored
                WHERE scored.exact_identity_rank IS NOT NULL
                ORDER BY scored.exact_identity_rank,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $23
            ),
            lexical_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.lexical_score,
                    row_number() OVER (
                        ORDER BY scored.lexical_score DESC,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS lexical_rank
                FROM scored
                WHERE scored.lexical_match
                ORDER BY scored.lexical_score DESC,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $24
            ),
            vector_channel AS MATERIALIZED (
                SELECT scored.case_id, scored.fact_id, scored.revision_id,
                    scored.vector_distance,
                    row_number() OVER (
                        ORDER BY scored.vector_distance,
                            scored.case_id, scored.fact_id, scored.revision_id
                    ) AS vector_rank
                FROM scored
                ORDER BY scored.vector_distance,
                    scored.case_id, scored.fact_id, scored.revision_id
                LIMIT $25
            ),
            candidate_keys AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id FROM exact_channel
                UNION
                SELECT case_id, fact_id, revision_id FROM lexical_channel
                UNION
                SELECT case_id, fact_id, revision_id FROM vector_channel
            ),
            fusion AS MATERIALIZED (
                SELECT eligible.case_id, eligible.fact_id,
                    eligible.revision_id,
                    exact_channel.exact_identity_rank,
                    exact_channel.exact_rank,
                    lexical_channel.lexical_rank,
                    lexical_channel.lexical_score,
                    vector_channel.vector_rank,
                    vector_channel.vector_distance,
                    CASE WHEN vector_channel.vector_distance IS NULL
                        THEN NULL::double precision
                        ELSE 1.0 - vector_channel.vector_distance
                    END AS vector_similarity,
                    CASE WHEN exact_channel.exact_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + exact_channel.exact_rank),
                            $26
                        )
                    END AS exact_rrf,
                    CASE WHEN lexical_channel.lexical_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + lexical_channel.lexical_rank),
                            $26
                        )
                    END AS lexical_rrf,
                    CASE WHEN vector_channel.vector_rank IS NULL THEN 0::numeric
                        ELSE round(
                            1::numeric / ($27 + vector_channel.vector_rank),
                            $26
                        )
                    END AS vector_rrf,
                    eligible.recency_profile_id,
                    eligible.recency_profile_version,
                    eligible.recency_profile_sha256,
                    eligible.recency_anchor_at,
                    eligible.recency_age_us,
                    eligible.confidence,
                    eligible.importance,
                    eligible.content_sha256,
                    eligible.projection_sha256,
                    eligible.embedding_input_sha256,
                    eligible.embedding_vector_sha256
                FROM candidate_keys
                JOIN eligible
                  ON eligible.case_id = candidate_keys.case_id
                 AND eligible.fact_id = candidate_keys.fact_id
                 AND eligible.revision_id = candidate_keys.revision_id
                LEFT JOIN exact_channel
                  ON exact_channel.case_id = candidate_keys.case_id
                 AND exact_channel.fact_id = candidate_keys.fact_id
                 AND exact_channel.revision_id = candidate_keys.revision_id
                LEFT JOIN lexical_channel
                  ON lexical_channel.case_id = candidate_keys.case_id
                 AND lexical_channel.fact_id = candidate_keys.fact_id
                 AND lexical_channel.revision_id = candidate_keys.revision_id
                LEFT JOIN vector_channel
                  ON vector_channel.case_id = candidate_keys.case_id
                 AND vector_channel.fact_id = candidate_keys.fact_id
                 AND vector_channel.revision_id = candidate_keys.revision_id
            ),
            ranked AS MATERIALIZED (
                SELECT fusion.*,
                    fusion.exact_rrf + fusion.lexical_rrf + fusion.vector_rrf
                        AS fused_score,
                    row_number() OVER (
                        ORDER BY
                            fusion.exact_rrf + fusion.lexical_rrf
                                + fusion.vector_rrf DESC,
                            fusion.exact_identity_rank ASC NULLS LAST,
                            fusion.exact_rank ASC NULLS LAST,
                            fusion.lexical_rank ASC NULLS LAST,
                            fusion.vector_rank ASC NULLS LAST,
                            fusion.case_id, fusion.fact_id, fusion.revision_id
                    ) AS final_rank
                FROM fusion
            ),
            limited AS MATERIALIZED (
                SELECT case_id, fact_id, revision_id,
                    exact_identity_rank, exact_rank, lexical_rank,
                    CASE WHEN lexical_rank IS NULL THEN NULL::text
                        ELSE round(lexical_score::numeric, $26)::text
                    END AS lexical_score,
                    vector_rank,
                    CASE WHEN vector_rank IS NULL THEN NULL::text
                        ELSE round(vector_distance::numeric, $26)::text
                    END AS vector_distance,
                    CASE WHEN vector_rank IS NULL THEN NULL::text
                        ELSE round(vector_similarity::numeric, $26)::text
                    END AS vector_similarity,
                    round(exact_rrf, $26)::text AS exact_rrf,
                    round(lexical_rrf, $26)::text AS lexical_rrf,
                    round(vector_rrf, $26)::text AS vector_rrf,
                    round(fused_score, $26)::text AS fused_score,
                    content_sha256, projection_sha256,
                    embedding_input_sha256, embedding_vector_sha256,
                    recency_profile_id, recency_profile_version,
                    recency_profile_sha256, recency_anchor_at,
                    recency_age_us, confidence, importance,
                    final_rank
                FROM ranked
                ORDER BY final_rank
                LIMIT CASE WHEN $29 THEN 150 ELSE $28 END
            )
            SELECT coverage.coverage_missing,
                candidate.fact_id IS NOT NULL AS candidate_present,
                candidate.case_id, candidate.fact_id, candidate.revision_id,
                candidate.exact_identity_rank, candidate.exact_rank,
                candidate.lexical_rank, candidate.lexical_score,
                candidate.vector_rank, candidate.vector_distance,
                candidate.vector_similarity, candidate.exact_rrf,
                candidate.lexical_rrf, candidate.vector_rrf,
                candidate.fused_score, candidate.content_sha256,
                candidate.projection_sha256,
                candidate.embedding_input_sha256,
                candidate.embedding_vector_sha256,
                candidate.recency_profile_id,
                candidate.recency_profile_version,
                candidate.recency_profile_sha256,
                candidate.recency_anchor_at,
                candidate.recency_age_us::text AS recency_age_us,
                (candidate.confidence * 10000)::bigint AS confidence_basis_points,
                (candidate.importance * 10000)::bigint AS importance_basis_points,
                candidate.final_rank
            FROM coverage
            LEFT JOIN limited AS candidate
              ON NOT coverage.coverage_missing
            ORDER BY candidate.final_rank
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
        .bind(projection_schema_sha256)
        .bind(retrieval.query.as_str())
        .bind(plan.fts_rank_normalization)
        .bind(query_vector)
        .bind(&plan.profile.id)
        .bind(&plan.profile.version)
        .bind(&plan.profile.digest)
        .bind(i32::try_from(plan.profile.dimensions).map_err(unexpected)?)
        .bind(&plan.projection_profile_id)
        .bind(&plan.projection_profile_version)
        .bind(&plan.projection_profile_sha256)
        .bind(plan.exact_candidate_limit)
        .bind(plan.lexical_candidate_limit)
        .bind(plan.vector_candidate_limit)
        .bind(plan.score_scale)
        .bind(plan.rrf_k)
        .bind(plan.manifest_limit)
        .bind(plan.temporal_scoring)
        .bind(perspective)
        .bind(current_projection_coverage)
        .fetch_all(&mut **transaction)
        .await
        .map_err(unexpected)?;
        let coverage_missing = rows
            .first()
            .ok_or_else(|| {
                RepositoryError::Unexpected("hybrid retrieval query returned no rows".to_owned())
            })?
            .try_get::<bool, _>("coverage_missing")
            .map_err(unexpected)?;
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
                candidates.push(hybrid_candidate_from_row(row, plan.temporal_scoring)?);
            }
        }
        if plan.temporal_scoring {
            candidates.sort_by(|left, right| {
                left.temporal
                    .as_ref()
                    .expect("temporal policy creates temporal candidates")
                    .order_key
                    .cmp(
                        &right
                            .temporal
                            .as_ref()
                            .expect("temporal policy creates temporal candidates")
                            .order_key,
                    )
            });
            candidates.truncate(usize::try_from(plan.manifest_limit).map_err(unexpected)?);
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
        sqlx::query(
            r#"
            INSERT INTO memory.retrieval_receipts (
                tenant_id, subject_id, retrieval_id, principal_id,
                idempotency_key, request_fingerprint, query_sha256,
                perspective, valid_at, recorded_at, evaluated_at,
                policy_id, policy_version, policy_sha256,
                projection_schema_version, projection_schema_sha256,
                authorization_scope_sha256, authorization_policy_version,
                outcome, abstention_reason, stage_timings_ms, manifest_sha256,
                page_size, schema_version,
                embedding_profile_id, embedding_profile_version,
                embedding_profile_sha256,
                embedding_projection_profile_id,
                embedding_projection_profile_version,
                embedding_projection_profile_sha256,
                query_input_sha256, query_vector_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, 'principal-scope-v1',
                $18, $19, $20, $21, $22, 1,
                $23, $24, $25, $26, $27, $28, $29, $30
            )
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
        .bind(&plan.policy_version)
        .bind(&plan.policy_sha256)
        .bind(projection_schema_version)
        .bind(projection_schema_sha256)
        .bind(&retrieval.authorization_scope_sha256)
        .bind(outcome)
        .bind(abstention_reason)
        .bind(&stage_timings_ms)
        .bind(&manifest_sha256)
        .bind(i16::try_from(retrieval.page_size).map_err(unexpected)?)
        .bind(&plan.profile.id)
        .bind(&plan.profile.version)
        .bind(&plan.profile.digest)
        .bind(&plan.projection_profile_id)
        .bind(&plan.projection_profile_version)
        .bind(&plan.projection_profile_sha256)
        .bind(&retrieval.query_sha256)
        .bind(&query_vector_sha256)
        .execute(&mut **transaction)
        .await
        .map_err(map_retrieval_sqlx)?;

        for (index, candidate) in candidates.iter().enumerate() {
            let ordinal = i16::try_from(index + 1).map_err(unexpected)?;
            sqlx::query(
                r#"
                INSERT INTO memory.retrieval_manifest_items (
                    tenant_id, subject_id, retrieval_id, principal_id,
                    ordinal, case_id, fact_id, revision_id,
                    exact_identity_rank, exact_rank, lexical_rank,
                    lexical_score, vector_rank, vector_distance,
                    vector_similarity, exact_rrf_contribution,
                    lexical_rrf_contribution, vector_rrf_contribution,
                    fused_score, final_rank, final_score,
                    source_content_sha256, projection_sha256, item_sha256,
                    embedding_profile_sha256,
                    embedding_projection_profile_sha256,
                    embedding_input_sha256, embedding_vector_sha256,
                    recency_profile_id, recency_profile_version,
                    recency_profile_sha256, recency_anchor_at,
                    recency_age_us, recency_factor, confidence_factor,
                    importance_factor, temporal_adjustment,
                    confidence_adjustment, importance_adjustment,
                    exact_identity_bonus,
                    schema_version
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, COALESCE($12::numeric, 0),
                    $13, $14::numeric, $15::numeric,
                    $16::numeric, $17::numeric, $18::numeric,
                    $19::numeric, $5, $27::numeric,
                    $20, $21, $22, $23, $24, $25, $26,
                    $28, $29, $30, $31, $32::numeric,
                    $33::numeric, $34::numeric, $35::numeric,
                    $36::numeric, $37::numeric, $38::numeric, $39::numeric,
                    $40
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
            .bind(candidate.exact_rank)
            .bind(candidate.lexical_rank)
            .bind(candidate.lexical_score.as_deref())
            .bind(candidate.vector_rank)
            .bind(candidate.vector_distance.as_deref())
            .bind(candidate.vector_similarity.as_deref())
            .bind(&candidate.exact_rrf)
            .bind(&candidate.lexical_rrf)
            .bind(&candidate.vector_rrf)
            .bind(&candidate.fused_score)
            .bind(&candidate.source_content_sha256)
            .bind(&candidate.projection_sha256)
            .bind(&candidate.item_sha256)
            .bind(&plan.profile.digest)
            .bind(&plan.projection_profile_sha256)
            .bind(&candidate.embedding_input_sha256)
            .bind(&candidate.embedding_vector_sha256)
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map_or(candidate.fused_score.as_str(), |value| {
                        value.final_score.as_str()
                    }),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_id.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_version.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_profile_sha256.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_anchor_at),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_age_us.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.recency_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.confidence_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.importance_factor.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.temporal_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.confidence_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.importance_adjustment.as_str()),
            )
            .bind(
                candidate
                    .temporal
                    .as_ref()
                    .map(|value| value.exact_identity_bonus.as_str()),
            )
            .bind(if candidate.temporal.is_some() {
                2_i32
            } else {
                1_i32
            })
            .execute(&mut **transaction)
            .await
            .map_err(unexpected)?;
        }

        select_retrieval_receipt(
            transaction,
            retrieval.tenant_id,
            retrieval.subject_id,
            retrieval.retrieval_id,
            None,
            &retrieval.authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)
    }
}
