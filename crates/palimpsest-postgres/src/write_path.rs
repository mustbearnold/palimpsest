//! write_path — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use palimpsest_application::{IdempotencyRequest, RepositoryError};
use palimpsest_domain::{
    AgentId, CaseId, CheckpointId, CheckpointRevisionId, EmbeddingProfile, EpisodeId, FactId,
    PrincipalScope, Q63_EXP2_CONSTANTS_SHA256, RetrievalId, RetrievalReceipt, RevisionId,
    SubjectId, TenantId, ThreadId,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

use super::retrieval::{HybridPolicyPlan, select_retrieval_receipt, set_retrieval_scope};
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

impl PostgresMemoryRepository {
    pub(crate) async fn current_projection_coverage_state(
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: TenantId,
        subject_id: SubjectId,
        perspective: &str,
        evaluated_at: OffsetDateTime,
    ) -> Result<String, RepositoryError> {
        if perspective != "current" {
            return Ok("not_current".to_owned());
        }
        let coverage = sqlx::query(
            r#"
            SELECT coverage_state, coverage_valid_until
            FROM memory.fact_revision_current_coverage
            WHERE tenant_id = $1 AND subject_id = $2
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(unexpected)?;
        match coverage {
            Some(coverage) => {
                let state: String = coverage.try_get("coverage_state").map_err(unexpected)?;
                let coverage_valid_until: Option<OffsetDateTime> = coverage
                    .try_get("coverage_valid_until")
                    .map_err(unexpected)?;
                let horizon_is_open = match coverage_valid_until {
                    Some(coverage_valid_until) => coverage_valid_until > evaluated_at,
                    None => true,
                };
                if state == "complete" && horizon_is_open {
                    Ok("complete".to_owned())
                } else {
                    Ok("repair_required".to_owned())
                }
            }
            None => Ok("repair_required".to_owned()),
        }
    }

    pub(crate) async fn authorized_current_projection_coverage_state(
        transaction: &mut Transaction<'_, Postgres>,
        tenant_id: TenantId,
        subject_id: SubjectId,
        perspective: &str,
        evaluated_at: OffsetDateTime,
        projection_schema_version: i32,
        projection_schema_sha256: &str,
    ) -> Result<String, RepositoryError> {
        if perspective != "current" {
            return Ok("not_current".to_owned());
        }
        let coverage = sqlx::query(
            r#"
            SELECT coverage_state, coverage_valid_until,
                projection_schema_version_min,
                btrim(projection_schema_sha256::text) AS projection_schema_sha256
            FROM memory.authorized_current_projection_coverage
            WHERE tenant_id = $1 AND subject_id = $2
            "#,
        )
        .bind(tenant_id.0)
        .bind(subject_id.0)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(unexpected)?;
        match coverage {
            Some(coverage) => {
                let state: String = coverage.try_get("coverage_state").map_err(unexpected)?;
                let coverage_valid_until: Option<OffsetDateTime> = coverage
                    .try_get("coverage_valid_until")
                    .map_err(unexpected)?;
                let schema_version_min: Option<i32> = coverage
                    .try_get("projection_schema_version_min")
                    .map_err(unexpected)?;
                let schema_sha256: Option<String> = coverage
                    .try_get("projection_schema_sha256")
                    .map_err(unexpected)?;
                let horizon_is_open = match coverage_valid_until {
                    Some(coverage_valid_until) => coverage_valid_until > evaluated_at,
                    None => true,
                };
                let schema_matches = match schema_version_min {
                    Some(version) => {
                        version == projection_schema_version
                            && schema_sha256.as_deref() == Some(projection_schema_sha256)
                    }
                    None => false,
                };
                if state == "complete" && horizon_is_open && schema_matches {
                    Ok("complete".to_owned())
                } else {
                    Ok("repair_required".to_owned())
                }
            }
            None => Ok("repair_required".to_owned()),
        }
    }
}

impl PostgresMemoryRepository {
    pub(crate) async fn get_receipt_once(
        &self,
        principal: &PrincipalScope,
        tenant_id: TenantId,
        subject_id: SubjectId,
        retrieval_id: RetrievalId,
        cursor: Option<String>,
        authorization_scope_sha256: &str,
    ) -> Result<RetrievalReceipt, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        set_retrieval_scope(
            &mut transaction,
            tenant_id,
            subject_id,
            &principal.principal_id,
            &principal.allowed_sensitivities,
        )
        .await?;
        let receipt = select_retrieval_receipt(
            &mut transaction,
            tenant_id,
            subject_id,
            retrieval_id,
            cursor.as_deref(),
            authorization_scope_sha256,
        )
        .await?
        .ok_or(RepositoryError::NotFound)?;
        transaction.commit().await.map_err(unexpected)?;
        Ok(receipt)
    }
}

pub(crate) struct GovernedWrite<'a> {
    pub(crate) tenant_id: TenantId,
    pub(crate) subject_id: SubjectId,
    pub(crate) case_id: CaseId,
    pub(crate) principal_id: &'a str,
    pub(crate) operation_id: &'a str,
    pub(crate) request_fingerprint: &'a str,
    pub(crate) resource_episode_id: Option<EpisodeId>,
    pub(crate) resource_fact_id: Option<FactId>,
    pub(crate) resource_revision_id: Option<RevisionId>,
    pub(crate) resource_checkpoint: Option<CheckpointResource>,
    pub(crate) event_type: &'a str,
}

#[derive(Clone, Copy)]
pub(crate) struct CheckpointResource {
    pub(crate) agent_id: AgentId,
    pub(crate) thread_id: ThreadId,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) revision_id: CheckpointRevisionId,
}

#[derive(Clone, Copy)]
pub(crate) struct IdempotencyScope<'a> {
    pub(crate) tenant_id: TenantId,
    pub(crate) subject_id: SubjectId,
    pub(crate) principal_id: &'a str,
    pub(crate) operation_id: &'a str,
}

pub(crate) struct IdempotencyCompletion<'a> {
    pub(crate) scope: IdempotencyScope<'a>,
    pub(crate) key: &'a str,
    pub(crate) resource_episode_id: Option<EpisodeId>,
    pub(crate) resource_fact_id: Option<FactId>,
    pub(crate) resource_checkpoint: Option<CheckpointResource>,
    pub(crate) status: i16,
    pub(crate) body: serde_json::Value,
    pub(crate) etag: &'a str,
    pub(crate) location: &'a str,
}

pub(crate) async fn reserve_idempotency(
    transaction: &mut Transaction<'_, Postgres>,
    scope: IdempotencyScope<'_>,
    idempotency: &IdempotencyRequest,
) -> Result<Option<serde_json::Value>, RepositoryError> {
    sqlx::query("SELECT set_config('palimpsest.principal_id', $1, true)")
        .bind(scope.principal_id)
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    let reserved = sqlx::query(
        r#"
        INSERT INTO memory.idempotency_receipts (
            tenant_id, subject_id, principal_id, operation_id,
            idempotency_key, request_fingerprint, state
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'in_progress')
        ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key)
            DO NOTHING
        RETURNING true AS reserved
        "#,
    )
    .bind(scope.tenant_id.0)
    .bind(scope.subject_id.0)
    .bind(scope.principal_id)
    .bind(scope.operation_id)
    .bind(&idempotency.key)
    .bind(&idempotency.fingerprint)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?
    .is_some();
    if reserved {
        return Ok(None);
    }

    let receipt = sqlx::query(
        r#"
        SELECT request_fingerprint, state, response_body
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND principal_id = $2
          AND operation_id = $3
          AND idempotency_key = $4
        FOR UPDATE
        "#,
    )
    .bind(scope.tenant_id.0)
    .bind(scope.principal_id)
    .bind(scope.operation_id)
    .bind(&idempotency.key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let stored_fingerprint: String = receipt.try_get("request_fingerprint").map_err(unexpected)?;
    if stored_fingerprint != idempotency.fingerprint {
        return Err(RepositoryError::IdempotencyKeyReused);
    }
    let state: String = receipt.try_get("state").map_err(unexpected)?;
    if state != "completed" {
        return Err(RepositoryError::IdempotencyInProgress);
    }
    receipt
        .try_get("response_body")
        .map(Some)
        .map_err(unexpected)
}

pub(crate) async fn complete_idempotency(
    transaction: &mut Transaction<'_, Postgres>,
    completion: IdempotencyCompletion<'_>,
) -> Result<(), RepositoryError> {
    let checkpoint = completion.resource_checkpoint;
    let result = sqlx::query(
        r#"
        UPDATE memory.idempotency_receipts
        SET state = 'completed', resource_episode_id = $1, resource_fact_id = $2,
            resource_checkpoint_agent_id = $3, resource_checkpoint_thread_id = $4,
            resource_checkpoint_id = $5, resource_checkpoint_revision_id = $6,
            response_status = $7, response_body = $8, response_etag = $9,
            response_location = $10, completed_at = clock_timestamp()
        WHERE tenant_id = $11
          AND subject_id = $12
          AND principal_id = $13
          AND operation_id = $14
          AND idempotency_key = $15
          AND state = 'in_progress'
        "#,
    )
    .bind(completion.resource_episode_id.map(|id| id.0))
    .bind(completion.resource_fact_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .bind(completion.status)
    .bind(completion.body)
    .bind(completion.etag)
    .bind(completion.location)
    .bind(completion.scope.tenant_id.0)
    .bind(completion.scope.subject_id.0)
    .bind(completion.scope.principal_id)
    .bind(completion.scope.operation_id)
    .bind(completion.key)
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    if result.rows_affected() != 1 {
        return Err(RepositoryError::Unexpected(
            "idempotency receipt completion did not update one row".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn record_governed_write(
    transaction: &mut Transaction<'_, Postgres>,
    write: GovernedWrite<'_>,
) -> Result<(), RepositoryError> {
    let checkpoint = write.resource_checkpoint;
    sqlx::query(
        r#"
        INSERT INTO memory.write_audit_receipts (
            tenant_id, subject_id, case_id, principal_id, operation_id,
            authorization_decision, authorization_context, request_fingerprint,
            resource_episode_id, resource_fact_id, resource_revision_id,
            resource_checkpoint_agent_id, resource_checkpoint_thread_id,
            resource_checkpoint_id, resource_checkpoint_revision_id
        )
        VALUES (
            $1, $2, $3, $4, $5, 'authorized',
            jsonb_build_object(
                'principal_id', $4::text,
                'tenant_id', $1::uuid,
                'subject_id', $2::uuid
            ),
            $6, $7, $8, $9, $10, $11, $12, $13
        )
        "#,
    )
    .bind(write.tenant_id.0)
    .bind(write.subject_id.0)
    .bind(write.case_id.0)
    .bind(write.principal_id)
    .bind(write.operation_id)
    .bind(write.request_fingerprint)
    .bind(write.resource_episode_id.map(|id| id.0))
    .bind(write.resource_fact_id.map(|id| id.0))
    .bind(write.resource_revision_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;

    sqlx::query(
        r#"
        INSERT INTO memory.outbox_intents (
            tenant_id, subject_id, case_id, event_type,
            resource_episode_id, resource_fact_id, resource_revision_id,
            resource_checkpoint_agent_id, resource_checkpoint_thread_id,
            resource_checkpoint_id, resource_checkpoint_revision_id, payload
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            jsonb_strip_nulls(jsonb_build_object(
                'schema_version', 1,
                'tenant_id', $1::uuid,
                'subject_id', $2::uuid,
                'case_id', $3::uuid,
                'episode_id', $5::uuid,
                'fact_id', $6::uuid,
                'revision_id', $7::uuid,
                'agent_id', $8::uuid,
                'thread_id', $9::uuid,
                'checkpoint_id', $10::uuid,
                'checkpoint_revision_id', $11::uuid
            ))
        )
        "#,
    )
    .bind(write.tenant_id.0)
    .bind(write.subject_id.0)
    .bind(write.case_id.0)
    .bind(write.event_type)
    .bind(write.resource_episode_id.map(|id| id.0))
    .bind(write.resource_fact_id.map(|id| id.0))
    .bind(write.resource_revision_id.map(|id| id.0))
    .bind(checkpoint.map(|resource| resource.agent_id.0))
    .bind(checkpoint.map(|resource| resource.thread_id.0))
    .bind(checkpoint.map(|resource| resource.checkpoint_id.0))
    .bind(checkpoint.map(|resource| resource.revision_id.0))
    .execute(&mut **transaction)
    .await
    .map_err(unexpected)?;
    Ok(())
}
