//! projection_helpers — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use palimpsest_conformance::{HybridFusionFixture, RetrievalIsolationFixture, Target};
use sqlx::{AssertSqlSafe, PgPool, Row};
use uuid::Uuid;

pub(crate) async fn verify_no_ann_indexes(pool: &PgPool) -> Result<()> {
    let ann_index_count: i64 = sqlx::query(
        r#"
        SELECT count(*) AS count
        FROM pg_index AS index
        JOIN pg_class AS relation ON relation.oid = index.indrelid
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        JOIN pg_class AS index_relation ON index_relation.oid = index.indexrelid
        JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
        WHERE namespace.nspname = 'memory'
          AND access_method.amname IN ('hnsw', 'ivfflat')
        "#,
    )
    .fetch_one(pool)
    .await?
    .try_get("count")?;
    ensure!(
        ann_index_count == 0,
        "an ANN index entered exact retrieval v1"
    );
    Ok(())
}

pub(crate) async fn delete_embedding_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let deleted = sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn stale_embedding_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let updated = sqlx::query(
        r#"
        UPDATE memory.fact_revision_embedding_projections
        SET status = 'pending',
            embedding = NULL,
            vector_sha256 = NULL,
            failure_code = NULL,
            generation_attempt_id = NULL,
            generation_started_at = NULL,
            generated_at = NULL
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
          AND status = 'ready'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(updated.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn verify_projection_failure_code(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
    expected_code: &str,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let row = sqlx::query(
        r#"
        SELECT status, failure_code, embedding IS NULL AS vector_absent,
            to_jsonb(projection)::text AS record
        FROM memory.fact_revision_embedding_projections AS projection
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(row.try_get::<String, _>("status")? == "failed");
    ensure!(row.try_get::<String, _>("failure_code")? == expected_code);
    ensure!(row.try_get::<bool, _>("vector_absent")?);
    let record: String = row.try_get("record")?;
    for forbidden in ["fixture-provider-outage-private-vector", "[1,0,0,0]", "NaN"] {
        ensure!(!record.contains(forbidden));
    }
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn verify_hybrid_manifest_isolation(
    pool: &PgPool,
    target: &Target,
    fixture: &HybridFusionFixture,
    receipt: &Value,
) -> Result<()> {
    let retrieval_id = Uuid::parse_str(
        receipt
            .get("retrieval_id")
            .and_then(Value::as_str)
            .context("hybrid receipt omitted retrieval_id")?,
    )?;
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let rows = sqlx::query(
        r#"
        SELECT revision_id, to_jsonb(item)::text AS record
        FROM memory.retrieval_manifest_items AS item
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .fetch_all(&mut *transaction)
    .await?;
    let revision_ids = rows
        .iter()
        .map(|row| row.try_get::<Uuid, _>("revision_id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        revision_ids
            == vec![
                fixture.exact_revision_id,
                fixture.beta_revision_id,
                fixture.alpha_revision_id,
                fixture.gamma_revision_id,
                fixture.delta_revision_id,
            ]
    );
    ensure!(!revision_ids.contains(&fixture.forbidden_revision_id));
    for row in rows {
        let record: String = row.try_get("record")?;
        for forbidden in [
            &fixture.forbidden_revision_id.to_string(),
            "restricted-vector-trap",
            "vector_fixture_forbidden_4d",
            "[1,0,0,0]",
        ] {
            ensure!(!record.contains(forbidden));
        }
    }
    transaction.commit().await?;
    verify_hybrid_manifest_rejects_invalid_fusion(pool, target, retrieval_id).await
}

pub(crate) async fn verify_hybrid_manifest_rejects_invalid_fusion(
    pool: &PgPool,
    target: &Target,
    retrieval_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let invalid_insert = sqlx::query(
        r#"
        INSERT INTO memory.retrieval_manifest_items (
            tenant_id, subject_id, retrieval_id, principal_id,
            ordinal, case_id, fact_id, revision_id,
            exact_identity_rank, lexical_rank, lexical_score,
            final_rank, final_score, source_content_sha256,
            projection_sha256, item_sha256, schema_version,
            exact_rank, vector_rank, vector_distance, vector_similarity,
            exact_rrf_contribution, lexical_rrf_contribution,
            vector_rrf_contribution, fused_score,
            embedding_profile_sha256,
            embedding_projection_profile_sha256,
            embedding_input_sha256, embedding_vector_sha256
        )
        SELECT
            tenant_id, subject_id, retrieval_id, principal_id,
            99, case_id, fact_id, revision_id,
            exact_identity_rank, lexical_rank, lexical_score,
            99, final_score, source_content_sha256,
            projection_sha256, repeat('0', 64), schema_version,
            exact_rank, vector_rank, vector_distance, vector_similarity,
            exact_rrf_contribution + 0.000000000001,
            lexical_rrf_contribution, vector_rrf_contribution, fused_score,
            embedding_profile_sha256,
            embedding_projection_profile_sha256,
            embedding_input_sha256, embedding_vector_sha256
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2
          AND retrieval_id = $3 AND ordinal = 1
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .execute(&mut *transaction)
    .await;
    transaction.rollback().await?;
    ensure!(
        invalid_insert.is_err(),
        "hybrid manifest accepted a mismatched RRF contribution"
    );
    Ok(())
}

pub(crate) async fn verify_hybrid_failure_metadata_is_redacted(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let record: String = sqlx::query(
        r#"
        SELECT concat_ws(
            '',
            COALESCE((SELECT jsonb_agg(to_jsonb(receipt))::text
                FROM memory.retrieval_receipts AS receipt
                WHERE tenant_id = $1 AND subject_id = $2), ''),
            COALESCE((SELECT jsonb_agg(to_jsonb(item))::text
                FROM memory.retrieval_manifest_items AS item
                WHERE tenant_id = $1 AND subject_id = $2), ''),
            COALESCE((SELECT jsonb_agg(to_jsonb(reservation))::text
                FROM memory.retrieval_idempotency_reservations AS reservation
                WHERE tenant_id = $1 AND subject_id = $2), '')
        ) AS record
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    for forbidden in [
        "case.retrieval:fusiontoken",
        "fixture-provider-outage-private-vector",
        "vector_fixture_forbidden_4d",
        "restricted-vector-trap",
        "[1,0,0,0]",
        "[-1,0,0,0]",
        "NaN",
    ] {
        ensure!(
            !record.contains(forbidden),
            "durable retrieval metadata disclosed {forbidden}"
        );
    }
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn set_retrieval_test_scope(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &Target,
) -> Result<()> {
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut **transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut **transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut **transaction)
        .await?;
    sqlx::query(
        r#"SELECT set_config(
            'palimpsest.allowed_sensitivities',
            '["internal","restricted"]',
            true
        )"#,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn rebuilds_the_current_fact_revision_projection(
    pool: &PgPool,
    migration_pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let fact_id: Uuid = sqlx::query_scalar(
        "SELECT fact_id FROM memory.facts
         WHERE tenant_id = $1 AND subject_id = $2
           AND namespace = 'case.profile' AND fact_key = 'shipping_address'",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(migration_pool)
    .await?;
    let expected_revision_id: Uuid = sqlx::query_scalar(
        "SELECT revision_id FROM memory.fact_revision_current
         WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fact_id)
    .fetch_one(migration_pool)
    .await?;
    let coverage = sqlx::query(
        "SELECT coverage_state, fact_count, projection_count
         FROM memory.fact_revision_current_coverage
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(migration_pool)
    .await?;
    ensure!(coverage.try_get::<String, _>("coverage_state")? == "complete");
    ensure!(coverage.try_get::<i64, _>("fact_count")? >= 1);
    ensure!(
        coverage.try_get::<i64, _>("fact_count")?
            == coverage.try_get::<i64, _>("projection_count")?
    );

    let quoted_session_role: String = sqlx::query_scalar("SELECT quote_ident(session_user::text)")
        .fetch_one(migration_pool)
        .await?;
    let denied_role = format!(
        "palimpsest_current_repair_denied_{}",
        Uuid::now_v7().simple()
    );
    sqlx::query(AssertSqlSafe(format!(
        "CREATE ROLE \"{denied_role}\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE \
         NOINHERIT NOREPLICATION NOBYPASSRLS"
    )))
    .execute(migration_pool)
    .await?;
    sqlx::query(AssertSqlSafe(format!(
        "GRANT \"{denied_role}\" TO {quoted_session_role}"
    )))
    .execute(migration_pool)
    .await?;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "GRANT USAGE ON SCHEMA memory TO \"{denied_role}\"; \
         GRANT SELECT ON memory.subject_lifecycles TO \"{denied_role}\"; \
         GRANT SELECT, DELETE ON memory.fact_revision_current TO \"{denied_role}\"; \
         GRANT EXECUTE ON FUNCTION \
         memory.subject_lifecycle_allows_content(uuid, uuid), \
         memory.deletion_workflow_allows(uuid, uuid) \
         TO \"{denied_role}\""
    )))
    .execute(migration_pool)
    .await?;
    let denied_result = async {
        let mut transaction = migration_pool.begin().await?;
        sqlx::query(AssertSqlSafe(format!("SET LOCAL ROLE \"{denied_role}\"")))
            .execute(&mut *transaction)
            .await?;
        let result =
            sqlx::query_scalar::<_, i64>("SELECT memory.rebuild_fact_revision_current($1, $2)")
                .bind(target.tenant_id)
                .bind(target.subject_id)
                .fetch_one(&mut *transaction)
                .await;
        ensure!(result.is_err(), "restricted role executed current repair");
        transaction.rollback().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cross_scope_result = async {
        let mut transaction = migration_pool.begin().await?;
        sqlx::query(AssertSqlSafe(format!("SET LOCAL ROLE \"{denied_role}\"")))
            .execute(&mut *transaction)
            .await?;
        set_retrieval_test_scope(&mut transaction, target).await?;
        sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
            .bind(target.principal_a_secondary_subject_id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "SELECT set_config(
                'palimpsest.fact_revision_current_repair',
                'palimpsest-fact-current-repair-v1',
                true
            )",
        )
        .execute(&mut *transaction)
        .await?;
        let result = sqlx::query(
            "DELETE FROM memory.fact_revision_current
             WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3",
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .bind(fact_id)
        .execute(&mut *transaction)
        .await?;
        ensure!(
            result.rows_affected() == 0,
            "restricted repair role crossed the configured subject scope"
        );
        transaction.rollback().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cleanup_role = sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP OWNED BY \"{denied_role}\"; \
         REVOKE \"{denied_role}\" FROM {quoted_session_role}; \
         DROP ROLE \"{denied_role}\""
    )))
    .execute(migration_pool)
    .await;
    denied_result?;
    cross_scope_result?;
    cleanup_role?;

    // The owner-only repair path must run as the role that owns the derived
    // tables (the conformance runtime role applied the migrations), because
    // rebuild_fact_revision_current requires session_user to be the table
    // owner. The migration pool only owns the outer test database.
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    sqlx::query(
        "SELECT set_config(
            'palimpsest.fact_revision_current_repair',
            'palimpsest-fact-current-repair-v1',
            true
        )",
    )
    .execute(&mut *transaction)
    .await?;
    let deleted = sqlx::query(
        "DELETE FROM memory.fact_revision_current
         WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fact_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    let coverage_after_delete: String = sqlx::query_scalar(
        "SELECT coverage_state
         FROM memory.fact_revision_current_coverage
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(coverage_after_delete == "repair_required");
    let rebuilt: i64 = sqlx::query_scalar("SELECT memory.rebuild_fact_revision_current($1, $2)")
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .fetch_one(&mut *transaction)
        .await?;
    ensure!(rebuilt >= 1);
    let repaired_revision_id: Uuid = sqlx::query_scalar(
        "SELECT revision_id FROM memory.fact_revision_current
         WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fact_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(repaired_revision_id == expected_revision_id);
    let coverage_after_rebuild: String = sqlx::query_scalar(
        "SELECT coverage_state
         FROM memory.fact_revision_current_coverage
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(coverage_after_rebuild == "complete");
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn verify_retrieval_manifest_is_authorized(
    pool: &PgPool,
    target: &Target,
    fixture: &RetrievalIsolationFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let revision_ids = sqlx::query(
        r#"
        SELECT revision_id
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| row.try_get::<Uuid, _>("revision_id"))
    .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        revision_ids == vec![fixture.allowed_revision_id],
        "the internal-only receipt manifest contains unauthorized candidates"
    );
    ensure!(
        fixture
            .forbidden_revision_ids
            .iter()
            .all(|revision_id| !revision_ids.contains(revision_id)),
        "a forbidden revision entered the durable retrieval manifest"
    );
    let receipt_record: String = sqlx::query(
        r#"
        SELECT to_jsonb(receipt)::text AS record
        FROM memory.retrieval_receipts AS receipt
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    let manifest_record: String = sqlx::query(
        r#"
        SELECT COALESCE(jsonb_agg(to_jsonb(item)), '[]'::jsonb)::text AS record
        FROM memory.retrieval_manifest_items AS item
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    let idempotency_record: String = sqlx::query(
        r#"
        SELECT to_jsonb(reservation)::text AS record
        FROM memory.retrieval_idempotency_reservations AS reservation
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("record")?;
    for private_text in [
        "cobalt-otter-731",
        "internal-visible-value",
        "restricted-hidden-value",
        "cross-subject-hidden-value",
        "cross-tenant-hidden-value",
    ] {
        ensure!(
            !receipt_record.contains(private_text)
                && !manifest_record.contains(private_text)
                && !idempotency_record.contains(private_text),
            "durable retrieval metadata stored raw private text"
        );
    }
    transaction.commit().await?;
    Ok(())
}
