//! projections — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use time::OffsetDateTime;

use palimpsest_application::EmbeddingProvider;
use palimpsest_conformance::{
    HybridFusionFixture, Target, hybrid_retrieval_fails_closed_without_leaking,
    hybrid_retrieval_recovers_after_projection_rebuild,
};
use palimpsest_domain::{SubjectId, TenantId};
use palimpsest_postgres::EmbeddingProjectionCoordinator;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::fixtures::{
    BlockingEmbeddingProvider, DeterministicEmbeddingProvider, EmbeddingFixtureMode,
};
use super::projection_helpers::{
    delete_embedding_projection, set_retrieval_test_scope, stale_embedding_projection,
    verify_projection_failure_code,
};

/// Proves the seeded projection lease policy and its immutability guard.
/// These checks complete without wall-clock waits.
pub(crate) async fn verify_projection_policy_seed_and_immutability(
    migration_pool: &PgPool,
) -> Result<()> {
    let lease_seconds: i32 = sqlx::query_scalar(
        "SELECT lease_seconds FROM memory.embedding_projection_lease_policies \
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .fetch_one(migration_pool)
    .await
    .context("read the seeded projection lease seconds")?;
    let renewal_interval_seconds: i32 = sqlx::query_scalar(
        "SELECT renewal_interval_seconds FROM memory.embedding_projection_lease_policies \
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .fetch_one(migration_pool)
    .await
    .context("read the seeded projection renewal interval")?;
    ensure!(
        lease_seconds == 60 && renewal_interval_seconds == 20,
        "the seeded projection lease policy changed: lease {lease_seconds}, \
         renewal {renewal_interval_seconds}"
    );
    let mutation = sqlx::query(
        "UPDATE memory.embedding_projection_lease_policies \
         SET schema_version = schema_version \
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .execute(migration_pool)
    .await;
    let Err(error) = mutation else {
        anyhow::bail!("the projection lease policy accepted a mutation");
    };
    let database_error = error
        .as_database_error()
        .context("the lease policy mutation failed without a database error")?;
    ensure!(
        database_error.code().as_deref() == Some("55000"),
        "the lease policy mutation returned an unexpected error: {database_error}"
    );
    Ok(())
}

/// Shrinks the projection lease policy in this scratch database so the
/// renewal observation stays fast. The guard trigger is disabled for exactly
/// one statement. The seeded values are proven separately by
/// `verify_projection_policy_seed_and_immutability`.
pub(crate) async fn shrink_projection_policy_for_verification(
    migration_pool: &PgPool,
) -> Result<()> {
    let rows = palimpsest_conformance::rewind_under_disabled_trigger(
        migration_pool,
        "memory.embedding_projection_lease_policies",
        "embedding_projection_lease_policies_reject_mutation",
        "UPDATE memory.embedding_projection_lease_policies \
         SET lease_seconds = 5, renewal_interval_seconds = 1 \
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .await
    .context("shrink the projection lease policy for verification")?;
    ensure!(
        rows == 1,
        "the projection lease policy shrink missed its row"
    );
    Ok(())
}

pub(crate) async fn exercise_concurrent_projection_claim(
    pool: &PgPool,
    migration_pool: &PgPool,
    target: &Target,
    fixture: &HybridFusionFixture,
) -> Result<()> {
    delete_embedding_projection(pool, target, fixture.delta_revision_id).await?;
    let provider = Arc::new(BlockingEmbeddingProvider::default());
    let provider_port: Arc<dyn EmbeddingProvider> = provider.clone();
    let first_coordinator =
        EmbeddingProjectionCoordinator::new(pool.clone(), provider_port.clone());
    let second_coordinator = EmbeddingProjectionCoordinator::new(pool.clone(), provider_port);
    let tenant_id = TenantId(target.tenant_id);
    let subject_id = SubjectId(target.subject_id);
    let first = tokio::spawn(async move {
        first_coordinator
            .rebuild_pending(tenant_id, subject_id, 1)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider.calls.load(Ordering::SeqCst) == 0 {
            crate::sleep_budget::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("first projection worker did not reach the provider")?;
    let projection_lease_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.subject_content_leases
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'worker:embedding-projection'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(migration_pool)
    .await?;
    ensure!(
        projection_lease_count == 1,
        "projection provider work did not retain exactly one subject content lease"
    );
    let initial_projection_lease: OffsetDateTime = sqlx::query_scalar(
        r#"
        SELECT generation_lease_expires_at
        FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1
          AND subject_id = $2
          AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.delta_revision_id)
    .fetch_one(migration_pool)
    .await?;
    let second = tokio::spawn(async move {
        second_coordinator
            .rebuild_pending(tenant_id, subject_id, 1)
            .await
    });
    let renewal_interval_seconds: i32 = sqlx::query_scalar(
        "SELECT renewal_interval_seconds FROM memory.embedding_projection_lease_policies \
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .fetch_one(migration_pool)
    .await
    .context("read the projection renewal interval")?;
    // Sleep one renewal interval plus a one second margin so the active
    // provider renews its claim lease once.
    let renewal_sleep_seconds = u64::try_from(renewal_interval_seconds)
        .context("the projection renewal interval is not a whole second count")?
        + 1;
    crate::sleep_budget::sleep(Duration::from_secs(renewal_sleep_seconds)).await;
    let calls_while_claimed = provider.calls.load(Ordering::SeqCst);
    let renewed_projection_lease: OffsetDateTime = sqlx::query_scalar(
        r#"
        SELECT generation_lease_expires_at
        FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1
          AND subject_id = $2
          AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.delta_revision_id)
    .fetch_one(migration_pool)
    .await?;
    ensure!(
        renewed_projection_lease > initial_projection_lease,
        "active projection provider work did not renew its claim lease"
    );
    provider.release.notify_waiters();
    let first_report = first.await??;
    let second_report = second.await??;
    ensure!(
        calls_while_claimed == 1,
        "two projection workers called the provider for one claimed row"
    );
    ensure!(first_report.attempted == 1 && first_report.ready == 1);
    ensure!(second_report.attempted == 0 && second_report.ready == 0);
    let released_projection_lease_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.subject_content_leases
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'worker:embedding-projection'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(migration_pool)
    .await?;
    ensure!(
        released_projection_lease_count == 0,
        "completed projection worker retained its subject content lease"
    );
    Ok(())
}

pub(crate) async fn exercise_projection_lease_expiry(
    migration_pool: &PgPool,
    target: &Target,
    fixture: &HybridFusionFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    let mut transaction = migration_pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let claimed = sqlx::query(
        r#"
        UPDATE memory.fact_revision_embedding_projections
        SET status = 'generating',
            embedding = NULL,
            vector_sha256 = NULL,
            failure_code = NULL,
            generation_attempt_id = $4,
            generation_started_at = clock_timestamp(),
            generation_lease_expires_at = clock_timestamp() + interval '1 hour',
            generated_at = NULL
        WHERE tenant_id = $1
          AND subject_id = $2
          AND revision_id = $3
          AND status = 'ready'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.alpha_revision_id)
    .bind(Uuid::now_v7())
    .execute(&mut *transaction)
    .await?;
    ensure!(claimed.rows_affected() == 1);
    transaction.commit().await?;

    let not_reclaimed = coordinator
        .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 1)
        .await?;
    ensure!(
        not_reclaimed.attempted == 0,
        "a live projection claim was reclaimed before its configured lease expired"
    );

    let mut transaction = migration_pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    sqlx::query(
        r#"
        UPDATE memory.fact_revision_embedding_projections
        SET generation_lease_expires_at = clock_timestamp() - interval '1 second'
        WHERE tenant_id = $1
          AND subject_id = $2
          AND revision_id = $3
          AND status = 'generating'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.alpha_revision_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    let reclaimed = coordinator
        .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 1)
        .await?;
    ensure!(reclaimed.attempted == 1 && reclaimed.ready == 1);
    Ok(())
}

pub(crate) async fn exercise_query_provider_contract_failures(
    pool: &PgPool,
    database_target: &Target,
    scenario_target: &Target,
    provider: &DeterministicEmbeddingProvider,
) -> Result<()> {
    for (mode, key, private_error) in [
        (
            EmbeddingFixtureMode::Unavailable,
            "hybrid-query-unavailable",
            "fixture-provider-outage-private-vector-[1,0,0,0]",
        ),
        (
            EmbeddingFixtureMode::MissingOutput,
            "hybrid-query-cardinality",
            "provider-cardinality",
        ),
        (
            EmbeddingFixtureMode::WrongProfileDigest,
            "hybrid-query-profile-digest",
            "provider-profile-digest",
        ),
        (
            EmbeddingFixtureMode::WrongInputDigest,
            "hybrid-query-input-digest",
            "provider-input-digest",
        ),
        (
            EmbeddingFixtureMode::NonFinite,
            "hybrid-query-nonfinite",
            "provider-NaN",
        ),
        (
            EmbeddingFixtureMode::WrongDimensions,
            "hybrid-query-dimensions",
            "provider-three-dimensional-vector",
        ),
        (
            EmbeddingFixtureMode::ZeroNorm,
            "hybrid-query-zero-norm",
            "provider-zero-vector",
        ),
        (
            EmbeddingFixtureMode::OutsideNormalizationTolerance,
            "hybrid-query-normalization-tolerance",
            "provider-outside-normalization-tolerance",
        ),
    ] {
        provider.set_mode(mode);
        expect_hybrid_failure_without_artifacts(
            pool,
            database_target,
            scenario_target,
            key,
            private_error,
        )
        .await?;
    }
    provider.set_mode(EmbeddingFixtureMode::Valid);
    Ok(())
}

pub(crate) async fn exercise_projection_provider_contract_failures(
    pool: &PgPool,
    target: &Target,
    scenario_target: &Target,
    fixture: &HybridFusionFixture,
    coordinator: &EmbeddingProjectionCoordinator,
    provider: &DeterministicEmbeddingProvider,
) -> Result<()> {
    for (mode, expected_code, key) in [
        (
            EmbeddingFixtureMode::WrongDimensions,
            "provider_response_invalid",
            "hybrid-document-dimensions",
        ),
        (
            EmbeddingFixtureMode::ZeroNorm,
            "provider_response_invalid",
            "hybrid-document-zero-norm",
        ),
    ] {
        delete_embedding_projection(pool, target, fixture.delta_revision_id).await?;
        provider.set_mode(mode);
        let report = coordinator
            .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
            .await?;
        ensure!(report.attempted == 1);
        ensure!(report.ready == 0);
        ensure!(report.failed == 1);
        verify_projection_failure_code(pool, target, fixture.delta_revision_id, expected_code)
            .await?;
        expect_hybrid_failure_without_artifacts(pool, target, scenario_target, key, expected_code)
            .await?;
        delete_embedding_projection(pool, target, fixture.delta_revision_id).await?;
        provider.set_mode(EmbeddingFixtureMode::Valid);
        let recovered = coordinator
            .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
            .await?;
        ensure!(recovered.attempted == 1);
        ensure!(recovered.ready == 1);
        ensure!(recovered.failed == 0);
    }
    Ok(())
}

pub(crate) async fn exercise_projection_rebuilds(
    pool: &PgPool,
    target: &Target,
    scenario_target: &Target,
    fixture: &HybridFusionFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    delete_embedding_projection(pool, target, fixture.delta_revision_id).await?;
    expect_hybrid_failure_without_artifacts(
        pool,
        target,
        scenario_target,
        "hybrid-missing-projection-retry",
        "missing-projection-private",
    )
    .await?;
    let rebuilt_missing = coordinator
        .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
        .await?;
    ensure!(rebuilt_missing.attempted == 1);
    ensure!(rebuilt_missing.ready == 1);
    ensure!(rebuilt_missing.failed == 0);
    hybrid_retrieval_recovers_after_projection_rebuild(
        scenario_target,
        fixture,
        "hybrid-missing-projection-retry",
    )
    .await?;

    stale_embedding_projection(pool, target, fixture.alpha_revision_id).await?;
    expect_hybrid_failure_without_artifacts(
        pool,
        target,
        scenario_target,
        "hybrid-stale-projection-retry",
        "stale-projection-private",
    )
    .await?;
    let rebuilt_stale = coordinator
        .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
        .await?;
    ensure!(rebuilt_stale.attempted == 1);
    ensure!(rebuilt_stale.ready == 1);
    ensure!(rebuilt_stale.failed == 0);
    hybrid_retrieval_recovers_after_projection_rebuild(
        scenario_target,
        fixture,
        "hybrid-stale-projection-retry",
    )
    .await
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StoredProjectionCorruption {
    VectorDigest,
    Dimensions,
    Profile,
}

pub(crate) async fn exercise_corrupt_ready_embedding_projections(
    pool: &PgPool,
    migration_pool: &PgPool,
    target: &Target,
    scenario_target: &Target,
    fixture: &HybridFusionFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    for (corruption, revision_id, idempotency_key) in [
        (
            StoredProjectionCorruption::VectorDigest,
            fixture.alpha_revision_id,
            "hybrid-corrupt-vector-digest",
        ),
        (
            StoredProjectionCorruption::Dimensions,
            fixture.beta_revision_id,
            "hybrid-corrupt-vector-dimensions",
        ),
        (
            StoredProjectionCorruption::Profile,
            fixture.gamma_revision_id,
            "hybrid-corrupt-vector-profile",
        ),
    ] {
        corrupt_ready_embedding_projection(migration_pool, target, revision_id, corruption).await?;

        let failure_result = expect_hybrid_failure_without_artifacts(
            pool,
            target,
            scenario_target,
            idempotency_key,
            "corrupt-ready-private-vector",
        )
        .await;
        let restore_result =
            restore_embedding_projection(migration_pool, pool, target, revision_id, coordinator)
                .await;

        if let Err(restore_error) = restore_result {
            if let Err(failure_error) = failure_result {
                return Err(restore_error).context(format!(
                    "failed to restore projection after expected retrieval failure: {failure_error:#}"
                ));
            }
            return Err(restore_error);
        }
        failure_result?;
    }
    Ok(())
}

pub(crate) async fn expect_hybrid_failure_without_artifacts(
    pool: &PgPool,
    database_target: &Target,
    scenario_target: &Target,
    idempotency_key: &str,
    forbidden_text: &str,
) -> Result<()> {
    hybrid_retrieval_fails_closed_without_leaking(scenario_target, idempotency_key, forbidden_text)
        .await?;
    verify_no_retrieval_artifacts_for_idempotency_key(pool, database_target, idempotency_key).await
}

pub(crate) async fn verify_no_retrieval_artifacts_for_idempotency_key(
    pool: &PgPool,
    target: &Target,
    idempotency_key: &str,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT count(*)
             FROM memory.retrieval_idempotency_reservations AS reservation
             WHERE reservation.tenant_id = $1
               AND reservation.subject_id = $2
               AND reservation.principal_id = 'principal-a'
               AND reservation.idempotency_key = $3) AS reservation_count,
            (SELECT count(*)
             FROM memory.retrieval_receipts AS receipt
             WHERE receipt.tenant_id = $1
               AND receipt.subject_id = $2
               AND receipt.principal_id = 'principal-a'
               AND receipt.idempotency_key = $3) AS receipt_count,
            (SELECT count(*)
             FROM memory.retrieval_manifest_items AS item
             JOIN memory.retrieval_receipts AS receipt
               ON receipt.tenant_id = item.tenant_id
              AND receipt.subject_id = item.subject_id
              AND receipt.retrieval_id = item.retrieval_id
             WHERE receipt.tenant_id = $1
               AND receipt.subject_id = $2
               AND receipt.principal_id = 'principal-a'
               AND receipt.idempotency_key = $3) AS manifest_count
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(idempotency_key)
    .fetch_one(&mut *transaction)
    .await?;
    let reservation_count: i64 = row.try_get("reservation_count")?;
    let receipt_count: i64 = row.try_get("receipt_count")?;
    let manifest_count: i64 = row.try_get("manifest_count")?;
    ensure!(
        reservation_count == 0 && receipt_count == 0 && manifest_count == 0,
        "failed hybrid retrieval persisted artifacts for {idempotency_key}: \
         reservations={reservation_count}, receipts={receipt_count}, \
         manifest_items={manifest_count}"
    );
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn corrupt_ready_embedding_projection(
    migration_pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
    corruption: StoredProjectionCorruption,
) -> Result<()> {
    sqlx::query("ALTER TABLE memory.fact_revision_embedding_projections DISABLE TRIGGER ALL")
        .execute(migration_pool)
        .await?;

    let mutation_result = match corruption {
        StoredProjectionCorruption::VectorDigest => {
            sqlx::query(
                r#"
                UPDATE memory.fact_revision_embedding_projections
                SET vector_sha256 = repeat('0', 64)
                WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
                  AND status = 'ready'
                "#,
            )
            .bind(target.tenant_id)
            .bind(target.subject_id)
            .bind(revision_id)
            .execute(migration_pool)
            .await
        }
        StoredProjectionCorruption::Dimensions => {
            sqlx::query(
                r#"
                UPDATE memory.fact_revision_embedding_projections
                SET embedding_dimensions = 3,
                    embedding = '[1,0,0]'::vector,
                    vector_sha256 = memory.embedding_vector_sha256_v1('[1,0,0]'::vector, 3)
                WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
                  AND status = 'ready'
                "#,
            )
            .bind(target.tenant_id)
            .bind(target.subject_id)
            .bind(revision_id)
            .execute(migration_pool)
            .await
        }
        StoredProjectionCorruption::Profile => {
            sqlx::query(
                r#"
                UPDATE memory.fact_revision_embedding_projections
                SET embedding_profile_id = 'corrupt-ready-profile'
                WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
                  AND status = 'ready'
                "#,
            )
            .bind(target.tenant_id)
            .bind(target.subject_id)
            .bind(revision_id)
            .execute(migration_pool)
            .await
        }
    };

    let enable_result =
        sqlx::query("ALTER TABLE memory.fact_revision_embedding_projections ENABLE TRIGGER ALL")
            .execute(migration_pool)
            .await;
    let mutation = mutation_result?;
    enable_result?;
    ensure!(
        mutation.rows_affected() == 1,
        "stored projection corruption did not target exactly one ready row"
    );
    Ok(())
}

pub(crate) async fn restore_embedding_projection(
    migration_pool: &PgPool,
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    let deleted = sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(migration_pool)
    .await?;
    ensure!(deleted.rows_affected() == 1);

    let report = coordinator
        .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
        .await?;
    ensure!(report.attempted == 1 && report.ready == 1 && report.failed == 0);
    verify_embedding_projection_rows_for_revisions(pool, target, &[revision_id]).await
}

pub(crate) async fn verify_embedding_projection_rows_for_revisions(
    pool: &PgPool,
    target: &Target,
    revision_ids: &[Uuid],
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let row_count: i64 = sqlx::query(
        r#"
        SELECT count(*) AS count
        FROM memory.retrieval_ready_fact_revision_embeddings
        WHERE tenant_id = $1 AND subject_id = $2
          AND revision_id = ANY($3)
          AND embedding_profile_id = 'embedding-conformance-4d-v1'
          AND embedding_profile_version = '1'
          AND embedding_projection_profile_id = 'fact-embedding-projection-v1'
          AND embedding_projection_profile_version = '1'
          AND embedding_dimensions = 4
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_ids)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("count")?;
    ensure!(
        row_count == i64::try_from(revision_ids.len())?,
        "restored embedding projection did not return to the verified ready seam"
    );
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn verify_hybrid_retrieval_policy_and_profiles(pool: &PgPool) -> Result<()> {
    let policy = sqlx::query(
        r#"
        SELECT policy_document, policy_sha256,
            encode(sha256(convert_to(policy_document::text, 'UTF8')), 'hex') AS calculated_sha256
        FROM memory.retrieval_policies
        WHERE policy_id = 'retrieval-hybrid-v1' AND policy_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let document: Value = policy.try_get("policy_document")?;
    for (pointer, expected) in [
        ("/candidate_limits/exact", json!(50)),
        ("/candidate_limits/lexical", json!(50)),
        ("/candidate_limits/vector", json!(50)),
        ("/manifest_limit", json!(50)),
        ("/fusion/method", json!("reciprocal-rank")),
        ("/fusion/k", json!(60)),
        ("/fusion/weights/exact", json!(1)),
        ("/fusion/weights/lexical", json!(1)),
        ("/fusion/weights/vector", json!(1)),
        ("/distance_metric", json!("cosine")),
        ("/score_scale", json!(12)),
        ("/rounding", json!("half-away-from-zero")),
        ("/exact_identity_precedence", json!(true)),
        (
            "/embedding_profile/id",
            json!("embedding-conformance-4d-v1"),
        ),
        ("/embedding_profile/version", json!("1")),
        (
            "/projection_profile/id",
            json!("fact-embedding-projection-v1"),
        ),
        ("/projection_profile/version", json!("1")),
        ("/fallback", json!("none")),
        (
            "/channel_tie_breaks/exact",
            json!([
                "exact_identity_rank_asc",
                "case_id_asc",
                "fact_id_asc",
                "revision_id_asc"
            ]),
        ),
        (
            "/channel_tie_breaks/lexical",
            json!([
                "lexical_score_desc",
                "case_id_asc",
                "fact_id_asc",
                "revision_id_asc"
            ]),
        ),
        (
            "/channel_tie_breaks/vector",
            json!([
                "vector_distance_asc",
                "case_id_asc",
                "fact_id_asc",
                "revision_id_asc"
            ]),
        ),
    ] {
        ensure!(
            document.pointer(pointer) == Some(&expected),
            "retrieval-hybrid-v1 did not pin {pointer}"
        );
    }
    let stored_sha256: String = policy.try_get("policy_sha256")?;
    let calculated_sha256: String = policy.try_get("calculated_sha256")?;
    ensure!(stored_sha256 == calculated_sha256);

    let embedding = sqlx::query(
        r#"
        SELECT profile_document, profile_sha256,
            encode(sha256(convert_to(profile_document::text, 'UTF8')), 'hex') AS calculated_sha256
        FROM memory.embedding_profiles
        WHERE profile_id = 'embedding-conformance-4d-v1' AND profile_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let embedding_document: Value = embedding.try_get("profile_document")?;
    for (pointer, expected) in [
        ("/provider", json!("palimpsest-conformance")),
        ("/model", json!("deterministic-fixture")),
        ("/model_revision", json!("fixture-4d-2026-07-29")),
        ("/dimensions", json!(4)),
        ("/normalization/kind", json!("unit_l2")),
        ("/normalization/tolerance", json!("0.000001")),
        ("/distance_metric", json!("cosine")),
        ("/task_modes/query", json!("query")),
        ("/task_modes/document", json!("document")),
        ("/serialization", json!("utf8")),
        ("/provider_contract_schema_version", json!(1)),
        ("/schema_version", json!(1)),
    ] {
        ensure!(
            embedding_document.pointer(pointer) == Some(&expected),
            "embedding profile did not pin {pointer}"
        );
    }
    let embedding_sha256: String = embedding.try_get("profile_sha256")?;
    let calculated_embedding_sha256: String = embedding.try_get("calculated_sha256")?;
    ensure!(embedding_sha256 == calculated_embedding_sha256);

    let projection = sqlx::query(
        r#"
        SELECT projection_document, projection_profile_sha256,
            encode(sha256(convert_to(projection_document::text, 'UTF8')), 'hex') AS calculated_sha256
        FROM memory.embedding_projection_profiles
        WHERE projection_profile_id = 'fact-embedding-projection-v1'
          AND projection_profile_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let projection_document: Value = projection.try_get("projection_document")?;
    for (pointer, expected) in [
        ("/memory_kind", json!("fact_revision")),
        ("/projection_schema_version", json!(1)),
        ("/serialization", json!("fact-projection-v1")),
        ("/input_schema_version", json!(1)),
        ("/schema_version", json!(1)),
        (
            "/embedding_profile/id",
            json!("embedding-conformance-4d-v1"),
        ),
        ("/embedding_profile/version", json!("1")),
        ("/source_projection/schema_version", json!(1)),
        ("/fields/0", json!("namespace")),
        ("/fields/1", json!("key")),
        ("/fields/2", json!("value")),
    ] {
        ensure!(
            projection_document.pointer(pointer) == Some(&expected),
            "embedding projection profile did not pin {pointer}"
        );
    }
    let projection_sha256: String = projection.try_get("projection_profile_sha256")?;
    let calculated_projection_sha256: String = projection.try_get("calculated_sha256")?;
    ensure!(projection_sha256 == calculated_projection_sha256);

    let inconsistent_embedding = sqlx::query(
        r#"
        INSERT INTO memory.embedding_profiles (
            profile_id, profile_version, provider, model, model_revision,
            dimensions, normalization, normalization_tolerance,
            distance_metric, scalar_type, input_serialization,
            query_task_mode, document_task_mode,
            provider_contract_schema_version,
            profile_document, profile_sha256, schema_version
        )
        SELECT 'invalid-embedding-profile', '1', 'wrong-provider', model,
            model_revision, dimensions, normalization, normalization_tolerance,
            distance_metric, scalar_type, input_serialization,
            query_task_mode, document_task_mode,
            provider_contract_schema_version,
            profile_document, profile_sha256, schema_version
        FROM memory.embedding_profiles
        WHERE profile_id = 'embedding-conformance-4d-v1'
          AND profile_version = '1'
        "#,
    )
    .execute(pool)
    .await;
    ensure!(
        inconsistent_embedding.is_err(),
        "embedding registry accepted columns that contradict the digested document"
    );

    let inconsistent_projection = sqlx::query(
        r#"
        INSERT INTO memory.embedding_projection_profiles (
            projection_profile_id, projection_profile_version,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            source_projection_schema_version,
            source_projection_schema_sha256,
            input_serialization, input_schema_version,
            projection_document, projection_profile_sha256, schema_version
        )
        SELECT 'invalid-projection-profile', '1',
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            source_projection_schema_version,
            source_projection_schema_sha256,
            'wrong-serialization', input_schema_version,
            projection_document, projection_profile_sha256, schema_version
        FROM memory.embedding_projection_profiles
        WHERE projection_profile_id = 'fact-embedding-projection-v1'
          AND projection_profile_version = '1'
        "#,
    )
    .execute(pool)
    .await;
    ensure!(
        inconsistent_projection.is_err(),
        "projection registry accepted columns that contradict the digested document"
    );

    let inconsistent_policy = sqlx::query(
        r#"
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        )
        SELECT 'invalid-hybrid-policy', '1',
            jsonb_set(
                policy_document,
                '{embedding_profile,id}',
                '"wrong-profile"'::jsonb
            ),
            encode(
                sha256(
                    convert_to(
                        jsonb_set(
                            policy_document,
                            '{embedding_profile,id}',
                            '"wrong-profile"'::jsonb
                        )::text,
                        'UTF8'
                    )
                ),
                'hex'
            ),
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        FROM memory.retrieval_policies
        WHERE policy_id = 'retrieval-hybrid-v1' AND policy_version = '1'
        "#,
    )
    .execute(pool)
    .await;
    ensure!(
        inconsistent_policy.is_err(),
        "retrieval policy accepted a digested document with contradictory profile lineage"
    );
    Ok(())
}

pub(crate) async fn verify_embedding_projection_rows(
    pool: &PgPool,
    target: &Target,
    fixture: &HybridFusionFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let rows = sqlx::query(
        r#"
        SELECT revision_id, status,
            embedding_profile_id AS profile_id,
            embedding_profile_version AS profile_version,
            length(embedding_profile_sha256) AS profile_digest_length,
            length(embedding_projection_profile_sha256)
                AS projection_profile_digest_length,
            length(source_projection_sha256) AS projection_digest_length,
            length(source_content_sha256) AS source_digest_length,
            length(input_sha256) AS input_digest_length,
            length(vector_sha256) AS vector_digest_length,
            vector_dims(embedding) AS dimensions,
            embedding = '[0,0,0,0]'::vector AS zero_norm,
            generated_at IS NOT NULL AS generated
        FROM memory.fact_revision_embedding_projections
        WHERE tenant_id = $1 AND subject_id = $2
          AND revision_id = ANY($3)
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(vec![
        fixture.exact_revision_id,
        fixture.alpha_revision_id,
        fixture.beta_revision_id,
        fixture.gamma_revision_id,
        fixture.delta_revision_id,
        fixture.forbidden_revision_id,
    ])
    .fetch_all(&mut *transaction)
    .await?;
    ensure!(rows.len() == 6);
    for row in rows {
        ensure!(row.try_get::<String, _>("status")? == "ready");
        ensure!(row.try_get::<String, _>("profile_id")? == "embedding-conformance-4d-v1");
        ensure!(row.try_get::<String, _>("profile_version")? == "1");
        for column in [
            "profile_digest_length",
            "projection_profile_digest_length",
            "projection_digest_length",
            "source_digest_length",
            "input_digest_length",
            "vector_digest_length",
        ] {
            ensure!(row.try_get::<i32, _>(column)? == 64);
        }
        ensure!(row.try_get::<i32, _>("dimensions")? == 4);
        ensure!(!row.try_get::<bool, _>("zero_norm")?);
        ensure!(row.try_get::<bool, _>("generated")?);
    }
    let vector_type: String = sqlx::query(
        r#"
        SELECT format_type(attribute.atttypid, attribute.atttypmod) AS vector_type
        FROM pg_attribute AS attribute
        JOIN pg_class AS relation ON relation.oid = attribute.attrelid
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'memory'
          AND relation.relname = 'fact_revision_embedding_projections'
          AND attribute.attname = 'embedding'
        "#,
    )
    .fetch_one(&mut *transaction)
    .await?
    .try_get("vector_type")?;
    ensure!(
        vector_type == "vector",
        "embedding storage used a global fixed-dimension typmod"
    );
    transaction.commit().await?;
    Ok(())
}
