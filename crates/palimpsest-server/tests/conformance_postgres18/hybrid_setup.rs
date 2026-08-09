//! hybrid_setup — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

use palimpsest_application::EmbeddingProvider;
use palimpsest_conformance::retrieval_evaluation::{
    enforce_issue_22_gates, evaluate_frozen_corpus, evaluate_full_policy_once, load_frozen_corpus,
    prepare_frozen_corpus, write_or_verify_artifact,
};
use palimpsest_conformance::{
    RetrievalIsolationFixture, Target, captures_temporal_lifecycle_receipts,
    creates_deterministic_hybrid_fusion_receipts, creates_hybrid_fusion_fixture,
    creates_temporal_lifecycle_fixture, creates_temporal_retrieval_fixture,
    replays_hybrid_receipt_before_provider_io, retrieves_with_the_fixed_temporal_policy,
    temporal_receipt_survives_service_restart, temporal_retrieval_survives_projection_rebuild,
};
use palimpsest_domain::{SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::EmbeddingProjectionCoordinator;
use sqlx::{PgPool, Row};
use tokio::net::TcpListener;

use super::corpus::{
    apply_corpus_lifecycle, rebuild_corpus_projections, verify_corpus_error_surface_redaction,
    verify_corpus_manifests_exclude_forbidden,
};
use super::crash::{reserve_local_address, spawn_production_server, wait_for_listener};
use super::deletion_ops::delete_temporal_lifecycle_successor;
use super::fixtures::{DeterministicEmbeddingProvider, EmbeddingFixtureMode};
use super::projection_helpers::{
    verify_hybrid_failure_metadata_is_redacted, verify_hybrid_manifest_isolation,
    verify_no_ann_indexes,
};
use super::projections::{
    exercise_concurrent_projection_claim, exercise_corrupt_ready_embedding_projections,
    exercise_projection_lease_expiry, exercise_projection_provider_contract_failures,
    exercise_projection_rebuilds, exercise_query_provider_contract_failures,
    shrink_projection_policy_for_verification, verify_embedding_projection_rows,
    verify_projection_policy_seed_and_immutability,
};
use super::temporal::{
    NonbypassTemporalRuntime, rebuild_temporal_fixture_projections, temporal_receipt_digests,
    verify_nonbypass_temporal_runtime, verify_temporal_persistence_rejects_tampering,
};

pub(crate) async fn install_deterministic_hybrid_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        WITH fixture AS (
            SELECT jsonb_build_object(
                'provider', 'palimpsest-conformance',
                'model', 'deterministic-fixture',
                'model_revision', 'fixture-4d-2026-07-29',
                'dimensions', 4,
                'normalization', jsonb_build_object(
                    'kind', 'unit_l2',
                    'tolerance', '0.000001'
                ),
                'distance_metric', 'cosine',
                'scalar_type', 'float32',
                'task_modes', jsonb_build_object(
                    'query', 'query',
                    'document', 'document'
                ),
                'serialization', 'utf8',
                'provider_contract_schema_version', 1,
                'schema_version', 1
            ) AS document
        )
        INSERT INTO memory.embedding_profiles (
            profile_id, profile_version, provider, model, model_revision,
            dimensions, normalization, normalization_tolerance,
            distance_metric, scalar_type, input_serialization,
            query_task_mode, document_task_mode,
            provider_contract_schema_version,
            profile_document, profile_sha256, schema_version
        )
        SELECT
            'embedding-conformance-4d-v1', '1',
            'palimpsest-conformance', 'deterministic-fixture',
            'fixture-4d-2026-07-29', 4, 'unit_l2', 0.000001,
            'cosine', 'float32', 'utf8', 'query', 'document', 1,
            document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1
        FROM fixture
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        WITH fixture AS (
            SELECT
                embedding.profile_id AS embedding_profile_id,
                embedding.profile_version AS embedding_profile_version,
                embedding.profile_sha256 AS embedding_profile_sha256,
                source.projection_schema_version,
                source.projection_sha256,
                jsonb_build_object(
                'memory_kind', 'fact_revision',
                'projection_schema_version', 1,
                'serialization', 'fact-projection-v1',
                'input_schema_version', 1,
                'schema_version', 1,
                'fields', jsonb_build_array('namespace', 'key', 'value'),
                'embedding_profile', jsonb_build_object(
                    'id', embedding.profile_id,
                    'version', embedding.profile_version,
                    'digest', embedding.profile_sha256
                ),
                'source_projection', jsonb_build_object(
                    'schema_version', source.projection_schema_version,
                    'digest', source.projection_sha256
                )
            ) AS document
            FROM memory.embedding_profiles AS embedding
            CROSS JOIN memory.search_projection_schemas AS source
            WHERE embedding.profile_id = 'embedding-conformance-4d-v1'
              AND embedding.profile_version = '1'
              AND source.projection_schema_version = 1
        )
        INSERT INTO memory.embedding_projection_profiles (
            projection_profile_id, projection_profile_version,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            source_projection_schema_version,
            source_projection_schema_sha256,
            input_serialization, input_schema_version,
            projection_document, projection_profile_sha256, schema_version
        )
        SELECT
            'fact-embedding-projection-v1', '1',
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            projection_schema_version, projection_sha256,
            'fact-projection-v1', 1,
            document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1
        FROM fixture
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        WITH plan AS (
            SELECT
                embedding.profile_id,
                embedding.profile_version,
                embedding.profile_sha256,
                projection.projection_profile_id,
                projection.projection_profile_version,
                projection.projection_profile_sha256,
                jsonb_build_object(
                    'candidate_limit', 50,
                    'candidate_limits', jsonb_build_object(
                        'exact', 50, 'lexical', 50, 'vector', 50
                    ),
                    'manifest_limit', 50,
                    'default_page_size', 10,
                    'maximum_page_size', 50,
                    'fts_configuration', 'pg_catalog.simple',
                    'fts_rank', 'ts_rank_cd',
                    'fts_rank_normalization', 32,
                    'exact_identity_precedence', true,
                    'distance_metric', 'cosine',
                    'fusion', jsonb_build_object(
                        'method', 'reciprocal-rank',
                        'k', 60,
                        'weights', jsonb_build_object(
                            'exact', 1, 'lexical', 1, 'vector', 1
                        )
                    ),
                    'score_scale', 12,
                    'rounding', 'half-away-from-zero',
                    'embedding_profile', jsonb_build_object(
                        'id', embedding.profile_id,
                        'version', embedding.profile_version,
                        'digest', embedding.profile_sha256
                    ),
                    'projection_profile', jsonb_build_object(
                        'id', projection.projection_profile_id,
                        'version', projection.projection_profile_version,
                        'digest', projection.projection_profile_sha256
                    ),
                    'fallback', 'none',
                    'channel_tie_breaks', jsonb_build_object(
                        'exact', jsonb_build_array(
                            'exact_identity_rank_asc',
                            'case_id_asc',
                            'fact_id_asc',
                            'revision_id_asc'
                        ),
                        'lexical', jsonb_build_array(
                            'lexical_score_desc',
                            'case_id_asc',
                            'fact_id_asc',
                            'revision_id_asc'
                        ),
                        'vector', jsonb_build_array(
                            'vector_distance_asc',
                            'case_id_asc',
                            'fact_id_asc',
                            'revision_id_asc'
                        )
                    ),
                    'tie_break', jsonb_build_array(
                        'fused_score_desc',
                        'exact_identity_rank_asc_nulls_last',
                        'exact_rank_asc_nulls_last',
                        'lexical_rank_asc_nulls_last',
                        'vector_rank_asc_nulls_last',
                        'case_id_asc',
                        'fact_id_asc',
                        'revision_id_asc'
                    )
                ) AS document
            FROM memory.embedding_profiles AS embedding
            JOIN memory.embedding_projection_profiles AS projection
              ON projection.embedding_profile_id = embedding.profile_id
             AND projection.embedding_profile_version = embedding.profile_version
             AND projection.embedding_profile_sha256 = embedding.profile_sha256
            WHERE embedding.profile_id = 'embedding-conformance-4d-v1'
              AND embedding.profile_version = '1'
              AND projection.projection_profile_id = 'fact-embedding-projection-v1'
              AND projection.projection_profile_version = '1'
        )
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        )
        SELECT
            'retrieval-hybrid-v1', '1', document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1, 'hybrid',
            profile_id, profile_version, profile_sha256,
            projection_profile_id, projection_profile_version,
            projection_profile_sha256
        FROM plan
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        WITH plan AS (
            SELECT
                embedding_profile_id,
                embedding_profile_version,
                embedding_profile_sha256,
                embedding_projection_profile_id,
                embedding_projection_profile_version,
                embedding_projection_profile_sha256,
                jsonb_set(
                    policy_document,
                    '{candidate_limits,lexical}',
                    '0'::jsonb
                ) AS document
            FROM memory.retrieval_policies
            WHERE policy_id = 'retrieval-hybrid-v1'
              AND policy_version = '1'
        )
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        )
        SELECT
            'retrieval-exact-vector-v1', '1', document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1, 'hybrid',
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256
        FROM plan
        "#,
    )
    .execute(pool)
    .await?;
    install_temporal_metadata_fixture(pool).await?;
    install_temporal_retrieval_policy(pool).await?;
    Ok(())
}

pub(crate) async fn install_temporal_metadata_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        WITH requested(policy_id, profile_id, importance) AS (
            VALUES
                ('temporal-stable-evidence', 'stable-v1', 0.5000::numeric),
                ('temporal-active-case-evidence', 'active-case-30d-v1', 0.5000::numeric),
                ('temporal-important-active-case-evidence', 'active-case-30d-v1', 0.7500::numeric)
        ), assignments AS (
            SELECT
                requested.policy_id,
                profile.profile_id,
                profile.profile_version,
                profile.profile_sha256,
                requested.importance,
                jsonb_build_object(
                    'write_policy', jsonb_build_object(
                        'id', requested.policy_id,
                        'version', '1'
                    ),
                    'recency_profile', jsonb_build_object(
                        'id', profile.profile_id,
                        'version', profile.profile_version,
                        'digest', profile.profile_sha256
                    ),
                    'recency_anchor_source', 'revision-observed-at',
                    'importance', requested.importance,
                    'schema_version', 1
                ) AS document
            FROM requested
            JOIN memory.recency_profiles AS profile
              ON profile.profile_id = requested.profile_id
             AND profile.profile_version = '1'
        )
        INSERT INTO memory.fact_retrieval_metadata_policies (
            policy_id, policy_version,
            recency_profile_id, recency_profile_version,
            recency_profile_sha256, recency_anchor_source, importance,
            policy_document, policy_sha256, schema_version
        )
        SELECT
            policy_id, '1', profile_id, profile_version, profile_sha256,
            'revision-observed-at', importance, document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'), 1
        FROM assignments
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn install_temporal_retrieval_policy(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        WITH recency AS (
            SELECT jsonb_object_agg(
                profile_id,
                jsonb_build_object(
                    'version', profile_version,
                    'digest', profile_sha256
                )
                ORDER BY profile_id
            ) AS lineage
            FROM memory.recency_profiles
            WHERE (profile_id, profile_version) IN (
                ('stable-v1', '1'),
                ('active-case-30d-v1', '1')
            )
        ), temporal AS (
            SELECT
                base.embedding_profile_id,
                base.embedding_profile_version,
                base.embedding_profile_sha256,
                base.embedding_projection_profile_id,
                base.embedding_projection_profile_version,
                base.embedding_projection_profile_sha256,
                base.policy_document || jsonb_build_object(
                    'rounding', 'half-even',
                    'tie_break', jsonb_build_array(
                        'exact_identity_rank_asc_nulls_last',
                        'final_score_units_desc',
                        'exact_rank_asc_nulls_last',
                        'lexical_rank_asc_nulls_last',
                        'vector_rank_asc_nulls_last',
                        'case_id_asc', 'fact_id_asc', 'revision_id_asc'
                    ),
                    'arithmetic', jsonb_build_object(
                        'id', 'score-units-q63-v1',
                        'score_scale', 12,
                        'rounding', 'half-even',
                        'overflow', 'reject',
                        'operation_order', jsonb_build_array(
                            'rrf-channel-half-even',
                            'fused-exact-sum',
                            'recency-half-even',
                            'confidence-half-even',
                            'importance-half-even',
                            'exact-identity-bonus'
                        )
                    ),
                    'temporal', jsonb_build_object(
                        'axis', 'request.valid_at',
                        'anchor', 'fact_revision_governance.recency_anchor_at',
                        'age_unit', 'microsecond',
                        'negative_age', 'clamp_zero',
                        'profile_lineage', recency.lineage,
                        'profiles', jsonb_build_object(
                            'stable-v1', jsonb_build_object(
                                'kind', 'constant',
                                'factor_units', '1000000000000'
                            ),
                            'active-case-30d-v1', jsonb_build_object(
                                'kind', 'continuous-half-life',
                                'half_life_us', '2592000000000',
                                'floor_units', '125000000000',
                                'arithmetic', 'q63-exp2-v1',
                                'constants_sha256',
                                    '769d34b440235c889ccf0eb34d4b69bb8eb8cff5a99af1919cf475f1c8b6a7aa'
                            )
                        )
                    ),
                    'quality_factors', jsonb_build_object(
                        'confidence', jsonb_build_object(
                            'source', 'fact_revisions.confidence',
                            'formula', 'identity',
                            'minimum_units', '0',
                            'maximum_units', '1000000000000'
                        ),
                        'importance', jsonb_build_object(
                            'source', 'fact_revision_governance.importance',
                            'formula', 'offset-plus-value',
                            'offset_units', '500000000000',
                            'minimum_units', '500000000000',
                            'maximum_units', '1500000000000'
                        )
                    ),
                    'exact_identity_bonus_units', jsonb_build_object(
                        'namespace_key', '16393442623',
                        'key', '8196721311',
                        'none', '0'
                    )
                ) AS document
            FROM memory.retrieval_policies AS base
            CROSS JOIN recency
            WHERE base.policy_id = 'retrieval-hybrid-v1'
              AND base.policy_version = '1'
              AND base.scoring_mode = 'channel-only'
        )
        INSERT INTO memory.retrieval_policies (
            policy_id, policy_version, policy_document, policy_sha256,
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            scoring_mode
        )
        SELECT
            'retrieval-hybrid-temporal-v1', '1', document,
            encode(sha256(convert_to(document::text, 'UTF8')), 'hex'),
            1, 'hybrid', embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256, embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256, 'temporal-v1'
        FROM temporal
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn verify_lexical_retrieval_policy(pool: &PgPool) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT policy_document, policy_sha256,
            encode(
                sha256(convert_to(policy_document::text, 'UTF8')),
                'hex'
            ) AS calculated_sha256
        FROM memory.retrieval_policies
        WHERE policy_id = 'retrieval-lexical-v1' AND policy_version = '1'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let policy_document: Value = row.try_get("policy_document")?;
    let expected = json!({
        "candidate_limit": 50,
        "default_page_size": 10,
        "exact_identity_precedence": true,
        "fts_configuration": "pg_catalog.simple",
        "fts_rank": "ts_rank_cd",
        "fts_rank_normalization": 32,
        "maximum_page_size": 50,
        "score_scale": 12,
        "tie_break": [
            "exact_identity_rank_asc_nulls_last",
            "lexical_rank_asc_nulls_last",
            "fact_id_asc",
            "revision_id_asc"
        ]
    });
    ensure!(
        policy_document == expected,
        "retrieval-lexical-v1 did not pin the complete lexical-only ranking policy"
    );
    let stored_sha256: String = row.try_get("policy_sha256")?;
    let calculated_sha256: String = row.try_get("calculated_sha256")?;
    ensure!(
        stored_sha256 == calculated_sha256,
        "retrieval-lexical-v1 digest does not hash its canonical policy document"
    );
    Ok(())
}

pub(crate) async fn runs_hybrid_retrieval_conformance(
    pool: &PgPool,
    migration_pool: &PgPool,
    authenticator: Arc<StaticAuthenticator>,
    target: &Target,
    database_url: &str,
    retrieval_isolation: &RetrievalIsolationFixture,
) -> Result<()> {
    let provider = Arc::new(DeterministicEmbeddingProvider::default());
    let provider_port: Arc<dyn EmbeddingProvider> = provider.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = palimpsest_server::app_with_embedding_provider(
        pool.clone(),
        pool.clone(),
        authenticator.clone(),
        provider_port.clone(),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let scenario_target = Target {
        base_url: format!("http://{address}"),
        ..target.clone()
    };
    let scenario = async {
        let fixture = creates_hybrid_fusion_fixture(&scenario_target).await?;
        let temporal_fixture = creates_temporal_retrieval_fixture(&scenario_target).await?;
        let corpus = load_frozen_corpus()?;
        let prepared_corpus = prepare_frozen_corpus(&scenario_target, &corpus).await?;
        let coordinator = EmbeddingProjectionCoordinator::new(pool.clone(), provider_port.clone());
        let initial = coordinator
            .rebuild_pending(
                TenantId(target.tenant_id),
                SubjectId(target.subject_id),
                1_000,
            )
            .await?;
        ensure!(initial.attempted >= 6);
        ensure!(initial.ready == initial.attempted);
        ensure!(initial.failed == 0);
        verify_embedding_projection_rows(pool, target, &fixture).await?;
        verify_no_ann_indexes(pool).await?;
        verify_projection_policy_seed_and_immutability(migration_pool).await?;
        shrink_projection_policy_for_verification(migration_pool).await?;
        exercise_concurrent_projection_claim(pool, migration_pool, target, &fixture).await?;
        exercise_projection_lease_expiry(migration_pool, target, &fixture, &coordinator).await?;

        apply_corpus_lifecycle(pool, target, &prepared_corpus).await?;
        crate::sleep_budget::sleep(Duration::from_millis(1_100)).await;
        let mut corpus_evaluation =
            evaluate_frozen_corpus(&scenario_target, &corpus, &prepared_corpus, 10).await?;
        verify_corpus_manifests_exclude_forbidden(migration_pool, &corpus, &prepared_corpus)
            .await?;
        corpus_evaluation.surface_coverage.durable_manifests_checked = true;
        verify_corpus_error_surface_redaction(
            &scenario_target,
            &corpus,
            &prepared_corpus,
            &provider,
        )
        .await?;
        corpus_evaluation.surface_coverage.error_responses_checked = true;
        rebuild_corpus_projections(pool, &coordinator, target, &prepared_corpus).await?;
        let rebuilt =
            evaluate_full_policy_once(&scenario_target, &corpus, &prepared_corpus, 10).await?;
        corpus_evaluation.rebuild_identical = rebuilt == corpus_evaluation.baselines[3];
        write_or_verify_artifact(&corpus_evaluation)?;
        enforce_issue_22_gates(&corpus_evaluation)?;

        let replay_fixture =
            creates_deterministic_hybrid_fusion_receipts(&scenario_target, &fixture).await?;
        verify_hybrid_manifest_isolation(pool, target, &fixture, &replay_fixture.receipt).await?;

        provider.set_mode(EmbeddingFixtureMode::Unavailable);
        let calls_before_replay = provider.calls();
        replays_hybrid_receipt_before_provider_io(&scenario_target, &replay_fixture).await?;
        ensure!(
            provider.calls() == calls_before_replay,
            "completed replay called the unavailable embedding provider"
        );
        provider.set_mode(EmbeddingFixtureMode::Valid);

        exercise_query_provider_contract_failures(pool, target, &scenario_target, &provider)
            .await?;
        exercise_projection_provider_contract_failures(
            pool,
            target,
            &scenario_target,
            &fixture,
            &coordinator,
            &provider,
        )
        .await?;
        exercise_projection_rebuilds(pool, target, &scenario_target, &fixture, &coordinator)
            .await?;
        exercise_corrupt_ready_embedding_projections(
            pool,
            migration_pool,
            target,
            &scenario_target,
            &fixture,
            &coordinator,
        )
        .await?;
        let temporal_replay =
            retrieves_with_the_fixed_temporal_policy(&scenario_target, &temporal_fixture).await?;
        let first_temporal_digests =
            temporal_receipt_digests(pool, target, temporal_replay.first_retrieval_id).await?;
        ensure!(
            temporal_replay.independent_retrieval_ids[1] == temporal_replay.second_retrieval_id
        );
        for retrieval_id in temporal_replay
            .independent_retrieval_ids
            .iter()
            .copied()
            .chain(std::iter::once(temporal_replay.paginated_retrieval_id))
        {
            ensure!(
                temporal_receipt_digests(pool, target, retrieval_id).await?
                    == first_temporal_digests,
                "repeat or paginated temporal receipt changed durable item or manifest digests"
            );
        }
        verify_temporal_persistence_rejects_tampering(
            migration_pool,
            target,
            temporal_replay.first_retrieval_id,
        )
        .await?;

        rebuild_temporal_fixture_projections(pool, target, &temporal_fixture, &coordinator).await?;
        let rebuilt_temporal_retrieval_id = temporal_retrieval_survives_projection_rebuild(
            &scenario_target,
            &temporal_fixture,
            &temporal_replay,
        )
        .await?;
        let rebuilt_temporal_digests =
            temporal_receipt_digests(pool, target, rebuilt_temporal_retrieval_id).await?;
        ensure!(
            first_temporal_digests == rebuilt_temporal_digests,
            "projection rebuild changed durable temporal item or manifest digests"
        );

        let temporal_lifecycle = creates_temporal_lifecycle_fixture(&scenario_target).await?;
        let lifecycle_rebuild = coordinator
            .rebuild_pending(TenantId(target.tenant_id), SubjectId(target.subject_id), 10)
            .await?;
        ensure!(lifecycle_rebuild.attempted == 4);
        ensure!(lifecycle_rebuild.ready == 4 && lifecycle_rebuild.failed == 0);
        let lifecycle_replay =
            captures_temporal_lifecycle_receipts(&scenario_target, &temporal_lifecycle).await?;
        delete_temporal_lifecycle_successor(pool, target, &temporal_lifecycle).await?;
        verify_nonbypass_temporal_runtime(NonbypassTemporalRuntime {
            migration_pool,
            database_url,
            authenticator: authenticator.clone(),
            provider: provider.clone(),
            provider_port: provider_port.clone(),
            target,
            temporal_fixture: &temporal_fixture,
            temporal_replay: &temporal_replay,
            isolation_fixture: retrieval_isolation,
            lifecycle_fixture: &temporal_lifecycle,
            lifecycle_replay: &lifecycle_replay,
        })
        .await?;
        verify_hybrid_failure_metadata_is_redacted(pool, target).await?;
        Ok::<_, anyhow::Error>(temporal_replay)
    }
    .await;
    server.abort();
    let _ = server.await;
    let temporal_replay = scenario?;

    drop(authenticator);
    drop(provider_port);
    let restart_address = reserve_local_address().await?;
    let mut restart_server = spawn_production_server(database_url, target, restart_address)?;
    wait_for_listener(restart_address).await?;
    let restart_target = Target {
        base_url: format!("http://{restart_address}"),
        principal_a_internal_bearer_token: target.bearer_token.clone(),
        ..target.clone()
    };
    let restart_result =
        temporal_receipt_survives_service_restart(&restart_target, &temporal_replay).await;
    let _ = restart_server.kill().await;
    let _ = restart_server.wait().await;
    restart_result
}
