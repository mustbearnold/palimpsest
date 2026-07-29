use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Path, Request},
    middleware::{self, Next},
    response::Response,
    routing::post,
};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    env,
    process::Stdio,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use palimpsest_application::{
    EmbeddingProvider, EmbeddingProviderError, EmbeddingRequest, EmbeddingResponse,
};
use palimpsest_conformance::{
    HybridFusionFixture, RetrievalIsolationFixture, RetrievalLifecycleFixture, Target,
    checkpoint_scopes_fail_closed, concurrent_retrievals_converge_on_one_receipt,
    creates_an_attributable_fact_revision, creates_and_replays_a_lexical_retrieval_receipt,
    creates_deterministic_hybrid_fusion_receipts, creates_hybrid_fusion_fixture,
    creates_retrieval_lifecycle_fixture, cross_scope_reads_fail_closed,
    expires_only_the_targeted_checkpoint, hybrid_retrieval_fails_closed_without_leaking,
    hybrid_retrieval_recovers_after_projection_rebuild,
    hybrid_retrieval_rejects_caller_ranking_internals,
    hybrid_retrieval_requires_an_available_provider, reconstructs_both_temporal_axes,
    records_and_reads_an_immutable_episode, rejects_cross_subject_idempotency_reuse,
    rejects_cross_subject_retrieval_idempotency_reuse, rejects_invalid_domain_and_timestamp_inputs,
    replays_hybrid_receipt_before_provider_io, retrieval_candidates_are_authorized_before_ranking,
    retrieval_fails_closed_when_projection_is_corrupt,
    retrieval_fails_closed_when_projection_is_missing,
    retrieval_paginates_and_rejects_invalid_replays,
    retrieval_receipt_does_not_resurrect_deleted_history, retrieval_receipt_hides_expired_content,
    retrieval_recovers_after_projection_rebuild, retrieval_succeeds_after_projection_rebuild,
    retrieves_the_effective_bitemporal_revision, saves_and_reads_a_resumable_checkpoint,
    supersedes_the_fact_head,
};
use palimpsest_domain::{
    EmbeddingOutput, EmbeddingTask, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::EmbeddingProjectionCoordinator;
use sqlx::{AssertSqlSafe, ConnectOptions, PgPool, Row, postgres::PgConnectOptions};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    sync::Notify,
};
use uuid::Uuid;

static PROVIDER_APPLICATIONS: AtomicUsize = AtomicUsize::new(0);
static PROVIDER_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static PROVIDER_EFFECTS: LazyLock<Mutex<HashSet<Uuid>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Clone, Copy, Debug)]
enum EmbeddingFixtureMode {
    Valid = 0,
    Unavailable = 1,
    MissingOutput = 2,
    WrongProfileDigest = 3,
    WrongInputDigest = 4,
    NonFinite = 5,
    WrongDimensions = 6,
    ZeroNorm = 7,
    OutsideNormalizationTolerance = 8,
}

impl EmbeddingFixtureMode {
    fn from_usize(value: usize) -> Self {
        match value {
            0 => Self::Valid,
            1 => Self::Unavailable,
            2 => Self::MissingOutput,
            3 => Self::WrongProfileDigest,
            4 => Self::WrongInputDigest,
            5 => Self::NonFinite,
            6 => Self::WrongDimensions,
            7 => Self::ZeroNorm,
            8 => Self::OutsideNormalizationTolerance,
            _ => panic!("unknown embedding fixture mode {value}"),
        }
    }
}

#[derive(Debug, Default)]
struct DeterministicEmbeddingProvider {
    mode: AtomicUsize,
    calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct BlockingEmbeddingProvider {
    calls: AtomicUsize,
    release: Notify,
}

#[async_trait]
impl EmbeddingProvider for BlockingEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> std::result::Result<EmbeddingResponse, EmbeddingProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(EmbeddingResponse {
            profile_digest: request.profile.digest,
            outputs: request
                .inputs
                .iter()
                .map(|input| EmbeddingOutput {
                    input_sha256: input.input_sha256.clone(),
                    values: fixture_embedding(&request.task, &input.content),
                })
                .collect(),
        })
    }
}

impl DeterministicEmbeddingProvider {
    fn set_mode(&self, mode: EmbeddingFixtureMode) {
        self.mode.store(mode as usize, Ordering::SeqCst);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EmbeddingProvider for DeterministicEmbeddingProvider {
    async fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> std::result::Result<EmbeddingResponse, EmbeddingProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = EmbeddingFixtureMode::from_usize(self.mode.load(Ordering::SeqCst));
        if matches!(mode, EmbeddingFixtureMode::Unavailable) {
            return Err(EmbeddingProviderError::Unavailable {
                code: "fixture-provider-outage-private-vector-[1,0,0,0]".to_owned(),
            });
        }

        assert_eq!(request.profile.id, "embedding-conformance-4d-v1");
        assert_eq!(request.profile.version, "1");
        assert_eq!(request.profile.provider, "palimpsest-conformance");
        assert_eq!(request.profile.model, "deterministic-fixture");
        assert_eq!(request.profile.model_revision, "fixture-4d-2026-07-29");
        assert_eq!(request.profile.dimensions, 4);
        assert_eq!(request.profile.normalization, "unit_l2");
        assert!((request.profile.normalization_tolerance - 0.000001).abs() < f64::EPSILON);
        assert_eq!(request.profile.distance_metric, "cosine");
        assert_eq!(request.profile.scalar_type, "float32");
        assert_eq!(request.profile.input_serialization, "utf8");
        assert_eq!(request.profile.query_task, "query");
        assert_eq!(request.profile.document_task, "document");
        assert_eq!(request.profile.provider_contract_schema_version, 1);
        assert_eq!(request.profile.digest.len(), 64);

        let mut outputs = request
            .inputs
            .iter()
            .map(|input| EmbeddingOutput {
                input_sha256: input.input_sha256.clone(),
                values: fixture_embedding(&request.task, &input.content),
            })
            .collect::<Vec<_>>();
        match mode {
            EmbeddingFixtureMode::Valid | EmbeddingFixtureMode::Unavailable => {}
            EmbeddingFixtureMode::MissingOutput => {
                outputs.pop();
            }
            EmbeddingFixtureMode::WrongProfileDigest => {}
            EmbeddingFixtureMode::WrongInputDigest => {
                if let Some(output) = outputs.first_mut() {
                    output.input_sha256 = "0".repeat(64);
                }
            }
            EmbeddingFixtureMode::NonFinite => {
                if let Some(output) = outputs.first_mut() {
                    output.values[0] = f32::NAN;
                }
            }
            EmbeddingFixtureMode::WrongDimensions => {
                if let Some(output) = outputs.first_mut() {
                    output.values.pop();
                }
            }
            EmbeddingFixtureMode::ZeroNorm => {
                if let Some(output) = outputs.first_mut() {
                    output.values = vec![0.0; 4];
                }
            }
            EmbeddingFixtureMode::OutsideNormalizationTolerance => {
                if let Some(output) = outputs.first_mut() {
                    output.values = vec![1.00001, 0.0, 0.0, 0.0];
                }
            }
        }
        Ok(EmbeddingResponse {
            profile_digest: if matches!(mode, EmbeddingFixtureMode::WrongProfileDigest) {
                "0".repeat(64)
            } else {
                request.profile.digest
            },
            outputs,
        })
    }
}

fn fixture_embedding(task: &EmbeddingTask, content: &str) -> Vec<f32> {
    if matches!(task, EmbeddingTask::Query) {
        assert_eq!(content, "case.retrieval:fusiontoken");
        return vec![1.0, 0.0, 0.0, 0.0];
    }
    for (marker, vector) in [
        ("vector_fixture_forbidden_4d", [1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_exact_4d", [-1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_alpha_4d", [-0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_beta_4d", [0.8, 0.6, 0.0, 0.0]),
        ("vector_fixture_gamma_4d", [0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_delta_4d", [0.0, 1.0, 0.0, 0.0]),
    ] {
        if content.contains(marker) {
            return vector.to_vec();
        }
    }
    vec![0.0, 0.0, 0.0, 1.0]
}

#[tokio::test]
async fn serves_the_bitemporal_lifecycle_over_http_and_postgres() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;

    let version_num: i32 = sqlx::query("SELECT current_setting('server_version_num')::integer")
        .fetch_one(&admin_pool)
        .await?
        .try_get(0)?;
    let vector_version: String =
        sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_one(&admin_pool)
            .await?
            .try_get(0)?;
    ensure!(version_num >= 180_000, "PostgreSQL 18+ is required");
    ensure!(vector_version == "0.8.5", "pgvector 0.8.5 is required");

    let database_name = format!("palimpsest_test_{}", Uuid::now_v7().simple());
    // The identifier is generated exclusively from a UUID's lowercase hex form.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;
    let options = PgConnectOptions::from_str(&database_url)?.database(&database_name);
    let test_database_url = options.to_url_lossy().to_string();
    let pool = PgPool::connect_with(options).await?;
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let migration_pool = PgPool::connect_with(migration_options).await?;
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000010")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000020")?;
    let target = Target {
        base_url: String::new(),
        bearer_token: "principal-a-test-token".to_owned(),
        tenant_id,
        subject_id,
        principal_a_secondary_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000021")?,
        principal_a_internal_bearer_token: "principal-a-internal-test-token".to_owned(),
        principal_b_bearer_token: "principal-b-test-token".to_owned(),
        principal_b_tenant_id: Uuid::parse_str("019be000-0000-7000-8000-000000000110")?,
        principal_b_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000120")?,
        principal_c_bearer_token: "principal-c-test-token".to_owned(),
        principal_c_subject_id: Uuid::parse_str("019be000-0000-7000-8000-000000000220")?,
    };
    let result = async {
        palimpsest_postgres::migrate(&pool).await?;
        verify_lexical_retrieval_policy(&migration_pool).await?;
        sqlx::query(
            r#"
            INSERT INTO memory.fact_retention_policies (
                retention_policy_id, retention_interval, policy_origin, schema_version
            )
            VALUES ('retrieval-test-1s-v1', interval '1 second', 'migration', 1)
            "#,
        )
        .execute(&migration_pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO memory.checkpoint_retention_policies (
                retention_policy_id, retention_interval
            )
            VALUES ('checkpoint-test-1s-v1', interval '1 second')
            "#,
        )
        .execute(&pool)
        .await?;
        let authenticator = Arc::new(StaticAuthenticator::new([
            (
                target.bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                },
            ),
            (
                target.principal_a_internal_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                    ],
                    allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                },
            ),
            (
                target.principal_b_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(target.principal_b_tenant_id),
                    subject_ids: vec![SubjectId(target.principal_b_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                },
            ),
            (
                target.principal_c_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(target.principal_c_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                },
            ),
        ]));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_pool = pool.clone();
        let server_authenticator = authenticator.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(server_pool, server_authenticator),
            )
            .await
        });
        let scenario_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        let scenario = async {
            records_and_reads_an_immutable_episode(&scenario_target).await?;
            creates_an_attributable_fact_revision(&scenario_target).await?;
            creates_and_replays_a_lexical_retrieval_receipt(&scenario_target).await?;
            supersedes_the_fact_head(&scenario_target).await?;
            reconstructs_both_temporal_axes(&scenario_target).await?;
            retrieves_the_effective_bitemporal_revision(&scenario_target).await?;
            cross_scope_reads_fail_closed(&scenario_target).await?;
            rejects_cross_subject_idempotency_reuse(&scenario_target).await?;
            rejects_invalid_domain_and_timestamp_inputs(&scenario_target).await?;
            verify_governed_write_records(&pool, &scenario_target).await?;
            let retrieval_isolation =
                retrieval_candidates_are_authorized_before_ranking(&scenario_target).await?;
            concurrent_retrievals_converge_on_one_receipt(&scenario_target).await?;
            rejects_cross_subject_retrieval_idempotency_reuse(&scenario_target).await?;
            verify_retrieval_manifest_is_authorized(&pool, &target, &retrieval_isolation).await?;
            delete_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_fails_closed_when_projection_is_missing(&scenario_target).await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_recovers_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            corrupt_retrieval_projection_digest(
                &pool,
                &target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            retrieval_fails_closed_when_projection_is_corrupt(
                &scenario_target,
                "retrieval-projection-digest-retry",
            )
            .await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_succeeds_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
                "retrieval-projection-digest-retry",
            )
            .await?;
            corrupt_retrieval_search_vector(
                &pool,
                &target,
                retrieval_isolation.allowed_revision_id,
            )
            .await?;
            retrieval_fails_closed_when_projection_is_corrupt(
                &scenario_target,
                "retrieval-projection-vector-retry",
            )
            .await?;
            rebuild_retrieval_projection(&pool, &target, retrieval_isolation.allowed_revision_id)
                .await?;
            retrieval_succeeds_after_projection_rebuild(
                &scenario_target,
                retrieval_isolation.allowed_revision_id,
                "retrieval-projection-vector-retry",
            )
            .await?;
            retrieval_paginates_and_rejects_invalid_replays(&scenario_target).await?;
            retrieval_receipt_hides_expired_content(&scenario_target).await?;
            let lifecycle = creates_retrieval_lifecycle_fixture(&scenario_target).await?;
            delete_retrieval_revision(&pool, &target, &lifecycle).await?;
            retrieval_receipt_does_not_resurrect_deleted_history(&scenario_target, &lifecycle)
                .await?;
            saves_and_reads_a_resumable_checkpoint(&scenario_target).await?;
            checkpoint_scopes_fail_closed(&scenario_target).await?;
            expires_only_the_targeted_checkpoint(&scenario_target).await?;
            verify_checkpoint_governance(&pool, &scenario_target).await?;
            install_deterministic_hybrid_fixture(&migration_pool).await?;
            verify_hybrid_retrieval_policy_and_profiles(&migration_pool).await?;
            hybrid_retrieval_rejects_caller_ranking_internals(&scenario_target).await?;
            hybrid_retrieval_requires_an_available_provider(&scenario_target).await?;
            verify_no_retrieval_artifacts_for_idempotency_key(
                &pool,
                &target,
                "hybrid-provider-unavailable-default",
            )
            .await
        }
        .await;
        server.abort();
        let _ = server.await;
        scenario?;
        runs_hybrid_retrieval_conformance(&pool, &migration_pool, authenticator, &target).await?;
        recovers_a_committed_effect_after_response_loss(&pool, &target, &test_database_url).await
    }
    .await;

    migration_pool.close().await;
    pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&admin_pool)
    .await?;
    result
}

async fn install_deterministic_hybrid_fixture(pool: &PgPool) -> Result<()> {
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
    Ok(())
}

async fn verify_lexical_retrieval_policy(pool: &PgPool) -> Result<()> {
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

async fn runs_hybrid_retrieval_conformance(
    pool: &PgPool,
    migration_pool: &PgPool,
    authenticator: Arc<StaticAuthenticator>,
    target: &Target,
) -> Result<()> {
    let provider = Arc::new(DeterministicEmbeddingProvider::default());
    let provider_port: Arc<dyn EmbeddingProvider> = provider.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = palimpsest_server::app_with_embedding_provider(
        pool.clone(),
        authenticator,
        provider_port.clone(),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let scenario_target = Target {
        base_url: format!("http://{address}"),
        ..target.clone()
    };
    let scenario = async {
        let fixture = creates_hybrid_fusion_fixture(&scenario_target).await?;
        let coordinator = EmbeddingProjectionCoordinator::new(pool.clone(), provider_port);
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
        exercise_concurrent_projection_claim(pool, target, &fixture).await?;

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
        verify_hybrid_failure_metadata_is_redacted(pool, target).await
    }
    .await;
    server.abort();
    let _ = server.await;
    scenario
}

async fn exercise_concurrent_projection_claim(
    pool: &PgPool,
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("first projection worker did not reach the provider")?;
    let second = tokio::spawn(async move {
        second_coordinator
            .rebuild_pending(tenant_id, subject_id, 1)
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls_while_claimed = provider.calls.load(Ordering::SeqCst);
    provider.release.notify_waiters();
    let first_report = first.await??;
    let second_report = second.await??;
    ensure!(
        calls_while_claimed == 1,
        "two projection workers called the provider for one claimed row"
    );
    ensure!(first_report.attempted == 1 && first_report.ready == 1);
    ensure!(second_report.attempted == 0 && second_report.ready == 0);
    Ok(())
}

async fn exercise_query_provider_contract_failures(
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

async fn exercise_projection_provider_contract_failures(
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

async fn exercise_projection_rebuilds(
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
enum StoredProjectionCorruption {
    VectorDigest,
    Dimensions,
    Profile,
}

async fn exercise_corrupt_ready_embedding_projections(
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

async fn expect_hybrid_failure_without_artifacts(
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

async fn verify_no_retrieval_artifacts_for_idempotency_key(
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

async fn corrupt_ready_embedding_projection(
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

async fn restore_embedding_projection(
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

async fn verify_embedding_projection_rows_for_revisions(
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

async fn verify_hybrid_retrieval_policy_and_profiles(pool: &PgPool) -> Result<()> {
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

async fn verify_embedding_projection_rows(
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

async fn verify_no_ann_indexes(pool: &PgPool) -> Result<()> {
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

async fn delete_embedding_projection(
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

async fn stale_embedding_projection(
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

async fn verify_projection_failure_code(
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

async fn verify_hybrid_manifest_isolation(
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

async fn verify_hybrid_manifest_rejects_invalid_fusion(
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

async fn verify_hybrid_failure_metadata_is_redacted(pool: &PgPool, target: &Target) -> Result<()> {
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

async fn set_retrieval_test_scope(
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

async fn verify_retrieval_manifest_is_authorized(
    pool: &PgPool,
    target: &Target,
    fixture: &RetrievalIsolationFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
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

async fn delete_retrieval_revision(
    pool: &PgPool,
    target: &Target,
    fixture: &RetrievalLifecycleFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let manifest_revision_ids = sqlx::query(
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
    ensure!(manifest_revision_ids == vec![fixture.deleted_revision_id]);
    ensure!(!manifest_revision_ids.contains(&fixture.superseded_revision_id));
    let pending = sqlx::query(
        r#"
        UPDATE memory.fact_revision_governance
        SET lifecycle_state = 'deletion_pending'
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.deleted_revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(pending.rows_affected() == 1);
    let deleted = sqlx::query(
        r#"
        UPDATE memory.fact_revision_governance
        SET lifecycle_state = 'deleted'
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
          AND lifecycle_state = 'deletion_pending'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(fixture.deleted_revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn delete_retrieval_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let deleted = sqlx::query(
        r#"
        DELETE FROM memory.fact_revision_search_documents
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

async fn corrupt_retrieval_projection_digest(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
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
    let updated = sqlx::query(
        r#"
        UPDATE memory.fact_revision_search_documents
        SET projection_sha256 = repeat('0', 64)
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
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

async fn corrupt_retrieval_search_vector(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
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
    let updated = sqlx::query(
        r#"
        UPDATE memory.fact_revision_search_documents
        SET search_vector = to_tsvector('pg_catalog.simple', 'corrupted projection')
        WHERE tenant_id = $1 AND subject_id = $2 AND revision_id = $3
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

async fn rebuild_retrieval_projection(
    pool: &PgPool,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let rebuilt = sqlx::query(
        r#"
        INSERT INTO memory.fact_revision_search_documents (
            tenant_id, subject_id, case_id, fact_id, revision_id,
            projection_schema_version, projection_schema_sha256,
            source_content_sha256, projection_sha256, search_vector
        )
        SELECT revision.tenant_id, revision.subject_id, revision.case_id,
            revision.fact_id, revision.revision_id,
            projection.projection_schema_version, projection.projection_sha256,
            revision.content_sha256,
            memory.fact_projection_sha256_v1(
                fact.namespace, fact.fact_key, revision.value
            ),
            memory.fact_search_vector_v1(
                fact.namespace, fact.fact_key, revision.value
            )
        FROM memory.fact_revisions AS revision
        JOIN memory.facts AS fact
          ON fact.tenant_id = revision.tenant_id
         AND fact.subject_id = revision.subject_id
         AND fact.case_id = revision.case_id
         AND fact.fact_id = revision.fact_id
        CROSS JOIN memory.search_projection_schemas AS projection
        WHERE revision.tenant_id = $1
          AND revision.subject_id = $2
          AND revision.revision_id = $3
          AND projection.projection_schema_version = 1
        ON CONFLICT (
            tenant_id, subject_id, case_id, fact_id, revision_id
        ) DO UPDATE SET
            projection_schema_sha256 = EXCLUDED.projection_schema_sha256,
            source_content_sha256 = EXCLUDED.source_content_sha256,
            projection_sha256 = EXCLUDED.projection_sha256,
            search_vector = EXCLUDED.search_vector
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(revision_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(rebuilt.rows_affected() == 1);
    transaction.commit().await?;
    Ok(())
}

async fn crash_after_selected_commit(request: Request, next: Next) -> Response {
    let should_crash = request
        .headers()
        .get("idempotency-key")
        .is_some_and(|value| value == "checkpoint-run-321-complete");
    let response = next.run(request).await;
    if should_crash {
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("idempotency-replayed").is_none());
        assert!(response.headers().get(header::ETAG).is_some());
        std::process::exit(86);
    }
    response
}

#[tokio::test]
#[ignore = "spawned by the crash-recovery conformance scenario"]
async fn crash_after_checkpoint_commit_child() -> Result<()> {
    if env::var("PALIMPSEST_CRASH_CHILD").as_deref() != Ok("1") {
        return Ok(());
    }
    let pool = PgPool::connect(&env::var("PALIMPSEST_TEST_CHILD_DATABASE_URL")?).await?;
    let tenant_id = Uuid::parse_str(&env::var("PALIMPSEST_TEST_CHILD_TENANT_ID")?)?;
    let subject_id = Uuid::parse_str(&env::var("PALIMPSEST_TEST_CHILD_SUBJECT_ID")?)?;
    let bearer_token = env::var("PALIMPSEST_TEST_CHILD_BEARER_TOKEN")?;
    let authenticator = Arc::new(StaticAuthenticator::new([(
        bearer_token,
        PrincipalScope {
            principal_id: PrincipalId("principal-a".to_owned()),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![SubjectId(subject_id)],
            allowed_sensitivities: vec![
                Sensitivity::try_from("internal".to_owned())?,
                Sensitivity::try_from("restricted".to_owned())?,
            ],
        },
    )]));
    let listener = TcpListener::bind(&env::var("PALIMPSEST_TEST_CHILD_BIND")?).await?;
    let router = palimpsest_server::app(pool, authenticator)
        .layer(middleware::from_fn(crash_after_selected_commit));
    axum::serve(listener, router).await?;
    Ok(())
}

async fn apply_mock_provider_effect(Path(effect_id): Path<Uuid>) -> StatusCode {
    PROVIDER_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    if PROVIDER_EFFECTS
        .lock()
        .expect("provider effect lock poisoned")
        .insert(effect_id)
    {
        PROVIDER_APPLICATIONS.fetch_add(1, Ordering::SeqCst);
    }
    StatusCode::OK
}

async fn recovers_a_committed_effect_after_response_loss(
    pool: &PgPool,
    target: &Target,
    database_url: &str,
) -> Result<()> {
    PROVIDER_APPLICATIONS.store(0, Ordering::SeqCst);
    PROVIDER_ATTEMPTS.store(0, Ordering::SeqCst);
    PROVIDER_EFFECTS
        .lock()
        .expect("provider effect lock poisoned")
        .clear();

    let provider_listener = TcpListener::bind("127.0.0.1:0").await?;
    let provider_address = provider_listener.local_addr()?;
    let provider_server = tokio::spawn(async move {
        axum::serve(
            provider_listener,
            Router::new().route("/effects/{effect_id}", post(apply_mock_provider_effect)),
        )
        .await
    });

    let scenario = async {
        let client = Client::new();
        let crash_address = reserve_local_address().await?;
        let mut crash_server = spawn_crash_server(database_url, target, crash_address)?;
        wait_for_listener(crash_address).await?;
        let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000321")?;
        let agent_id = Uuid::parse_str("019be000-0000-7000-8000-000000000322")?;
        let thread_id = Uuid::parse_str("019be000-0000-7000-8000-000000000323")?;
        let checkpoint_path = format!(
            "/v1/tenants/{}/subjects/{}/agents/{agent_id}/threads/{thread_id}/checkpoint",
            target.tenant_id, target.subject_id
        );
        let fault_url = format!("http://{crash_address}{checkpoint_path}");
        let provenance = json!({
            "source_type": "conformance.crash-recovery",
            "source_uri": null,
            "external_id": "checkpoint-run-321"
        });
        let create_body = json!({
            "case_id": case_id,
            "parent_revision_id": null,
            "state": {"step": "created"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let create_response = client
            .put(&fault_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", "checkpoint-run-321-create")
            .header(header::IF_NONE_MATCH, "*")
            .json(&create_body)
            .send()
            .await?;
        ensure!(create_response.status() == StatusCode::CREATED);
        let create_etag = create_response
            .headers()
            .get(header::ETAG)
            .context("crash scenario create omitted ETag")?
            .to_str()?
            .to_owned();
        let created: Value = create_response.json().await?;
        let created_revision_id = created["checkpoint_revision_id"]
            .as_str()
            .context("crash scenario create omitted revision ID")?;

        let prepare_body = json!({
            "case_id": case_id,
            "parent_revision_id": created_revision_id,
            "state": {"step": "effect-prepared"},
            "state_schema_version": 1,
            "effect_transitions": [{
                "type": "prepare",
                "effect_key": "apply-case-321",
                "kind": "test-provider.apply",
                "recovery_mode": "idempotency_key"
            }],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let prepare_response = client
            .put(&fault_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", "checkpoint-run-321-prepare")
            .header(header::IF_MATCH, create_etag)
            .json(&prepare_body)
            .send()
            .await?;
        ensure!(prepare_response.status() == StatusCode::OK);
        let prepare_etag = prepare_response
            .headers()
            .get(header::ETAG)
            .context("crash scenario prepare omitted ETag")?
            .to_str()?
            .to_owned();
        let prepared: Value = prepare_response.json().await?;
        let prepared_revision_id = prepared["checkpoint_revision_id"]
            .as_str()
            .context("crash scenario prepare omitted revision ID")?
            .to_owned();
        let effect_id = prepared["effects"][0]["effect_id"]
            .as_str()
            .context("crash scenario prepare omitted effect ID")?
            .to_owned();

        let provider_response = client
            .post(format!("http://{provider_address}/effects/{effect_id}"))
            .send()
            .await?;
        ensure!(provider_response.status() == StatusCode::OK);
        ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 1);
        ensure!(PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1);

        crash_server.kill().await?;
        let _ = crash_server.wait().await?;
        let recovery_address = reserve_local_address().await?;
        let mut recovery_server = spawn_production_server(database_url, target, recovery_address)?;
        wait_for_listener(recovery_address).await?;
        let recovery_url = format!("http://{recovery_address}{checkpoint_path}");
        let recovered_prepared: Value = client
            .get(&recovery_url)
            .bearer_auth(&target.bearer_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(recovered_prepared["checkpoint_revision_id"] == prepared_revision_id);
        ensure!(recovered_prepared["effects"][0]["effect_id"] == effect_id);
        ensure!(recovered_prepared["effects"][0]["status"] == "prepared");

        let provider_retry = client
            .post(format!("http://{provider_address}/effects/{effect_id}"))
            .send()
            .await?;
        ensure!(provider_retry.status() == StatusCode::OK);
        ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 2);
        ensure!(
            PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1,
            "recovery retried the provider with the stable effect ID but applied it twice"
        );
        recovery_server.kill().await?;
        let _ = recovery_server.wait().await?;

        let completion_crash_address = reserve_local_address().await?;
        let mut crash_server = spawn_crash_server(database_url, target, completion_crash_address)?;
        wait_for_listener(completion_crash_address).await?;
        let completion_url = format!("http://{completion_crash_address}{checkpoint_path}");

        let complete_body = json!({
            "case_id": case_id,
            "parent_revision_id": prepared_revision_id,
            "state": {"step": "effect-completed", "private_marker": "never-audit-this"},
            "state_schema_version": 1,
            "effect_transitions": [{
                "type": "complete",
                "effect_id": effect_id,
                "receipt": {
                    "observed_at": "2026-07-29T02:00:00Z",
                    "external_reference": "mock-provider-321",
                    "outcome_sha256": "b".repeat(64)
                }
            }],
            "provenance": provenance,
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        });
        let completion_task = tokio::spawn({
            let client = client.clone();
            let completion_url = completion_url.clone();
            let token = target.bearer_token.clone();
            let prepare_etag = prepare_etag.clone();
            let complete_body = complete_body.clone();
            async move {
                client
                    .put(completion_url)
                    .bearer_auth(token)
                    .header("Idempotency-Key", "checkpoint-run-321-complete")
                    .header(header::IF_MATCH, prepare_etag)
                    .json(&complete_body)
                    .send()
                    .await
            }
        });

        let crash_status = tokio::time::timeout(Duration::from_secs(5), crash_server.wait())
            .await
            .context("checkpoint crash child did not terminate after commit")??;
        ensure!(
            crash_status.code() == Some(86),
            "checkpoint crash child exited with {crash_status}"
        );
        let lost_response = tokio::time::timeout(Duration::from_secs(5), completion_task)
            .await
            .context("terminated checkpoint response did not close the client connection")??;
        ensure!(
            lost_response.is_err(),
            "fault injection unexpectedly delivered the committed response"
        );
        let committed_etag =
            checkpoint_idempotency_etag(pool, target, "checkpoint-run-321-complete").await?;

        let restart_address = reserve_local_address().await?;
        let mut restart_server = spawn_production_server(database_url, target, restart_address)?;
        wait_for_listener(restart_address).await?;
        let restarted_url = format!("http://{restart_address}{checkpoint_path}");
        let verification = async {
            let replay_response = client
                .put(&restarted_url)
                .bearer_auth(&target.bearer_token)
                .header("Idempotency-Key", "checkpoint-run-321-complete")
                .header(header::IF_MATCH, &prepare_etag)
                .json(&complete_body)
                .send()
                .await?;
            ensure!(replay_response.status() == StatusCode::OK);
            ensure!(
                replay_response
                    .headers()
                    .get("idempotency-replayed")
                    .is_some_and(|value| value == "true")
            );
            let replay_etag = replay_response
                .headers()
                .get(header::ETAG)
                .context("completion replay omitted ETag")?
                .to_str()?
                .to_owned();
            ensure!(
                committed_etag == replay_etag,
                "completion replay did not preserve the withheld response ETag"
            );
            let replayed: Value = replay_response.json().await?;
            ensure!(replayed["revision_number"] == 3);
            ensure!(replayed["effects"][0]["status"] == "completed");
            ensure!(
                replayed["effects"][0]["receipt"]
                    == complete_body["effect_transitions"][0]["receipt"]
            );

            let current_response = client
                .get(&restarted_url)
                .bearer_auth(&target.bearer_token)
                .send()
                .await?
                .error_for_status()?;
            ensure!(
                current_response.headers().get(header::ETAG)
                    == Some(&header::HeaderValue::from_str(&replay_etag)?),
                "current checkpoint ETag differs from the replayed completion"
            );
            let current: Value = current_response.json().await?;
            ensure!(current == replayed);
            ensure!(
                PROVIDER_APPLICATIONS.load(Ordering::SeqCst) == 1,
                "completed replay caused the external effect to be applied twice"
            );
            ensure!(PROVIDER_ATTEMPTS.load(Ordering::SeqCst) == 2);
            verify_crash_recovery_records(pool, target, agent_id, thread_id).await
        }
        .await;
        let _ = restart_server.kill().await;
        let _ = restart_server.wait().await;
        verification
    }
    .await;

    provider_server.abort();
    let _ = provider_server.await;
    scenario
}

async fn reserve_local_address() -> Result<std::net::SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn spawn_crash_server(
    database_url: &str,
    target: &Target,
    address: std::net::SocketAddr,
) -> Result<tokio::process::Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("crash_after_checkpoint_commit_child")
        .arg("--test-threads=1")
        .env("PALIMPSEST_CRASH_CHILD", "1")
        .env("PALIMPSEST_TEST_CHILD_DATABASE_URL", database_url)
        .env(
            "PALIMPSEST_TEST_CHILD_TENANT_ID",
            target.tenant_id.to_string(),
        )
        .env(
            "PALIMPSEST_TEST_CHILD_SUBJECT_ID",
            target.subject_id.to_string(),
        )
        .env("PALIMPSEST_TEST_CHILD_BEARER_TOKEN", &target.bearer_token)
        .env("PALIMPSEST_TEST_CHILD_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn().context("spawn checkpoint crash child")
}

fn spawn_production_server(
    database_url: &str,
    target: &Target,
    address: std::net::SocketAddr,
) -> Result<tokio::process::Child> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"));
    command
        .env("PALIMPSEST_DATABASE_URL", database_url)
        .env("PALIMPSEST_BEARER_TOKEN", &target.bearer_token)
        .env("PALIMPSEST_PRINCIPAL_ID", "principal-a")
        .env("PALIMPSEST_TENANT_ID", target.tenant_id.to_string())
        .env("PALIMPSEST_SUBJECT_ID", target.subject_id.to_string())
        .env("PALIMPSEST_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
        .spawn()
        .context("restart production checkpoint server")
}

async fn wait_for_listener(address: std::net::SocketAddr) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    drop(stream);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .with_context(|| format!("server did not listen on {address}"))?;
    Ok(())
}

async fn checkpoint_idempotency_etag(
    pool: &PgPool,
    target: &Target,
    idempotency_key: &str,
) -> Result<String> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let etag = sqlx::query_scalar(
        r#"
        SELECT response_etag
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'principal-a'
          AND operation_id = 'saveCheckpoint'
          AND idempotency_key = $3
          AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(idempotency_key)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(etag)
}

async fn verify_crash_recovery_records(
    pool: &PgPool,
    target: &Target,
    agent_id: Uuid,
    thread_id: Uuid,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let counts = sqlx::query(
        r#"
        SELECT
            (SELECT count(*) FROM memory.checkpoint_revisions
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS revision_count,
            (SELECT count(*) FROM memory.checkpoint_effect_intents
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS prepared_count,
            (SELECT count(*) FROM memory.checkpoint_effect_receipts
             WHERE tenant_id = $1 AND subject_id = $2 AND agent_id = $3 AND thread_id = $4)
                AS completed_count,
            (SELECT count(*) FROM memory.write_audit_receipts
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND authorization_context::text NOT LIKE '%never-audit-this%'
               AND authorization_context::text NOT LIKE '%mock-provider-321%')
                AS audit_count,
            (SELECT count(*) FROM memory.outbox_intents
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND payload::text NOT LIKE '%never-audit-this%'
               AND payload::text NOT LIKE '%mock-provider-321%')
                AS outbox_count
            ,
            (SELECT count(*) FROM memory.idempotency_receipts
             WHERE tenant_id = $1 AND subject_id = $2
               AND resource_checkpoint_agent_id = $3 AND resource_checkpoint_thread_id = $4
               AND state = 'completed')
                AS idempotency_count
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(agent_id)
    .bind(thread_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(counts.try_get::<i64, _>("revision_count")? == 3);
    ensure!(counts.try_get::<i64, _>("prepared_count")? == 1);
    ensure!(counts.try_get::<i64, _>("completed_count")? == 1);
    ensure!(counts.try_get::<i64, _>("audit_count")? == 3);
    ensure!(counts.try_get::<i64, _>("outbox_count")? == 3);
    ensure!(counts.try_get::<i64, _>("idempotency_count")? == 3);
    transaction.commit().await?;
    Ok(())
}

async fn verify_governed_write_records(pool: &PgPool, target: &Target) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;

    let audit = sqlx::query(
        r#"
        SELECT
            count(*) FILTER (WHERE operation_id = 'appendEpisode') AS episode_count,
            count(*) FILTER (WHERE operation_id = 'createFact') AS create_fact_count,
            count(*) FILTER (WHERE operation_id = 'supersedeFact') AS supersede_fact_count,
            count(*) AS total_count
        FROM memory.write_audit_receipts
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(audit.try_get::<i64, _>("episode_count")? == 3);
    ensure!(audit.try_get::<i64, _>("create_fact_count")? == 1);
    ensure!(audit.try_get::<i64, _>("supersede_fact_count")? == 1);
    ensure!(audit.try_get::<i64, _>("total_count")? == 5);

    let outbox = sqlx::query(
        r#"
        SELECT
            count(*) FILTER (WHERE event_type = 'memory.episode.appended.v1') AS episode_count,
            count(*) FILTER (WHERE event_type = 'memory.fact.created.v1') AS create_fact_count,
            count(*) FILTER (WHERE event_type = 'memory.fact.superseded.v1') AS supersede_fact_count,
            count(*) AS total_count
        FROM memory.outbox_intents
        WHERE tenant_id = $1 AND subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(outbox.try_get::<i64, _>("episode_count")? == 3);
    ensure!(outbox.try_get::<i64, _>("create_fact_count")? == 1);
    ensure!(outbox.try_get::<i64, _>("supersede_fact_count")? == 1);
    ensure!(outbox.try_get::<i64, _>("total_count")? == 5);

    let paired_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.write_audit_receipts AS audit
        JOIN memory.outbox_intents AS outbox
          ON outbox.tenant_id = audit.tenant_id
         AND outbox.subject_id = audit.subject_id
         AND outbox.case_id = audit.case_id
         AND outbox.resource_episode_id IS NOT DISTINCT FROM audit.resource_episode_id
         AND outbox.resource_fact_id IS NOT DISTINCT FROM audit.resource_fact_id
         AND outbox.resource_revision_id IS NOT DISTINCT FROM audit.resource_revision_id
        WHERE audit.tenant_id = $1 AND audit.subject_id = $2
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        paired_count == 5,
        "every durable mutation needs one audit/outbox pair"
    );

    let receipt_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1 AND principal_id = 'principal-a' AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        receipt_count == 5,
        "idempotent replays must not create durable write records"
    );

    let published = sqlx::query(
        r#"
        UPDATE memory.outbox_intents
        SET published_at = clock_timestamp()
        WHERE tenant_id = $1
          AND subject_id = $2
          AND intent_id = (
            SELECT intent_id
            FROM memory.outbox_intents
            WHERE tenant_id = $1
              AND subject_id = $2
              AND published_at IS NULL
            ORDER BY created_at, intent_id
            LIMIT 1
        )
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        published.rows_affected() == 1,
        "scoped outbox publisher could not mark one intent published"
    );
    transaction.commit().await?;
    Ok(())
}

async fn verify_checkpoint_governance(pool: &PgPool, target: &Target) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;

    let checkpoint_audits: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.write_audit_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND operation_id = 'saveCheckpoint'
          AND resource_checkpoint_id IS NOT NULL
          AND authorization_context::text NOT LIKE '%provider-call%'
          AND authorization_context::text NOT LIKE '%provider-result-301%'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        checkpoint_audits == 5,
        "checkpoint retries or failures duplicated audit records, or audit content leaked state"
    );

    let checkpoint_outbox: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.outbox_intents
        WHERE tenant_id = $1
          AND subject_id = $2
          AND event_type = 'memory.checkpoint.saved.v1'
          AND resource_checkpoint_id IS NOT NULL
          AND payload::text NOT LIKE '%provider-call%'
          AND payload::text NOT LIKE '%provider-result-301%'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(
        checkpoint_outbox == 5,
        "checkpoint retries or failures duplicated outbox records, or outbox content leaked state"
    );

    let checkpoint_receipts: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM memory.idempotency_receipts
        WHERE tenant_id = $1
          AND subject_id = $2
          AND principal_id = 'principal-a'
          AND operation_id = 'saveCheckpoint'
          AND state = 'completed'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(checkpoint_receipts == 5);

    let prepared_effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.checkpoint_effect_intents WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    let completed_effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.checkpoint_effect_receipts WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_one(&mut *transaction)
    .await?;
    ensure!(prepared_effects == 1);
    ensure!(completed_effects == 1);
    transaction.commit().await?;
    Ok(())
}
