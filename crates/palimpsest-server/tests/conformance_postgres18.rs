use anyhow::{Context, Result, bail, ensure};
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
    collections::{BTreeMap, HashSet},
    env,
    process::Stdio,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use palimpsest_application::{
    CreateDeletionRequest, DELETION_MAX_ATTEMPTS, DeletionRepository, EmbeddingProvider,
    EmbeddingProviderError, EmbeddingRequest, EmbeddingResponse, MemoryService, RestoreFenceEntry,
    RestoreFenceLedger,
};
use palimpsest_conformance::retrieval_evaluation::{
    LifecycleFixture, PreparedCorpus, enforce_issue_22_gates, evaluate_frozen_corpus,
    evaluate_full_policy_once, load_frozen_corpus, prepare_frozen_corpus, write_or_verify_artifact,
};
use palimpsest_conformance::{
    HybridFusionFixture, RetrievalIsolationFixture, RetrievalLifecycleFixture, Target,
    TemporalLifecycleFixture, TemporalLifecycleReplayFixture, TemporalReplayFixture,
    TemporalRetrievalFixture, captures_temporal_lifecycle_receipts, checkpoint_scopes_fail_closed,
    concurrent_retrievals_converge_on_one_receipt, creates_an_attributable_fact_revision,
    creates_and_replays_a_lexical_retrieval_receipt, creates_deterministic_hybrid_fusion_receipts,
    creates_hybrid_fusion_fixture, creates_retrieval_lifecycle_fixture,
    creates_temporal_lifecycle_fixture, creates_temporal_receipt_through_nonbypass_runtime,
    creates_temporal_retrieval_fixture, cross_scope_reads_fail_closed,
    expires_only_the_targeted_checkpoint, hybrid_retrieval_fails_closed_without_leaking,
    hybrid_retrieval_recovers_after_projection_rebuild,
    hybrid_retrieval_rejects_caller_ranking_internals,
    hybrid_retrieval_requires_an_available_provider, reconstructs_both_temporal_axes,
    records_and_reads_an_immutable_episode, rejects_cross_subject_idempotency_reuse,
    rejects_cross_subject_retrieval_idempotency_reuse, rejects_invalid_domain_and_timestamp_inputs,
    rejects_unregistered_write_policies, replays_hybrid_receipt_before_provider_io,
    replays_temporal_receipt_through_nonbypass_runtime,
    retrieval_candidates_are_authorized_before_ranking,
    retrieval_fails_closed_when_projection_is_corrupt,
    retrieval_fails_closed_when_projection_is_missing,
    retrieval_paginates_and_rejects_invalid_replays,
    retrieval_receipt_does_not_resurrect_deleted_history, retrieval_receipt_hides_expired_content,
    retrieval_recovers_after_projection_rebuild, retrieval_succeeds_after_projection_rebuild,
    retrieves_the_effective_bitemporal_revision, retrieves_with_the_fixed_temporal_policy,
    saves_and_reads_a_resumable_checkpoint, supersedes_the_fact_head,
    temporal_policy_does_not_resurrect_ineligible_successors,
    temporal_receipt_survives_service_restart, temporal_retrieval_survives_projection_rebuild,
};
use palimpsest_domain::{
    DeletionOperationState, DeletionTargetCapability, DeletionTargetState,
    DeletionTargetVerification, EmbeddingOutput, EmbeddingTask, OperationGrant, PrincipalId,
    PrincipalScope, RecencyProfile, Sensitivity, SubjectId, TenantId, temporal_factor_q63,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::{EmbeddingProjectionCoordinator, PostgresMemoryRepository};
use sqlx::{
    AssertSqlSafe, ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
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
        if content.contains("palimpsest-") {
            return vec![1.0, 0.0, 0.0, 0.0];
        }
        assert!(
            matches!(
                content,
                "case.retrieval:fusiontoken" | "case.temporal:chronotoken"
            ),
            "unexpected conformance query embedding input"
        );
        return vec![1.0, 0.0, 0.0, 0.0];
    }
    if content.contains("embedding_fixture_role") {
        if content.contains("\"relevant\"") || content.contains("\"trap\"") {
            return vec![1.0, 0.0, 0.0, 0.0];
        }
        if content.contains("\"near\"") {
            return vec![0.8, 0.6, 0.0, 0.0];
        }
        if content.contains("\"distractor\"") {
            return vec![0.0, 1.0, 0.0, 0.0];
        }
    }
    for (marker, vector) in [
        ("vector_fixture_forbidden_4d", [1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_exact_4d", [-1.0, 0.0, 0.0, 0.0]),
        ("vector_fixture_alpha_4d", [-0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_beta_4d", [0.8, 0.6, 0.0, 0.0]),
        ("vector_fixture_gamma_4d", [0.6, 0.8, 0.0, 0.0]),
        ("vector_fixture_delta_4d", [0.0, 1.0, 0.0, 0.0]),
        ("temporal_vector_fixture_exact_4d", [-1.0, 0.0, 0.0, 0.0]),
        ("temporal_vector_fixture_alpha_4d", [-0.6, 0.8, 0.0, 0.0]),
        ("temporal_vector_fixture_beta_4d", [0.8, 0.6, 0.0, 0.0]),
        ("temporal_vector_fixture_gamma_4d", [0.6, 0.8, 0.0, 0.0]),
        ("temporal_vector_fixture_delta_4d", [0.0, 1.0, 0.0, 0.0]),
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
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_admin_pool = PgPool::connect(&migration_database_url)
        .await
        .with_context(|| {
            format!("connect to migration-authority PostgreSQL through {migration_database_url}")
        })?;

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
        principal_d_same_scope_bearer_token: "principal-d-same-scope-test-token".to_owned(),
    };
    let result = async {
        palimpsest_postgres::migrate(&pool).await?;
        exercise_restore_fence_replay(&pool, &migration_pool).await?;
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
        .await
        .context("create deletion operation")?;
        sqlx::query(
            r#"
            INSERT INTO memory.checkpoint_retention_policies (
                retention_policy_id, retention_interval
            )
            VALUES ('checkpoint-test-1s-v1', interval '1 second')
            "#,
        )
        .execute(&pool)
        .await
        .context("poll leased deletion target")?;
        let authenticator = Arc::new(StaticAuthenticator::new([
            (
                target.bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                        SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000022")?),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![],
                },
            ),
            (
                "principal-a-export-delete-test-token".to_owned(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-a".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![
                        SubjectId(subject_id),
                        SubjectId(target.principal_a_secondary_subject_id),
                        SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000022")?),
                    ],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![
                        OperationGrant::CanonicalHistoryExport,
                        OperationGrant::SubjectDelete,
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
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_b_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-b".to_owned()),
                    tenant_id: TenantId(target.principal_b_tenant_id),
                    subject_ids: vec![SubjectId(target.principal_b_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_c_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-c".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(target.principal_c_subject_id)],
                    allowed_sensitivities: vec![Sensitivity::try_from("restricted".to_owned())?],
                    operation_grants: vec![],
                },
            ),
            (
                target.principal_d_same_scope_bearer_token.clone(),
                PrincipalScope {
                    principal_id: PrincipalId("principal-d".to_owned()),
                    tenant_id: TenantId(tenant_id),
                    subject_ids: vec![SubjectId(subject_id)],
                    allowed_sensitivities: vec![
                        Sensitivity::try_from("internal".to_owned())?,
                        Sensitivity::try_from("restricted".to_owned())?,
                    ],
                    operation_grants: vec![],
                },
            ),
        ]));
        deletion_target_lease_recovers_after_worker_expiry(&pool, &migration_pool).await?;
        deletion_failed_operation_can_be_repaired_and_resumed(&pool, &migration_pool).await?;
        deletion_target_retry_exhaustion_remains_fenced(&pool, &migration_pool).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_pool = pool.clone();
        let server_authenticator = authenticator.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(server_pool.clone(), server_pool, server_authenticator),
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
            rejects_unregistered_write_policies(&scenario_target).await?;
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
            .await?;
            Ok::<_, anyhow::Error>(retrieval_isolation)
        }
        .await;
        let retrieval_isolation = scenario?;
        server.abort();
        let _ = server.await;
        runs_hybrid_retrieval_conformance(
            &pool,
            &migration_pool,
            authenticator.clone(),
            &target,
            &test_database_url,
            &retrieval_isolation,
        )
        .await?;
        let export_listener = TcpListener::bind("127.0.0.1:0").await?;
        let export_address = export_listener.local_addr()?;
        let export_pool = pool.clone();
        let export_authenticator = authenticator.clone();
        let export_server = tokio::spawn(async move {
            axum::serve(
                export_listener,
                palimpsest_server::app(export_pool.clone(), export_pool, export_authenticator),
            )
            .await
        });
        let export_target = Target {
            base_url: format!("http://{export_address}"),
            ..target.clone()
        };
        let export_result = exercise_export_and_deletion_http(
            &export_target,
            &target,
            "principal-a-export-delete-test-token",
            &migration_pool,
        )
        .await;
        export_server.abort();
        let _ = export_server.await;
        export_result?;
        recovers_a_committed_effect_after_response_loss(&pool, &target, &test_database_url).await
    }
    .await;

    migration_pool.close().await;
    pool.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP DATABASE \"{database_name}\" WITH (FORCE)"
    )))
    .execute(&migration_admin_pool)
    .await?;
    migration_admin_pool.close().await;
    result
}

async fn exercise_restore_fence_replay(pool: &PgPool, migration_pool: &PgPool) -> Result<()> {
    let tenant_id = Uuid::parse_str("019be000-0000-7000-8000-000000000310")?;
    let subject_id = Uuid::parse_str("019be000-0000-7000-8000-000000000311")?;
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000312")?;
    let episode_id = Uuid::parse_str("019be000-0000-7000-8000-000000000313")?;
    let payload = r#"{"restore":"private"}"#;

    sqlx::query(
        r#"
        INSERT INTO memory.subject_lifecycles (
            tenant_id, subject_id, lifecycle_state, state_version
        )
        VALUES ($1, $2, 'active', 0)
        "#,
    )
    .bind(tenant_id)
    .bind(subject_id)
    .execute(migration_pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO memory.episodes (
            tenant_id, subject_id, case_id, episode_id, kind, observed_at,
            writer_principal_id, source_type, sensitivity, retention_policy_id,
            schema_version, payload, payload_sha256
        )
        VALUES (
            $1, $2, $3, $4, 'observation', clock_timestamp(),
            'restore-conformance', 'restore-fixture', 'internal', 'standard',
            1, $5::jsonb,
            encode(public.digest(convert_to($5, 'UTF8'), 'sha256'), 'hex')
        )
        "#,
    )
    .bind(tenant_id)
    .bind(subject_id)
    .bind(case_id)
    .bind(episode_id)
    .bind(payload)
    .execute(migration_pool)
    .await?;

    let scope_digest: String = sqlx::query_scalar("SELECT memory.deletion_scope_digest($1, $2)")
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_one(migration_pool)
        .await?;
    let now = OffsetDateTime::now_utc();
    let ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            scope_digest,
            1,
            now - TimeDuration::minutes(1),
            now + TimeDuration::hours(1),
        )?],
    )?;
    let ledger_bytes = ledger.to_bytes()?;
    let repository = PostgresMemoryRepository::new(migration_pool.clone());
    assert!(
        repository
            .replay_restore_fence_ledger(&ledger_bytes, &"0".repeat(64))
            .await
            .is_err(),
        "restore replay must reject a mismatched independent digest"
    );
    let report = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay fixture failed: {error}"))?;
    assert_eq!(report.scopes_found, 1);
    assert_eq!(report.scopes_purged, 1);
    assert_eq!(report.residual_rows, 0);
    assert_eq!(report.ledger_sha256, ledger.ledger_sha256);

    let state: String = sqlx::query_scalar(
        "SELECT lifecycle_state FROM memory.subject_lifecycles WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(state, "deleted");
    let episode_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.episodes WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id)
    .bind(subject_id)
    .fetch_one(migration_pool)
    .await?;
    assert_eq!(episode_count, 0);

    let replayed = repository
        .replay_restore_fence_ledger(&ledger_bytes, &ledger.ledger_sha256)
        .await
        .map_err(|error| anyhow::anyhow!("restore replay idempotency failed: {error}"))?;
    assert_eq!(replayed, report);

    let mut runtime_transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *runtime_transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.to_string())
        .execute(&mut *runtime_transaction)
        .await?;
    let runtime_episode_count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.episodes")
        .fetch_one(&mut *runtime_transaction)
        .await?;
    assert_eq!(runtime_episode_count, 0);
    runtime_transaction.rollback().await?;
    Ok(())
}

async fn deletion_target_lease_recovers_after_worker_expiry(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000030")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000031")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-recovery-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-target-lease-recovery".to_owned(),
            request_fingerprint_sha256: "1".repeat(64),
            configured_targets: vec![
                palimpsest_domain::DeletionTargetName::Canonical,
                palimpsest_domain::DeletionTargetName::Projections,
            ],
            retention_hours: 24 * 90,
        })
        .await
        .context("create deletion operation")?;
    let seed_counts: (i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT count(*) FROM memory.deletion_tombstone_seeds
             WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3),
            (SELECT count(*) FROM memory.deletion_audit_seeds
             WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3)
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(seed_counts == (1, 1));
    let first_worker = Uuid::now_v7();
    let claimed = repository
        .claim_next_deletion_operation(first_worker, 5)
        .await?
        .context("deletion operation was not claimable")?;
    ensure!(claimed.operation_id == created.operation_id);
    let advanced = repository
        .advance_deletion_operation(&claimed, first_worker, 5)
        .await
        .context("advance operation into purging")?;
    ensure!(advanced.lifecycle_state == DeletionOperationState::Purging);
    let first_target = repository
        .claim_next_deletion_target(&claimed, first_worker, 1)
        .await
        .context("claim first deletion target")?
        .context("first deletion target was not claimable")?;
    let leased_view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll leased deletion target")?;
    let leased_target = leased_view
        .targets
        .iter()
        .find(|target| target.target_name == first_target.target_name)
        .context("claimed target was not visible in the public deletion view")?;
    ensure!(leased_target.state == DeletionTargetState::Leased);
    ensure!(leased_target.verification == DeletionTargetVerification::Pending);
    ensure!(leased_target.target_key_digest == first_target.target_key_digest);
    ensure!(leased_target.lease_id == Some(first_target.target_lease_id));

    repository
        .renew_deletion_operation_lease(&claimed, first_worker, 5)
        .await
        .context("renew deletion operation lease")?;
    repository
        .renew_deletion_target_lease(&first_target, 5)
        .await
        .context("renew deletion target lease")?;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let second_worker = Uuid::now_v7();
    ensure!(
        repository
            .claim_next_deletion_operation(second_worker, 5)
            .await?
            .is_none(),
        "a renewed deletion operation lease was reclaimed early"
    );
    tokio::time::sleep(Duration::from_millis(5_000)).await;
    let reclaimed_operation = repository
        .claim_next_deletion_operation(second_worker, 5)
        .await
        .context("reclaim expired deletion operation")?
        .context("expired deletion operation lease was not reclaimable")?;
    let reclaimed_target = repository
        .claim_next_deletion_target(&reclaimed_operation, second_worker, 1)
        .await
        .context("reclaim expired deletion target")?
        .context("expired deletion target lease was not reclaimable")?;
    ensure!(reclaimed_target.target_name == first_target.target_name);
    ensure!(reclaimed_target.target_key_digest == first_target.target_key_digest);
    ensure!(reclaimed_target.target_lease_id != first_target.target_lease_id);
    ensure!(reclaimed_target.attempts == first_target.attempts + 1);

    tokio::time::sleep(Duration::from_millis(6_500)).await;
    let completed = {
        let mut completed = None;
        for attempt in 0..=DELETION_MAX_ATTEMPTS {
            service
                .run_deletion_worker_once()
                .await
                .context("finish recovered deletion")?;
            let view = service
                .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
                .await
                .context("poll recovered deletion")?;
            match view.lifecycle_state {
                DeletionOperationState::Completed => {
                    completed = Some(view);
                    break;
                }
                DeletionOperationState::RetryWait => {
                    ensure!(view.retry_count > 0);
                    ensure!(
                        attempt < DELETION_MAX_ATTEMPTS,
                        "recovered deletion remained in retry_wait after the retry budget"
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                state => bail!("recovered deletion entered unexpected state {state:?}"),
            }
        }
        completed.context("recovered deletion did not reach completed")?
    };
    ensure!(
        completed
            .targets
            .iter()
            .filter(|target| target.capability
                == palimpsest_domain::DeletionTargetCapability::Configured)
            .all(|target| {
                target.state == DeletionTargetState::Done
                    && target.verification
                        == palimpsest_domain::DeletionTargetVerification::Verified
            })
    );
    ensure!(
        completed
            .targets
            .iter()
            .filter(|target| target.capability
                == palimpsest_domain::DeletionTargetCapability::Configured)
            .all(|target| target.effect_receipt_sha256.is_some())
    );
    let operation_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM memory.deletion_operations WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(operation_rows == 0);
    let tombstone = sqlx::query(
        "SELECT scope_digest, target_summary, idempotency_key_digest, request_fingerprint_sha256
         FROM memory.deletion_tombstones WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id.0)
    .bind(created.operation_id.0)
    .fetch_one(migration_pool)
    .await?;
    let scope_digest: String = tombstone.try_get("scope_digest")?;
    ensure!(scope_digest.starts_with("v1:"));
    ensure!(scope_digest.len() == 67);
    let target_summary: serde_json::Value = tombstone.try_get("target_summary")?;
    ensure!(target_summary.is_array());
    ensure!(
        tombstone
            .try_get::<String, _>("idempotency_key_digest")?
            .trim()
            .len()
            == 64
    );
    ensure!(
        tombstone
            .try_get::<String, _>("request_fingerprint_sha256")?
            .trim()
            .len()
            == 64
    );
    Ok(())
}

async fn deletion_failed_operation_can_be_repaired_and_resumed(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000040")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000041")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-repair-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-repair".to_owned(),
            request_fingerprint_sha256: "2".repeat(64),
            configured_targets: vec![palimpsest_domain::DeletionTargetName::Canonical],
            retention_hours: 24 * 90,
        })
        .await
        .context("create repairable deletion")?;

    sqlx::query(
        "UPDATE memory.deletion_operations
         SET lifecycle_state = 'failed', failure_reason = 'injected_failure',
             completed_at = clock_timestamp()
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;
    sqlx::query(
        "UPDATE memory.deletion_targets
         SET state = 'failed', sanitized_error = 'injected_failure'
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3
           AND capability = 'configured'",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;

    let repaired = service
        .repair_subject_deletion(
            &principal,
            tenant_id,
            subject_id,
            created.operation_id,
            "operator_retry".to_owned(),
        )
        .await
        .context("repair failed deletion")?;
    ensure!(repaired.lifecycle_state == DeletionOperationState::RetryWait);
    ensure!(repaired.failure_reason.is_none());
    ensure!(repaired.targets.iter().all(|target| {
        target.capability == palimpsest_domain::DeletionTargetCapability::NotConfigured
            || target.state == DeletionTargetState::Pending
    }));

    service
        .run_deletion_worker_once()
        .await
        .context("resume repaired deletion")?;
    let completed = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await
        .context("poll repaired deletion")?;
    ensure!(completed.lifecycle_state == DeletionOperationState::Completed);
    let tombstone_text: String = sqlx::query_scalar(
        "SELECT target_summary::text || ':' || verification_digest
         FROM memory.deletion_tombstones
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id.0)
    .bind(created.operation_id.0)
    .fetch_optional(migration_pool)
    .await?
    .context("repairable deletion tombstone is missing")?;
    ensure!(
        !tombstone_text.contains("operator_retry"),
        "repair reason must not enter the tombstone"
    );
    Ok(())
}

async fn deletion_target_retry_exhaustion_remains_fenced(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000050")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000051")?);
    let principal = PrincipalScope {
        principal_id: PrincipalId("deletion-retry-exhaustion-principal".to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![],
        operation_grants: vec![OperationGrant::SubjectDelete],
    };
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    );
    let created = repository
        .create_deletion_operation(CreateDeletionRequest {
            tenant_id,
            subject_id,
            principal_id: principal.principal_id.clone(),
            idempotency_key: "deletion-retry-exhaustion".to_owned(),
            request_fingerprint_sha256: "3".repeat(64),
            configured_targets: vec![palimpsest_domain::DeletionTargetName::Canonical],
            retention_hours: 24 * 90,
        })
        .await
        .context("create retry-exhaustion deletion")?;
    let worker_id = Uuid::now_v7();
    let claimed = repository
        .claim_next_deletion_operation(worker_id, 30)
        .await?
        .context("retry-exhaustion deletion was not claimable")?;
    let advanced = repository
        .advance_deletion_operation(&claimed, worker_id, 5)
        .await
        .context("advance retry-exhaustion deletion")?;
    ensure!(advanced.lifecycle_state == DeletionOperationState::Purging);

    for attempt in 1..=5 {
        let target = repository
            .claim_next_deletion_target(&claimed, worker_id, 30)
            .await?
            .context("retry-exhaustion target was not claimable")?;
        repository
            .fail_deletion_target(&target, "injected_failure", 5)
            .await
            .context("record retry-exhaustion target failure")?;
        let view = service
            .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
            .await?;
        let target_view = view
            .targets
            .iter()
            .find(|candidate| candidate.target_name == target.target_name)
            .context("retry-exhaustion target disappeared")?;
        if attempt < 5 {
            ensure!(target_view.state == DeletionTargetState::Pending);
        } else {
            ensure!(target_view.state == DeletionTargetState::Failed);
            ensure!(target_view.sanitized_error.as_deref() == Some("injected_failure"));
        }
    }

    let failed = repository
        .advance_deletion_operation(&claimed, worker_id, 5)
        .await
        .context("fail retry-exhaustion deletion")?;
    ensure!(failed.lifecycle_state == DeletionOperationState::Failed);
    sqlx::query(
        "UPDATE memory.deletion_operations
         SET expires_at = clock_timestamp() - interval '1 second'
         WHERE tenant_id = $1 AND subject_id = $2 AND operation_id = $3",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(created.operation_id.0)
    .execute(migration_pool)
    .await?;
    let view = service
        .poll_subject_deletion(&principal, tenant_id, subject_id, created.operation_id)
        .await?;
    ensure!(view.lifecycle_state == DeletionOperationState::Failed);
    ensure!(!view.expired);
    ensure!(
        view.targets
            .iter()
            .filter(|target| target.capability == DeletionTargetCapability::Configured)
            .all(|target| target.verification == DeletionTargetVerification::NotVerified)
    );
    let outcome = view
        .outcome
        .as_ref()
        .context("failed deletion omitted terminal outcome")?;
    ensure!(
        outcome.live_disposition == palimpsest_domain::DeletionLiveDisposition::FencedNotVerified
    );
    ensure!(
        outcome.backup_disposition == palimpsest_domain::DeletionBackupDisposition::NotConfigured
    );
    ensure!(outcome.verification_digest.is_none());
    let lifecycle_state: String = sqlx::query_scalar(
        "SELECT lifecycle_state FROM memory.subject_lifecycles
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .fetch_one(migration_pool)
    .await?;
    ensure!(lifecycle_state == "deletion_pending");
    Ok(())
}

async fn exercise_export_and_deletion_http(
    target: &Target,
    scope: &Target,
    bearer_token: &str,
    migration_pool: &PgPool,
) -> Result<()> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let base_url = target.base_url.trim_end_matches('/');
    let secondary_subject = scope.principal_a_secondary_subject_id;
    let secondary_prefix = format!(
        "{base_url}/v1/tenants/{}/subjects/{secondary_subject}",
        target.tenant_id
    );

    let cross_tenant_deletion = client
        .post(format!(
            "{base_url}/v1/tenants/{}/subjects/{}/deletions",
            scope.principal_b_tenant_id, scope.principal_b_subject_id
        ))
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "cross-tenant-deletion-attempt")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(
        cross_tenant_deletion.status() == StatusCode::NOT_FOUND,
        "cross-tenant deletion disclosed an operation: {}",
        cross_tenant_deletion.status()
    );

    let episode_url = format!("{secondary_prefix}/episodes");
    let episode_body = json!({
        "case_id": Uuid::from_u128(0x501),
        "kind": "message",
        "observed_at": "2026-07-31T09:00:00Z",
        "provenance": {
            "source_type": "export-deletion-conformance",
            "source_uri": null,
            "external_id": "export-delete-episode"
        },
        "sensitivity": "internal",
        "retention_policy_id": "standard",
        "payload": {"marker": "export-delete-private-marker"}
    });
    let episode_response = client
        .post(&episode_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-episode")
        .json(&episode_body)
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode_location = episode_response
        .headers()
        .get(header::LOCATION)
        .context("export/deletion episode omitted Location")?
        .to_str()?
        .to_owned();
    let episode_location = if episode_location.starts_with("http") {
        episode_location
    } else {
        format!("{base_url}{episode_location}")
    };

    let export_url = format!("{secondary_prefix}/exports");
    let export_response = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-export")
        .send()
        .await?;
    if export_response.status() != StatusCode::ACCEPTED {
        let status = export_response.status();
        let body = export_response.text().await?;
        bail!("export creation returned {status}: {body}");
    }
    ensure!(
        export_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let export_status_url = export_response
        .headers()
        .get(header::LOCATION)
        .context("export creation omitted Location")?
        .to_str()?
        .to_owned();
    let export_operation: Value = export_response.json().await?;
    let export_id = export_operation["export_id"]
        .as_str()
        .context("export response omitted export_id")?
        .to_owned();
    let export_status_url = if export_status_url.starts_with("http") {
        export_status_url
    } else {
        format!("{base_url}{export_status_url}")
    };
    let replay = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-export")
        .send()
        .await?;
    ensure!(replay.status() == StatusCode::ACCEPTED);
    ensure!(
        replay.headers().get("idempotency-replayed")
            == Some(&header::HeaderValue::from_static("true"))
    );
    let replay_operation: Value = replay.json().await?;
    ensure!(replay_operation["export_id"] == export_id);

    let mut ready_etag = None;
    let mut content_url = None;
    let mut ready_operation = None;
    let mut last_export_body = Value::Null;
    for _ in 0..100 {
        let response = client
            .get(&export_status_url)
            .bearer_auth(bearer_token)
            .send()
            .await?;
        if response.status() == StatusCode::SEE_OTHER {
            ready_etag = response
                .headers()
                .get(header::ETAG)
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()?;
            let location = response
                .headers()
                .get(header::LOCATION)
                .context("ready export omitted content Location")?
                .to_str()?
                .to_owned();
            content_url = Some(if location.starts_with("http") {
                location
            } else {
                format!("{base_url}{location}")
            });
            ready_operation = Some(response);
            break;
        }
        ensure!(response.status() == StatusCode::OK);
        let body: Value = response.json().await?;
        last_export_body = body.clone();
        ensure!(body["state"] != "failed", "export failed: {body}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ready_response = ready_operation
        .with_context(|| format!("export did not become ready; last status: {last_export_body}"))?;
    let ready_etag = ready_etag.context("ready export omitted ETag")?;
    let content_url = content_url.context("ready export omitted content URL")?;
    let not_modified = client
        .get(&export_status_url)
        .bearer_auth(bearer_token)
        .header(header::IF_NONE_MATCH, &ready_etag)
        .send()
        .await?;
    ensure!(not_modified.status() == StatusCode::NOT_MODIFIED);
    ensure!(not_modified.headers().get(header::CACHE_CONTROL).is_some());

    let content_response = client
        .get(&content_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(content_response.status() == StatusCode::OK);
    ensure!(content_response.headers().get(header::ETAG).is_some());
    let content = content_response.bytes().await?;
    ensure!(
        String::from_utf8_lossy(&content).contains("export-delete-private-marker"),
        "export package omitted the authorized marker"
    );

    let hidden_export = client
        .get(format!(
            "{base_url}/v1/tenants/{}/subjects/{}/exports/{export_id}",
            scope.principal_b_tenant_id, scope.principal_b_subject_id
        ))
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(hidden_export.status() == StatusCode::NOT_FOUND);

    let deletion_url = format!("{secondary_prefix}/deletions");
    let deletion_response = client
        .post(&deletion_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-deletion")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(
        deletion_response.status() == StatusCode::ACCEPTED,
        "deletion creation returned {}",
        deletion_response.status()
    );
    ensure!(
        deletion_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let export_after_fence = client
        .post(&export_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-after-deletion-fence")
        .send()
        .await?;
    ensure!(
        export_after_fence.status() == StatusCode::NOT_FOUND,
        "export creation after deletion fence disclosed a new operation: {}",
        export_after_fence.status()
    );
    let deletion_status_url = deletion_response
        .headers()
        .get(header::LOCATION)
        .context("deletion creation omitted Location")?
        .to_str()?
        .to_owned();
    let deletion_body: Value = deletion_response.json().await?;
    let deletion_id = deletion_body["operation_id"]
        .as_str()
        .context("deletion response omitted operation_id")?
        .to_owned();
    let deletion_status_url = if deletion_status_url.starts_with("http") {
        deletion_status_url
    } else {
        format!("{base_url}{deletion_status_url}")
    };
    let deletion_replay = client
        .post(&deletion_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", "export-delete-deletion")
        .json(&json!({}))
        .send()
        .await?;
    ensure!(deletion_replay.status() == StatusCode::ACCEPTED);
    ensure!(
        deletion_replay.headers().get("idempotency-replayed")
            == Some(&header::HeaderValue::from_static("true"))
    );
    let deletion_replay_body: Value = deletion_replay.json().await?;
    ensure!(deletion_replay_body["operation_id"] == deletion_id);

    let mut completed_etag = None;
    let mut last_deletion_body = Value::Null;
    for _ in 0..200 {
        let response = client
            .get(&deletion_status_url)
            .bearer_auth(bearer_token)
            .send()
            .await?;
        ensure!(response.status() == StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .context("deletion status omitted ETag")?
            .to_str()?
            .to_owned();
        let body: Value = response.json().await?;
        last_deletion_body = body.clone();
        if body["lifecycle_state"] == "completed" {
            let outcome = body["outcome"]
                .as_object()
                .context("completed deletion omitted terminal outcome")?;
            ensure!(outcome["live_disposition"] == "purged_and_verified");
            ensure!(outcome["backup_disposition"] == "not_configured");
            ensure!(outcome["backup_policy_id"].is_null());
            ensure!(outcome["deletion_watermark"].is_null());
            ensure!(outcome["restore_gate_version"].is_null());
            ensure!(
                body["targets"]
                    .as_array()
                    .context("completed deletion omitted target ledger")?
                    .iter()
                    .filter(|target| target["capability"] == "configured")
                    .all(|target| target["verification"] == "verified")
            );
            ensure!(
                outcome["verification_digest"]
                    .as_str()
                    .is_some_and(|digest| digest.len() == 64)
            );
            completed_etag = Some(etag);
            break;
        }
        ensure!(
            body["lifecycle_state"] != "failed",
            "deletion failed: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let completed_etag = completed_etag
        .with_context(|| format!("deletion did not complete: {last_deletion_body}"))?;
    let deletion_not_modified = client
        .get(&deletion_status_url)
        .bearer_auth(bearer_token)
        .header(header::IF_NONE_MATCH, completed_etag)
        .send()
        .await?;
    ensure!(deletion_not_modified.status() == StatusCode::NOT_MODIFIED);
    ensure!(
        deletion_not_modified
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some()
    );

    sqlx::query(
        "UPDATE memory.deletion_tombstones
         SET expires_at = clock_timestamp() - interval '1 second'
         WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(target.tenant_id)
    .bind(Uuid::parse_str(&deletion_id)?)
    .execute(migration_pool)
    .await?;
    let expired_status = client
        .get(&deletion_status_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(expired_status.status() == StatusCode::OK);
    let expired_body: Value = expired_status.json().await?;
    ensure!(expired_body["lifecycle_state"] == "expired");
    ensure!(expired_body["outcome"]["live_disposition"] == "purged_and_verified");

    let deleted_episode = client
        .get(&episode_location)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(deleted_episode.status() == StatusCode::NOT_FOUND);
    let revoked_export = client
        .get(&content_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(
        matches!(
            revoked_export.status(),
            StatusCode::NOT_FOUND | StatusCode::GONE
        ),
        "revoked export remained readable: {}",
        revoked_export.status()
    );
    let deleted_export_status = client
        .get(&export_status_url)
        .bearer_auth(bearer_token)
        .send()
        .await?;
    ensure!(deleted_export_status.status() == StatusCode::NOT_FOUND);
    Ok(())
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

async fn install_temporal_metadata_fixture(pool: &PgPool) -> Result<()> {
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

async fn install_temporal_retrieval_policy(pool: &PgPool) -> Result<()> {
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
        exercise_concurrent_projection_claim(pool, migration_pool, target, &fixture).await?;
        exercise_projection_lease_expiry(migration_pool, target, &fixture, &coordinator).await?;

        apply_corpus_lifecycle(pool, target, &prepared_corpus).await?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
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

async fn apply_corpus_lifecycle(
    pool: &PgPool,
    target: &Target,
    prepared: &PreparedCorpus,
) -> Result<()> {
    for mutation in &prepared.lifecycle {
        ensure!(
            mutation.tenant_id == target.tenant_id && mutation.subject_id == target.subject_id,
            "corpus lifecycle mutation escaped the primary test scope"
        );
        match mutation.lifecycle {
            LifecycleFixture::Deleted => {
                let mut transaction = pool.begin().await?;
                set_retrieval_test_scope(&mut transaction, target).await?;
                transition_revision_to_deleted(&mut transaction, target, mutation.revision_id)
                    .await?;
                transaction.commit().await?;
            }
            LifecycleFixture::Expired => {}
            LifecycleFixture::Active => bail!("active corpus fact requested a lifecycle mutation"),
        }
    }
    Ok(())
}

async fn verify_corpus_error_surface_redaction(
    target: &Target,
    corpus: &palimpsest_conformance::retrieval_evaluation::Corpus,
    prepared: &PreparedCorpus,
    provider: &DeterministicEmbeddingProvider,
) -> Result<()> {
    let scenario = corpus
        .scenarios
        .iter()
        .find(|scenario| !scenario.forbidden_ids.is_empty())
        .context("corpus has no forbidden-ID error probe")?;
    provider.set_mode(EmbeddingFixtureMode::Unavailable);
    let response = Client::new()
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "corpus-forbidden-error-redaction")
        .json(&json!({
            "query": scenario.query,
            "perspective": {"kind": "current"},
            "page_size": 10,
            "policy_id": "retrieval-hybrid-temporal-v1",
            "filters": {"case_ids": [scenario.case_id]}
        }))
        .send()
        .await;
    provider.set_mode(EmbeddingFixtureMode::Valid);
    let response = response?;
    ensure!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    let raw = response.text().await?;
    for logical_id in corpus
        .scenarios
        .iter()
        .flat_map(|scenario| &scenario.forbidden_ids)
    {
        let revision_id = prepared
            .revisions
            .get(logical_id)
            .context("missing forbidden revision for error probe")?;
        ensure!(!raw.contains(logical_id));
        ensure!(!raw.contains(&revision_id.to_string()));
    }
    Ok(())
}

async fn verify_corpus_manifests_exclude_forbidden(
    pool: &PgPool,
    corpus: &palimpsest_conformance::retrieval_evaluation::Corpus,
    prepared: &PreparedCorpus,
) -> Result<()> {
    let forbidden = corpus
        .scenarios
        .iter()
        .flat_map(|scenario| scenario.forbidden_ids.iter())
        .map(|logical| {
            prepared
                .revisions
                .get(logical)
                .copied()
                .with_context(|| format!("missing forbidden corpus revision {logical}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let leaked: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM memory.retrieval_manifest_items
        WHERE revision_id = ANY($1::uuid[])
        "#,
    )
    .bind(&forbidden)
    .fetch_one(pool)
    .await?;
    ensure!(
        leaked == 0,
        "forbidden corpus revisions entered durable manifests"
    );
    Ok(())
}

async fn rebuild_corpus_projections(
    pool: &PgPool,
    coordinator: &EmbeddingProjectionCoordinator,
    target: &Target,
    prepared: &PreparedCorpus,
) -> Result<()> {
    let mut scopes = BTreeMap::<(Uuid, Uuid), Vec<Uuid>>::new();
    for projection in &prepared.projections {
        scopes
            .entry((projection.tenant_id, projection.subject_id))
            .or_default()
            .push(projection.revision_id);
    }
    for ((tenant_id, subject_id), revision_ids) in scopes {
        let scoped_target = Target {
            tenant_id,
            subject_id,
            ..target.clone()
        };
        let mut transaction = pool.begin().await?;
        set_retrieval_test_scope(&mut transaction, &scoped_target).await?;
        sqlx::query(
            r#"
            DELETE FROM memory.fact_revision_embedding_projections
            WHERE revision_id = ANY($1::uuid[])
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM memory.fact_revision_search_documents
            WHERE revision_id = ANY($1::uuid[])
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        let rebuilt_search = sqlx::query(
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
            WHERE revision.revision_id = ANY($1::uuid[])
              AND projection.projection_schema_version = 1
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        ensure!(
            rebuilt_search.rows_affected() == revision_ids.len() as u64,
            "corpus search-projection rebuild was incomplete"
        );
        transaction.commit().await?;
        let rebuilt = coordinator
            .rebuild_pending(TenantId(tenant_id), SubjectId(subject_id), 1_000)
            .await?;
        ensure!(rebuilt.failed == 0, "corpus projection rebuild failed");
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct TemporalReceiptDigests {
    manifest_sha256: String,
    ordered_item_sha256: Vec<(Uuid, String)>,
}

async fn temporal_receipt_digests(
    pool: &PgPool,
    target: &Target,
    retrieval_id: Uuid,
) -> Result<TemporalReceiptDigests> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    let manifest_sha256: String = sqlx::query(
        r#"
        SELECT manifest_sha256
        FROM memory.retrieval_receipts
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
          AND policy_id = 'retrieval-hybrid-temporal-v1'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .fetch_one(&mut *transaction)
    .await?
    .try_get("manifest_sha256")?;
    let ordered_item_sha256 = sqlx::query(
        r#"
        SELECT revision_id, item_sha256
        FROM memory.retrieval_manifest_items
        WHERE tenant_id = $1 AND subject_id = $2 AND retrieval_id = $3
        ORDER BY ordinal
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(retrieval_id)
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get::<Uuid, _>("revision_id")?,
            row.try_get::<String, _>("item_sha256")?,
        ))
    })
    .collect::<Result<Vec<_>>>()?;
    ensure!(manifest_sha256.len() == 64);
    ensure!(ordered_item_sha256.len() == 4);
    ensure!(
        ordered_item_sha256
            .iter()
            .all(|(_, item_sha256)| item_sha256.len() == 64)
    );
    transaction.commit().await?;
    Ok(TemporalReceiptDigests {
        manifest_sha256,
        ordered_item_sha256,
    })
}

async fn verify_temporal_persistence_rejects_tampering(
    migration_pool: &PgPool,
    target: &Target,
    retrieval_id: Uuid,
) -> Result<()> {
    for (name, profile_id, field, malformed_value) in [
        (
            "active half-life",
            "active-case-30d-v1",
            "half_life_us",
            json!(2592000000001_u64),
        ),
        (
            "active floor",
            "active-case-30d-v1",
            "floor_q63_units",
            json!(1152921504606846977_u64),
        ),
        (
            "active Q63 scale",
            "active-case-30d-v1",
            "q63_scale_units",
            json!(9223372036854775809_u64),
        ),
        (
            "active algorithm",
            "active-case-30d-v1",
            "q63_algorithm",
            json!("tampered-exp2"),
        ),
        (
            "stable factor",
            "stable-v1",
            "factor_units",
            json!(999999999999_u64),
        ),
    ] {
        let mut transaction = migration_pool.begin().await?;
        let malformed_profile = sqlx::query(
            r#"
            WITH source AS (
                SELECT profile.*,
                    jsonb_set(
                        profile_document,
                        ARRAY[$2]::text[],
                        $3::jsonb,
                        false
                    ) AS malformed_document
                FROM memory.recency_profiles AS profile
                WHERE profile_id = $1 AND profile_version = '1'
            )
            INSERT INTO memory.recency_profiles (
                profile_id, profile_version, profile_document,
                profile_sha256, schema_version
            )
            SELECT profile_id, profile_version, malformed_document,
                encode(sha256(convert_to(malformed_document::text, 'UTF8')), 'hex'),
                schema_version
            FROM source
            "#,
        )
        .bind(profile_id)
        .bind(field)
        .bind(malformed_value)
        .execute(&mut *transaction)
        .await;
        transaction.rollback().await?;
        ensure!(
            malformed_profile
                .as_ref()
                .err()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.constraint())
                == Some("recency_profile_registration_consistent"),
            "a correctly rehashed recency profile with tampered {name} reached another constraint"
        );
    }

    let factors = sqlx::query(
        r#"
        SELECT
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 1
            )::text AS one_microsecond,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 1296000000000
            )::text AS fifteen_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 2592000000000
            )::text AS thirty_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 7775999999999
            )::text AS just_before_ninety_days,
            memory.temporal_recency_factor_units_v1(
                'active-case-30d-v1', '1', 7776000000000
            )::text AS ninety_days
        "#,
    )
    .fetch_one(migration_pool)
    .await?;
    for (column, age_us, exact_units) in [
        ("one_microsecond", 1_i128, "1000000000000"),
        ("fifteen_days", 1_296_000_000_000_i128, "707106781187"),
        ("thirty_days", 2_592_000_000_000_i128, "500000000000"),
        (
            "just_before_ninety_days",
            7_775_999_999_999_i128,
            "125000000000",
        ),
        ("ninety_days", 7_776_000_000_000_i128, "125000000000"),
    ] {
        let sql_units = factors.try_get::<String, _>(column)?;
        let rust_units = temporal_factor_q63(RecencyProfile::ActiveCase30dV1, age_us, 0)
            .and_then(|factor| factor.to_score_units())
            .map_err(|error| anyhow::anyhow!("Rust recency vector {column} failed: {error:?}"))?
            .raw_units()
            .to_string();
        ensure!(
            sql_units == exact_units,
            "SQL recency vector {column} drifted"
        );
        ensure!(
            sql_units == rust_units,
            "SQL and Rust recency vectors disagree at {column}"
        );
    }

    let mut policy_transaction = migration_pool.begin().await?;
    let malformed_policy = sqlx::query(
        r#"
        WITH source AS (
            SELECT policy.*,
                jsonb_set(
                    policy_document,
                    '{arithmetic,operation_order}',
                    '["exact-identity-bonus","importance-half-even"]'::jsonb
                ) AS malformed_document
            FROM memory.retrieval_policies AS policy
            WHERE policy_id = 'retrieval-hybrid-temporal-v1'
              AND policy_version = '1'
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
        SELECT policy_id, '2', malformed_document,
            encode(sha256(convert_to(malformed_document::text, 'UTF8')), 'hex'),
            schema_version, retrieval_mode,
            embedding_profile_id, embedding_profile_version,
            embedding_profile_sha256,
            embedding_projection_profile_id,
            embedding_projection_profile_version,
            embedding_projection_profile_sha256,
            scoring_mode
        FROM source
        "#,
    )
    .execute(&mut *policy_transaction)
    .await;
    policy_transaction.rollback().await?;
    ensure!(
        malformed_policy
            .as_ref()
            .err()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|error| error.constraint())
            == Some("retrieval_policy_registration_consistent"),
        "a correctly rehashed policy with a malformed operation order was registered"
    );

    for (name, patch) in [
        (
            "partial lineage",
            json!({
                "ordinal": 98,
                "final_rank": 98,
                "cursor_token": Uuid::now_v7(),
                "confidence_factor": null,
                "item_sha256": "1".repeat(64)
            }),
        ),
        (
            "plausible wrong recency factor",
            json!({
                "ordinal": 99,
                "final_rank": 99,
                "cursor_token": Uuid::now_v7(),
                "recency_factor": "0.500000000001",
                "item_sha256": "2".repeat(64)
            }),
        ),
    ] {
        let mut transaction = migration_pool.begin().await?;
        set_retrieval_test_scope(&mut transaction, target).await?;
        let insert = sqlx::query(
            r#"
            INSERT INTO memory.retrieval_manifest_items
            SELECT (jsonb_populate_record(item, $4::jsonb)).*
            FROM memory.retrieval_manifest_items AS item
            WHERE item.tenant_id = $1
              AND item.subject_id = $2
              AND item.retrieval_id = $3
              AND item.recency_profile_id = 'active-case-30d-v1'
              AND item.recency_age_us = 2592000000000
            LIMIT 1
            "#,
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .bind(retrieval_id)
        .bind(patch)
        .execute(&mut *transaction)
        .await;
        transaction.rollback().await?;
        ensure!(insert.is_err(), "temporal manifest accepted {name}");
    }
    Ok(())
}

async fn rebuild_temporal_fixture_projections(
    pool: &PgPool,
    target: &Target,
    fixture: &TemporalRetrievalFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    let revision_ids = [
        fixture.exact_revision_id,
        fixture.alpha_root_revision_id,
        fixture.alpha_successor_revision_id,
        fixture.beta_revision_id,
        fixture.gamma_revision_id,
        fixture.delta_revision_id,
    ];
    for revision_id in revision_ids {
        delete_retrieval_projection(pool, target, revision_id).await?;
    }
    for revision_id in revision_ids {
        rebuild_retrieval_projection(pool, target, revision_id).await?;
    }
    let report = coordinator
        .rebuild_pending(
            TenantId(target.tenant_id),
            SubjectId(target.subject_id),
            revision_ids.len(),
        )
        .await?;
    ensure!(report.attempted == revision_ids.len());
    ensure!(report.ready == revision_ids.len() && report.failed == 0);
    verify_embedding_projection_rows_for_revisions(pool, target, &revision_ids).await
}

async fn exercise_concurrent_projection_claim(
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
            tokio::time::sleep(Duration::from_millis(10)).await;
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
    tokio::time::sleep(Duration::from_secs(21)).await;
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

async fn exercise_projection_lease_expiry(
    migration_pool: &PgPool,
    target: &Target,
    fixture: &HybridFusionFixture,
    coordinator: &EmbeddingProjectionCoordinator,
) -> Result<()> {
    let policy = sqlx::query(
        r#"
        SELECT lease_seconds, renewal_interval_seconds
        FROM memory.embedding_projection_lease_policies
        WHERE policy_id = 'embedding-projection-v1'
        "#,
    )
    .fetch_one(migration_pool)
    .await?;
    ensure!(policy.try_get::<i32, _>("lease_seconds")? == 60);
    ensure!(policy.try_get::<i32, _>("renewal_interval_seconds")? == 20);

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

struct NonbypassTemporalRuntime<'a> {
    migration_pool: &'a PgPool,
    database_url: &'a str,
    authenticator: Arc<StaticAuthenticator>,
    provider: Arc<DeterministicEmbeddingProvider>,
    provider_port: Arc<dyn EmbeddingProvider>,
    target: &'a Target,
    temporal_fixture: &'a TemporalRetrievalFixture,
    temporal_replay: &'a TemporalReplayFixture,
    isolation_fixture: &'a RetrievalIsolationFixture,
    lifecycle_fixture: &'a TemporalLifecycleFixture,
    lifecycle_replay: &'a TemporalLifecycleReplayFixture,
}

async fn verify_nonbypass_temporal_runtime(runtime: NonbypassTemporalRuntime<'_>) -> Result<()> {
    let NonbypassTemporalRuntime {
        migration_pool,
        database_url,
        authenticator,
        provider,
        provider_port,
        target,
        temporal_fixture,
        temporal_replay,
        isolation_fixture,
        lifecycle_fixture,
        lifecycle_replay,
    } = runtime;
    let login_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(database_url)?)
        .await?;
    let login_role = sqlx::query(
        "SELECT session_user::text AS role_name, quote_ident(session_user) AS quoted_role_name",
    )
    .fetch_one(&login_pool)
    .await?;
    let login_role_name: String = login_role.try_get("role_name")?;
    let quoted_login_role_name: String = login_role.try_get("quoted_role_name")?;
    login_pool.close().await;

    let role_name = format!("palimpsest_test_runtime_{}", Uuid::now_v7().simple());
    sqlx::query(AssertSqlSafe(format!(
        "CREATE ROLE \"{role_name}\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
    )))
    .execute(migration_pool)
    .await?;

    let verification = async {
        sqlx::query(AssertSqlSafe(format!(
            "GRANT \"{role_name}\" TO {quoted_login_role_name}"
        )))
        .execute(migration_pool)
        .await?;
        sqlx::raw_sql(AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA memory TO \"{role_name}\"; \
             GRANT SELECT ON \
                 memory.retrieval_policies, \
                 memory.recency_profiles, \
                 memory.embedding_profiles, \
                 memory.embedding_projection_profiles, \
                 memory.embedding_projection_lease_policies, \
                 memory.fact_retrieval_metadata_policies, \
                 memory.fact_retention_policies, \
                 memory.search_projection_schemas, \
                 memory.subject_lifecycles, \
                 memory.subject_content_leases, \
                 memory.facts, \
                 memory.fact_revisions, \
                 memory.fact_revision_evidence, \
                 memory.checkpoints, \
                 memory.checkpoint_revisions, \
                 memory.fact_revision_governance, \
                 memory.fact_revision_search_documents, \
                 memory.fact_revision_embedding_projections, \
                 memory.retrieval_idempotency_reservations, \
                 memory.retrieval_receipts, \
                 memory.retrieval_manifest_items, \
                 memory.retrieval_ready_fact_revision_embeddings, \
                 memory.authorized_retrieval_manifest, \
                 memory.episodes, \
                 memory.idempotency_receipts, \
                 memory.write_audit_receipts, \
                 memory.outbox_intents, \
                 memory.export_operations, \
                 memory.export_manifest_items \
             TO \"{role_name}\"; \
             GRANT INSERT ON \
                 memory.subject_lifecycles, \
                 memory.subject_content_leases, \
                 memory.retrieval_idempotency_reservations, \
                 memory.retrieval_receipts, \
                 memory.retrieval_manifest_items, \
                 memory.episodes, \
                 memory.idempotency_receipts, \
                 memory.write_audit_receipts, \
                 memory.outbox_intents \
             TO \"{role_name}\"; \
             GRANT UPDATE ON memory.idempotency_receipts TO \"{role_name}\"; \
             GRANT INSERT, UPDATE, DELETE ON \
                 memory.export_operations, memory.export_manifest_items \
             TO \"{role_name}\"; \
             GRANT DELETE ON memory.subject_content_leases TO \"{role_name}\"; \
             GRANT EXECUTE ON FUNCTION \
                 memory.round_half_even_integer_v1(numeric, numeric), \
                 memory.temporal_recency_factor_units_v1(text, text, numeric), \
                 memory.acquire_subject_content_lease(uuid, uuid, uuid, text), \
                 memory.release_subject_content_lease(uuid, uuid, uuid, text), \
                 memory.claim_next_export_operation(uuid, integer), \
                 memory.claim_next_expired_export_operation(uuid, integer), \
                 memory.deletion_workflow_allows(uuid, uuid), \
                 memory.create_deletion_operation(uuid, uuid, uuid, text, text, character, text[], integer), \
                 memory.poll_deletion_operation(uuid, uuid, uuid), \
                 memory.claim_next_deletion_operation(uuid, integer), \
                 memory.renew_deletion_operation_lease(uuid, uuid, uuid, uuid, integer), \
                 memory.claim_next_deletion_target(uuid, uuid, uuid, uuid, uuid, integer), \
                 memory.renew_deletion_target_lease(uuid, uuid, uuid, uuid, character, uuid, integer), \
                 memory.fail_deletion_target(uuid, uuid, uuid, uuid, text, character, uuid, text, integer), \
                 memory.purge_deletion_target(uuid, uuid, text), \
                 memory.complete_deletion_target(uuid, uuid, uuid, uuid, text, character, uuid, character), \
                 memory.advance_deletion_operation(uuid, uuid, uuid, uuid, integer) \
             TO \"{role_name}\""
        )))
        .execute(migration_pool)
        .await?;

        let role_statement = format!("SET ROLE \"{role_name}\"");
        let runtime_pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _metadata| {
                let role_statement = role_statement.clone();
                Box::pin(async move {
                    sqlx::query(AssertSqlSafe(role_statement))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(PgConnectOptions::from_str(database_url)?)
            .await?;
        let role = sqlx::query(
            "SELECT current_user AS role_name, session_user AS login_role_name, \
                    rolsuper, rolbypassrls \
             FROM pg_roles WHERE rolname = current_user",
        )
        .fetch_one(&runtime_pool)
        .await?;
        ensure!(role.try_get::<String, _>("role_name")? == role_name);
        ensure!(role.try_get::<String, _>("login_role_name")? == login_role_name);
        ensure!(!role.try_get::<bool, _>("rolsuper")?);
        ensure!(!role.try_get::<bool, _>("rolbypassrls")?);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app_with_embedding_provider(
                    runtime_pool.clone(),
                    runtime_pool.clone(),
                    authenticator,
                    provider_port,
                ),
            )
            .await
        });
        let runtime_target = Target {
            base_url: format!("http://{address}"),
            ..target.clone()
        };
        let mut nonbypass_export_target = runtime_target.clone();
        nonbypass_export_target.principal_a_secondary_subject_id =
            Uuid::parse_str("019be000-0000-7000-8000-000000000022")?;
        let runtime_scenario = async {
            let runtime_replay = creates_temporal_receipt_through_nonbypass_runtime(
                &runtime_target,
                temporal_fixture,
                temporal_replay,
                isolation_fixture,
            )
            .await?;
            temporal_policy_does_not_resurrect_ineligible_successors(
                &runtime_target,
                lifecycle_fixture,
                lifecycle_replay,
            )
            .await?;

            provider.set_mode(EmbeddingFixtureMode::Unavailable);
            let calls_before_replay = provider.calls();
            let replay_result = replays_temporal_receipt_through_nonbypass_runtime(
                &runtime_target,
                &runtime_replay,
            )
            .await;
            let calls_after_replay = provider.calls();
            provider.set_mode(EmbeddingFixtureMode::Valid);
            replay_result?;
            ensure!(
                calls_after_replay == calls_before_replay,
                "non-bypass durable replay called the unavailable embedding provider"
            );
            exercise_export_and_deletion_http(
                &runtime_target,
                &nonbypass_export_target,
                "principal-a-export-delete-test-token",
                migration_pool,
            )
            .await?;
            let residual: i64 = sqlx::query_scalar(
                r#"
                SELECT
                    (SELECT count(*) FROM memory.episodes
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.facts
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.fact_revisions
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.checkpoints
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.export_operations
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.export_manifest_items
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.deletion_tombstone_seeds
                     WHERE tenant_id = $1 AND subject_id = $2)
                  + (SELECT count(*) FROM memory.deletion_audit_seeds
                     WHERE tenant_id = $1 AND subject_id = $2)
                "#,
            )
            .bind(target.tenant_id)
            .bind(nonbypass_export_target.principal_a_secondary_subject_id)
            .fetch_one(migration_pool)
            .await?;
            ensure!(residual == 0, "non-bypass deletion left residual rows: {residual}");
            Ok::<(), anyhow::Error>(())
        }
        .await;
        server.abort();
        let _ = server.await;
        runtime_scenario
    }
    .await;

    let cleanup = async {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP OWNED BY \"{role_name}\"; \
             REVOKE \"{role_name}\" FROM {quoted_login_role_name}; \
             DROP ROLE \"{role_name}\""
        )))
        .execute(migration_pool)
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    verification?;
    cleanup?;
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
    transition_revision_to_deleted(&mut transaction, target, fixture.deleted_revision_id).await?;
    transaction.commit().await?;
    Ok(())
}

async fn delete_temporal_lifecycle_successor(
    pool: &PgPool,
    target: &Target,
    fixture: &TemporalLifecycleFixture,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    set_retrieval_test_scope(&mut transaction, target).await?;
    transition_revision_to_deleted(
        &mut transaction,
        target,
        fixture.deleted_successor_revision_id,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn transition_revision_to_deleted(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &Target,
    revision_id: Uuid,
) -> Result<()> {
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
    .bind(revision_id)
    .execute(&mut **transaction)
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
    .bind(revision_id)
    .execute(&mut **transaction)
    .await?;
    ensure!(deleted.rows_affected() == 1);
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
            operation_grants: vec![],
        },
    )]));
    let listener = TcpListener::bind(&env::var("PALIMPSEST_TEST_CHILD_BIND")?).await?;
    let router = palimpsest_server::app(pool.clone(), pool, authenticator)
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
        .env("PALIMPSEST_ALLOWED_SENSITIVITIES", "internal,restricted")
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
