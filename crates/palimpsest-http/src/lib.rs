use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, Bytes, HttpBody},
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use http_body::{Frame, SizeHint};
use palimpsest_application::{
    CANONICAL_HISTORY_EXPORT_PROFILE, ContentLeasePermit, ExportOperationState,
    FactAsOfCoordinates, MemoryService, NewConsolidationInterpreterConfig, NewConsolidationJob,
    NewConsolidationPolicy, NewSurfacePolicy, NewSurfaceRequest,
    SURFACE_DEFAULT_MAX_CONTEXT_TOKENS, SURFACE_DEFAULT_MAX_ITEMS,
    SURFACE_DEFAULT_MAX_RESULT_TOKENS, ServiceError,
};
use palimpsest_domain::{
    AgentId, AppendEpisode, CaseId, CheckpointPrecondition, CheckpointRevisionId, CheckpointView,
    CreateFact, CreateRetrieval, DeletionOperationId, EffectTransition, Episode, EpisodeId,
    EpisodeKind, ExportId, FactId, FactKey, FactNamespace, FactView, OperationGrant,
    PrincipalScope, Provenance, RetentionPolicyId, RetrievalFilters, RetrievalId,
    RetrievalPerspective, RetrievalPolicyId, RetrievalQuery, RetrievalReceipt, RevisionId,
    SaveCheckpoint, Sensitivity, SubjectId, SupersedeFact, TenantId, ThreadId, ValidTime,
    WritePolicy, WritePolicyId, WritePolicyVersion, parse_utc_microsecond_timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;
use uuid::Uuid;

static CONTENT_LEASE_RELEASE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static CONTENT_LEASE_RELEASE_RUNTIME_UNAVAILABLE_TOTAL: AtomicU64 = AtomicU64::new(0);
static CONTENT_LEASE_RELEASE_OUTSTANDING: AtomicU64 = AtomicU64::new(0);
static CONTENT_LEASE_RELEASE_DEFERRED_TO_EXPIRY_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Fixed cumulative latency buckets (milliseconds), Prometheus `le`-style.
/// `record_request_latency` increments every bucket whose upper bound the
/// request duration does not exceed, so the last bucket is requests_total.
pub const REQUEST_LATENCY_BUCKET_MS: &[u64] = &[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];
static REQUEST_LATENCY_LE_TOTAL: [AtomicU64; 10] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static REQUEST_LATENCY_SUM_MICROS: AtomicU64 = AtomicU64::new(0);
static PROJECTION_LEASE_SECONDS: AtomicU64 = AtomicU64::new(0);
static PROJECTION_RENEWAL_INTERVAL_SECONDS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContentLeaseCleanupCounters {
    pub release_retries: u64,
    pub runtime_unavailable: u64,
    pub outstanding: u64,
    pub deferred_to_expiry: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServerMetricsSnapshot {
    pub latency_bucket_totals: [u64; 10],
    pub latency_sum_micros: u64,
    pub projection_lease_seconds: u64,
    pub projection_renewal_interval_seconds: u64,
}

pub fn content_lease_cleanup_counters() -> ContentLeaseCleanupCounters {
    ContentLeaseCleanupCounters {
        release_retries: CONTENT_LEASE_RELEASE_RETRY_TOTAL.load(Ordering::Relaxed),
        runtime_unavailable: CONTENT_LEASE_RELEASE_RUNTIME_UNAVAILABLE_TOTAL
            .load(Ordering::Relaxed),
        outstanding: CONTENT_LEASE_RELEASE_OUTSTANDING.load(Ordering::Relaxed),
        deferred_to_expiry: CONTENT_LEASE_RELEASE_DEFERRED_TO_EXPIRY_TOTAL.load(Ordering::Relaxed),
    }
}

/// Records one completed request's duration into the cumulative latency
/// histogram. Content-free by construction: only a duration is stored.
pub fn record_request_latency(duration: Duration) {
    let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    REQUEST_LATENCY_SUM_MICROS.fetch_add(micros, Ordering::Relaxed);
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    for (index, bound) in REQUEST_LATENCY_BUCKET_MS.iter().enumerate() {
        if millis <= *bound {
            REQUEST_LATENCY_LE_TOTAL[index].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Records the deployed embedding-projection lease policy values so
/// `/metrics` can expose them without database access (spec 010 R3).
pub fn record_projection_lease_policy(lease_seconds: u64, renewal_interval_seconds: u64) {
    PROJECTION_LEASE_SECONDS.store(lease_seconds, Ordering::Relaxed);
    PROJECTION_RENEWAL_INTERVAL_SECONDS.store(renewal_interval_seconds, Ordering::Relaxed);
}

pub fn server_metrics_snapshot() -> ServerMetricsSnapshot {
    let mut buckets = [0u64; 10];
    for (index, cell) in REQUEST_LATENCY_LE_TOTAL.iter().enumerate() {
        buckets[index] = cell.load(Ordering::Relaxed);
    }
    ServerMetricsSnapshot {
        latency_bucket_totals: buckets,
        latency_sum_micros: REQUEST_LATENCY_SUM_MICROS.load(Ordering::Relaxed),
        projection_lease_seconds: PROJECTION_LEASE_SECONDS.load(Ordering::Relaxed),
        projection_renewal_interval_seconds: PROJECTION_RENEWAL_INTERVAL_SECONDS
            .load(Ordering::Relaxed),
    }
}

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, bearer_token: &str) -> Option<PrincipalScope>;

    /// Returns a current worker authorization for a persisted principal
    /// identity. Implementations backed by a real policy service should
    /// resolve this from that service; the default fails closed.
    fn authorize_export_worker(
        &self,
        _principal_id: &palimpsest_domain::PrincipalId,
        _tenant_id: TenantId,
        _subject_id: SubjectId,
        _authorization_scope_sha256: &str,
    ) -> Option<PrincipalScope> {
        None
    }
}

#[derive(Default)]
pub struct StaticAuthenticator {
    principals: HashMap<String, PrincipalScope>,
}

impl StaticAuthenticator {
    pub fn new(principals: impl IntoIterator<Item = (String, PrincipalScope)>) -> Self {
        Self {
            principals: principals.into_iter().collect(),
        }
    }
}

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, bearer_token: &str) -> Option<PrincipalScope> {
        self.principals.get(bearer_token).cloned()
    }

    fn authorize_export_worker(
        &self,
        principal_id: &palimpsest_domain::PrincipalId,
        tenant_id: TenantId,
        subject_id: SubjectId,
        authorization_scope_sha256: &str,
    ) -> Option<PrincipalScope> {
        self.principals.values().find_map(|principal| {
            (principal.principal_id == *principal_id
                && principal.authorizes(tenant_id, subject_id)
                && principal.authorizes_operation(OperationGrant::CanonicalHistoryExport))
            .then(|| {
                palimpsest_application::export_authorization_scope_sha256(
                    principal, tenant_id, subject_id,
                )
                .ok()
                .filter(|scope| scope == authorization_scope_sha256)
                .map(|_| principal.clone())
            })
            .flatten()
        })
    }
}

#[derive(Clone)]
struct AppState {
    service: MemoryService,
    authenticator: Arc<dyn Authenticator>,
    lease_cleanup: ContentLeaseCleanupQueue,
}

struct ContentLeaseGuard {
    cleanup: ContentLeaseCleanupQueue,
    permit: Option<ContentLeasePermit>,
}

#[derive(Clone)]
struct ContentLeaseCleanupQueue {
    sender: tokio::sync::mpsc::UnboundedSender<palimpsest_application::ContentLeaseRelease>,
}

impl ContentLeaseCleanupQueue {
    fn start(service: MemoryService) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                while let Some(release) = receiver.recv().await {
                    retry_content_lease_release(|| service.release_subject_content_lease(&release))
                        .await;
                    CONTENT_LEASE_RELEASE_OUTSTANDING.fetch_sub(1, Ordering::Relaxed);
                }
            });
        } else {
            CONTENT_LEASE_RELEASE_RUNTIME_UNAVAILABLE_TOTAL.fetch_add(1, Ordering::Relaxed);
            drop(receiver);
        }
        Self { sender }
    }

    fn enqueue(&self, release: palimpsest_application::ContentLeaseRelease) {
        CONTENT_LEASE_RELEASE_OUTSTANDING.fetch_add(1, Ordering::Relaxed);
        if self.sender.send(release).is_err() {
            CONTENT_LEASE_RELEASE_OUTSTANDING.fetch_sub(1, Ordering::Relaxed);
            CONTENT_LEASE_RELEASE_DEFERRED_TO_EXPIRY_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ContentLeaseGuard {
    fn attach(mut self, response: Response) -> Response {
        let (parts, body) = response.into_parts();
        let expires_at = self.permit().expires_at();
        let body = Body::new(ContentLeaseBody {
            inner: Box::pin(body),
            expires_at,
            deadline: Box::pin(tokio::time::sleep(duration_until(expires_at))),
            guard: Some(Self {
                cleanup: self.cleanup.clone(),
                permit: self.permit.take(),
            }),
        });
        Response::from_parts(parts, body)
    }

    fn release(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let release = permit.into_release();
        self.cleanup.enqueue(release);
    }

    fn permit(&self) -> &ContentLeasePermit {
        self.permit
            .as_ref()
            .expect("a content lease guard owns its permit until attached")
    }
}

impl Drop for ContentLeaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct ContentLeaseBody {
    inner: Pin<Box<Body>>,
    expires_at: OffsetDateTime,
    deadline: Pin<Box<tokio::time::Sleep>>,
    guard: Option<ContentLeaseGuard>,
}

impl HttpBody for ContentLeaseBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.deadline.as_mut().poll(context).is_ready()
            || OffsetDateTime::now_utc() >= self.expires_at
        {
            self.guard.take();
            return Poll::Ready(None);
        }
        let result = self.inner.as_mut().poll_frame(context);
        if matches!(result, Poll::Ready(None)) {
            self.guard.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        OffsetDateTime::now_utc() >= self.expires_at || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        if OffsetDateTime::now_utc() >= self.expires_at {
            SizeHint::with_exact(0)
        } else {
            self.inner.size_hint()
        }
    }
}

fn duration_until(expires_at: OffsetDateTime) -> Duration {
    (expires_at - OffsetDateTime::now_utc())
        .try_into()
        .unwrap_or(Duration::ZERO)
}

async fn retry_content_lease_release<F, Fut, Error>(mut release: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Error>>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        match release().await {
            Ok(()) => return,
            Err(_) => {
                CONTENT_LEASE_RELEASE_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

async fn acquire_content_lease(
    state: &AppState,
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<ContentLeaseGuard, Problem> {
    let permit = state
        .service
        .acquire_subject_content_lease(principal, tenant_id, subject_id)
        .await
        .map_err(Problem::from_service)?;
    Ok(ContentLeaseGuard {
        cleanup: state.lease_cleanup.clone(),
        permit: Some(permit),
    })
}

pub fn router(service: MemoryService, authenticator: Arc<dyn Authenticator>) -> Router {
    let lease_cleanup = ContentLeaseCleanupQueue::start(service.clone());
    let state = AppState {
        service,
        authenticator,
        lease_cleanup,
    };
    Router::new()
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/episodes",
            post(append_episode),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/episodes/{episode_id}",
            get(get_episode),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/facts",
            post(create_fact),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/facts/{fact_id}",
            get(get_current_fact).merge(put(supersede_fact)),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/facts/{fact_id}/as-of",
            get(get_fact_as_of),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/retrievals",
            post(create_retrieval).layer(DefaultBodyLimit::max(65_536)),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/retrievals/{retrieval_id}",
            get(get_retrieval),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/deletions",
            post(create_deletion),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/deletions/{operation_id}",
            get(get_deletion),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/exports",
            post(create_export),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}",
            get(get_export),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}/content",
            get(get_export_content),
        )
        .route(
            "/v1/tenants/{tenant_id}/consolidation-policies",
            post(create_consolidation_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/consolidation-interpreter-configs",
            post(register_consolidation_interpreter_config),
        )
        .route(
            "/v1/tenants/{tenant_id}/consolidation-policies/{source_kind}/{policy_id}",
            get(get_consolidation_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/consolidations",
            post(create_consolidation),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/consolidations/{job_id}",
            get(get_consolidation),
        )
        .route(
            "/v1/tenants/{tenant_id}/surface-policies",
            post(register_surface_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/surface-policies/{host_id}/{principal_id}",
            get(get_surface_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/surfaces",
            post(create_surface),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/surfaces/{surface_id}",
            get(get_surface),
        )
        .route(
            "/v1/tenants/{tenant_id}/subjects/{subject_id}/agents/{agent_id}/threads/{thread_id}/checkpoint",
            get(get_checkpoint).put(save_checkpoint),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendEpisodeRequest {
    case_id: Uuid,
    kind: EpisodeKind,
    observed_at: String,
    provenance: Provenance,
    sensitivity: Sensitivity,
    retention_policy_id: RetentionPolicyId,
    payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDeletionRequest {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterConsolidationPolicyRequest {
    source_kind: String,
    policy_id: String,
    interpreter_config_id: Uuid,
    write_policy_id: WritePolicyId,
    write_policy_version: WritePolicyVersion,
    retention_policy_id: RetentionPolicyId,
    confidence_auto_promote_min: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateConsolidationRequest {
    source_kind: String,
    policy_id: String,
    window_from: String,
    window_until: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterSurfacePolicyRequest {
    host_id: String,
    principal_id: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    max_items: Option<i16>,
    #[serde(default)]
    max_context_tokens: Option<i32>,
    #[serde(default)]
    max_result_tokens: Option<i32>,
    #[serde(default)]
    sensitivity_ceiling: Option<String>,
    #[serde(default)]
    window_from: Option<String>,
    #[serde(default)]
    window_until: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSurfaceRequest {
    host_id: String,
    principal_id: String,
    #[serde(default)]
    context_terms: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidTimeRequest {
    from: String,
    until: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WritePolicyRequest {
    id: WritePolicyId,
    version: WritePolicyVersion,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateFactRequest {
    case_id: Uuid,
    namespace: FactNamespace,
    key: FactKey,
    value: Value,
    observed_at: String,
    valid_time: ValidTimeRequest,
    evidence_episode_ids: Vec<Uuid>,
    write_policy: WritePolicyRequest,
    confidence: f64,
    sensitivity: Sensitivity,
    retention_policy_id: RetentionPolicyId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupersedeFactRequest {
    supersedes_revision_id: Uuid,
    value: Value,
    observed_at: String,
    valid_time: ValidTimeRequest,
    evidence_episode_ids: Vec<Uuid>,
    write_policy: WritePolicyRequest,
    confidence: f64,
    sensitivity: Sensitivity,
    retention_policy_id: RetentionPolicyId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AsOfQuery {
    valid_at: String,
    recorded_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RetrievalPerspectiveRequest {
    Current,
    AsOf {
        valid_at: String,
        recorded_at: String,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetrievalFiltersRequest {
    case_ids: Option<Vec<Uuid>>,
    namespaces: Option<Vec<FactNamespace>>,
    keys: Option<Vec<FactKey>>,
    sensitivities: Option<Vec<Sensitivity>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRetrievalRequest {
    query: String,
    perspective: RetrievalPerspectiveRequest,
    #[serde(default = "default_retrieval_page_size")]
    page_size: u16,
    policy_id: Option<RetrievalPolicyId>,
    #[serde(default)]
    filters: RetrievalFiltersRequest,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetrievalPageQuery {
    cursor: Option<String>,
}

const fn default_retrieval_page_size() -> u16 {
    10
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveCheckpointRequest {
    case_id: Uuid,
    parent_revision_id: NullableUuid,
    state: Value,
    state_schema_version: u32,
    effect_transitions: Vec<EffectTransition>,
    provenance: Provenance,
    sensitivity: Sensitivity,
    retention_policy_id: RetentionPolicyId,
}

#[derive(Debug, Deserialize)]
struct NullableUuid(Option<Uuid>);

async fn save_checkpoint(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, agent_id, thread_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    payload: Result<Json<SaveCheckpointRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let agent_id = AgentId(parse_uuid("agent_id", &agent_id)?);
    let thread_id = ThreadId(parse_uuid("thread_id", &thread_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let precondition = require_checkpoint_precondition(&headers)?;
    let Json(request) = payload.map_err(Problem::from_json_rejection)?;
    let outcome = state
        .service
        .save_checkpoint(
            content_lease.permit(),
            &principal,
            idempotency_key,
            precondition,
            SaveCheckpoint {
                tenant_id,
                subject_id,
                agent_id,
                thread_id,
                case_id: CaseId(request.case_id),
                parent_revision_id: request.parent_revision_id.0.map(CheckpointRevisionId),
                state: request.state,
                state_schema_version: request.state_schema_version,
                effect_transitions: request.effect_transitions,
                provenance: request.provenance,
                sensitivity: request.sensitivity,
                retention_policy_id: request.retention_policy_id,
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let status = match precondition {
        CheckpointPrecondition::Create => StatusCode::CREATED,
        CheckpointPrecondition::Match(_) => StatusCode::OK,
    };
    let location = format!(
        "/v1/tenants/{}/subjects/{}/agents/{}/threads/{}/checkpoint",
        tenant_id.0, subject_id.0, agent_id.0, thread_id.0
    );
    let response = checkpoint_response(status, outcome.view, Some(location), outcome.replayed)?;
    Ok(content_lease.attach(response))
}

async fn get_checkpoint(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, agent_id, thread_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let agent_id = AgentId(parse_uuid("agent_id", &agent_id)?);
    let thread_id = ThreadId(parse_uuid("thread_id", &thread_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let view = state
        .service
        .get_checkpoint(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            agent_id,
            thread_id,
        )
        .await
        .map_err(Problem::from_service)?;
    let response = checkpoint_response(StatusCode::OK, view, None, false)?;
    Ok(content_lease.attach(response))
}

async fn append_episode(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    payload: Result<Json<AppendEpisodeRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let Json(request) = payload.map_err(Problem::from_json_rejection)?;
    let observed_at = parse_time("observed_at", &request.observed_at)?;

    let episode = state
        .service
        .append_episode(
            content_lease.permit(),
            &principal,
            idempotency_key,
            AppendEpisode {
                tenant_id,
                subject_id,
                case_id: CaseId(request.case_id),
                kind: request.kind,
                observed_at,
                provenance: request.provenance,
                sensitivity: request.sensitivity,
                retention_policy_id: request.retention_policy_id,
                payload: request.payload,
            },
        )
        .await
        .map_err(Problem::from_service)?;

    let location = format!(
        "/v1/tenants/{}/subjects/{}/episodes/{}",
        tenant_id.0, subject_id.0, episode.episode.episode_id.0
    );
    let response = resource_response(
        StatusCode::CREATED,
        episode.episode,
        Some(location),
        episode.replayed,
    )?;
    Ok(content_lease.attach(response))
}

async fn get_episode(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, episode_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let episode_id = EpisodeId(parse_uuid("episode_id", &episode_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;

    let episode = state
        .service
        .get_episode(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            episode_id,
        )
        .await
        .map_err(Problem::from_service)?;
    let response = resource_response(StatusCode::OK, episode, None, false)?;
    Ok(content_lease.attach(response))
}

async fn create_fact(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    payload: Result<Json<CreateFactRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let Json(request) = payload.map_err(Problem::from_json_rejection)?;
    let observed_at = parse_time("observed_at", &request.observed_at)?;
    let valid_from = parse_time("valid_time.from", &request.valid_time.from)?;
    let valid_until = request
        .valid_time
        .until
        .as_deref()
        .map(|value| parse_time("valid_time.until", value))
        .transpose()?;

    let outcome = state
        .service
        .create_fact(
            content_lease.permit(),
            &principal,
            idempotency_key,
            CreateFact {
                tenant_id,
                subject_id,
                case_id: CaseId(request.case_id),
                namespace: request.namespace,
                key: request.key,
                value: request.value,
                observed_at,
                valid_time: ValidTime {
                    from: valid_from,
                    until: valid_until,
                },
                evidence_episode_ids: request
                    .evidence_episode_ids
                    .into_iter()
                    .map(EpisodeId)
                    .collect(),
                write_policy: WritePolicy {
                    id: request.write_policy.id,
                    version: request.write_policy.version,
                },
                confidence: request.confidence,
                sensitivity: request.sensitivity,
                retention_policy_id: request.retention_policy_id,
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/facts/{}",
        tenant_id.0, subject_id.0, outcome.view.fact_id.0
    );
    let response = fact_response(
        StatusCode::CREATED,
        outcome.view,
        Some(location),
        outcome.replayed,
    )?;
    Ok(content_lease.attach(response))
}

async fn get_current_fact(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, fact_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let fact_id = FactId(parse_uuid("fact_id", &fact_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let view = state
        .service
        .get_current_fact(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            fact_id,
        )
        .await
        .map_err(Problem::from_service)?;
    let response = fact_response(StatusCode::OK, view, None, false)?;
    Ok(content_lease.attach(response))
}

async fn supersede_fact(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, fact_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    payload: Result<Json<SupersedeFactRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let fact_id = FactId(parse_uuid("fact_id", &fact_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let expected_head_revision_id = require_if_match(&headers)?;
    let Json(request) = payload.map_err(Problem::from_json_rejection)?;
    let observed_at = parse_time("observed_at", &request.observed_at)?;
    let valid_from = parse_time("valid_time.from", &request.valid_time.from)?;
    let valid_until = request
        .valid_time
        .until
        .as_deref()
        .map(|value| parse_time("valid_time.until", value))
        .transpose()?;
    let outcome = state
        .service
        .supersede_fact(
            content_lease.permit(),
            &principal,
            idempotency_key,
            expected_head_revision_id,
            SupersedeFact {
                tenant_id,
                subject_id,
                fact_id,
                supersedes_revision_id: RevisionId(request.supersedes_revision_id),
                value: request.value,
                observed_at,
                valid_time: ValidTime {
                    from: valid_from,
                    until: valid_until,
                },
                evidence_episode_ids: request
                    .evidence_episode_ids
                    .into_iter()
                    .map(EpisodeId)
                    .collect(),
                write_policy: WritePolicy {
                    id: request.write_policy.id,
                    version: request.write_policy.version,
                },
                confidence: request.confidence,
                sensitivity: request.sensitivity,
                retention_policy_id: request.retention_policy_id,
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let response = fact_response(StatusCode::OK, outcome.view, None, outcome.replayed)?;
    Ok(content_lease.attach(response))
}

async fn get_fact_as_of(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, fact_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    query: Result<Query<AsOfQuery>, QueryRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let fact_id = FactId(parse_uuid("fact_id", &fact_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let Query(query) = query.map_err(|error| {
        Problem::bad_request(
            "invalid_query",
            "Temporal query is invalid",
            error.body_text(),
        )
    })?;
    let valid_at = parse_time("valid_at", &query.valid_at)?;
    let recorded_at = parse_time("recorded_at", &query.recorded_at)?;
    let view = state
        .service
        .get_fact_as_of(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            fact_id,
            FactAsOfCoordinates {
                valid_at,
                recorded_at,
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let response = fact_response(StatusCode::OK, view, None, false)?;
    Ok(content_lease.attach(response))
}

async fn create_retrieval(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    payload: Result<Json<CreateRetrievalRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let Json(request) = payload.map_err(Problem::from_retrieval_json_rejection)?;
    if request.query.len() > 4096 {
        return Err(Problem::new(
            "retrieval-request-too-large",
            "Retrieval request is too large",
            StatusCode::PAYLOAD_TOO_LARGE,
            "retrieval_request_too_large",
            "query must contain at most 4096 bytes.",
        ));
    }
    let query = RetrievalQuery::try_from(request.query)
        .map_err(|error| Problem::unprocessable("invalid_retrieval_query", error.to_string()))?;
    let perspective = match request.perspective {
        RetrievalPerspectiveRequest::Current => RetrievalPerspective::Current,
        RetrievalPerspectiveRequest::AsOf {
            valid_at,
            recorded_at,
        } => RetrievalPerspective::AsOf {
            valid_at: parse_time("perspective.valid_at", &valid_at)?,
            recorded_at: parse_time("perspective.recorded_at", &recorded_at)?,
        },
    };
    let outcome = state
        .service
        .create_retrieval(
            content_lease.permit(),
            &principal,
            idempotency_key,
            CreateRetrieval {
                tenant_id,
                subject_id,
                query,
                perspective,
                page_size: request.page_size,
                policy_id: request.policy_id,
                filters: RetrievalFilters {
                    case_ids: request
                        .filters
                        .case_ids
                        .map(|values| values.into_iter().map(CaseId).collect()),
                    namespaces: request.filters.namespaces,
                    keys: request.filters.keys,
                    sensitivities: request.filters.sensitivities,
                },
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/retrievals/{}",
        tenant_id.0, subject_id.0, outcome.receipt.retrieval_id.0
    );
    let response = retrieval_response(
        StatusCode::CREATED,
        outcome.receipt,
        Some(location),
        outcome.replayed,
    )?;
    Ok(content_lease.attach(response))
}

async fn get_retrieval(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, retrieval_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    query: Result<Query<RetrievalPageQuery>, QueryRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let retrieval_id = RetrievalId(parse_uuid("retrieval_id", &retrieval_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let Query(query) = query.map_err(|error| {
        Problem::bad_request(
            "invalid_query",
            "Retrieval page query is invalid",
            error.body_text(),
        )
    })?;
    let receipt = state
        .service
        .get_retrieval(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            retrieval_id,
            query.cursor,
        )
        .await
        .map_err(Problem::from_service)?;
    let response = retrieval_response(StatusCode::OK, receipt, None, false)?;
    Ok(content_lease.attach(response))
}

async fn create_deletion(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    payload: Option<Json<CreateDeletionRequest>>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    // Parse an optional empty object only to reject unknown request members;
    // target selection is a server-owned deletion policy.
    let _ = payload;
    let outcome = state
        .service
        .create_subject_deletion(&principal, tenant_id, subject_id, idempotency_key)
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/deletions/{}",
        tenant_id.0, subject_id.0, outcome.operation_id.0
    );
    let mut response = (
        StatusCode::ACCEPTED,
        [(header::CONTENT_TYPE, "application/json")],
        Json(json!({
            "operation_id": outcome.operation_id.0,
            "lifecycle_state": outcome.lifecycle_state,
            "state_version": outcome.state_version,
            "targets": serde_json::to_value(&outcome.targets)
                .map_err(|error| Problem::internal(error.to_string()))?,
        })),
    )
        .into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location)
            .map_err(|_| Problem::internal("The service could not construct Location."))?,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    if outcome.replayed {
        response.headers_mut().insert(
            header::HeaderName::from_static("idempotency-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

async fn get_deletion(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, operation_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let operation_id = DeletionOperationId(parse_uuid("operation_id", &operation_id)?);
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .poll_subject_deletion(&principal, tenant_id, subject_id, operation_id)
        .await
        .map_err(Problem::from_service)?;
    let etag = format!(
        "\"{}-{}\"",
        view.lifecycle_state.as_str(),
        view.state_version
    );
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
        let matches = if_none_match
            .to_str()
            .ok()
            .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == etag));
        if matches {
            let mut response = StatusCode::NOT_MODIFIED.into_response();
            response.headers_mut().insert(
                header::ETAG,
                HeaderValue::from_str(&etag)
                    .map_err(|_| Problem::internal("The service could not construct ETag."))?,
            );
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("private, no-store"),
            );
            return Ok(response);
        }
    }
    let mut response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(json!({
            "operation_id": view.operation_id.0,
            "lifecycle_state": view.lifecycle_state,
            "state_version": view.state_version,
            "retry_count": view.retry_count,
            "failure_reason": view.failure_reason,
            "targets": serde_json::to_value(&view.targets)
                .map_err(|error| Problem::internal(error.to_string()))?,
            "outcome": serde_json::to_value(&view.outcome)
                .map_err(|error| Problem::internal(error.to_string()))?,
            "updated_at": serde_json::to_value(view.updated_at)
                .map_err(|error| Problem::internal(error.to_string()))?,
        })),
    )
        .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag)
            .map_err(|_| Problem::internal("The service could not construct ETag."))?,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    Ok(response)
}

async fn create_export(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let profile = body
        .and_then(|Json(value)| {
            value
                .get("profile")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| CANONICAL_HISTORY_EXPORT_PROFILE.to_owned());
    let outcome = state
        .service
        .create_export(&principal, tenant_id, subject_id, idempotency_key, &profile)
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/exports/{}",
        tenant_id.0, subject_id.0, outcome.operation.export_id.0
    );
    export_status_response(
        StatusCode::ACCEPTED,
        outcome.operation,
        Some(location),
        outcome.replayed,
    )
}

async fn get_export(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, export_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let export_id = ExportId(parse_uuid("export_id", &export_id)?);
    let principal = authenticate(&state, &headers)?;
    let operation = state
        .service
        .get_export(&principal, tenant_id, subject_id, export_id)
        .await
        .map_err(Problem::from_service)?;
    let etag = export_etag(&operation);
    if if_none_match_matches(&headers, &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag)
                .map_err(|_| Problem::internal("The service could not construct ETag."))?,
        );
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        );
        return Ok(response);
    }
    if operation.state == ExportOperationState::Ready {
        let location = format!(
            "/v1/tenants/{}/subjects/{}/exports/{}/content",
            tenant_id.0, subject_id.0, export_id.0
        );
        let mut response = StatusCode::SEE_OTHER.into_response();
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&location)
                .map_err(|_| Problem::internal("The service could not construct Location."))?,
        );
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag)
                .map_err(|_| Problem::internal("The service could not construct ETag."))?,
        );
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        );
        return Ok(response);
    }
    export_status_response(StatusCode::OK, operation, None, false)
}

async fn get_export_content(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, export_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let export_id = ExportId(parse_uuid("export_id", &export_id)?);
    let principal = authenticate(&state, &headers)?;
    let content_lease = acquire_content_lease(&state, &principal, tenant_id, subject_id).await?;
    let (operation, bytes) = state
        .service
        .get_export_content(
            content_lease.permit(),
            &principal,
            tenant_id,
            subject_id,
            export_id,
        )
        .await
        .map_err(Problem::from_service)?;
    let content_sha256 = operation
        .content_sha256
        .ok_or_else(|| Problem::internal("The export package has no integrity digest."))?;
    let mut response = (StatusCode::OK, Body::from(bytes)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"palimpsest-export-{}.zip\"",
            export_id.0
        ))
        .map_err(|_| Problem::internal("The service could not construct Content-Disposition."))?,
    );
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{content_sha256}\""))
            .map_err(|_| Problem::internal("The service could not construct ETag."))?,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    Ok(content_lease.attach(response))
}

fn export_status_response(
    status: StatusCode,
    operation: palimpsest_application::ExportOperationView,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let mut response = (status, Json(operation.clone())).into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&export_etag(&operation))
            .map_err(|_| Problem::internal("The service could not construct ETag."))?,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    if let Some(location) = location {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&location)
                .map_err(|_| Problem::internal("The service could not construct Location."))?,
        );
    }
    if idempotency_replayed {
        response.headers_mut().insert(
            header::HeaderName::from_static("idempotency-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

fn export_etag(operation: &palimpsest_application::ExportOperationView) -> String {
    format!("\"{}-{}\"", operation.export_id.0, operation.status_version)
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == etag))
}

fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<PrincipalScope, Problem> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
        .ok_or_else(Problem::unauthorized)?;
    state
        .authenticator
        .authenticate(token)
        .ok_or_else(Problem::unauthorized)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterInterpreterConfigRequest {
    provider_kind: String,
    prompt_policy_version: String,
}

async fn register_consolidation_interpreter_config(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RegisterInterpreterConfigRequest>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .register_consolidation_interpreter_config(
            &principal,
            tenant_id,
            NewConsolidationInterpreterConfig {
                provider_kind: request.provider_kind,
                prompt_policy_version: request.prompt_policy_version,
                created_by_principal_id: principal.principal_id.clone(),
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/consolidation-interpreter-configs/{}",
        tenant_id.0, view.interpreter_config_id
    );
    let body = serde_json::to_value(&view).map_err(|error| {
        Problem::internal(format!("Interpreter config does not serialize: {error}"))
    })?;
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(body),
    )
        .into_response())
}

async fn create_consolidation_policy(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RegisterConsolidationPolicyRequest>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .register_consolidation_policy(
            &principal,
            tenant_id,
            NewConsolidationPolicy {
                source_kind: request.source_kind,
                policy_id: request.policy_id,
                interpreter_config_id: request.interpreter_config_id,
                write_policy_id: request.write_policy_id.as_str().to_owned(),
                write_policy_version: request.write_policy_version.as_str().to_owned(),
                retention_policy_id: request.retention_policy_id.as_str().to_owned(),
                confidence_auto_promote_min: request.confidence_auto_promote_min,
                created_by_principal_id: principal.principal_id.clone(),
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/consolidation-policies/{}/{}",
        tenant_id.0, view.source_kind, view.policy_id
    );
    let body = serde_json::to_value(&view)
        .map_err(|error| Problem::internal(format!("Policy view does not serialize: {error}")))?;
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(body),
    )
        .into_response())
}

async fn get_consolidation_policy(
    State(state): State<AppState>,
    Path((tenant_id, source_kind, policy_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .get_consolidation_policy(&principal, tenant_id, &source_kind, &policy_id)
        .await
        .map_err(Problem::from_service)?;
    let body = serde_json::to_value(&view)
        .map_err(|error| Problem::internal(format!("Policy view does not serialize: {error}")))?;
    Ok(Json(body).into_response())
}

async fn create_consolidation(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CreateConsolidationRequest>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let window_from = parse_datetime("window_from", &request.window_from)?;
    let window_until = parse_datetime("window_until", &request.window_until)?;
    let outcome = state
        .service
        .create_consolidation_job(
            &principal,
            tenant_id,
            subject_id,
            NewConsolidationJob {
                source_kind: request.source_kind,
                policy_id: request.policy_id,
                window_from,
                window_until,
                principal_id: principal.principal_id.clone(),
            },
            idempotency_key,
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/consolidations/{}",
        tenant_id.0, subject_id.0, outcome.job_id
    );
    let body = serde_json::to_value(&outcome).map_err(|error| {
        Problem::internal(format!("Consolidation outcome does not serialize: {error}"))
    })?;
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(body),
    )
        .into_response())
}

async fn get_consolidation(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, job_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let job_id = Uuid::parse_str(&job_id).map_err(|error| {
        Problem::bad_request("invalid_job_id", "Job id is invalid", error.to_string())
    })?;
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .poll_consolidation_job(&principal, tenant_id, subject_id, job_id)
        .await
        .map_err(Problem::from_service)?;
    let body = serde_json::to_value(&view).map_err(|error| {
        Problem::internal(format!("Consolidation view does not serialize: {error}"))
    })?;
    Ok(Json(body).into_response())
}

async fn register_surface_policy(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RegisterSurfacePolicyRequest>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let principal = authenticate(&state, &headers)?;
    let window_from = request
        .window_from
        .as_deref()
        .map(|value| parse_datetime("window_from", value))
        .transpose()?;
    let window_until = request
        .window_until
        .as_deref()
        .map(|value| parse_datetime("window_until", value))
        .transpose()?;
    let view = state
        .service
        .register_surface_policy(
            &principal,
            tenant_id,
            NewSurfacePolicy {
                host_id: request.host_id,
                principal_id: request.principal_id,
                enabled: request.enabled.unwrap_or(true),
                max_items: request.max_items.unwrap_or(SURFACE_DEFAULT_MAX_ITEMS),
                max_context_tokens: request
                    .max_context_tokens
                    .unwrap_or(SURFACE_DEFAULT_MAX_CONTEXT_TOKENS),
                max_result_tokens: request
                    .max_result_tokens
                    .unwrap_or(SURFACE_DEFAULT_MAX_RESULT_TOKENS),
                sensitivity_ceiling: request.sensitivity_ceiling,
                window_from,
                window_until,
                created_by_principal_id: principal.principal_id.clone(),
            },
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/surface-policies/{}/{}",
        tenant_id.0, view.host_id, view.principal_id
    );
    let body = serde_json::to_value(&view)
        .map_err(|error| Problem::internal(format!("Policy view does not serialize: {error}")))?;
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(body),
    )
        .into_response())
}

async fn get_surface_policy(
    State(state): State<AppState>,
    Path((tenant_id, host_id, principal_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let principal = authenticate(&state, &headers)?;
    let view = state
        .service
        .get_surface_policy(&principal, tenant_id, &host_id, &principal_id)
        .await
        .map_err(Problem::from_service)?;
    let body = serde_json::to_value(&view)
        .map_err(|error| Problem::internal(format!("Policy view does not serialize: {error}")))?;
    Ok(Json(body).into_response())
}

async fn create_surface(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CreateSurfaceRequest>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let outcome = state
        .service
        .create_surface(
            &principal,
            tenant_id,
            subject_id,
            NewSurfaceRequest {
                host_id: request.host_id,
                principal_id: request.principal_id,
                context_terms: request.context_terms,
            },
            idempotency_key,
        )
        .await
        .map_err(Problem::from_service)?;
    let location = format!(
        "/v1/tenants/{}/subjects/{}/surfaces/{}",
        tenant_id.0, subject_id.0, outcome.bundle.surface_id
    );
    let body = serde_json::to_value(&outcome.bundle).map_err(|error| {
        Problem::internal(format!("Surface bundle does not serialize: {error}"))
    })?;
    let mut response = (
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(body),
    )
        .into_response();
    if outcome.replayed {
        response.headers_mut().insert(
            header::HeaderName::from_static("idempotency-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

async fn get_surface(
    State(state): State<AppState>,
    Path((tenant_id, subject_id, surface_id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let surface_id = Uuid::parse_str(&surface_id).map_err(|error| {
        Problem::bad_request(
            "invalid_surface_id",
            "Surface id is invalid",
            error.to_string(),
        )
    })?;
    let principal = authenticate(&state, &headers)?;
    let bundle = state
        .service
        .get_surface(&principal, tenant_id, subject_id, surface_id)
        .await
        .map_err(Problem::from_service)?;
    let body = serde_json::to_value(&bundle).map_err(|error| {
        Problem::internal(format!("Surface bundle does not serialize: {error}"))
    })?;
    Ok(Json(body).into_response())
}

fn parse_datetime(field: &str, value: &str) -> Result<OffsetDateTime, Problem> {
    parse_utc_microsecond_timestamp(value).map_err(|error| {
        Problem::bad_request(
            "invalid_datetime",
            "Datetime is invalid",
            format!("{field}: {error}"),
        )
    })
}

fn require_idempotency_key(headers: &HeaderMap) -> Result<&str, Problem> {
    let value = headers
        .get("idempotency-key")
        .ok_or_else(Problem::idempotency_key_required)?;
    value
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 255)
        .ok_or_else(|| {
            Problem::bad_request(
                "invalid_idempotency_key",
                "Idempotency-Key is required",
                "Supply a non-empty Idempotency-Key header of at most 255 characters.",
            )
        })
}

fn require_if_match(headers: &HeaderMap) -> Result<RevisionId, Problem> {
    let value = headers
        .get(header::IF_MATCH)
        .ok_or_else(Problem::precondition_required)?
        .to_str()
        .map_err(|_| {
            Problem::bad_request(
                "invalid_if_match",
                "If-Match is invalid",
                "If-Match must contain one strong quoted fact ETag.",
            )
        })?;
    if value.starts_with("W/") || value == "*" || value.contains(',') {
        return Err(Problem::bad_request(
            "invalid_if_match",
            "If-Match is invalid",
            "If-Match must contain one strong quoted fact ETag.",
        ));
    }
    let unquoted = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'));
    let revision_id = unquoted
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| {
            Problem::bad_request(
                "invalid_if_match",
                "If-Match is invalid",
                "If-Match must contain one strong quoted fact ETag.",
            )
        })?;
    Ok(RevisionId(revision_id))
}

fn require_checkpoint_precondition(headers: &HeaderMap) -> Result<CheckpointPrecondition, Problem> {
    let if_match = headers.get(header::IF_MATCH);
    let if_none_match = headers.get(header::IF_NONE_MATCH);
    match (if_match, if_none_match) {
        (Some(_), Some(_)) => Err(Problem::bad_request(
            "invalid_checkpoint_precondition",
            "Checkpoint precondition is invalid",
            "Supply exactly one of If-None-Match: * or one strong If-Match ETag.",
        )),
        (None, Some(value)) if value == "*" => Ok(CheckpointPrecondition::Create),
        (None, Some(_)) => Err(Problem::bad_request(
            "invalid_checkpoint_precondition",
            "Checkpoint precondition is invalid",
            "Initial checkpoint creation requires If-None-Match: *.",
        )),
        (Some(value), None) => {
            let value = value.to_str().map_err(|_| {
                Problem::bad_request(
                    "invalid_checkpoint_precondition",
                    "Checkpoint precondition is invalid",
                    "If-Match must contain one strong quoted checkpoint ETag.",
                )
            })?;
            if value.starts_with("W/") || value == "*" || value.contains(',') {
                return Err(Problem::bad_request(
                    "invalid_checkpoint_precondition",
                    "Checkpoint precondition is invalid",
                    "If-Match must contain one strong quoted checkpoint ETag.",
                ));
            }
            let revision_id = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or_else(|| {
                    Problem::bad_request(
                        "invalid_checkpoint_precondition",
                        "Checkpoint precondition is invalid",
                        "If-Match must contain one strong quoted checkpoint ETag.",
                    )
                })?;
            Ok(CheckpointPrecondition::Match(CheckpointRevisionId(
                revision_id,
            )))
        }
        (None, None) => Err(Problem::checkpoint_precondition_required()),
    }
}

fn parse_uuid(field: &str, value: &str) -> Result<Uuid, Problem> {
    Uuid::parse_str(value).map_err(|_| {
        Problem::bad_request(
            "invalid_path_parameter",
            "Path parameter is invalid",
            format!("{field} must be a UUID"),
        )
    })
}

fn parse_time(field: &str, value: &str) -> Result<OffsetDateTime, Problem> {
    parse_utc_microsecond_timestamp(value).map_err(|error| {
        Problem::bad_request(
            "invalid_timestamp",
            "Timestamp is invalid",
            format!("{field}: {error}"),
        )
    })
}

fn resource_response(
    status: StatusCode,
    episode: Episode,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = format!("\"{}\"", episode.payload_sha256);
    versioned_json_response(status, episode, etag, location, idempotency_replayed)
}

fn fact_response(
    status: StatusCode,
    view: FactView,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = format!("\"{}\"", view.head_revision_id.0);
    versioned_json_response(status, view, etag, location, idempotency_replayed)
}

fn checkpoint_response(
    status: StatusCode,
    view: CheckpointView,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = format!("\"{}\"", view.checkpoint_revision_id.0);
    versioned_json_response(status, view, etag, location, idempotency_replayed)
}

fn retrieval_response(
    status: StatusCode,
    receipt: RetrievalReceipt,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let mut response = (status, Json(receipt)).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    if let Some(location) = location {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&location)
                .map_err(|_| Problem::internal("The service could not construct Location."))?,
        );
    }
    if idempotency_replayed {
        response.headers_mut().insert(
            header::HeaderName::from_static("idempotency-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

fn versioned_json_response<T: Serialize>(
    status: StatusCode,
    value: T,
    etag: String,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = HeaderValue::from_str(&etag)
        .map_err(|_| Problem::internal("The service could not construct the resource ETag."))?;
    let mut response = (status, Json(value)).into_response();
    response.headers_mut().insert(header::ETAG, etag);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    if let Some(location) = location {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(&location)
                .map_err(|_| Problem::internal("The service could not construct Location."))?,
        );
    }
    if idempotency_replayed {
        response.headers_mut().insert(
            header::HeaderName::from_static("idempotency-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

#[derive(Debug, Serialize)]
struct Problem {
    #[serde(rename = "type")]
    type_uri: String,
    title: &'static str,
    status: u16,
    code: &'static str,
    detail: String,
    trace_id: String,
}

impl Problem {
    fn new(
        type_suffix: &'static str,
        title: &'static str,
        status: StatusCode,
        code: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            type_uri: format!("https://palimpsest.dev/problems/{type_suffix}"),
            title,
            status: status.as_u16(),
            code,
            detail: detail.into(),
            trace_id: Uuid::now_v7().to_string(),
        }
    }

    fn bad_request(code: &'static str, title: &'static str, detail: impl Into<String>) -> Self {
        Self::new(
            "invalid-request",
            title,
            StatusCode::BAD_REQUEST,
            code,
            detail,
        )
    }

    fn unprocessable(code: &'static str, detail: impl Into<String>) -> Self {
        Self::new(
            "unprocessable-request",
            "Request cannot be processed",
            StatusCode::UNPROCESSABLE_ENTITY,
            code,
            detail,
        )
    }

    fn from_json_rejection(error: JsonRejection) -> Self {
        if error.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE {
            Self::new(
                "unsupported-media-type",
                "Content type is unsupported",
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Use Content-Type: application/json.",
            )
        } else {
            Self::bad_request("invalid_json", "Request JSON is invalid", error.body_text())
        }
    }

    fn from_retrieval_json_rejection(error: JsonRejection) -> Self {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                "retrieval-request-too-large",
                "Retrieval request is too large",
                StatusCode::PAYLOAD_TOO_LARGE,
                "retrieval_request_too_large",
                "Reduce the query or retrieval filter set.",
            )
        } else {
            Self::from_json_rejection(error)
        }
    }

    fn unauthorized() -> Self {
        Self::new(
            "authentication-required",
            "Authentication required",
            StatusCode::UNAUTHORIZED,
            "authentication_required",
            "Supply a valid bearer token.",
        )
    }

    fn idempotency_key_required() -> Self {
        Self::new(
            "idempotency-key-required",
            "Idempotency key is required",
            StatusCode::BAD_REQUEST,
            "idempotency_key_required",
            "Supply a non-empty Idempotency-Key header.",
        )
    }

    fn precondition_required() -> Self {
        Self::new(
            "fact-precondition-required",
            "Fact precondition is required",
            StatusCode::PRECONDITION_REQUIRED,
            "fact_precondition_required",
            "Supply exactly one strong fact ETag in If-Match.",
        )
    }

    fn checkpoint_precondition_required() -> Self {
        Self::new(
            "checkpoint-precondition-required",
            "Checkpoint precondition is required",
            StatusCode::PRECONDITION_REQUIRED,
            "checkpoint_precondition_required",
            "Supply If-None-Match: * to create or one strong checkpoint ETag in If-Match to advance.",
        )
    }

    fn from_service(error: ServiceError) -> Self {
        match error {
            ServiceError::NotFound | ServiceError::CheckpointExpired => Self::new(
                "resource-not-found",
                "Resource not found",
                StatusCode::NOT_FOUND,
                "resource_not_found",
                "No resource was found in the authorized scope.",
            ),
            ServiceError::Gone => Self::new(
                "deletion-operation-expired",
                "Deletion operation expired",
                StatusCode::GONE,
                "deletion_operation_expired",
                "The deletion operation record is no longer available.",
            ),
            ServiceError::ExportExpired => Self::new(
                "export-expired",
                "Export expired",
                StatusCode::GONE,
                "export_expired",
                "The export package is no longer available.",
            ),
            ServiceError::Conflict => Self::new(
                "fact-key-conflict",
                "Fact key already exists",
                StatusCode::CONFLICT,
                "fact_key_conflict",
                "The fact identity already exists in this scope.",
            ),
            ServiceError::IdempotencyKeyReused => Self::new(
                "idempotency-key-reused",
                "Idempotency key was already used",
                StatusCode::CONFLICT,
                "idempotency_key_reused",
                "Use a new Idempotency-Key for a different request.",
            ),
            ServiceError::IdempotencyInProgress => Self::new(
                "idempotency-in-progress",
                "Idempotent request is in progress",
                StatusCode::CONFLICT,
                "idempotency_in_progress",
                "Retry the identical request after a short delay.",
            ),
            ServiceError::PreconditionFailed => Self::new(
                "stale-fact",
                "Fact precondition failed",
                StatusCode::PRECONDITION_FAILED,
                "stale_fact",
                "Fetch the current fact and retry with its ETag.",
            ),
            ServiceError::SupersessionConflict => Self::new(
                "supersession-conflict",
                "Fact supersession conflicts with the current head",
                StatusCode::CONFLICT,
                "supersession_conflict",
                "supersedes_revision_id must identify the current fact head.",
            ),
            ServiceError::FutureRecordedTime => Self::new(
                "future-recorded-time",
                "Recorded-time coordinate is in the future",
                StatusCode::UNPROCESSABLE_ENTITY,
                "future_recorded_time",
                "recorded_at cannot be later than the request snapshot.",
            ),
            ServiceError::InvalidValidTime(detail) => Self::new(
                "invalid-valid-time",
                "Valid-time interval is invalid",
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_valid_time",
                detail,
            ),
            ServiceError::CheckpointPreconditionFailed => Self::new(
                "stale-checkpoint",
                "Checkpoint precondition failed",
                StatusCode::PRECONDITION_FAILED,
                "stale_checkpoint",
                "Load the current checkpoint and retry with its strong ETag.",
            ),
            ServiceError::CheckpointParentConflict => Self::new(
                "checkpoint-parent-conflict",
                "Checkpoint parent conflicts with the current head",
                StatusCode::CONFLICT,
                "checkpoint_parent_conflict",
                "parent_revision_id and If-Match must identify the current checkpoint head.",
            ),
            ServiceError::CheckpointCaseConflict => Self::new(
                "checkpoint-case-conflict",
                "Checkpoint case conflicts with the existing lineage",
                StatusCode::CONFLICT,
                "checkpoint_case_conflict",
                "case_id is fixed when the checkpoint lineage is created.",
            ),
            ServiceError::CheckpointAlreadyExists => Self::new(
                "checkpoint-already-exists",
                "Checkpoint already exists",
                StatusCode::PRECONDITION_FAILED,
                "checkpoint_already_exists",
                "Load the current checkpoint and advance it with If-Match.",
            ),
            ServiceError::EffectKeyConflict => Self::new(
                "effect-key-conflict",
                "Effect key conflicts with an existing effect",
                StatusCode::CONFLICT,
                "effect_key_conflict",
                "Reuse an effect key only for its original prepared effect.",
            ),
            ServiceError::InvalidEffectTransition => Self::new(
                "invalid-effect-transition",
                "Effect transition is invalid",
                StatusCode::CONFLICT,
                "invalid_effect_transition",
                "Prepare each effect once and complete only an existing prepared effect.",
            ),
            ServiceError::RetentionPolicyRejected => Self::new(
                "retention-policy-rejected",
                "Retention policy rejected the checkpoint",
                StatusCode::UNPROCESSABLE_ENTITY,
                "retention_policy_rejected",
                "Use an active checkpoint retention policy.",
            ),
            ServiceError::WritePolicyRejected => Self::new(
                "write-policy-rejected",
                "Fact write policy is not registered",
                StatusCode::UNPROCESSABLE_ENTITY,
                "write_policy_rejected",
                "Use a write policy registered by the migration authority.",
            ),
            ServiceError::CheckpointTooLarge => Self::new(
                "checkpoint-too-large",
                "Checkpoint exceeds the supported size",
                StatusCode::PAYLOAD_TOO_LARGE,
                "checkpoint_too_large",
                "Reduce the checkpoint state or effect transition batch.",
            ),
            ServiceError::RetrievalTooLarge => Self::new(
                "retrieval-request-too-large",
                "Retrieval request is too large",
                StatusCode::PAYLOAD_TOO_LARGE,
                "retrieval_request_too_large",
                "Reduce the query or retrieval filter set.",
            ),
            ServiceError::DeletionWorkerRecoveryFailed => Self::new(
                "deletion-worker-recovery-failed",
                "Deletion worker recovery failed",
                StatusCode::SERVICE_UNAVAILABLE,
                "deletion_worker_recovery_failed",
                "The deletion worker could not release its operation lease after a target failure.",
            ),
            ServiceError::ExportWorkerRecoveryFailed => Self::new(
                "export-worker-recovery-failed",
                "Export worker recovery failed",
                StatusCode::SERVICE_UNAVAILABLE,
                "export_worker_recovery_failed",
                "The export worker could not release its content lease after a failure.",
            ),
            ServiceError::Invalid(detail) => {
                Self::bad_request("invalid_request", "Request is invalid", detail)
            }
            ServiceError::Unprocessable(detail) => {
                Self::unprocessable("unprocessable_request", detail)
            }
            ServiceError::Unavailable => Self::new(
                "service-unavailable",
                "Service unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "The persistence operation failed.",
            ),
        }
    }

    fn internal(detail: impl Into<String>) -> Self {
        Self::new(
            "internal-error",
            "Internal service error",
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            detail,
        )
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let retryable = self.type_uri.ends_with("/idempotency-in-progress")
            || status == StatusCode::SERVICE_UNAVAILABLE;
        let mut response = (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            Json(self),
        )
            .into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store"),
        );
        if status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if retryable {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_domain::{OperationGrant, PrincipalId, SubjectId, TenantId};
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
        task::Waker,
    };

    #[test]
    fn reused_idempotency_key_is_a_conflict_problem() {
        let problem = Problem::from_service(ServiceError::IdempotencyKeyReused);

        assert_eq!(problem.status, StatusCode::CONFLICT.as_u16());
        assert_eq!(problem.code, "idempotency_key_reused");
    }

    #[test]
    fn export_worker_authorization_requires_the_export_grant() {
        let tenant_id = TenantId(Uuid::from_u128(10));
        let subject_id = SubjectId(Uuid::from_u128(11));
        let principal_id = PrincipalId("shared-principal".to_owned());
        let scope = PrincipalScope {
            principal_id: principal_id.clone(),
            tenant_id,
            subject_ids: vec![subject_id],
            allowed_sensitivities: vec![],
            operation_grants: vec![OperationGrant::CanonicalHistoryExport],
        };
        let authenticator = StaticAuthenticator::new([
            (
                "read-token".to_owned(),
                PrincipalScope {
                    operation_grants: vec![],
                    ..scope.clone()
                },
            ),
            ("export-token".to_owned(), scope.clone()),
        ]);
        let digest = palimpsest_application::export_authorization_scope_sha256(
            &scope, tenant_id, subject_id,
        )
        .unwrap();

        let authorized =
            authenticator.authorize_export_worker(&principal_id, tenant_id, subject_id, &digest);
        assert_eq!(authorized, Some(scope));
    }

    struct PendingBody;

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn expired_content_lease_never_yields_buffered_response_content() {
        let mut body = Box::pin(ContentLeaseBody {
            inner: Box::pin(Body::from("must-not-cross-expired-content-lease")),
            expires_at: OffsetDateTime::now_utc() - time::Duration::SECOND,
            deadline: Box::pin(tokio::time::sleep(Duration::ZERO)),
            guard: None,
        });
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            body.as_mut().poll_frame(&mut context),
            Poll::Ready(None)
        ));
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    }

    #[tokio::test]
    async fn content_lease_deadline_wakes_a_stalled_response_body() {
        let expires_at = OffsetDateTime::now_utc() + time::Duration::milliseconds(20);
        let mut body = Box::pin(ContentLeaseBody {
            inner: Box::pin(Body::new(PendingBody)),
            expires_at,
            deadline: Box::pin(tokio::time::sleep(duration_until(expires_at))),
            guard: None,
        });

        let frame = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|context| body.as_mut().poll_frame(context)),
        )
        .await
        .expect("the lease deadline must wake a response whose inner body never wakes");

        assert!(frame.is_none());
        assert!(body.is_end_stream());
    }

    #[tokio::test]
    async fn lease_release_retries_until_cleanup_succeeds() {
        let counters_before = content_lease_cleanup_counters();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = attempts.clone();
        retry_content_lease_release(move || {
            let attempts = attempts.clone();
            async move {
                if attempts.fetch_add(1, AtomicOrdering::SeqCst) < 2 {
                    Err(())
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(observed_attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(
            content_lease_cleanup_counters().release_retries,
            counters_before.release_retries + 2
        );
    }
}
