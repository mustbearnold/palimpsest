use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
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
use palimpsest_application::{MemoryService, ServiceError};
use palimpsest_domain::{
    AgentId, AppendEpisode, CaseId, CheckpointPrecondition, CheckpointRevisionId, CheckpointView,
    CreateFact, CreateRetrieval, EffectTransition, Episode, EpisodeId, EpisodeKind, FactId,
    FactKey, FactNamespace, FactView, PrincipalScope, Provenance, RetentionPolicyId,
    RetrievalFilters, RetrievalId, RetrievalPerspective, RetrievalPolicyId, RetrievalQuery,
    RetrievalReceipt, RevisionId, SaveCheckpoint, Sensitivity, SubjectContentLease, SubjectId,
    SupersedeFact, TenantId, ThreadId, ValidTime, WritePolicy, WritePolicyId, WritePolicyVersion,
    parse_utc_microsecond_timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, bearer_token: &str) -> Option<PrincipalScope>;
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
}

#[derive(Clone)]
struct AppState {
    service: MemoryService,
    authenticator: Arc<dyn Authenticator>,
}

struct ContentLeaseGuard {
    service: MemoryService,
    lease: Option<SubjectContentLease>,
}

impl ContentLeaseGuard {
    fn attach(mut self, response: Response) -> Response {
        let (parts, body) = response.into_parts();
        let body = Body::new(ContentLeaseBody {
            inner: Box::pin(body),
            guard: Some(Self {
                service: self.service.clone(),
                lease: self.lease.take(),
            }),
        });
        Response::from_parts(parts, body)
    }

    fn release(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let service = self.service.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = service.release_subject_content_lease(lease).await;
            });
        }
    }
}

impl Drop for ContentLeaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct ContentLeaseBody {
    inner: Pin<Box<Body>>,
    guard: Option<ContentLeaseGuard>,
}

impl HttpBody for ContentLeaseBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = self.inner.as_mut().poll_frame(context);
        if matches!(result, Poll::Ready(None)) {
            self.guard.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

async fn acquire_content_lease(
    state: &AppState,
    principal: &PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<ContentLeaseGuard, Problem> {
    let lease = state
        .service
        .acquire_subject_content_lease(principal, tenant_id, subject_id)
        .await
        .map_err(Problem::from_service)?;
    Ok(ContentLeaseGuard {
        service: state.service.clone(),
        lease: Some(lease),
    })
}

pub fn router(service: MemoryService, authenticator: Arc<dyn Authenticator>) -> Router {
    let state = AppState {
        service,
        authenticator,
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
        .get_checkpoint(&principal, tenant_id, subject_id, agent_id, thread_id)
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
        .get_episode(&principal, tenant_id, subject_id, episode_id)
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
        .get_current_fact(&principal, tenant_id, subject_id, fact_id)
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
            &principal,
            tenant_id,
            subject_id,
            fact_id,
            valid_at,
            recorded_at,
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
                StatusCode::UNPROCESSABLE_ENTITY,
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
