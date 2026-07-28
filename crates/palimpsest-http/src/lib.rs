use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use palimpsest_application::{MemoryService, ServiceError};
use palimpsest_domain::{
    AppendEpisode, CaseId, CreateFact, Episode, EpisodeId, EpisodeKind, FactId, FactKey,
    FactNamespace, FactView, PrincipalScope, Provenance, RetentionPolicyId, RevisionId,
    Sensitivity, SubjectId, SupersedeFact, TenantId, ValidTime, WritePolicy, WritePolicyId,
    WritePolicyVersion,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
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

async fn append_episode(
    State(state): State<AppState>,
    Path((tenant_id, subject_id)): Path<(String, String)>,
    headers: HeaderMap,
    payload: Result<Json<AppendEpisodeRequest>, JsonRejection>,
) -> Result<Response, Problem> {
    let tenant_id = TenantId(parse_uuid("tenant_id", &tenant_id)?);
    let subject_id = SubjectId(parse_uuid("subject_id", &subject_id)?);
    let principal = authenticate(&state, &headers)?;
    let idempotency_key = require_idempotency_key(&headers)?.to_owned();
    let Json(request) = payload.map_err(Problem::from_json_rejection)?;
    let observed_at = OffsetDateTime::parse(&request.observed_at, &Rfc3339).map_err(|error| {
        Problem::bad_request(
            "invalid_observed_at",
            "observed_at must be an RFC 3339 timestamp",
            error.to_string(),
        )
    })?;

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
    resource_response(
        StatusCode::CREATED,
        episode.episode,
        Some(location),
        episode.replayed,
    )
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

    let episode = state
        .service
        .get_episode(&principal, tenant_id, subject_id, episode_id)
        .await
        .map_err(Problem::from_service)?;
    resource_response(StatusCode::OK, episode, None, false)
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
    fact_response(
        StatusCode::CREATED,
        outcome.view,
        Some(location),
        outcome.replayed,
    )
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
    let view = state
        .service
        .get_current_fact(&principal, tenant_id, subject_id, fact_id)
        .await
        .map_err(Problem::from_service)?;
    fact_response(StatusCode::OK, view, None, false)
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
    fact_response(StatusCode::OK, outcome.view, None, outcome.replayed)
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
    fact_response(StatusCode::OK, view, None, false)
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
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| {
        Problem::bad_request(
            "invalid_timestamp",
            "Timestamp is invalid",
            format!("{field} must be RFC 3339: {error}"),
        )
    })
}

fn resource_response(
    status: StatusCode,
    episode: Episode,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = HeaderValue::from_str(&format!("\"{}\"", episode.payload_sha256)).map_err(|_| {
        Problem::internal("The service could not construct the resource validator.")
    })?;
    let mut response = (status, Json(episode)).into_response();
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

fn fact_response(
    status: StatusCode,
    view: FactView,
    location: Option<String>,
    idempotency_replayed: bool,
) -> Result<Response, Problem> {
    let etag = HeaderValue::from_str(&format!("\"{}\"", view.head_revision_id.0))
        .map_err(|_| Problem::internal("The service could not construct the fact validator."))?;
    let mut response = (status, Json(view)).into_response();
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

    fn from_service(error: ServiceError) -> Self {
        match error {
            ServiceError::NotFound => Self::new(
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
            ServiceError::Invalid(detail) => {
                Self::bad_request("invalid_request", "Request is invalid", detail)
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
