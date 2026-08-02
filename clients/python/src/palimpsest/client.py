"""A thin Python boundary over Palimpsest's versioned HTTP API.

The client deliberately has no database, model, or framework dependencies. The
server remains the authority for authorization, temporal semantics, write
policies, and deletion state.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from urllib import error, parse, request


JsonObject = dict[str, Any]


class PalimpsestError(RuntimeError):
    """Base class for client and server failures."""


class PalimpsestConfigurationError(PalimpsestError):
    """The client was constructed with an invalid value."""


class PalimpsestTransportError(PalimpsestError):
    """The service could not be reached or did not return a response."""


class PalimpsestProtocolError(PalimpsestError):
    """The service returned a response outside the JSON contract."""


class PalimpsestTimeoutError(PalimpsestError):
    """A bounded client-side wait expired before an operation was terminal."""


class PalimpsestHttpError(PalimpsestError):
    """The service returned a non-success HTTP response."""

    def __init__(
        self,
        status_code: int,
        method: str,
        path: str,
        problem: object,
        headers: Mapping[str, str],
    ) -> None:
        self.status_code = status_code
        self.method = method
        self.path = path
        self.problem = problem
        self.headers = dict(headers)
        problem_type = problem.get("type") if isinstance(problem, dict) else None
        label = f" ({problem_type})" if isinstance(problem_type, str) else ""
        super().__init__(f"Palimpsest returned HTTP {status_code}{label} for {method} {path}")


class PartialRememberError(PalimpsestError):
    """An episode was committed but its governed fact was not."""

    def __init__(self, episode: JsonObject, cause: PalimpsestError) -> None:
        self.episode = episode
        self.cause = cause
        episode_id = episode.get("episode_id", "unknown")
        super().__init__(f"episode {episode_id} was saved, but fact promotion failed: {cause}")


@dataclass(frozen=True)
class PalimpsestResponse:
    """JSON response plus HTTP metadata needed for conditional mutations."""

    data: JsonObject
    status_code: int
    headers: Mapping[str, str]

    @property
    def etag(self) -> str | None:
        for name, value in self.headers.items():
            if name.lower() == "etag":
                return value
        return None


@dataclass(frozen=True)
class _HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes


class PalimpsestClient:
    """Use one authorized tenant/subject scope through the HTTP API.

    ``idempotency_key`` arguments are optional for convenience. Callers that
    retry a mutation should provide the same key again; generated keys are
    intentionally unique to one call.
    """

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        tenant_id: str,
        subject_id: str,
        case_id: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.base_url = _base_url(base_url)
        if not isinstance(bearer_token, str) or not bearer_token.strip():
            raise PalimpsestConfigurationError("bearer_token must be a non-empty string")
        self.bearer_token = bearer_token
        self.tenant_id = _uuid_string(tenant_id, "tenant_id")
        self.subject_id = _uuid_string(subject_id, "subject_id")
        self.case_id = None if case_id is None else _uuid_string(case_id, "case_id")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise PalimpsestConfigurationError("timeout_seconds must be a number")
        if timeout_seconds <= 0:
            raise PalimpsestConfigurationError("timeout_seconds must be greater than zero")
        self.timeout_seconds = float(timeout_seconds)

    def append_episode(
        self,
        *,
        kind: str,
        observed_at: str,
        provenance: Mapping[str, Any],
        sensitivity: str,
        retention_policy_id: str,
        payload: Any,
        case_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        body = {
            "case_id": self._case(case_id),
            "kind": _non_empty_text(kind, "kind"),
            "observed_at": _non_empty_text(observed_at, "observed_at"),
            "provenance": dict(provenance),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(retention_policy_id, "retention_policy_id"),
            "payload": payload,
        }
        return self._json_request(
            "POST",
            f"{self._scope_path()}/episodes",
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
        )

    def create_fact(
        self,
        *,
        namespace: str,
        key: str,
        value: Any,
        observed_at: str,
        valid_time: Mapping[str, Any],
        evidence_episode_ids: Sequence[str],
        write_policy: Mapping[str, Any],
        confidence: float,
        sensitivity: str,
        retention_policy_id: str,
        case_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        body = {
            "case_id": self._case(case_id),
            "namespace": _non_empty_text(namespace, "namespace"),
            "key": _non_empty_text(key, "key"),
            "value": _non_null(value, "value"),
            "observed_at": _non_empty_text(observed_at, "observed_at"),
            "valid_time": dict(valid_time),
            "evidence_episode_ids": [_uuid_string(value, "evidence_episode_id") for value in evidence_episode_ids],
            "write_policy": dict(write_policy),
            "confidence": _confidence(confidence),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(retention_policy_id, "retention_policy_id"),
        }
        return self._json_request(
            "POST",
            f"{self._scope_path()}/facts",
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
        )

    def get_fact(self, fact_id: str) -> JsonObject:
        return self.get_fact_response(fact_id).data

    def get_fact_response(self, fact_id: str) -> PalimpsestResponse:
        return self._json_response("GET", f"{self._scope_path()}/facts/{_uuid_string(fact_id, 'fact_id')}")

    get_current_fact = get_fact

    def get_fact_as_of(self, fact_id: str, *, valid_at: str, recorded_at: str) -> JsonObject:
        query = parse.urlencode(
            {
                "valid_at": _non_empty_text(valid_at, "valid_at"),
                "recorded_at": _non_empty_text(recorded_at, "recorded_at"),
            }
        )
        path = f"{self._scope_path()}/facts/{_uuid_string(fact_id, 'fact_id')}/as-of?{query}"
        return self._json_request("GET", path)

    def supersede_fact(
        self,
        fact_id: str,
        *,
        supersedes_revision_id: str,
        value: Any,
        observed_at: str,
        valid_time: Mapping[str, Any],
        evidence_episode_ids: Sequence[str],
        write_policy: Mapping[str, Any],
        confidence: float,
        sensitivity: str,
        retention_policy_id: str,
        if_match: str,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        body = {
            "supersedes_revision_id": _uuid_string(supersedes_revision_id, "supersedes_revision_id"),
            "value": _non_null(value, "value"),
            "observed_at": _non_empty_text(observed_at, "observed_at"),
            "valid_time": dict(valid_time),
            "evidence_episode_ids": [_uuid_string(value, "evidence_episode_id") for value in evidence_episode_ids],
            "write_policy": dict(write_policy),
            "confidence": _confidence(confidence),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(retention_policy_id, "retention_policy_id"),
        }
        return self._json_request(
            "PUT",
            f"{self._scope_path()}/facts/{_uuid_string(fact_id, 'fact_id')}",
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
            if_match=_non_empty_text(if_match, "if_match"),
        )

    def retrieve(
        self,
        query: str,
        *,
        perspective: Mapping[str, Any] | str | None = None,
        page_size: int = 10,
        policy_id: str | None = None,
        filters: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        query = _non_empty_text(query, "query")
        if len(query.encode("utf-8")) > 4096:
            raise PalimpsestConfigurationError("query must contain at most 4096 UTF-8 bytes")
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 50:
            raise PalimpsestConfigurationError("page_size must be an integer from 1 to 50")
        if perspective is None or perspective == "current":
            normalized_perspective: object = {"kind": "current"}
        elif isinstance(perspective, Mapping):
            normalized_perspective = dict(perspective)
        else:
            raise PalimpsestConfigurationError("perspective must be 'current' or a mapping")
        body: JsonObject = {
            "query": query,
            "perspective": normalized_perspective,
            "page_size": page_size,
            "filters": dict(filters or {}),
        }
        if policy_id is not None:
            body["policy_id"] = _non_empty_text(policy_id, "policy_id")
        return self._json_request(
            "POST",
            f"{self._scope_path()}/retrievals",
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
        )

    recall = retrieve

    def get_retrieval(self, retrieval_id: str, *, cursor: str | None = None) -> JsonObject:
        path = f"{self._scope_path()}/retrievals/{_uuid_string(retrieval_id, 'retrieval_id')}"
        if cursor is not None:
            path += f"?{parse.urlencode({'cursor': _non_empty_text(cursor, 'cursor')})}"
        return self._json_request("GET", path)

    def forget(self, *, idempotency_key: str | None = None) -> JsonObject:
        return self._json_request(
            "POST",
            f"{self._scope_path()}/deletions",
            body={},
            idempotency_key=_idempotency_key(idempotency_key),
        )

    delete_subject = forget

    def get_deletion(self, operation_id: str, *, if_none_match: str | None = None) -> JsonObject:
        return self.get_deletion_response(operation_id, if_none_match=if_none_match).data

    def get_deletion_response(
        self, operation_id: str, *, if_none_match: str | None = None
    ) -> PalimpsestResponse:
        response = self._request(
            "GET",
            f"{self._scope_path()}/deletions/{_uuid_string(operation_id, 'operation_id')}",
            if_none_match=if_none_match,
        )
        if response.status_code == 304:
            return PalimpsestResponse({}, response.status_code, response.headers)
        return self._decode_json_response(response)

    def wait_for_deletion(
        self,
        operation_id: str,
        *,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.5,
    ) -> JsonObject:
        """Poll a deletion with conditional requests until it reaches a terminal state."""

        timeout_seconds = _positive_number(timeout_seconds, "timeout_seconds")
        poll_interval_seconds = _positive_number(poll_interval_seconds, "poll_interval_seconds")
        deadline = time.monotonic() + timeout_seconds
        etag: str | None = None
        latest: JsonObject | None = None
        while True:
            response = self.get_deletion_response(operation_id, if_none_match=etag)
            if response.status_code != 304:
                latest = response.data
                etag = response.etag
            if latest is not None and latest.get("lifecycle_state") in {"completed", "failed", "expired"}:
                return latest
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PalimpsestTimeoutError(
                    f"deletion {operation_id} did not reach a terminal state within {timeout_seconds:g} seconds"
                )
            time.sleep(min(poll_interval_seconds, remaining))

    def remember(
        self,
        content: str,
        *,
        key: str,
        metadata: Mapping[str, Any] | None = None,
        kind: str = "python_memory",
        source_type: str = "palimpsest.python",
        source_uri: str | None = None,
        external_id: str | None = None,
        namespace: str = "python",
        sensitivity: str = "internal",
        retention_policy_id: str = "standard",
        confidence: float = 1.0,
        observed_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        content = _non_empty_text(content, "content")
        if len(content.encode("utf-8")) > 65_536:
            raise PalimpsestConfigurationError("content must contain at most 65536 UTF-8 bytes")
        key = _non_empty_text(key, "key")
        observed_at = observed_at or _utc_now()
        episode_payload = {"content": content, "metadata": dict(metadata or {})}
        provenance = {
            "source_type": _non_empty_text(source_type, "source_type"),
            "source_uri": source_uri,
            "external_id": external_id,
        }
        base_key = _idempotency_base(idempotency_key)
        episode = self.append_episode(
            case_id=None,
            kind=kind,
            observed_at=observed_at,
            provenance=provenance,
            sensitivity=sensitivity,
            retention_policy_id=retention_policy_id,
            payload=episode_payload,
            idempotency_key=f"{base_key}:episode",
        )
        episode_id = episode.get("episode_id")
        if not isinstance(episode_id, str) or not episode_id:
            raise PalimpsestProtocolError("Palimpsest created an episode without returning its identifier")
        try:
            fact = self.create_fact(
                namespace=namespace,
                key=key,
                value=episode_payload,
                observed_at=observed_at,
                valid_time={"from": observed_at},
                evidence_episode_ids=[episode_id],
                write_policy={"id": "direct-evidence", "version": "1"},
                confidence=confidence,
                sensitivity=sensitivity,
                retention_policy_id=retention_policy_id,
                idempotency_key=f"{base_key}:fact",
            )
        except PalimpsestError as exc:
            raise PartialRememberError(episode, exc) from exc
        return {"episode": episode, "fact": fact}

    def correct(self, fact_id: str, **kwargs: Any) -> JsonObject:
        """Append a governed fact revision using the current strong ETag."""

        return self.supersede_fact(fact_id, **kwargs)

    def _case(self, case_id: str | None) -> str:
        selected = self.case_id if case_id is None else _uuid_string(case_id, "case_id")
        if selected is None:
            raise PalimpsestConfigurationError("case_id is required for this operation")
        return selected

    def _scope_path(self) -> str:
        return f"/v1/tenants/{parse.quote(self.tenant_id, safe='')}/subjects/{parse.quote(self.subject_id, safe='')}"

    def _json_request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        idempotency_key: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> JsonObject:
        return self._json_response(
            method,
            path,
            body=body,
            idempotency_key=idempotency_key,
            if_match=if_match,
            if_none_match=if_none_match,
        ).data

    def _json_response(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        idempotency_key: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> PalimpsestResponse:
        response = self._request(
            method,
            path,
            body=body,
            idempotency_key=idempotency_key,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        return self._decode_json_response(response)

    @staticmethod
    def _decode_json_response(response: _HttpResponse) -> PalimpsestResponse:
        try:
            decoded = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PalimpsestProtocolError("Palimpsest returned invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise PalimpsestProtocolError("Palimpsest returned a non-object JSON response")
        return PalimpsestResponse(decoded, response.status_code, response.headers)

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        idempotency_key: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> _HttpResponse:
        encoded_body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}",
        }
        if body is not None:
            encoded_body = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            headers["Idempotency-Key"] = _idempotency_key(idempotency_key)
        if if_match is not None:
            headers["If-Match"] = if_match
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match
        http_request = request.Request(
            f"{self.base_url}{path}", data=encoded_body, headers=headers, method=method
        )
        try:
            with request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                return _HttpResponse(response.status, dict(response.headers.items()), response.read())
        except error.HTTPError as exc:
            response_body = exc.read()
            headers = dict(exc.headers.items())
            exc.close()
            if exc.code == 304:
                return _HttpResponse(exc.code, headers, response_body)
            try:
                problem = json.loads(response_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                problem = None
            raise PalimpsestHttpError(exc.code, method, path, problem, headers) from None
        except (error.URLError, TimeoutError) as exc:
            reason = getattr(exc, "reason", str(exc))
            raise PalimpsestTransportError(f"Palimpsest is unavailable: {reason}") from None


def _base_url(value: str) -> str:
    if not isinstance(value, str):
        raise PalimpsestConfigurationError("base_url must be a string")
    parsed = parse.urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise PalimpsestConfigurationError("base_url must be an HTTP(S) URL")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise PalimpsestConfigurationError("base_url must not contain credentials, a query, or a fragment")
    return value.rstrip("/")


def _uuid_string(value: str, name: str) -> str:
    if not isinstance(value, str):
        raise PalimpsestConfigurationError(f"{name} must be a UUID string")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise PalimpsestConfigurationError(f"{name} must be a UUID string") from exc


def _non_empty_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PalimpsestConfigurationError(f"{name} must be a non-empty string")
    return value.strip()


def _non_null(value: Any, name: str) -> Any:
    if value is None:
        raise PalimpsestConfigurationError(f"{name} must not be null")
    return value


def _confidence(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
        raise PalimpsestConfigurationError("confidence must be a number from 0 to 1")
    return float(value)


def _positive_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise PalimpsestConfigurationError(f"{name} must be greater than zero")
    return float(value)


def _idempotency_key(value: str | None) -> str:
    if value is None:
        return f"palimpsest-python-{uuid.uuid4()}"
    if not isinstance(value, str) or not value.strip() or len(value) > 255:
        raise PalimpsestConfigurationError("idempotency_key must contain 1 to 255 characters")
    return value


def _idempotency_base(value: str | None) -> str:
    base = _idempotency_key(value) if value is not None else f"palimpsest-python-{uuid.uuid4()}"
    if len(base) > 243:
        raise PalimpsestConfigurationError("idempotency_key must leave room for the operation suffix")
    return base


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
