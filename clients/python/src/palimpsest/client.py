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

from .ingest import project_namespace
from .comparison import compare_project_bundles
from .review import (
    PROJECT_CONSOLIDATION_PROFILE,
    prepare_project_consolidation,
    validate_project_review,
)


JsonObject = dict[str, Any]


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


_HTTP_OPENER = request.build_opener(_NoRedirect)


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
        super().__init__(
            f"Palimpsest returned HTTP {status_code}{label} for {method} {path}"
        )


class PartialRememberError(PalimpsestError):
    """An episode was committed but its governed fact was not."""

    def __init__(self, episode: JsonObject, cause: PalimpsestError) -> None:
        self.episode = episode
        self.cause = cause
        episode_id = episode.get("episode_id", "unknown")
        super().__init__(
            f"episode {episode_id} was saved, but fact promotion failed: {cause}"
        )


class PartialConsolidationError(PalimpsestError):
    """Some per-claim consolidation writes committed before a later failure."""

    def __init__(
        self,
        consolidation_id: str,
        completed: list[JsonObject],
        failed_write: JsonObject,
        cause: PalimpsestError,
    ) -> None:
        self.consolidation_id = consolidation_id
        self.completed = completed
        self.failed_write = failed_write
        self.cause = cause
        claim_id = failed_write.get("claim_id", "unknown")
        super().__init__(
            f"consolidation {consolidation_id} committed {len(completed)} claim(s), "
            f"but claim {claim_id} failed: {cause}"
        )


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

    @property
    def location(self) -> str | None:
        for name, value in self.headers.items():
            if name.lower() == "location":
                return value
        return None


@dataclass(frozen=True)
class PalimpsestBinaryResponse:
    """Binary response plus HTTP metadata, used for export packages."""

    content: bytes
    status_code: int
    headers: Mapping[str, str]

    @property
    def etag(self) -> str | None:
        for name, value in self.headers.items():
            if name.lower() == "etag":
                return value
        return None

    @property
    def location(self) -> str | None:
        for name, value in self.headers.items():
            if name.lower() == "location":
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
            raise PalimpsestConfigurationError(
                "bearer_token must be a non-empty string"
            )
        self.bearer_token = bearer_token
        self.tenant_id = _uuid_string(tenant_id, "tenant_id")
        self.subject_id = _uuid_string(subject_id, "subject_id")
        self.case_id = None if case_id is None else _uuid_string(case_id, "case_id")
        if isinstance(timeout_seconds, bool) or not isinstance(
            timeout_seconds, (int, float)
        ):
            raise PalimpsestConfigurationError("timeout_seconds must be a number")
        if timeout_seconds <= 0:
            raise PalimpsestConfigurationError(
                "timeout_seconds must be greater than zero"
            )
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
            "retention_policy_id": _non_empty_text(
                retention_policy_id, "retention_policy_id"
            ),
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
            "evidence_episode_ids": [
                _uuid_string(value, "evidence_episode_id")
                for value in evidence_episode_ids
            ],
            "write_policy": dict(write_policy),
            "confidence": _confidence(confidence),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(
                retention_policy_id, "retention_policy_id"
            ),
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
        return self._json_response(
            "GET", f"{self._scope_path()}/facts/{_uuid_string(fact_id, 'fact_id')}"
        )

    get_current_fact = get_fact

    def get_fact_as_of(
        self, fact_id: str, *, valid_at: str, recorded_at: str
    ) -> JsonObject:
        query = parse.urlencode(
            {
                "valid_at": _non_empty_text(valid_at, "valid_at"),
                "recorded_at": _non_empty_text(recorded_at, "recorded_at"),
            }
        )
        path = f"{self._scope_path()}/facts/{_uuid_string(fact_id, 'fact_id')}/as-of?{query}"
        return self._json_request("GET", path)

    def get_checkpoint(self, agent_id: str, thread_id: str) -> JsonObject:
        return self.get_checkpoint_response(agent_id, thread_id).data

    def get_checkpoint_response(
        self, agent_id: str, thread_id: str
    ) -> PalimpsestResponse:
        path = self._checkpoint_path(agent_id, thread_id)
        return self._json_response("GET", path)

    def save_checkpoint(
        self,
        agent_id: str,
        thread_id: str,
        *,
        state: Any,
        state_schema_version: int,
        effect_transitions: Sequence[Mapping[str, Any]],
        provenance: Mapping[str, Any],
        sensitivity: str,
        retention_policy_id: str,
        case_id: str | None = None,
        parent_revision_id: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        return self.save_checkpoint_response(
            agent_id,
            thread_id,
            state=state,
            state_schema_version=state_schema_version,
            effect_transitions=effect_transitions,
            provenance=provenance,
            sensitivity=sensitivity,
            retention_policy_id=retention_policy_id,
            case_id=case_id,
            parent_revision_id=parent_revision_id,
            if_match=if_match,
            if_none_match=if_none_match,
            idempotency_key=idempotency_key,
        ).data

    def save_checkpoint_response(
        self,
        agent_id: str,
        thread_id: str,
        *,
        state: Any,
        state_schema_version: int,
        effect_transitions: Sequence[Mapping[str, Any]],
        provenance: Mapping[str, Any],
        sensitivity: str,
        retention_policy_id: str,
        case_id: str | None = None,
        parent_revision_id: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        idempotency_key: str | None = None,
    ) -> PalimpsestResponse:
        if (if_match is None) == (if_none_match is None):
            raise PalimpsestConfigurationError(
                "supply exactly one of if_match or if_none_match"
            )
        if if_none_match is not None and if_none_match != "*":
            raise PalimpsestConfigurationError("if_none_match must be '*'")
        if (
            isinstance(state_schema_version, bool)
            or not isinstance(state_schema_version, int)
            or state_schema_version < 1
        ):
            raise PalimpsestConfigurationError(
                "state_schema_version must be a positive integer"
            )
        body = {
            "case_id": self._case(case_id),
            "parent_revision_id": None
            if parent_revision_id is None
            else _uuid_string(parent_revision_id, "parent_revision_id"),
            "state": state,
            "state_schema_version": state_schema_version,
            "effect_transitions": [dict(effect) for effect in effect_transitions],
            "provenance": dict(provenance),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(
                retention_policy_id, "retention_policy_id"
            ),
        }
        return self._json_response(
            "PUT",
            self._checkpoint_path(agent_id, thread_id),
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
            if_match=if_match,
            if_none_match=if_none_match,
        )

    def start_export(self, *, idempotency_key: str | None = None) -> JsonObject:
        return self.start_export_response(idempotency_key=idempotency_key).data

    def start_export_response(
        self, *, idempotency_key: str | None = None
    ) -> PalimpsestResponse:
        return self._json_response(
            "POST",
            f"{self._scope_path()}/exports",
            idempotency_key=_idempotency_key(idempotency_key),
        )

    def get_export(
        self, export_id: str, *, if_none_match: str | None = None
    ) -> JsonObject:
        return self.get_export_response(export_id, if_none_match=if_none_match).data

    def get_export_response(
        self, export_id: str, *, if_none_match: str | None = None
    ) -> PalimpsestResponse:
        response = self._request(
            "GET",
            f"{self._scope_path()}/exports/{_uuid_string(export_id, 'export_id')}",
            if_none_match=if_none_match,
        )
        if response.status_code in {303, 304}:
            return PalimpsestResponse({}, response.status_code, response.headers)
        return self._decode_json_response(response)

    def download_export(self, export_id: str) -> bytes:
        return self.download_export_response(export_id).content

    def download_export_response(self, export_id: str) -> PalimpsestBinaryResponse:
        response = self._request(
            "GET",
            f"{self._scope_path()}/exports/{_uuid_string(export_id, 'export_id')}/content",
        )
        return PalimpsestBinaryResponse(
            response.body, response.status_code, response.headers
        )

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
            "supersedes_revision_id": _uuid_string(
                supersedes_revision_id, "supersedes_revision_id"
            ),
            "value": _non_null(value, "value"),
            "observed_at": _non_empty_text(observed_at, "observed_at"),
            "valid_time": dict(valid_time),
            "evidence_episode_ids": [
                _uuid_string(value, "evidence_episode_id")
                for value in evidence_episode_ids
            ],
            "write_policy": dict(write_policy),
            "confidence": _confidence(confidence),
            "sensitivity": _non_empty_text(sensitivity, "sensitivity"),
            "retention_policy_id": _non_empty_text(
                retention_policy_id, "retention_policy_id"
            ),
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
            raise PalimpsestConfigurationError(
                "query must contain at most 4096 UTF-8 bytes"
            )
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or not 1 <= page_size <= 50
        ):
            raise PalimpsestConfigurationError(
                "page_size must be an integer from 1 to 50"
            )
        if perspective is None or perspective == "current":
            normalized_perspective: object = {"kind": "current"}
        elif isinstance(perspective, Mapping):
            normalized_perspective = dict(perspective)
        else:
            raise PalimpsestConfigurationError(
                "perspective must be 'current' or a mapping"
            )
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

    def recall_by_project(
        self,
        query: str,
        project_ids: Sequence[str],
        *,
        perspective: Mapping[str, Any] | str | None = None,
        page_size: int = 10,
        policy_id: str | None = None,
        filters: Mapping[str, Any] | None = None,
        namespace_prefix: str = "agent_session",
        idempotency_key_prefix: str | None = None,
    ) -> dict[str, JsonObject]:
        """Recall one isolated evidence bundle per project.

        The helper deliberately returns separate retrieval responses. It does
        not invent a semantic diff or combine candidate sets before the caller
        chooses how to compare them.
        """

        if isinstance(project_ids, (str, bytes)):
            raise PalimpsestConfigurationError(
                "project_ids must be a non-empty sequence"
            )
        ordered_ids: list[str] = []
        namespaces: dict[str, str] = {}
        for project_id in project_ids:
            if not isinstance(project_id, str) or not project_id.strip():
                raise PalimpsestConfigurationError(
                    "project_ids must contain non-empty strings"
                )
            project_id = project_id.strip()
            if project_id in namespaces:
                continue
            try:
                namespace = project_namespace(project_id, namespace_prefix)
            except (TypeError, ValueError) as exc:
                raise PalimpsestConfigurationError(str(exc)) from exc
            ordered_ids.append(project_id)
            namespaces[project_id] = namespace
        if not ordered_ids:
            raise PalimpsestConfigurationError(
                "project_ids must be a non-empty sequence"
            )
        base_filters = dict(filters or {})
        if "namespaces" in base_filters:
            raise PalimpsestConfigurationError(
                "recall_by_project owns the namespaces filter"
            )
        base_key = (
            _idempotency_base(idempotency_key_prefix)
            if idempotency_key_prefix is not None
            else None
        )
        results: dict[str, JsonObject] = {}
        for project_id in ordered_ids:
            idempotency_key = None if base_key is None else f"{base_key}:{project_id}"
            if idempotency_key is not None and len(idempotency_key) > 255:
                raise PalimpsestConfigurationError(
                    "idempotency_key_prefix leaves insufficient room for project IDs"
                )
            project_filters = {**base_filters, "namespaces": [namespaces[project_id]]}
            results[project_id] = self.retrieve(
                query,
                perspective=perspective,
                page_size=page_size,
                policy_id=policy_id,
                filters=project_filters,
                idempotency_key=idempotency_key,
            )
        return results

    def compare_by_project(
        self,
        query: str,
        project_ids: Sequence[str],
        *,
        perspective: Mapping[str, Any] | str | None = None,
        page_size: int = 10,
        policy_id: str | None = None,
        filters: Mapping[str, Any] | None = None,
        namespace_prefix: str = "agent_session",
        idempotency_key_prefix: str | None = None,
    ) -> JsonObject:
        """Return isolated bundles plus a structural comparison summary.

        Same-key/different-value groups are review candidates only. No model
        inference, conflict fact, or other durable write occurs here.
        """

        query = _non_empty_text(query, "query")
        bundles = self.recall_by_project(
            query,
            project_ids,
            perspective=perspective,
            page_size=page_size,
            policy_id=policy_id,
            filters=filters,
            namespace_prefix=namespace_prefix,
            idempotency_key_prefix=idempotency_key_prefix,
        )
        try:
            comparison = compare_project_bundles(bundles)
        except ValueError as exc:
            raise PalimpsestProtocolError(
                "retrieval bundles cannot be compared"
            ) from exc
        return {
            "profile": comparison["profile"],
            "query": query,
            "bundles": bundles,
            "comparison": comparison,
        }

    def consolidate_project_review(
        self,
        comparison_result: Mapping[str, Any],
        review: Mapping[str, Any],
        writes: Sequence[Mapping[str, Any]],
        *,
        consolidation_id: str,
    ) -> JsonObject:
        """Durably write explicit facts for validated cross-project claims.

        Validation and plan preparation complete before the first request. The
        service receives only episode IDs cited by the validated review. Each
        claim is a separate idempotent fact write; a later failure raises
        ``PartialConsolidationError`` with completed writes so the same inputs
        and consolidation ID can be retried safely.
        """

        try:
            validated_review = validate_project_review(comparison_result, review)
            plan = prepare_project_consolidation(
                validated_review,
                writes,
                consolidation_id=consolidation_id,
            )
        except ValueError as exc:
            raise PalimpsestConfigurationError(str(exc)) from exc

        self._case(None)
        for planned_write in plan["writes"]:
            for episode_id in planned_write["evidence_episode_ids"]:
                _uuid_string(episode_id, "evidence_episode_id")

        completed: list[JsonObject] = []
        for planned_write in plan["writes"]:
            try:
                fact = self.create_fact(
                    namespace=planned_write["namespace"],
                    key=planned_write["key"],
                    value=planned_write["value"],
                    observed_at=planned_write["observed_at"],
                    valid_time=planned_write["valid_time"],
                    evidence_episode_ids=planned_write["evidence_episode_ids"],
                    write_policy=planned_write["write_policy"],
                    confidence=planned_write["confidence"],
                    sensitivity=planned_write["sensitivity"],
                    retention_policy_id=planned_write["retention_policy_id"],
                    idempotency_key=planned_write["idempotency_key"],
                )
            except PalimpsestConfigurationError:
                raise
            except PalimpsestError as exc:
                raise PartialConsolidationError(
                    plan["consolidation_id"], completed, planned_write, exc
                ) from exc
            completed.append(
                {
                    "claim_id": planned_write["claim_id"],
                    "classification": planned_write["classification"],
                    "claim_summary": planned_write["claim_summary"],
                    "evidence_episode_ids": planned_write["evidence_episode_ids"],
                    "idempotency_key": planned_write["idempotency_key"],
                    "fact": fact,
                }
            )

        return {
            "profile": PROJECT_CONSOLIDATION_PROFILE,
            "consolidation_id": plan["consolidation_id"],
            "reviewer": plan["reviewer"],
            "review_policy": plan["review_policy"],
            "claim_ids": plan["claim_ids"],
            "source_episode_ids": plan["source_episode_ids"],
            "writes": completed,
            "durable_write": True,
        }

    def get_retrieval(
        self, retrieval_id: str, *, cursor: str | None = None
    ) -> JsonObject:
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

    def get_deletion(
        self, operation_id: str, *, if_none_match: str | None = None
    ) -> JsonObject:
        return self.get_deletion_response(
            operation_id, if_none_match=if_none_match
        ).data

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
        poll_interval_seconds = _positive_number(
            poll_interval_seconds, "poll_interval_seconds"
        )
        deadline = time.monotonic() + timeout_seconds
        etag: str | None = None
        latest: JsonObject | None = None
        while True:
            response = self.get_deletion_response(operation_id, if_none_match=etag)
            if response.status_code != 304:
                latest = response.data
                etag = response.etag
            if latest is not None and latest.get("lifecycle_state") in {
                "completed",
                "failed",
                "expired",
            }:
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
            raise PalimpsestConfigurationError(
                "content must contain at most 65536 UTF-8 bytes"
            )
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
            raise PalimpsestProtocolError(
                "Palimpsest created an episode without returning its identifier"
            )
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

    def _checkpoint_path(self, agent_id: str, thread_id: str) -> str:
        return (
            f"{self._scope_path()}/agents/{_uuid_string(agent_id, 'agent_id')}"
            f"/threads/{_uuid_string(thread_id, 'thread_id')}/checkpoint"
        )

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
            raise PalimpsestProtocolError(
                "Palimpsest returned a non-object JSON response"
            )
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
            encoded_body = json.dumps(
                body, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
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
            with _HTTP_OPENER.open(
                http_request, timeout=self.timeout_seconds
            ) as response:
                return _HttpResponse(
                    response.status, dict(response.headers.items()), response.read()
                )
        except error.HTTPError as exc:
            response_body = exc.read()
            headers = dict(exc.headers.items())
            exc.close()
            if exc.code in {303, 304}:
                return _HttpResponse(exc.code, headers, response_body)
            try:
                problem = json.loads(response_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                problem = None
            raise PalimpsestHttpError(
                exc.code, method, path, problem, headers
            ) from None
        except (error.URLError, TimeoutError) as exc:
            reason = getattr(exc, "reason", str(exc))
            raise PalimpsestTransportError(
                f"Palimpsest is unavailable: {reason}"
            ) from None


def _base_url(value: str) -> str:
    if not isinstance(value, str):
        raise PalimpsestConfigurationError("base_url must be a string")
    parsed = parse.urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise PalimpsestConfigurationError("base_url must be an HTTP(S) URL")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise PalimpsestConfigurationError(
            "base_url must not contain credentials, a query, or a fragment"
        )
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
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 <= value <= 1
    ):
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
        raise PalimpsestConfigurationError(
            "idempotency_key must contain 1 to 255 characters"
        )
    return value


def _idempotency_base(value: str | None) -> str:
    base = (
        _idempotency_key(value)
        if value is not None
        else f"palimpsest-python-{uuid.uuid4()}"
    )
    if len(base) > 243:
        raise PalimpsestConfigurationError(
            "idempotency_key must leave room for the operation suffix"
        )
    return base


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
