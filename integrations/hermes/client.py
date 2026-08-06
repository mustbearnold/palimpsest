"""Self-contained Palimpsest HTTP client for the Hermes memory plugin.

Stdlib only (``urllib``, ``json``, ``uuid``) so the plugin installs into
Hermes with zero pip dependencies. The Palimpsest HTTP service remains the
single authority for authorization, temporal semantics, write policies, and
deletion state; this client never touches PostgreSQL.

Configuration precedence: environment variables, then
``$HERMES_HOME/palimpsest.json``, then local-development defaults. The
``PALIMPSEST_*`` variable names are shared with the Codex MCP adapter
(``tools/palimpsest_mcp.py``) so one environment block serves every agent.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
LOCAL_DEFAULT_TOKEN = "palimpsest-local-development-token"
LOCAL_DEFAULT_TENANT = "019be000-0000-7000-8000-000000000010"
LOCAL_DEFAULT_SUBJECT = "019be000-0000-7000-8000-000000000020"
LOCAL_DEFAULT_CASE = "019be000-0000-7000-8000-000000000030"
DEFAULT_NAMESPACE = "hermes"

_CONFIG_FILE = "palimpsest.json"

_ENV_NAMES = {
    "base_url": "PALIMPSEST_BASE_URL",
    "bearer_token": "PALIMPSEST_BEARER_TOKEN",
    "tenant_id": "PALIMPSEST_TENANT_ID",
    "subject_id": "PALIMPSEST_SUBJECT_ID",
    "case_id": "PALIMPSEST_CASE_ID",
    "namespace": "PALIMPSEST_NAMESPACE",
}

# The Codex MCP adapter spells its endpoint PALIMPSEST_MCP_BASE_URL; accept it
# as a fallback so one environment block configures every agent integration.
_MCP_BASE_URL_FALLBACK = "PALIMPSEST_MCP_BASE_URL"


class PalimpsestError(RuntimeError):
    """Base class for client and server failures."""


class PalimpsestConfigError(PalimpsestError):
    """The configuration is invalid."""


class PalimpsestTransportError(PalimpsestError):
    """The service could not be reached or did not return a response."""


class PalimpsestHttpError(PalimpsestError):
    """The service returned a non-success HTTP response."""

    def __init__(self, status_code: int, method: str, path: str, detail: Any) -> None:
        self.status_code = status_code
        self.method = method
        self.path = path
        self.detail = detail
        super().__init__(
            f"Palimpsest returned HTTP {status_code} for {method} {path}: {detail}"
        )


def _utc_now() -> str:
    """RFC 3339 UTC ending in Z, at most six fractional digits (server contract)."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


_TS_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$")


def _normalize_timestamp(value: str) -> str:
    """Return *value* as server-valid RFC 3339 UTC (Z suffix, <=6 fraction digits).

    Legacy values with a ``+00:00`` offset are re-rendered, preserving the
    exact instant; anything unparseable falls back to now. This keeps
    already-buffered rows flushable after a format change (spec R4: a
    committed turn must never be lost to a format defect).
    """
    if _TS_Z_RE.match(value):
        return value
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return _utc_now()
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _strip(value: str) -> str:
    return value.strip()


def _base_url(value: str) -> str:
    parsed = parse.urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise PalimpsestConfigError(
            "base_url must be an HTTP(S) URL without a query or fragment"
        )
    return value.rstrip("/")


def _uuid_string(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PalimpsestConfigError(f"{label} must be a non-empty string")
    try:
        uuid.UUID(value)
    except ValueError as exc:
        raise PalimpsestConfigError(f"{label} must be a UUID") from exc
    return value


def _non_empty_text(value: str, name: str, max_len: int = 4096) -> str:
    """Validate non-empty text within a UTF-8 byte cap (server contract).

    The Palimpsest HTTP service caps retrieval queries at 4096 UTF-8 bytes;
    the first-party client enforces the same byte semantics. Validating
    client-side turns the server's 422 into a clean ``PalimpsestConfigError``
    before any network call.
    """
    if not isinstance(value, str) or not value.strip():
        raise PalimpsestConfigError(f"{name} must be a non-empty string")
    if len(value.encode("utf-8")) > max_len:
        raise PalimpsestConfigError(f"{name} must be at most {max_len} UTF-8 bytes")
    return value


def _idempotency_key(value: str | None) -> str:
    """Normalize a caller-supplied key or auto-generate one (server contract).

    The Palimpsest HTTP service requires a non-empty ``Idempotency-Key`` on
    every durable operation (retrievals, episodes, facts). Auto-generated
    keys are unique to one call, mirroring the first-party client; callers
    that retry must pass the same key explicitly.
    """
    if value is None:
        return f"palimpsest-hermes-{uuid.uuid4()}"
    stripped = _strip(value)
    if not stripped or len(stripped) > 255:
        raise PalimpsestConfigError("idempotency_key must contain 1 to 255 characters")
    return stripped


@dataclass(frozen=True)
class PalimpsestConfig:
    """Resolved connection and scope configuration."""

    base_url: str
    bearer_token: str
    tenant_id: str
    subject_id: str
    case_id: str
    namespace: str
    timeout_seconds: float = 30.0

    @classmethod
    def load(cls, hermes_home: str | None = None) -> PalimpsestConfig:
        """Resolve configuration: environment overrides file over defaults."""
        values = cls._from_file(hermes_home)
        values.update(cls._from_environment())
        base_url = _base_url(
            values.get("base_url")
            or os.environ.get(_MCP_BASE_URL_FALLBACK)
            or DEFAULT_BASE_URL
        )
        bearer_token = values.get("bearer_token") or ""
        if not bearer_token:
            hostname = parse.urlparse(base_url).hostname
            if hostname in {"127.0.0.1", "localhost", "::1"}:
                bearer_token = LOCAL_DEFAULT_TOKEN
            else:
                raise PalimpsestConfigError(
                    "PALIMPSEST_BEARER_TOKEN is required for a non-local Palimpsest URL"
                )
        tenant_id = _uuid_string(
            values.get("tenant_id") or LOCAL_DEFAULT_TENANT, "tenant_id"
        )
        subject_id = _uuid_string(
            values.get("subject_id") or LOCAL_DEFAULT_SUBJECT, "subject_id"
        )
        case_id = _uuid_string(values.get("case_id") or LOCAL_DEFAULT_CASE, "case_id")
        namespace = (
            _strip(values.get("namespace") or DEFAULT_NAMESPACE) or DEFAULT_NAMESPACE
        )
        try:
            timeout_seconds = float(values.get("timeout_seconds") or 30.0)
        except (TypeError, ValueError) as exc:
            raise PalimpsestConfigError("timeout_seconds must be a number") from exc
        if timeout_seconds <= 0:
            raise PalimpsestConfigError("timeout_seconds must be greater than zero")
        return cls(
            base_url=base_url,
            bearer_token=bearer_token,
            tenant_id=tenant_id,
            subject_id=subject_id,
            case_id=case_id,
            namespace=namespace,
            timeout_seconds=timeout_seconds,
        )

    @staticmethod
    def config_path(hermes_home: str | None) -> Path | None:
        if hermes_home:
            return Path(hermes_home) / _CONFIG_FILE
        return None

    @classmethod
    def _from_file(cls, hermes_home: str | None) -> dict:
        path = cls.config_path(hermes_home)
        if path is None or not path.is_file():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        return {
            key: data[key]
            for key in (
                "base_url",
                "bearer_token",
                "tenant_id",
                "subject_id",
                "case_id",
                "namespace",
                "timeout_seconds",
            )
            if key in data
        }

    @classmethod
    def _from_environment(cls) -> dict:
        values: dict = {}
        for key, name in _ENV_NAMES.items():
            if name in os.environ:
                values[key] = os.environ[name]
        return values

    def public_dict(self) -> dict:
        """Content-free view safe to print and to serve to UI clients."""
        return {
            "base_url": self.base_url,
            "tenant_id": self.tenant_id,
            "subject_id": self.subject_id,
            "case_id": self.case_id,
            "namespace": self.namespace,
            "token_configured": bool(self.bearer_token),
        }


class PalimpsestClient:
    """Thin HTTP client over one authorized tenant/subject/case scope."""

    def __init__(self, config: PalimpsestConfig) -> None:
        self.config = config

    # -- transport -----------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        body: Any = None,
        idempotency_key: str | None = None,
    ) -> dict:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.bearer_token}",
        }
        encoded = None
        if body is not None:
            encoded = json.dumps(
                body, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        http_request = request.Request(
            f"{self.config.base_url}{path}",
            data=encoded,
            headers=headers,
            method=method,
        )
        try:
            with request.urlopen(
                http_request, timeout=self.config.timeout_seconds
            ) as response:
                raw = response.read()
        except error.HTTPError as exc:
            raw = exc.read()
            detail = _decode_detail(raw, exc.code)
            exc.close()
            raise PalimpsestHttpError(exc.code, method, path, detail) from None
        except error.URLError as exc:
            raise PalimpsestTransportError(f"{method} {path}: {exc.reason}") from None
        return _decode_json(raw, method, path)

    def _scope_path(self) -> str:
        return (
            f"/v1/tenants/{parse.quote(self.config.tenant_id, safe='')}"
            f"/subjects/{parse.quote(self.config.subject_id, safe='')}"
        )

    # -- operations -----------------------------------------------------------

    def health(self) -> bool:
        """Content-free liveness probe: /healthz answers 200 with an empty body."""
        http_request = request.Request(
            f"{self.config.base_url}/healthz",
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            with request.urlopen(
                http_request, timeout=self.config.timeout_seconds
            ) as response:
                return response.status == 200
        except error.HTTPError:
            return False
        except error.URLError:
            return False

    def recall(self, query: str, page_size: int = 10) -> dict:
        """Create an authorized current retrieval receipt and return its items.

        Retrievals are durable operations: the server requires an
        Idempotency-Key (auto-generated per call when the caller does not
        supply one, mirroring the first-party client).
        """
        query = _non_empty_text(query, "query")
        body = {
            "query": query,
            "perspective": {"kind": "current"},
            "page_size": page_size,
            "filters": {},
        }
        return self._request(
            "POST",
            f"{self._scope_path()}/retrievals",
            body=body,
            idempotency_key=_idempotency_key(None),
        )

    def append_episode(
        self,
        *,
        kind: str,
        observed_at: str,
        provenance: Mapping[str, Any],
        sensitivity: str,
        retention_policy_id: str,
        payload: Any,
        idempotency_key: str | None = None,
    ) -> dict:
        body = {
            "case_id": self.config.case_id,
            "kind": kind,
            "observed_at": _normalize_timestamp(observed_at),
            "provenance": dict(provenance),
            "sensitivity": sensitivity,
            "retention_policy_id": retention_policy_id,
            "payload": payload,
        }
        return self._request(
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
        idempotency_key: str | None = None,
    ) -> dict:
        body = {
            "case_id": self.config.case_id,
            "namespace": namespace,
            "key": key,
            "value": value,
            "observed_at": _normalize_timestamp(observed_at),
            "valid_time": dict(valid_time),
            "evidence_episode_ids": list(evidence_episode_ids),
            "write_policy": dict(write_policy),
            "confidence": confidence,
            "sensitivity": sensitivity,
            "retention_policy_id": retention_policy_id,
        }
        return self._request(
            "POST",
            f"{self._scope_path()}/facts",
            body=body,
            idempotency_key=_idempotency_key(idempotency_key),
        )

    def remember(
        self,
        content: str,
        *,
        key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        kind: str = "hermes_memory",
        source_type: str = "hermes.memory",
        source_uri: str | None = None,
        external_id: str | None = None,
        namespace: str | None = None,
        sensitivity: str = "internal",
        retention_policy_id: str = "standard",
        confidence: float = 1.0,
        observed_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """Append an immutable episode, then a governed direct-evidence fact."""
        content = _non_empty_text(content, "content")
        observed_at = observed_at or _utc_now()
        base_key = _idempotency_key(idempotency_key)  # one base for both writes
        episode = self.append_episode(
            kind=kind,
            observed_at=observed_at,
            provenance={
                "source_type": source_type,
                "source_uri": source_uri,
                "external_id": external_id,
            },
            sensitivity=sensitivity,
            retention_policy_id=retention_policy_id,
            payload={"content": content, "metadata": dict(metadata or {})},
            idempotency_key=f"{base_key}:episode",
        )
        episode_id = episode.get("episode_id")
        if not isinstance(episode_id, str) or not episode_id:
            raise PalimpsestError(
                "Palimpsest created an episode without returning its identifier"
            )
        fact = self.create_fact(
            namespace=namespace or self.config.namespace,
            key=key or _content_key(content),
            value={"content": content, "metadata": dict(metadata or {})},
            observed_at=observed_at,
            valid_time={"from": observed_at},
            evidence_episode_ids=[episode_id],
            write_policy={"id": "direct-evidence", "version": "1"},
            confidence=confidence,
            sensitivity=sensitivity,
            retention_policy_id=retention_policy_id,
            idempotency_key=f"{base_key}:fact",
        )
        return {"episode": episode, "fact": fact}


RECALL_TOP_K_MIN = 1
RECALL_TOP_K_MAX = 50
PREFETCH_PAGE_SIZE = 5


def _content_key(content: str) -> str:
    return f"hermes-{hashlib.sha1(content.encode('utf-8')).hexdigest()[:16]}"


def _episode_id(episode: Mapping[str, Any]) -> str:
    """Extract an episode identifier from a server response, tolerating shapes."""
    value = episode.get("episode_id") or episode.get("id")
    return str(value) if value else ""


def _fact_id(fact: Mapping[str, Any]) -> str:
    """Extract a fact identifier from a server response, tolerating shapes."""
    value = fact.get("fact_id") or fact.get("id") or fact.get("revision_id")
    return str(value) if value else ""


def resolve_hermes_home() -> str:
    """Resolve the active Hermes home (profile-safe) with a plain fallback."""
    try:
        from hermes_constants import get_hermes_home

        return str(get_hermes_home())
    except Exception:  # noqa: BLE001 - import fallback must never raise
        import os

        return os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes")


def format_receipt(receipt: Mapping[str, Any], limit: int = 5) -> str:
    """Compact, content-scoped context block from a retrieval receipt.

    Returns an empty string when nothing readable was retrieved, so callers
    can treat it as an optional context block.
    """
    items = receipt.get("items")
    if not isinstance(items, list):
        return ""
    lines: list = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        text = value.get("content") if isinstance(value, dict) else value
        if not isinstance(text, str) or not text.strip():
            continue
        compact = " ".join(text.split())[:320]
        key = item.get("key", "")
        namespace = item.get("namespace", "")
        label = f"[{namespace}:{key}]" if key else "[memory]"
        lines.append(f"- {label} {compact}")
    if not lines:
        return ""
    return "[Palimpsest Memory]\n" + "\n".join(lines)


def _decode_detail(raw: bytes, status_code: int) -> Any:
    if not raw:
        return f"HTTP {status_code}"
    try:
        detail = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", errors="replace")[:500]
    return detail


def _decode_json(raw: bytes, method: str, path: str) -> dict:
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PalimpsestTransportError(
            f"{method} {path}: invalid JSON response"
        ) from exc
    if not isinstance(decoded, dict):
        raise PalimpsestTransportError(f"{method} {path}: non-object JSON response")
    return decoded
