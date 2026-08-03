"""Project-aware ingestion primitives for explicitly selected agent sessions.

The ingestion runtime is deliberately separate from the MemoryService policy
engine. It reads source-owned transcripts, removes obvious credential-shaped
values, and writes through :class:`palimpsest.PalimpsestClient`.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, Sequence
from urllib.parse import quote


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(?:api[_-]?key|authorization|password|secret|token)\s*[:=]\s*([^\s,;]+)"
)
_TOKEN_SHAPES = re.compile(
    r"(?i)\b(?:ghp_[a-z0-9_\-]{8,}|github_pat_[a-z0-9_\-]{8,}|xox[baprs]-[a-z0-9-]{8,}|sk-[a-z0-9_-]{8,})\b"
)
_PRIVATE_KEY = re.compile(
    r"(?is)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----"
)


@dataclass(frozen=True)
class ProjectIdentity:
    """Stable project scope attached to an ingested session event."""

    project_id: str
    root: str | None
    branch: str | None

    @classmethod
    def from_context(
        cls,
        cwd: str | None,
        *,
        repo_root: str | None = None,
        branch: str | None = None,
        fallback: str | None = None,
    ) -> "ProjectIdentity":
        candidate = repo_root or cwd or fallback or "unknown"
        candidate_path = Path(candidate).expanduser().resolve() if candidate else None
        root_path = candidate_path
        if repo_root is None and candidate_path is not None:
            for parent in (candidate_path, *candidate_path.parents):
                if (parent / ".git").exists():
                    root_path = parent
                    break
        root = str(root_path) if root_path is not None else None
        canonical = root or "unknown"
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        return cls(f"project-{digest}", root, branch or None)


@dataclass(frozen=True)
class SessionEvent:
    """One safe, text-only event ready for governed ingestion."""

    source: str
    event_id: str
    session_id: str
    role: str
    content: str
    observed_at: str
    project: ProjectIdentity


@dataclass(frozen=True)
class SourceSpec:
    """An explicitly authorized local source path."""

    kind: str
    path: Path

    def __post_init__(self) -> None:
        if self.kind not in {"codex", "claude", "hermes"}:
            raise ValueError("source kind must be codex, claude, or hermes")
        object.__setattr__(self, "path", Path(self.path).expanduser().resolve())


@dataclass(frozen=True)
class IngestReport:
    """Content-free operational result for one polling pass."""

    seen: int
    ingested: int
    skipped: int
    baselined: int
    project_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "seen": self.seen,
            "ingested": self.ingested,
            "skipped": self.skipped,
            "baselined": self.baselined,
            "project_ids": list(self.project_ids),
        }


class IngestionError(RuntimeError):
    """The selected source or cursor state cannot be processed safely."""


class _RememberClient(Protocol):
    def remember(self, content: str, **kwargs: Any) -> Any:
        ...


class IngestionRunner:
    """Poll selected agent transcripts and write project-scoped memories.

    The first pass is a privacy-preserving baseline unless ``backfill`` is
    enabled. New files created after that baseline are read from their first
    line. Cursor updates happen only after a successful governed write, so a
    retry can safely reuse the same idempotency key.
    """

    def __init__(
        self,
        client: _RememberClient,
        sources: Sequence[SourceSpec],
        *,
        state_path: str | Path,
        backfill: bool = False,
        project_root: str | Path | None = None,
        namespace_prefix: str = "agent_session",
        sensitivity: str = "internal",
        retention_policy_id: str = "standard",
    ) -> None:
        if not sources:
            raise ValueError("at least one source is required")
        if not isinstance(namespace_prefix, str) or not namespace_prefix.strip():
            raise ValueError("namespace_prefix must be a non-empty string")
        if not isinstance(sensitivity, str) or not sensitivity.strip():
            raise ValueError("sensitivity must be a non-empty string")
        if not isinstance(retention_policy_id, str) or not retention_policy_id.strip():
            raise ValueError("retention_policy_id must be a non-empty string")
        self.client = client
        self.sources = tuple(sources)
        self.state_path = Path(state_path).expanduser().resolve()
        self.backfill = backfill
        self.project_root = (
            str(Path(project_root).expanduser().resolve()) if project_root is not None else None
        )
        self.namespace_prefix = namespace_prefix.strip()
        self.sensitivity = sensitivity.strip()
        self.retention_policy_id = retention_policy_id.strip()

    def run_once(self) -> IngestReport:
        state = _load_state(self.state_path)
        counters = {"seen": 0, "ingested": 0, "skipped": 0, "baselined": 0}
        projects: set[str] = set()
        source_states = state.setdefault("sources", {})
        for source in self.sources:
            if not source.path.exists():
                raise IngestionError("selected ingestion source does not exist")
            source_state = source_states.setdefault(_source_key(source), {})
            if source.kind == "hermes":
                self._run_hermes(source, source_state, counters, projects)
            else:
                self._run_jsonl(source, source_state, counters, projects)
        _save_state(self.state_path, state)
        return IngestReport(
            counters["seen"],
            counters["ingested"],
            counters["skipped"],
            counters["baselined"],
            tuple(sorted(projects)),
        )

    def _run_jsonl(
        self,
        source: SourceSpec,
        source_state: dict[str, Any],
        counters: dict[str, int],
        projects: set[str],
    ) -> None:
        files = _jsonl_files(source.path)
        files_state = source_state.setdefault("files", {})
        initialized = bool(source_state.get("initialized"))
        if not initialized and not self.backfill:
            for path in files:
                stat = path.stat()
                offset, line = _complete_jsonl_cursor(path)
                files_state[str(path)] = {
                    "inode": stat.st_ino,
                    "offset": offset,
                    "line": line,
                }
            source_state["initialized"] = True
            counters["baselined"] += len(files)
            return

        for path in files:
            cursor = files_state.setdefault(str(path), {"inode": None, "offset": 0, "line": 0})
            stat = path.stat()
            if cursor.get("inode") != stat.st_ino or int(cursor.get("offset", 0)) > stat.st_size:
                cursor.update({"inode": stat.st_ino, "offset": 0, "line": 0})
            session_meta = _codex_session_meta(path) if source.kind == "codex" else {}
            for line_number, end_offset, record in _read_jsonl(path, cursor):
                counters["seen"] += 1
                event = (
                    parse_codex_record(
                        record,
                        line_number=line_number,
                        source_path=str(path),
                        session_meta=session_meta,
                    )
                    if source.kind == "codex"
                    else parse_claude_record(record, line_number=line_number, source_path=str(path))
                )
                if event is None or not self._project_allowed(event):
                    cursor["line"] = line_number
                    cursor["offset"] = end_offset
                    counters["skipped"] += 1
                    continue
                self._remember(event)
                cursor["line"] = line_number
                cursor["offset"] = end_offset
                counters["ingested"] += 1
                projects.add(event.project.project_id)
        source_state["initialized"] = True

    def _run_hermes(
        self,
        source: SourceSpec,
        source_state: dict[str, Any],
        counters: dict[str, int],
        projects: set[str],
    ) -> None:
        connection = _open_hermes(source.path)
        try:
            max_id = int(connection.execute("SELECT COALESCE(MAX(id), 0) FROM messages").fetchone()[0])
            if not source_state.get("initialized") and not self.backfill:
                source_state.update({"initialized": True, "last_id": max_id})
                counters["baselined"] += 1
                return
            last_id = int(source_state.get("last_id", 0))
            query = _hermes_query(connection)
            for row in connection.execute(query, (last_id,)):
                counters["seen"] += 1
                event = parse_hermes_row(dict(row), source_path=str(source.path))
                if event is None or not self._project_allowed(event):
                    source_state["last_id"] = int(row["id"])
                    counters["skipped"] += 1
                    continue
                self._remember(event)
                source_state["last_id"] = int(row["id"])
                counters["ingested"] += 1
                projects.add(event.project.project_id)
            source_state["initialized"] = True
        finally:
            connection.close()

    def _project_allowed(self, event: SessionEvent) -> bool:
        return self.project_root is None or event.project.root == self.project_root

    def _remember(self, event: SessionEvent) -> None:
        metadata = {
            "source": event.source,
            "session_id": event.session_id,
            "role": event.role,
            "event_id": event.event_id,
            "project_id": event.project.project_id,
            "project_root": event.project.root,
            "branch": event.project.branch,
            "privacy": {"credential_redaction": "common-patterns-v1"},
        }
        self.client.remember(
            event.content,
            key=f"{event.source}:{event.event_id}",
            metadata=metadata,
            kind=f"{event.source}_session_message",
            source_type=f"{event.source}.session",
            source_uri=f"agent-session://{event.source}/{event.session_id}",
            external_id=f"{event.source}:{event.event_id}",
            namespace=project_namespace(event.project.project_id, self.namespace_prefix),
            sensitivity=self.sensitivity,
            retention_policy_id=self.retention_policy_id,
            observed_at=event.observed_at,
            idempotency_key=f"palimpsest-ingest:{event.source}:{event.event_id}",
        )


def parse_codex_record(
    record: Mapping[str, Any],
    *,
    line_number: int,
    source_path: str,
    session_meta: Mapping[str, Any] | None = None,
) -> SessionEvent | None:
    """Parse only Codex user/assistant message events, never tool payloads."""

    if record.get("type") != "event_msg":
        return None
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    event_type = payload.get("type")
    role = {"user_message": "user", "agent_message": "assistant"}.get(event_type)
    if role is None:
        return None
    content = _text_content(payload.get("message", payload.get("text")))
    observed_at = _timestamp(record.get("timestamp"))
    if content is None or observed_at is None:
        return None
    metadata = dict(session_meta or {})
    session_id = _text_value(metadata.get("session_id")) or Path(source_path).stem
    project = ProjectIdentity.from_context(
        _text_value(metadata.get("cwd")),
        repo_root=_text_value(metadata.get("repo_root")),
        branch=_text_value(metadata.get("branch")),
        fallback=source_path,
    )
    return SessionEvent(
        source="codex",
        event_id=_event_id("codex", session_id, line_number, role, content),
        session_id=session_id,
        role=role,
        content=content,
        observed_at=observed_at,
        project=project,
    )


def parse_claude_record(
    record: Mapping[str, Any],
    *,
    line_number: int,
    source_path: str,
) -> SessionEvent | None:
    """Parse Claude Code text messages while excluding thinking and tools."""

    if record.get("type") not in {"user", "assistant"}:
        return None
    message = record.get("message")
    if not isinstance(message, Mapping):
        return None
    role = message.get("role")
    if role not in {"user", "assistant"}:
        return None
    content = _text_content(message.get("content"))
    observed_at = _timestamp(record.get("timestamp"))
    if content is None or observed_at is None:
        return None
    session_id = _text_value(record.get("sessionId")) or Path(source_path).stem
    project = ProjectIdentity.from_context(
        _text_value(record.get("cwd")),
        branch=_text_value(record.get("gitBranch")),
        fallback=source_path,
    )
    supplied_id = _text_value(record.get("uuid"))
    return SessionEvent(
        source="claude",
        event_id=supplied_id or _event_id("claude", session_id, line_number, role, content),
        session_id=session_id,
        role=role,
        content=content,
        observed_at=observed_at,
        project=project,
    )


def parse_hermes_row(row: Mapping[str, Any], *, source_path: str) -> SessionEvent | None:
    """Parse one Hermes ``state.db`` message row without tool content."""

    role = row.get("role")
    message_id = row.get("id")
    if role not in {"user", "assistant"} or message_id is None:
        return None
    content = row.get("content")
    if isinstance(content, str) and content.startswith("\x00json:"):
        try:
            import json

            content = json.loads(content[len("\x00json:"):])
        except (TypeError, ValueError):
            return None
    text = _text_content(content)
    observed_at = _unix_timestamp(row.get("timestamp"))
    if text is None or observed_at is None:
        return None
    session_id = _text_value(row.get("session_id")) or "unknown"
    project = ProjectIdentity.from_context(
        _text_value(row.get("cwd")),
        repo_root=_text_value(row.get("git_repo_root")),
        branch=_text_value(row.get("git_branch")),
        fallback=source_path,
    )
    return SessionEvent(
        source="hermes",
        event_id=str(message_id),
        session_id=session_id,
        role=role,
        content=text,
        observed_at=observed_at,
        project=project,
    )


def project_namespace(project_id: str, prefix: str = "agent_session") -> str:
    """Return the exact retrieval namespace reserved for one project."""

    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("project_id must be a non-empty string")
    if not isinstance(prefix, str) or not prefix.strip():
        raise ValueError("prefix must be a non-empty string")
    namespace = f"{prefix.strip()}:{project_id.strip()}"
    if len(namespace) > 255:
        raise ValueError("project namespace must contain at most 255 characters")
    return namespace


def redact_sensitive_text(content: str) -> str:
    """Replace common credential-shaped values before they leave the host."""

    if not isinstance(content, str):
        raise TypeError("content must be a string")
    redacted = _PRIVATE_KEY.sub("[REDACTED]", content)
    redacted = _SECRET_ASSIGNMENT.sub(_redact_assignment, redacted)
    return _TOKEN_SHAPES.sub("[REDACTED]", redacted)


def _text_value(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _redact_assignment(match: re.Match[str]) -> str:
    prefix_end = match.group(0).find(match.group(1))
    return match.group(0)[:prefix_end] + "[REDACTED]"


def _text_content(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, Mapping):
        text = value.get("text", "") if isinstance(value.get("text"), str) else ""
        text = text.strip()
    elif isinstance(value, list):
        parts = [
            part.get("text", "")
            for part in value
            if isinstance(part, Mapping)
            and part.get("type") in {"text", "input_text", "output_text"}
            and isinstance(part.get("text"), str)
        ]
        text = "\n".join(part for part in parts if part.strip()).strip()
    else:
        return None
    if not text:
        return None
    return redact_sensitive_text(text)


def _timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _unix_timestamp(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _timestamp(value)
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _event_id(source: str, session_id: str, line_number: int, role: str, content: str) -> str:
    material = f"{source}\x1f{session_id}\x1f{line_number}\x1f{role}\x1f{content}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _source_key(source: SourceSpec) -> str:
    return f"{source.kind}:{source.path}"


def _jsonl_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(candidate for candidate in path.rglob("*.jsonl") if candidate.is_file())


def _complete_jsonl_cursor(path: Path) -> tuple[int, int]:
    """Return the byte offset and line number through the last whole line."""

    offset = 0
    line_number = 0
    with path.open("rb") as handle:
        while True:
            raw = handle.readline()
            if not raw or not raw.endswith(b"\n"):
                break
            offset = handle.tell()
            line_number += 1
    return offset, line_number


def _read_jsonl(
    path: Path,
    cursor: Mapping[str, Any],
) -> Iterator[tuple[int, int, Mapping[str, Any]]]:
    offset = int(cursor.get("offset", 0))
    line_number = int(cursor.get("line", 0))
    with path.open("rb") as handle:
        handle.seek(offset)
        while True:
            line_start = handle.tell()
            raw = handle.readline()
            if not raw:
                return
            if not raw.endswith(b"\n"):
                return
            line_number += 1
            try:
                decoded = json.loads(raw.decode("utf-8"))
                record = decoded if isinstance(decoded, Mapping) else {}
            except (UnicodeDecodeError, json.JSONDecodeError):
                record = {}
            yield line_number, handle.tell(), record
            if handle.tell() <= line_start:
                return


def _codex_session_meta(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8") as handle:
        for _ in range(128):
            line = handle.readline()
            if not line:
                break
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, Mapping) or record.get("type") != "session_meta":
                continue
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                continue
            git = payload.get("git") if isinstance(payload.get("git"), Mapping) else {}
            return {
                key: value
                for key, value in {
                    "session_id": payload.get("session_id") or payload.get("id"),
                    "cwd": payload.get("cwd"),
                    "branch": git.get("branch") or payload.get("git_branch"),
                    "repo_root": git.get("repo_root") or payload.get("git_repo_root"),
                }.items()
                if isinstance(value, str) and value.strip()
            }
    return {}


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "sources": {}}
    try:
        with path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestionError("ingestion cursor state is unreadable") from exc
    if not isinstance(state, dict) or state.get("version") != 1 or not isinstance(state.get("sources"), dict):
        raise IngestionError("ingestion cursor state has an unsupported schema")
    return state


def _save_state(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _open_hermes(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 2000")
        connection.execute("PRAGMA query_only = ON")
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if not {"messages", "sessions"}.issubset(tables):
            raise IngestionError("Hermes state database schema is unsupported")
        return connection
    except IngestionError:
        if connection is not None:
            connection.close()
        raise
    except (OSError, sqlite3.Error) as exc:
        if connection is not None:
            connection.close()
        raise IngestionError("Hermes state database cannot be read safely") from exc


def _hermes_query(connection: sqlite3.Connection) -> str:
    session_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(sessions)")
    }
    message_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(messages)")
    }
    message_required = {"id", "session_id", "role", "content", "timestamp"}
    if not {"id"}.issubset(session_columns) or not message_required.issubset(message_columns):
        raise IngestionError("Hermes sessions schema is unsupported")
    optional = {
        name: f"s.{name}" if name in session_columns else f"NULL AS {name}"
        for name in ("cwd", "git_branch", "git_repo_root")
    }
    return (
        "SELECT m.id, m.session_id, m.role, m.content, m.timestamp, "
        f"{optional['cwd']}, {optional['git_branch']}, {optional['git_repo_root']} "
        "FROM messages AS m LEFT JOIN sessions AS s ON s.id = m.session_id "
        "WHERE m.id > ? AND m.role IN ('user', 'assistant') AND m.content IS NOT NULL "
        "ORDER BY m.id"
    )
