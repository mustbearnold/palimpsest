"""Small, dependency-free Python client for the Palimpsest HTTP API."""

from .client import (
    PalimpsestBinaryResponse,
    PalimpsestClient,
    PalimpsestConfigurationError,
    PalimpsestError,
    PalimpsestHttpError,
    PalimpsestResponse,
    PalimpsestProtocolError,
    PalimpsestTimeoutError,
    PalimpsestTransportError,
    PartialRememberError,
)
from .comparison import compare_project_bundles
from .ingest import (
    IngestReport,
    IngestionError,
    IngestionRunner,
    ProjectIdentity,
    SessionEvent,
    SourceSpec,
    parse_claude_record,
    parse_codex_record,
    parse_hermes_row,
    discover_local_sources,
    project_namespace,
    redact_sensitive_text,
)
from .review import PROJECT_REVIEW_PROFILE, validate_project_review

__all__ = [
    "PalimpsestBinaryResponse",
    "PalimpsestClient",
    "PalimpsestConfigurationError",
    "PalimpsestError",
    "PalimpsestHttpError",
    "PalimpsestResponse",
    "PalimpsestProtocolError",
    "PalimpsestTimeoutError",
    "PalimpsestTransportError",
    "PartialRememberError",
    "compare_project_bundles",
    "PROJECT_REVIEW_PROFILE",
    "validate_project_review",
    "IngestReport",
    "IngestionError",
    "IngestionRunner",
    "ProjectIdentity",
    "SessionEvent",
    "SourceSpec",
    "parse_claude_record",
    "parse_codex_record",
    "parse_hermes_row",
    "discover_local_sources",
    "project_namespace",
    "redact_sensitive_text",
]
