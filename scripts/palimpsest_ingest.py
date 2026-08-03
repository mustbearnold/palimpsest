#!/usr/bin/env python3
"""Opt-in polling bridge from agent session stores to Palimpsest.

This process has no source defaults. The caller must name each local source
path explicitly, which is especially important when a path belongs to another
user. It writes only through the authorized Palimpsest HTTP client.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients/python/src"))
from palimpsest import (  # noqa: E402
    IngestionError,
    IngestionRunner,
    PalimpsestClient,
    PalimpsestError,
    SourceSpec,
)


LOCAL_DEFAULT_TOKEN = "palimpsest-local-development-token"
LOCAL_DEFAULT_TENANT = "019be000-0000-7000-8000-000000000010"
LOCAL_DEFAULT_SUBJECT = "019be000-0000-7000-8000-000000000020"
LOCAL_DEFAULT_CASE = "019be000-0000-7000-8000-000000000030"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly ingest new text-only Codex, Claude Code, or Hermes "
            "session events into project-scoped Palimpsest namespaces."
        )
    )
    parser.add_argument("command", choices=("once", "watch"))
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        metavar="KIND=PATH",
        help="explicit source path; KIND is codex, claude, or hermes (repeatable)",
    )
    parser.add_argument("--base-url", default=os.environ.get("PALIMPSEST_INGEST_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--bearer-token", default=os.environ.get("PALIMPSEST_INGEST_BEARER_TOKEN"))
    parser.add_argument("--tenant-id", default=os.environ.get("PALIMPSEST_INGEST_TENANT_ID", LOCAL_DEFAULT_TENANT))
    parser.add_argument("--subject-id", default=os.environ.get("PALIMPSEST_INGEST_SUBJECT_ID", LOCAL_DEFAULT_SUBJECT))
    parser.add_argument("--case-id", default=os.environ.get("PALIMPSEST_INGEST_CASE_ID", LOCAL_DEFAULT_CASE))
    parser.add_argument(
        "--state-path",
        default=os.environ.get(
            "PALIMPSEST_INGEST_STATE_PATH",
            str(Path.home() / ".local/state/palimpsest/ingest-state.json"),
        ),
    )
    parser.add_argument("--project-root", default=os.environ.get("PALIMPSEST_INGEST_PROJECT_ROOT"))
    parser.add_argument("--namespace-prefix", default="agent_session")
    parser.add_argument("--sensitivity", default="internal")
    parser.add_argument("--retention-policy-id", default="standard")
    parser.add_argument("--backfill", action="store_true", help="ingest existing source history on the first pass")
    parser.add_argument("--interval-seconds", type=float, default=5.0, help="watch polling interval")
    return parser


def _source_specs(values: list[str]) -> list[SourceSpec]:
    specs = []
    for value in values:
        kind, separator, path = value.partition("=")
        if not separator or not path:
            raise ValueError("--source must use KIND=PATH")
        specs.append(SourceSpec(kind.strip(), Path(path)))
    return specs


def _client(arguments: argparse.Namespace) -> PalimpsestClient:
    base_url = arguments.base_url.rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("--base-url must be an HTTP(S) URL without a query or fragment")
    bearer_token = arguments.bearer_token
    if not bearer_token:
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("--bearer-token or PALIMPSEST_INGEST_BEARER_TOKEN is required for a non-local URL")
        bearer_token = LOCAL_DEFAULT_TOKEN
    return PalimpsestClient(
        base_url=base_url,
        bearer_token=bearer_token,
        tenant_id=arguments.tenant_id,
        subject_id=arguments.subject_id,
        case_id=arguments.case_id,
    )


def _run_once(arguments: argparse.Namespace, runner: IngestionRunner) -> int:
    report = runner.run_once()
    print(json.dumps({"report": report.as_dict()}, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be greater than zero")
    runner = IngestionRunner(
        _client(arguments),
        _source_specs(arguments.source),
        state_path=arguments.state_path,
        backfill=arguments.backfill,
        project_root=arguments.project_root,
        namespace_prefix=arguments.namespace_prefix,
        sensitivity=arguments.sensitivity,
        retention_policy_id=arguments.retention_policy_id,
    )
    if arguments.command == "once":
        return _run_once(arguments, runner)
    while True:
        _run_once(arguments, runner)
        time.sleep(arguments.interval_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (IngestionError, PalimpsestError, ValueError) as exc:
        print(f"palimpsest-ingest: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    except KeyboardInterrupt:
        raise SystemExit(0) from None
