#!/usr/bin/env python3
"""Opt-in polling bridge from agent session stores to Palimpsest.

The caller must either name each local source path explicitly or opt into the
narrow current-user discovery mode. It writes only through the authorized
Palimpsest HTTP client.
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
    discover_local_sources,
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
        default=[],
        metavar="KIND=PATH",
        help="explicit source path; KIND is codex, claude, or hermes (repeatable)",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="also check the exact current-user Codex, Claude, and Hermes locations",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PALIMPSEST_INGEST_BASE_URL", "http://127.0.0.1:8080"),
    )
    parser.add_argument(
        "--bearer-token", default=os.environ.get("PALIMPSEST_INGEST_BEARER_TOKEN")
    )
    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("PALIMPSEST_INGEST_TENANT_ID", LOCAL_DEFAULT_TENANT),
    )
    parser.add_argument(
        "--subject-id",
        default=os.environ.get("PALIMPSEST_INGEST_SUBJECT_ID", LOCAL_DEFAULT_SUBJECT),
    )
    parser.add_argument(
        "--case-id",
        default=os.environ.get("PALIMPSEST_INGEST_CASE_ID", LOCAL_DEFAULT_CASE),
    )
    parser.add_argument(
        "--state-path",
        default=os.environ.get(
            "PALIMPSEST_INGEST_STATE_PATH",
            str(Path.home() / ".local/state/palimpsest/ingest-state.json"),
        ),
    )
    parser.add_argument(
        "--project-root", default=os.environ.get("PALIMPSEST_INGEST_PROJECT_ROOT")
    )
    parser.add_argument(
        "--codex-sessions", default=os.environ.get("PALIMPSEST_INGEST_CODEX_SESSIONS")
    )
    parser.add_argument(
        "--claude-projects", default=os.environ.get("PALIMPSEST_INGEST_CLAUDE_PROJECTS")
    )
    parser.add_argument(
        "--hermes-state-db", default=os.environ.get("PALIMPSEST_INGEST_HERMES_STATE_DB")
    )
    parser.add_argument("--namespace-prefix", default="agent_session")
    parser.add_argument("--sensitivity", default="internal")
    parser.add_argument("--retention-policy-id", default="standard")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="ingest existing source history on the first pass",
    )
    parser.add_argument(
        "--interval-seconds", type=float, default=5.0, help="watch polling interval"
    )
    return parser


def _source_specs(values: list[str]) -> list[SourceSpec]:
    specs = []
    for value in values:
        kind, separator, path = value.partition("=")
        if not separator or not path:
            raise ValueError("--source must use KIND=PATH")
        specs.append(SourceSpec(kind.strip(), Path(path)))
    return specs


def _sources(
    arguments: argparse.Namespace, *, allow_empty: bool = False
) -> list[SourceSpec]:
    specs = _source_specs(arguments.source)
    if arguments.discover:
        specs.extend(
            discover_local_sources(
                codex_sessions=arguments.codex_sessions,
                claude_projects=arguments.claude_projects,
                hermes_state_db=arguments.hermes_state_db,
            )
        )
    deduplicated = []
    seen = set()
    for spec in specs:
        key = (spec.kind, spec.path)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(spec)
    if not deduplicated and not allow_empty:
        raise IngestionError("no ingestion sources were selected or discovered")
    return deduplicated


def _client(arguments: argparse.Namespace) -> PalimpsestClient:
    base_url = arguments.base_url.rstrip("/")
    parsed = urlparse(base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "--base-url must be an HTTP(S) URL without a query or fragment"
        )
    bearer_token = arguments.bearer_token
    if not bearer_token:
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError(
                "--bearer-token or PALIMPSEST_INGEST_BEARER_TOKEN is required for a non-local URL"
            )
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
    if not arguments.source and not arguments.discover:
        raise ValueError("provide --source KIND=PATH or opt into --discover")
    if arguments.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be greater than zero")
    client = _client(arguments)
    if arguments.command == "once":
        runner = IngestionRunner(
            client,
            _sources(arguments),
            state_path=arguments.state_path,
            backfill=arguments.backfill,
            project_root=arguments.project_root,
            namespace_prefix=arguments.namespace_prefix,
            sensitivity=arguments.sensitivity,
            retention_policy_id=arguments.retention_policy_id,
        )
        return _run_once(arguments, runner)
    while True:
        sources = _sources(arguments, allow_empty=True)
        if sources:
            runner = IngestionRunner(
                client,
                sources,
                state_path=arguments.state_path,
                backfill=arguments.backfill,
                project_root=arguments.project_root,
                namespace_prefix=arguments.namespace_prefix,
                sensitivity=arguments.sensitivity,
                retention_policy_id=arguments.retention_policy_id,
            )
            _run_once(arguments, runner)
        else:
            print(
                json.dumps(
                    {
                        "report": {
                            "seen": 0,
                            "ingested": 0,
                            "skipped": 0,
                            "baselined": 0,
                            "project_ids": [],
                        },
                        "sources": [],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        time.sleep(arguments.interval_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (IngestionError, PalimpsestError, ValueError) as exc:
        print(f"palimpsest-ingest: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    except KeyboardInterrupt:
        raise SystemExit(0) from None
