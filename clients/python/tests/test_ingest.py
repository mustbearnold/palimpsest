#!/usr/bin/env python3
"""Behavioral tests for project-aware agent-session ingestion."""

from __future__ import annotations

import sys
import unittest
import json
import sqlite3
import tempfile
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from palimpsest.ingest import (  # noqa: E402
    ProjectIdentity,
    IngestionRunner,
    parse_claude_record,
    parse_codex_record,
    parse_hermes_row,
    discover_local_sources,
    project_namespace,
    redact_sensitive_text,
    SourceSpec,
)


class IngestionBoundaryTests(unittest.TestCase):
    def test_local_discovery_checks_only_exact_provider_locations(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            home = Path(temporary_directory)
            (home / ".codex" / "sessions").mkdir(parents=True)
            (home / ".claude" / "projects").mkdir(parents=True)
            (home / ".hermes").mkdir()
            (home / ".hermes" / "state.db").write_bytes(b"sqlite placeholder")
            (home / "unrelated" / "nested").mkdir(parents=True)
            (home / "unrelated" / "nested" / "secret.jsonl").write_text("{}\n", encoding="utf-8")

            sources = discover_local_sources(home=home)

            self.assertEqual(
                [(source.kind, source.path) for source in sources],
                [
                    ("codex", (home / ".codex" / "sessions").resolve()),
                    ("claude", (home / ".claude" / "projects").resolve()),
                    ("hermes", (home / ".hermes" / "state.db").resolve()),
                ],
            )

    def test_local_discovery_omits_missing_stores_for_later_watch_polls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            home = Path(temporary_directory)
            self.assertEqual(discover_local_sources(home=home), ())
            (home / ".codex" / "sessions").mkdir(parents=True)
            sources = discover_local_sources(home=home)
            self.assertEqual([source.kind for source in sources], ["codex"])

    def test_local_discovery_does_not_follow_a_conventional_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            home = root / "home"
            external = root / "external-codex"
            external.mkdir(parents=True)
            home.mkdir()
            (home / ".codex").mkdir()
            (home / ".codex" / "sessions").symlink_to(external, target_is_directory=True)

            self.assertEqual(discover_local_sources(home=home), ())

    def test_project_identity_and_secret_redaction_are_stable(self) -> None:
        first = ProjectIdentity.from_context("/tmp/project-a", branch="main")
        second = ProjectIdentity.from_context("/tmp/project-b", branch="main")

        self.assertNotEqual(first.project_id, second.project_id)
        self.assertEqual(first.project_id, ProjectIdentity.from_context("/tmp/project-a", branch="dev").project_id)
        self.assertEqual(first.branch, "main")
        self.assertEqual(
            project_namespace(first.project_id),
            f"agent_session:{first.project_id}",
        )
        self.assertNotIn("ghp_example_secret", redact_sensitive_text("token=ghp_example_secret"))
        self.assertIn("[REDACTED]", redact_sensitive_text("token=ghp_example_secret"))

    def test_codex_and_claude_text_events_preserve_role_and_project_scope(self) -> None:
        codex_record = {
            "type": "event_msg",
            "timestamp": "2026-08-03T03:04:05Z",
            "payload": {
                "type": "user_message",
                "message": "Fix project A; token=ghp_example_secret",
            },
        }
        codex = parse_codex_record(
            codex_record,
            line_number=7,
            source_path="/tmp/codex/session-a.jsonl",
            session_meta={
                "session_id": "codex-session-a",
                "cwd": "/tmp/project-a",
                "branch": "main",
            },
        )
        self.assertIsNotNone(codex)
        assert codex is not None
        self.assertEqual(codex.source, "codex")
        self.assertEqual(codex.role, "user")
        self.assertNotIn("ghp_example_secret", codex.content)
        self.assertEqual(codex.project.project_id, ProjectIdentity.from_context("/tmp/project-a").project_id)

        claude_record = {
            "type": "assistant",
            "sessionId": "claude-session-a",
            "cwd": "/tmp/project-a",
            "gitBranch": "main",
            "timestamp": "2026-08-03T03:04:06Z",
            "uuid": "claude-message-a",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "private reasoning"},
                    {"type": "text", "text": "Project A uses the Rust service."},
                    {"type": "tool_use", "name": "rg", "input": {"pattern": "secret"}},
                ],
            },
        }
        claude = parse_claude_record(
            claude_record,
            line_number=8,
            source_path="/tmp/claude/project-a.jsonl",
        )
        self.assertIsNotNone(claude)
        assert claude is not None
        self.assertEqual(claude.source, "claude")
        self.assertEqual(claude.role, "assistant")
        self.assertEqual(claude.content, "Project A uses the Rust service.")
        self.assertEqual(claude.project.project_id, codex.project.project_id)

        self.assertIsNone(
            parse_codex_record(
                {"type": "event_msg", "payload": {"type": "agent_reasoning", "message": "skip"}},
                line_number=9,
                source_path="/tmp/codex/session-a.jsonl",
                session_meta={},
            )
        )
        self.assertIsNone(
            parse_claude_record(
                {
                    **claude_record,
                    "message": {"role": "assistant", "content": [{"type": "tool_use", "name": "rg"}]},
                },
                line_number=10,
                source_path="/tmp/claude/project-a.jsonl",
            )
        )

    def test_hermes_rows_use_readable_messages_and_unix_observed_time(self) -> None:
        event = parse_hermes_row(
            {
                "id": 42,
                "session_id": "hermes-session-a",
                "role": "user",
                "content": '\x00json:' + json.dumps([{"type": "text", "text": "Project B note"}]),
                "timestamp": 1_754_209_446.0,
                "cwd": "/tmp/project-b",
                "git_branch": "feature-x",
                "git_repo_root": "/tmp/project-b",
            },
            source_path="/tmp/hermes/state.db",
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.source, "hermes")
        self.assertEqual(event.event_id, "42")
        self.assertEqual(event.role, "user")
        self.assertEqual(event.content, "Project B note")
        self.assertEqual(event.project.project_id, ProjectIdentity.from_context("/tmp/project-b").project_id)
        self.assertIsNone(
            parse_hermes_row(
                {"id": 43, "session_id": "hermes-session-a", "role": "tool", "content": "skip"},
                source_path="/tmp/hermes/state.db",
            )
        )

    def test_runner_deduplicates_and_keeps_projects_in_separate_namespaces(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = []

            def remember(self, content: str, **kwargs: object) -> dict[str, object]:
                self.calls.append((content, kwargs))
                return {"episode": {"episode_id": "episode"}, "fact": {"fact_id": "fact"}}

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            codex_path = root / "codex" / "session.jsonl"
            claude_path = root / "claude" / "session.jsonl"
            for project in (root / "project-a", root / "project-b"):
                (project / ".git").mkdir(parents=True)
            codex_path.parent.mkdir()
            claude_path.parent.mkdir()
            codex_path.write_text(
                json.dumps({
                    "type": "session_meta",
                    "payload": {"session_id": "codex-a", "cwd": str(root / "project-a")},
                })
                + "\n"
                + json.dumps({
                    "type": "event_msg",
                    "timestamp": "2026-08-03T03:04:05Z",
                    "payload": {"type": "user_message", "message": "Project A decision"},
                })
                + "\n",
                encoding="utf-8",
            )
            claude_path.write_text(
                json.dumps({
                    "type": "user",
                    "sessionId": "claude-b",
                    "cwd": str(root / "project-b"),
                    "timestamp": "2026-08-03T03:04:06Z",
                    "uuid": "claude-b-message",
                    "message": {"role": "user", "content": "Project B decision"},
                })
                + "\n",
                encoding="utf-8",
            )
            client = FakeClient()
            runner = IngestionRunner(
                client,
                [
                    SourceSpec("codex", codex_path),
                    SourceSpec("claude", claude_path),
                ],
                state_path=root / "state.json",
                backfill=True,
            )

            first = runner.run_once()
            self.assertEqual(first.ingested, 2)
            self.assertEqual(len(client.calls), 2)
            self.assertNotEqual(client.calls[0][1]["namespace"], client.calls[1][1]["namespace"])
            self.assertTrue(all(call[1]["metadata"]["project_id"].startswith("project-") for call in client.calls))

            second = runner.run_once()
            self.assertEqual(second.ingested, 0)
            self.assertEqual(len(client.calls), 2)

            with codex_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({
                        "type": "event_msg",
                        "timestamp": "2026-08-03T03:04:07Z",
                        "payload": {"type": "agent_message", "message": "Project A follow-up"},
                    })
                    + "\n"
                )
            third = runner.run_once()
            self.assertEqual(third.ingested, 1)
            self.assertEqual(len(client.calls), 3)

    def test_runner_reads_hermes_state_database_read_only_and_resumes(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = []

            def remember(self, content: str, **kwargs: object) -> dict[str, object]:
                self.calls.append((content, kwargs))
                return {"episode": {"episode_id": "episode"}, "fact": {"fact_id": "fact"}}

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            database_path = root / "state.db"
            connection = sqlite3.connect(database_path)
            connection.executescript(
                """
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    cwd TEXT,
                    git_branch TEXT,
                    git_repo_root TEXT
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT,
                    timestamp REAL NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO sessions (id, cwd, git_branch, git_repo_root) VALUES (?, ?, ?, ?)",
                ("hermes-a", str(root / "project-c"), "main", str(root / "project-c")),
            )
            connection.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                ("hermes-a", "user", "first Hermes message", 1_754_209_446.0),
            )
            connection.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                ("hermes-a", "tool", "tool output must not be ingested", 1_754_209_447.0),
            )
            connection.commit()
            connection.close()

            client = FakeClient()
            runner = IngestionRunner(
                client,
                [SourceSpec("hermes", database_path)],
                state_path=root / "hermes-state.json",
                backfill=True,
            )
            first = runner.run_once()
            self.assertEqual(first.ingested, 1)
            self.assertEqual(len(client.calls), 1)

            second = runner.run_once()
            self.assertEqual(second.ingested, 0)

            connection = sqlite3.connect(database_path)
            connection.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                ("hermes-a", "assistant", "second Hermes message", 1_754_209_448.0),
            )
            connection.commit()
            connection.close()
            third = runner.run_once()
            self.assertEqual(third.ingested, 1)
            self.assertEqual(len(client.calls), 2)

    def test_default_first_poll_baselines_old_transcript_content(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = []

            def remember(self, content: str, **kwargs: object) -> dict[str, object]:
                self.calls.append((content, kwargs))
                return {"episode": {"episode_id": "episode"}, "fact": {"fact_id": "fact"}}

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "session.jsonl"
            path.write_text(
                json.dumps({
                    "type": "event_msg",
                    "timestamp": "2026-08-03T03:04:05Z",
                    "payload": {"type": "user_message", "message": "old transcript"},
                })
                + "\n",
                encoding="utf-8",
            )
            client = FakeClient()
            runner = IngestionRunner(
                client,
                [SourceSpec("codex", path)],
                state_path=root / "state.json",
            )
            baseline = runner.run_once()
            self.assertEqual(baseline.baselined, 1)
            self.assertEqual(baseline.ingested, 0)
            self.assertEqual(client.calls, [])

            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({
                        "type": "event_msg",
                        "timestamp": "2026-08-03T03:04:06Z",
                        "payload": {"type": "user_message", "message": "new transcript"},
                    })
                    + "\n"
                )
            report = runner.run_once()
            self.assertEqual(report.ingested, 1)
            self.assertEqual(client.calls[0][0], "new transcript")

    def test_incomplete_jsonl_record_is_retried_when_the_writer_finishes_it(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls = []

            def remember(self, content: str, **kwargs: object) -> dict[str, object]:
                self.calls.append((content, kwargs))
                return {"episode": {"episode_id": "episode"}, "fact": {"fact_id": "fact"}}

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = root / "session.jsonl"
            record = json.dumps({
                "type": "event_msg",
                "timestamp": "2026-08-03T03:04:05Z",
                "payload": {"type": "user_message", "message": "event after flush"},
            })
            path.write_text(record, encoding="utf-8")
            client = FakeClient()
            runner = IngestionRunner(client, [SourceSpec("codex", path)], state_path=root / "state.json")
            runner.run_once()
            with path.open("a", encoding="utf-8") as handle:
                handle.write("\n")
            report = runner.run_once()
            self.assertEqual(report.ingested, 1)
            self.assertEqual(client.calls[0][0], "event after flush")


if __name__ == "__main__":
    unittest.main()
