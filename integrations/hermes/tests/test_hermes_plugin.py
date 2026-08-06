"""Tests for the Palimpsest Hermes memory provider plugin (spec 013).

Runs standalone (stdlib unittest + a fake HTTP server) — no Hermes core, no
live Palimpsest service required. The plugin modules are loaded under a
synthetic package name so relative imports resolve exactly as the Hermes
memory plugin loader resolves them.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

PLUGIN_DIR = Path(__file__).resolve().parents[1]
_PKG = "_palimpsest_plugin_under_test"

_TENANT = "019be000-0000-7000-8000-000000000010"
_SUBJECT = "019be000-0000-7000-8000-000000000020"
_CASE = "019be000-0000-7000-8000-000000000030"

_SCOPE = f"/v1/tenants/{_TENANT}/subjects/{_SUBJECT}"

# RFC 3339 UTC ending in Z with at most six fractional digits (server contract).
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$")


def _load_plugin():
    """Load the plugin __init__.py as a package named _PKG (relative imports work)."""
    if _PKG in sys.modules:
        return sys.modules[_PKG]
    spec = importlib.util.spec_from_file_location(
        _PKG, PLUGIN_DIR / "__init__.py", submodule_search_locations=[str(PLUGIN_DIR)]
    )
    if spec is None or spec.loader is None:
        raise ImportError("cannot load plugin under test")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[_PKG] = mod
    spec.loader.exec_module(mod)
    return mod


plugin = _load_plugin()
PalimpsestClient = plugin.PalimpsestClient
PalimpsestConfig = plugin.PalimpsestConfig
PalimpsestError = plugin.PalimpsestError
PalimpsestConfigError = plugin.PalimpsestConfigError
PalimpsestMemoryProvider = plugin.PalimpsestMemoryProvider
PalimpsestWriteQueue = plugin.PalimpsestWriteQueue
_content_key = plugin._content_key
format_receipt = plugin.format_receipt


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


# ---------------------------------------------------------------------------
# Fake Palimpsest HTTP service
# ---------------------------------------------------------------------------


class FakePalimpsestServer:
    """Threaded HTTP server recording requests and serving canned responses."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.fail_episodes = False
        self.episode_fail_countdown = 0
        self.fail_recall = False
        self.health_ok = True
        self.recall_delay_seconds = 0.0
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_factory())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[0], self._server.server_address[1]
        return f"http://{host}:{port}"

    def _handler_factory(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                pass

            def _record(self, body: bytes = b"") -> None:
                try:
                    payload = json.loads(body.decode("utf-8")) if body else None
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload = body.decode("utf-8", errors="replace")
                server.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": payload,
                    }
                )

            def _send(self, status: int, payload: dict | None) -> None:
                encoded = (
                    json.dumps(payload).encode("utf-8") if payload is not None else b""
                )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_GET(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                self._record(self.rfile.read(length))
                if self.path == "/healthz":
                    if server.health_ok:
                        self._send(200, None)  # real /healthz is content-free
                    else:
                        self._send(503, {"type": "unavailable"})
                    return
                self._send(404, {"type": "not_found"})

            def _require_idempotency_key(self) -> bool:
                if "idempotency-key" in {k.lower() for k in self.headers}:
                    return True
                self._send(
                    400,
                    {
                        "type": "https://palimpsest.dev/problems/idempotency-key-required",
                        "title": "Idempotency key is required",
                        "status": 400,
                    },
                )
                return False

            def _require_valid_timestamp(self, body: dict | None) -> bool:
                observed = (body or {}).get("observed_at", "")
                if isinstance(observed, str) and _TS_RE.match(observed):
                    return True
                self._send(
                    400,
                    {
                        "type": "https://palimpsest.dev/problems/invalid-request",
                        "title": "Timestamp is invalid",
                        "status": 400,
                        "code": "invalid_timestamp",
                        "detail": "observed_at: timestamp must be RFC 3339 UTC ending in Z",
                    },
                )
                return False

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                self._record(self.rfile.read(length))
                if (
                    self.path
                    in {
                        f"{_SCOPE}/retrievals",
                        f"{_SCOPE}/episodes",
                        f"{_SCOPE}/facts",
                    }
                    and not self._require_idempotency_key()
                ):
                    return  # the real server 400s durable calls without the key
                if self.path in {
                    f"{_SCOPE}/episodes",
                    f"{_SCOPE}/facts",
                } and not self._require_valid_timestamp(server.requests[-1]["body"]):
                    return  # the real server validates observed_at strictly
                if self.path == f"{_SCOPE}/retrievals":
                    if server.fail_recall:
                        self._send(503, {"type": "unavailable"})
                        return
                    if server.recall_delay_seconds > 0:
                        time.sleep(server.recall_delay_seconds)
                    self._send(
                        200,
                        {
                            "tenant_id": _TENANT,
                            "subject_id": _SUBJECT,
                            "retrieval_id": str(uuid.uuid4()),
                            "status": "results",
                            "items": [
                                {
                                    "memory_kind": "fact_revision",
                                    "fact_id": str(uuid.uuid4()),
                                    "revision_id": str(uuid.uuid4()),
                                    "namespace": "hermes",
                                    "key": "hermes-abc123",
                                    "value": {
                                        "content": "The user prefers duck-typed interfaces.",
                                        "metadata": {},
                                    },
                                    "evidence_episode_ids": [str(uuid.uuid4())],
                                    "scores": [{"component": "lexical", "value": 1.0}],
                                }
                            ],
                        },
                    )
                    return
                if self.path == f"{_SCOPE}/episodes":
                    if server.episode_fail_countdown > 0:
                        server.episode_fail_countdown -= 1
                        self._send(503, {"type": "unavailable"})
                        return
                    if server.fail_episodes:
                        self._send(503, {"type": "unavailable"})
                        return
                    self._send(200, {"episode_id": str(uuid.uuid4())})
                    return
                if self.path == f"{_SCOPE}/facts":
                    self._send(200, {"fact_id": str(uuid.uuid4())})
                    return
                self._send(404, {"type": "not_found"})

        return Handler

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def requests_for(self, method: str, suffix: str) -> list[dict[str, Any]]:
        return [
            r
            for r in self.requests
            if r["method"] == method and r["path"].endswith(suffix)
        ]


# ---------------------------------------------------------------------------
# Test case
# ---------------------------------------------------------------------------


class PluginTestCase(unittest.TestCase):
    """Base: fake server + isolated HERMES_HOME and PALIMPSEST_* env."""

    def setUp(self) -> None:
        self.server = FakePalimpsestServer()
        self._tmp = tempfile.TemporaryDirectory()
        self.hermes_home = Path(self._tmp.name) / ".hermes"
        self.hermes_home.mkdir()
        self._saved_env = {
            name: os.environ.get(name)
            for name in (
                "HERMES_HOME",
                "PALIMPSEST_BASE_URL",
                "PALIMPSEST_MCP_BASE_URL",
                "PALIMPSEST_BEARER_TOKEN",
                "PALIMPSEST_TENANT_ID",
                "PALIMPSEST_SUBJECT_ID",
                "PALIMPSEST_CASE_ID",
                "PALIMPSEST_NAMESPACE",
            )
        }
        os.environ["HERMES_HOME"] = str(self.hermes_home)
        os.environ["PALIMPSEST_BASE_URL"] = self.server.base_url
        os.environ.pop("PALIMPSEST_MCP_BASE_URL", None)
        os.environ.pop("PALIMPSEST_BEARER_TOKEN", None)  # localhost default applies
        os.environ["PALIMPSEST_TENANT_ID"] = _TENANT
        os.environ["PALIMPSEST_SUBJECT_ID"] = _SUBJECT
        os.environ["PALIMPSEST_CASE_ID"] = _CASE
        os.environ.pop("PALIMPSEST_NAMESPACE", None)

    def tearDown(self) -> None:
        self.server.stop()
        for name, value in self._saved_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self._tmp.cleanup()

    def make_provider(self, session_id: str = "sess-1") -> PalimpsestMemoryProvider:
        provider = PalimpsestMemoryProvider()
        provider.initialize(
            session_id, hermes_home=str(self.hermes_home), platform="cli"
        )
        self.addCleanup(provider.shutdown)
        return provider


# -- configuration ------------------------------------------------------------


class TestConfig(PluginTestCase):
    def test_env_over_file_over_defaults(self) -> None:
        config = PalimpsestConfig.load(str(self.hermes_home))
        self.assertEqual(config.base_url, self.server.base_url)
        self.assertEqual(config.tenant_id, _TENANT)
        self.assertEqual(config.namespace, "hermes")
        self.assertEqual(config.bearer_token, "palimpsest-local-development-token")

    def test_file_values_used_when_env_unset(self) -> None:
        for name in (
            "PALIMPSEST_BASE_URL",
            "PALIMPSEST_TENANT_ID",
            "PALIMPSEST_SUBJECT_ID",
            "PALIMPSEST_CASE_ID",
        ):
            os.environ.pop(name, None)
        (self.hermes_home / "palimpsest.json").write_text(
            json.dumps(
                {
                    "base_url": self.server.base_url,
                    "tenant_id": _TENANT,
                    "subject_id": _SUBJECT,
                    "case_id": _CASE,
                    "namespace": "hermes-test",
                }
            ),
            encoding="utf-8",
        )
        config = PalimpsestConfig.load(str(self.hermes_home))
        self.assertEqual(config.base_url, self.server.base_url)
        self.assertEqual(config.namespace, "hermes-test")

    def test_mcp_base_url_fallback(self) -> None:
        os.environ.pop("PALIMPSEST_BASE_URL", None)
        os.environ["PALIMPSEST_MCP_BASE_URL"] = self.server.base_url
        config = PalimpsestConfig.load(str(self.hermes_home))
        self.assertEqual(config.base_url, self.server.base_url)

    def test_remote_url_requires_token(self) -> None:
        os.environ["PALIMPSEST_BASE_URL"] = "https://palimpsest.example.com"
        with self.assertRaises(PalimpsestError):
            PalimpsestConfig.load(str(self.hermes_home))

    def test_invalid_base_url_rejected(self) -> None:
        os.environ["PALIMPSEST_BASE_URL"] = "not-a-url"
        with self.assertRaises(PalimpsestError):
            PalimpsestConfig.load(str(self.hermes_home))

    def test_invalid_tenant_rejected(self) -> None:
        os.environ["PALIMPSEST_TENANT_ID"] = "not-a-uuid"
        with self.assertRaises(PalimpsestError):
            PalimpsestConfig.load(str(self.hermes_home))

    def test_public_dict_never_exposes_token(self) -> None:
        config = PalimpsestConfig.load(str(self.hermes_home))
        public = config.public_dict()
        self.assertNotIn("bearer_token", public)
        self.assertNotIn("local-development-token", json.dumps(public, sort_keys=True))
        self.assertIsInstance(public["token_configured"], bool)
        self.assertTrue(public["token_configured"])


# -- HTTP client --------------------------------------------------------------


class TestClient(PluginTestCase):
    def test_health(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        self.assertTrue(client.health())
        self.server.health_ok = False
        self.assertFalse(client.health())

    def test_recall_posts_to_retrievals(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        receipt = client.recall("duck typing", page_size=3)
        self.assertEqual(receipt["status"], "results")
        self.assertTrue(receipt["retrieval_id"])
        self.assertTrue(receipt["items"])
        request = self.server.requests_for("POST", "/retrievals")[0]
        self.assertEqual(request["body"]["query"], "duck typing")
        self.assertEqual(request["body"]["page_size"], 3)
        self.assertEqual(
            request["headers"]["authorization"],
            "Bearer palimpsest-local-development-token",
        )
        self.assertIn(
            "idempotency-key",
            request["headers"],
            "the server requires an Idempotency-Key on durable calls",
        )
        self.assertTrue(
            request["headers"]["idempotency-key"].startswith("palimpsest-hermes-")
        )

    def test_append_episode_normalizes_legacy_timestamp(self) -> None:
        """Buffered rows with the legacy +00:00 format must still flush (spec R4)."""
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        client.append_episode(
            kind="hermes_turn",
            observed_at="2026-08-05T23:58:36.085+00:00",
            provenance={
                "source_type": "hermes.test",
                "source_uri": None,
                "external_id": None,
            },
            sensitivity="internal",
            retention_policy_id="standard",
            payload={"content": "legacy row"},
            idempotency_key="ts-test-1",
        )
        request = self.server.requests_for("POST", "/episodes")[0]
        self.assertEqual(request["body"]["observed_at"], "2026-08-05T23:58:36.085000Z")

    def test_recall_rejects_empty_query(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        with self.assertRaises(PalimpsestConfigError):
            client.recall("   ")

    def test_recall_rejects_overlong_query_ascii(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        with self.assertRaises(PalimpsestConfigError):
            client.recall("x" * 4097)

    def test_recall_rejects_overlong_query_multibyte(self) -> None:
        # 2049 x "é" = 4098 UTF-8 bytes; a char-count check would let it through
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        with self.assertRaises(PalimpsestConfigError):
            client.recall("é" * 2049)

    def test_remember_rejects_empty_content(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        with self.assertRaises(PalimpsestConfigError):
            client.remember("   ", key="k")

    def test_remember_appends_episode_then_fact(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        observed = client.remember("remember this", key="hermes-key-1")
        episodes = self.server.requests_for("POST", "/episodes")
        facts = self.server.requests_for("POST", "/facts")
        self.assertEqual(len(episodes), 1)
        self.assertEqual(len(facts), 1)
        self.assertEqual(episodes[0]["body"]["kind"], "hermes_memory")
        self.assertEqual(
            episodes[0]["body"]["provenance"]["source_type"], "hermes.memory"
        )
        self.assertEqual(facts[0]["body"]["namespace"], "hermes")
        self.assertEqual(
            facts[0]["body"]["write_policy"], {"id": "direct-evidence", "version": "1"}
        )
        self.assertEqual(
            facts[0]["body"]["evidence_episode_ids"],
            [
                episodes[0]["body"].get("episode_id")
                or observed["episode"]["episode_id"]
            ],
        )
        self.assertTrue(observed["episode"]["episode_id"])
        self.assertTrue(observed["fact"]["fact_id"])

    def test_http_error_raises(self) -> None:
        client = PalimpsestClient(PalimpsestConfig.load(str(self.hermes_home)))
        self.server.fail_episodes = True
        with self.assertRaises(PalimpsestError):
            client.remember("boom")
        self.server.fail_episodes = False

    def test_format_receipt(self) -> None:
        receipt = {
            "items": [
                {
                    "namespace": "hermes",
                    "key": "k1",
                    "value": {"content": "  Some   long   text  "},
                },
                {"namespace": "hermes", "key": "k2", "value": {"content": ""}},
            ]
        }
        block = format_receipt(receipt)
        self.assertIn("[Palimpsest Memory]", block)
        self.assertIn("[hermes:k1] Some long text", block)
        self.assertNotIn("k2", block)
        self.assertEqual(format_receipt({"items": []}), "")


# -- provider identity and lifecycle ------------------------------------------


class TestProviderLifecycle(PluginTestCase):
    def test_name(self) -> None:
        self.assertEqual(PalimpsestMemoryProvider().name, "palimpsest")

    def test_is_available_no_network(self) -> None:
        provider = PalimpsestMemoryProvider()
        self.assertTrue(provider.is_available())
        self.assertEqual(len(self.server.requests), 0)  # no network call

    def test_is_available_false_when_invalid(self) -> None:
        os.environ["PALIMPSEST_BASE_URL"] = "https://remote.example.com"  # no token
        self.assertFalse(PalimpsestMemoryProvider().is_available())

    def test_initialize_sets_queue_and_client(self) -> None:
        provider = self.make_provider()
        self.assertIsNotNone(provider._queue)
        self.assertIsNotNone(provider._client)
        self.assertTrue((self.hermes_home / "palimpsest" / "pending.db").exists())

    def test_config_schema_marks_token_secret(self) -> None:
        schema = {
            field["key"]: field
            for field in PalimpsestMemoryProvider().get_config_schema()
        }
        self.assertEqual(schema["bearer_token"]["env_var"], "PALIMPSEST_BEARER_TOKEN")
        self.assertTrue(schema["bearer_token"]["secret"])
        self.assertEqual(schema["base_url"]["default"], "http://127.0.0.1:8080")

    def test_config_schema_case_id_optional(self) -> None:
        schema = {
            field["key"]: field
            for field in PalimpsestMemoryProvider().get_config_schema()
        }
        self.assertFalse(schema["case_id"]["required"], "spec R7: case_id is optional")

    def test_save_config_writes_non_secrets(self) -> None:
        provider = PalimpsestMemoryProvider()
        provider.save_config(
            {
                "base_url": self.server.base_url,
                "tenant_id": _TENANT,
                "namespace": "hermes-x",
            },
            str(self.hermes_home),
        )
        written = json.loads(
            (self.hermes_home / "palimpsest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(written["namespace"], "hermes-x")
        self.assertNotIn("bearer_token", written)

    def test_system_prompt_block_mentions_tools(self) -> None:
        block = PalimpsestMemoryProvider().system_prompt_block()
        self.assertIn("palimpsest_recall", block)


# -- tools --------------------------------------------------------------------


class TestProviderTools(PluginTestCase):
    def test_tool_surface_is_exactly_three_no_delete(self) -> None:
        names = {
            schema["name"] for schema in PalimpsestMemoryProvider().get_tool_schemas()
        }
        self.assertEqual(
            names, {"palimpsest_recall", "palimpsest_remember", "palimpsest_status"}
        )
        for name in names:
            self.assertNotIn("delete", name)
            self.assertNotIn("export", name)

    def test_recall_tool(self) -> None:
        provider = self.make_provider()
        result = json.loads(
            provider.handle_tool_call(
                "palimpsest_recall", {"query": "duck typing", "top_k": 3}
            )
        )
        self.assertIn("result", result)
        self.assertEqual(result["result"]["status"], "results")

    def test_recall_tool_rejects_bad_args(self) -> None:
        provider = self.make_provider()
        result = json.loads(
            provider.handle_tool_call("palimpsest_recall", {"query": ""})
        )
        self.assertIn("error", result)
        result = json.loads(
            provider.handle_tool_call("palimpsest_recall", {"query": "x", "top_k": 999})
        )
        self.assertIn("error", result)

    def test_remember_tool(self) -> None:
        provider = self.make_provider()
        result = json.loads(
            provider.handle_tool_call(
                "palimpsest_remember",
                {"content": "user-approved fact", "metadata": {"origin": "test"}},
            )
        )
        self.assertEqual(result["result"]["status"], "saved")
        self.assertTrue(result["result"]["episode_id"])
        self.assertTrue(result["result"]["fact_id"])
        episodes = self.server.requests_for("POST", "/episodes")
        self.assertEqual(
            episodes[0]["body"]["provenance"]["source_type"], "hermes.remember"
        )
        self.assertIn("idempotency-key", episodes[0]["headers"])

    def test_status_tool(self) -> None:
        provider = self.make_provider()
        result = json.loads(provider.handle_tool_call("palimpsest_status", {}))
        payload = result["result"]
        self.assertTrue(payload["reachable"])
        self.assertEqual(payload["tenant_id"], _TENANT)
        self.assertNotIn("bearer_token", payload)

    def test_unknown_tool_returns_error(self) -> None:
        provider = self.make_provider()
        result = json.loads(provider.handle_tool_call("palimpsest_delete", {}))
        self.assertIn("error", result)


# -- write-behind queue -------------------------------------------------------


class TestWriteQueue(PluginTestCase):
    def _flush_recorder(self):
        flushed: list[dict] = []
        failures = {"count": 0}

        def flush(payload: dict) -> None:
            if failures["count"] > 0:
                failures["count"] -= 1
                raise RuntimeError("transient failure")
            flushed.append(payload)

        return flushed, flush, failures

    def test_flush_success_removes_row(self) -> None:
        flushed, flush, _ = self._flush_recorder()
        queue = PalimpsestWriteQueue(
            self.hermes_home / "palimpsest" / "pending.db",
            flush,
            retry_delay_seconds=0.05,
        )
        self.addCleanup(queue.shutdown)
        queue.enqueue_episode({"kind": "hermes_turn", "idempotency_key": "k1"})
        self.assertTrue(_wait_until(lambda: len(flushed) == 1))
        self.assertEqual(queue.pending_count(), 0)

    def test_retry_then_success(self) -> None:
        flushed, flush, failures = self._flush_recorder()
        failures["count"] = 1
        queue = PalimpsestWriteQueue(
            self.hermes_home / "palimpsest" / "pending.db",
            flush,
            retry_delay_seconds=0.05,
        )
        self.addCleanup(queue.shutdown)
        queue.enqueue_episode({"kind": "hermes_turn", "idempotency_key": "k2"})
        self.assertTrue(_wait_until(lambda: len(flushed) == 1))
        self.assertEqual(queue.pending_count(), 0)

    def test_turn_numbers_are_stable_per_session(self) -> None:
        _, flush, _ = self._flush_recorder()
        queue = PalimpsestWriteQueue(
            self.hermes_home / "palimpsest" / "pending.db",
            flush,
            retry_delay_seconds=0.05,
        )
        self.addCleanup(queue.shutdown)
        self.assertEqual(queue.next_turn_number("sess-a"), 1)
        self.assertEqual(queue.next_turn_number("sess-a"), 2)
        self.assertEqual(queue.next_turn_number("sess-b"), 1)

    def test_crash_replay_requeues_pending_rows(self) -> None:
        def always_fail(payload: dict) -> None:
            raise RuntimeError("crash before flush")

        db_path = self.hermes_home / "palimpsest" / "pending.db"
        first = PalimpsestWriteQueue(db_path, always_fail, retry_delay_seconds=0.05)
        first.enqueue_episode({"kind": "hermes_turn", "idempotency_key": "k3"})
        first.shutdown()  # simulate crash: row committed to SQLite, never flushed
        self.assertEqual(first.pending_count(), 1)
        replay_flushed: list[dict] = []
        second = PalimpsestWriteQueue(
            db_path, replay_flushed.append, retry_delay_seconds=0.05
        )
        self.addCleanup(second.shutdown)
        self.assertTrue(_wait_until(lambda: len(replay_flushed) == 1))
        self.assertEqual(replay_flushed[0]["idempotency_key"], "k3")
        self.assertEqual(second.pending_count(), 0)

    def test_backlog_beyond_startup_batch_drains(self) -> None:
        """Rows beyond the first 500-batch must drain without a restart (spec R4)."""
        flushed, flush, _ = self._flush_recorder()
        queue = PalimpsestWriteQueue(
            self.hermes_home / "palimpsest" / "pending.db",
            flush,
            retry_delay_seconds=0.02,
        )
        self.addCleanup(queue.shutdown)
        for index in range(510):
            queue.enqueue_episode(
                {"kind": "hermes_turn", "idempotency_key": f"k{index}"}
            )
        self.assertTrue(_wait_until(lambda: len(flushed) == 510, timeout=15.0))
        self.assertEqual(queue.pending_count(), 0)

    def test_sync_turn_enqueues_episode_with_idempotency_key(self) -> None:
        provider = self.make_provider()
        provider.sync_turn("hello", "hi there", session_id="sess-x")
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 1)
        )
        episodes = self.server.requests_for("POST", "/episodes")
        request = episodes[0]
        self.assertEqual(request["body"]["kind"], "hermes_turn")
        self.assertEqual(
            request["body"]["provenance"]["source_type"], "hermes.sync_turn"
        )
        self.assertEqual(request["body"]["payload"]["content"], "hello")
        self.assertEqual(request["body"]["payload"]["assistant_content"], "hi there")
        self.assertEqual(request["body"]["payload"]["session_id"], "sess-x")
        self.assertIn("idempotency-key", request["headers"])
        self.assertEqual(request["headers"]["idempotency-key"], "hermes-turn:sess-x:1")
        # a second turn must not reuse the first key
        provider.sync_turn("again", "ok", session_id="sess-x")
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 2)
        )
        second = self.server.requests_for("POST", "/episodes")[1]
        self.assertEqual(second["headers"]["idempotency-key"], "hermes-turn:sess-x:2")

    def test_sync_turn_never_promotes_facts(self) -> None:
        provider = self.make_provider()
        provider.sync_turn("hello", "hi", session_id="sess-y")
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 1)
        )
        self.assertEqual(len(self.server.requests_for("POST", "/facts")), 0)

    def test_sync_turn_observed_at_is_turn_time_not_flush_time(self) -> None:
        from datetime import datetime, timedelta, timezone

        provider = self.make_provider()
        self.server.recall_delay_seconds = 0.0
        t_before = datetime.now(timezone.utc)
        provider.sync_turn("when", "now", session_id="sess-t")
        t_after = datetime.now(timezone.utc)
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 1)
        )
        observed = self.server.requests_for("POST", "/episodes")[0]["body"][
            "observed_at"
        ]
        parsed = datetime.fromisoformat(observed)
        slack = timedelta(milliseconds=100)  # _utc_now truncates to milliseconds
        self.assertGreaterEqual(
            parsed, t_before - slack, "observed_at must be the turn time"
        )
        self.assertLessEqual(
            parsed, t_after + slack, "observed_at must be the turn time"
        )


# -- prefetch -----------------------------------------------------------------


class TestPrefetch(PluginTestCase):
    def test_prefetch_returns_cached_recall_block(self) -> None:
        provider = self.make_provider()
        provider.queue_prefetch("duck typing", session_id="s")
        self.assertTrue(_wait_until(lambda: bool(provider._prefetch_cache)))
        block = provider.prefetch("duck typing", session_id="s")
        self.assertIn("[Palimpsest Memory]", block)
        self.assertIn("duck-typed interfaces", block)

    def test_trivial_prompt_skips_prefetch(self) -> None:
        provider = self.make_provider()
        provider.queue_prefetch("ok", session_id="s")
        time.sleep(0.15)
        self.assertEqual(provider._prefetch_cache, "")
        self.assertEqual(len(self.server.requests), 0)
        self.assertEqual(provider.prefetch("thanks", session_id="s"), "")

    def test_prefetch_failure_yields_empty_cache(self) -> None:
        provider = self.make_provider()
        self.server.fail_recall = True
        provider.queue_prefetch("x", session_id="s")
        self.assertTrue(
            _wait_until(
                lambda: (
                    provider._prefetch_thread is not None
                    and not provider._prefetch_thread.is_alive()
                )
            )
        )
        self.assertEqual(provider._prefetch_cache, "")

    def test_prefetch_skips_when_previous_recall_still_running(self) -> None:
        """queue_prefetch must never block the agent loop (spec R6)."""
        provider = self.make_provider()
        self.server.recall_delay_seconds = 0.5
        provider.queue_prefetch("slow query", session_id="s")
        self.assertTrue(_wait_until(lambda: provider._prefetch_thread is not None))
        first_thread = provider._prefetch_thread
        start = time.monotonic()
        provider.queue_prefetch("second query", session_id="s")
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.2, "queue_prefetch must not block on a live thread")
        self.assertIs(provider._prefetch_thread, first_thread)
        self.server.recall_delay_seconds = 0.0


class TestFallbackTrivialPrompt(unittest.TestCase):
    """The standalone fallback must match the core grammar (spec R6)."""

    def test_trivial_phrases(self) -> None:
        for phrase in (
            "",
            "   ",
            "ok",
            "OK!",
            "thanks",
            "thank you",
            "sure",
            "yes",
            "nope",
            "continue",
            "go ahead",
            "done",
            "/command",
            "lgtm",
        ):
            self.assertTrue(
                plugin.fallback_is_trivial_prompt(phrase),
                f"expected {phrase!r} to be trivial",
            )

    def test_non_trivial_phrases(self) -> None:
        for phrase in (
            "okay so here is the plan",
            "thanks for the help, now what about X",
            "k8s deployment",
            "note the difference",
            "yolo",
            "hindsight is 20/20",
            "continue with the refactor",
            "tell me about memory",
        ):
            self.assertFalse(
                plugin.fallback_is_trivial_prompt(phrase),
                f"expected {phrase!r} not to be trivial",
            )


# -- session lifecycle and memory mirroring ------------------------------------


class TestSessionAndMirror(PluginTestCase):
    def test_session_switch_updates_session(self) -> None:
        provider = self.make_provider(session_id="old-session")
        provider.on_session_switch(
            "new-session", parent_session_id="old-session", reset=True
        )
        provider.sync_turn("moved", "yes", session_id="")
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 1)
        )
        request = self.server.requests_for("POST", "/episodes")[0]
        self.assertEqual(request["body"]["payload"]["session_id"], "new-session")
        self.assertEqual(
            request["headers"]["idempotency-key"], "hermes-turn:new-session:1"
        )

    def test_on_memory_write_add_mirrors_as_fact(self) -> None:
        import hashlib

        provider = self.make_provider()
        provider.on_memory_write(
            "add", "memory", "a durable preference", {"write_origin": "test"}
        )
        self.assertTrue(
            _wait_until(lambda: len(self.server.requests_for("POST", "/episodes")) == 1)
        )
        facts = self.server.requests_for("POST", "/facts")
        self.assertEqual(len(facts), 1)
        expected_key = f"builtin:memory:hermes-{hashlib.sha1(b'a durable preference').hexdigest()[:16]}"
        self.assertEqual(facts[0]["body"]["key"], expected_key)
        self.assertEqual(facts[0]["body"]["namespace"], "hermes")

    def test_on_memory_write_replace_and_remove_are_skipped(self) -> None:
        provider = self.make_provider()
        provider.on_memory_write("replace", "memory", "new value")
        provider.on_memory_write("remove", "user", "old value")
        time.sleep(0.15)
        self.assertEqual(len(self.server.requests), 0)

    def test_mirror_retries_then_succeeds(self) -> None:
        """Mirrored writes retry with bounded backoff before dropping (spec R5)."""
        original_backoff = plugin._MIRROR_BACKOFF_SECONDS  # type: ignore[attr-defined]
        plugin._MIRROR_BACKOFF_SECONDS = 0.05  # type: ignore[attr-defined]
        self.addCleanup(setattr, plugin, "_MIRROR_BACKOFF_SECONDS", original_backoff)
        provider = self.make_provider()
        self.server.episode_fail_countdown = 2  # two 503s, then success
        provider.on_memory_write("add", "memory", "survives retries")
        self.assertTrue(
            _wait_until(
                lambda: len(self.server.requests_for("POST", "/facts")) == 1,
                timeout=5.0,
            )
        )
        episodes = self.server.requests_for("POST", "/episodes")
        self.assertEqual(len(episodes), 3, "two failures then one success")
        expected_key = "hermes-mwrite:memory:" + _content_key("survives retries")
        self.assertEqual(
            [request["headers"].get("idempotency-key") for request in episodes],
            [expected_key + ":episode"] * 3,
            "retries must reuse the same idempotency key",
        )


# -- invariants ---------------------------------------------------------------


class TestInvariants(PluginTestCase):
    def test_no_database_imports_in_plugin_source(self) -> None:
        for source_file in PLUGIN_DIR.glob("*.py"):
            source = source_file.read_text(encoding="utf-8")
            for forbidden in (
                "psycopg",
                "asyncpg",
                "sqlalchemy",
                "pg8000",
                "postgresql://",
            ):
                self.assertNotIn(
                    forbidden, source, f"{source_file.name} references {forbidden}"
                )

    def test_no_delete_or_export_calls_in_plugin_source(self) -> None:
        for source_file in PLUGIN_DIR.glob("*.py"):
            source = source_file.read_text(encoding="utf-8")
            for forbidden in ("/deletions", "delete_memory", "forget(", "export("):
                self.assertNotIn(
                    forbidden, source, f"{source_file.name} references {forbidden}"
                )

    def test_provider_never_imports_hermes_core_standalone(self) -> None:
        # The guarded base-class import must make the module importable with
        # OR without the Hermes core (object fallback), and register() must
        # always be present for the discovery loader.
        source = (PLUGIN_DIR / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("except ImportError", source)
        self.assertIn("_MemoryProviderBase = object", source)
        self.assertTrue(callable(getattr(plugin, "register", None)))
        instance = PalimpsestMemoryProvider()
        self.assertEqual(instance.name, "palimpsest")
        try:
            from agent.memory_provider import MemoryProvider as RealABC
        except ImportError:
            RealABC = None
        if RealABC is not None:
            self.assertTrue(issubclass(PalimpsestMemoryProvider, RealABC))


# -- real Hermes discovery (integration, skips without hermes-agent source) ------


class TestHermesDiscoveryIntegration(PluginTestCase):
    """Prove the plugin is discoverable by the real Hermes plugin loader."""

    def test_discovery_finds_installed_plugin(self) -> None:
        try:
            import importlib.util as _util

            if _util.find_spec("plugins.memory") is None:
                self.skipTest("hermes-agent source not importable on this host")
        except Exception:  # noqa: BLE001 - any import failure means skip
            self.skipTest("hermes-agent source not importable on this host")
        plugins_dir = self.hermes_home / "plugins"
        plugins_dir.mkdir()
        (plugins_dir / "palimpsest").symlink_to(PLUGIN_DIR, target_is_directory=True)
        from plugins.memory import discover_memory_providers, load_memory_provider

        discovered = discover_memory_providers()
        names = {name for name, _desc, _available in discovered}
        self.assertIn("palimpsest", names)
        entry = next(entry for entry in discovered if entry[0] == "palimpsest")
        self.assertTrue(
            entry[2], "provider must report available (no network required)"
        )
        provider = load_memory_provider("palimpsest")
        self.assertIsNotNone(provider)
        assert provider is not None
        self.assertEqual(provider.name, "palimpsest")
        self.assertTrue(callable(provider.get_config_schema))


if __name__ == "__main__":
    unittest.main()
