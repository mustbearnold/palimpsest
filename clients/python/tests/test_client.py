#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from palimpsest import (  # noqa: E402
    PalimpsestClient,
    PalimpsestConfigurationError,
    PalimpsestHttpError,
    PartialConsolidationError,
    PartialRememberError,
    compare_project_bundles,
)


TENANT = "019be000-0000-7000-8000-000000000010"
SUBJECT = "019be000-0000-7000-8000-000000000020"
CASE = "019be000-0000-7000-8000-000000000030"
EPISODE = "019be000-0000-7000-8000-000000000040"
FACT = "019be000-0000-7000-8000-000000000050"
REVISION = "019be000-0000-7000-8000-000000000060"
AGENT = "019be000-0000-7000-8000-000000000090"
THREAD = "019be000-0000-7000-8000-0000000000a0"
CHECKPOINT_REVISION = "019be000-0000-7000-8000-0000000000b0"
EXPORT = "019be000-0000-7000-8000-0000000000c0"
EXPORT_PACKAGE = b"PK\x03\x04palimpsest"
PROJECT_EPISODE_A = "019be000-0000-7000-8000-0000000000d0"
PROJECT_EPISODE_B = "019be000-0000-7000-8000-0000000000e0"


def project_comparison_result() -> dict[str, object]:
    bundles = {
        "project-a": {
            "items": [
                {
                    "fact_id": "fact-a",
                    "revision_id": "revision-a",
                    "namespace": "agent_session:project-a",
                    "key": "release-target",
                    "value": {"content": "ship version one"},
                    "evidence_episode_ids": [PROJECT_EPISODE_A],
                }
            ]
        },
        "project-b": {
            "items": [
                {
                    "fact_id": "fact-b",
                    "revision_id": "revision-b",
                    "namespace": "agent_session:project-b",
                    "key": "release-target",
                    "value": {"content": "ship version two"},
                    "evidence_episode_ids": [PROJECT_EPISODE_B],
                }
            ]
        },
    }
    comparison = compare_project_bundles(bundles)
    return {
        "profile": comparison["profile"],
        "bundles": bundles,
        "comparison": comparison,
    }


def project_review() -> dict[str, object]:
    return {
        "reviewer": {
            "principal_id": "agent:project-review",
            "provider": "openai",
            "model": "gpt-5",
            "model_revision": "2026-08-03",
            "prompt_sha256": "a" * 64,
        },
        "review_policy": {
            "id": "project-review-v1",
            "version": "1",
            "sha256": "b" * 64,
        },
        "claims": [
            {
                "claim_id": "claim-release-target",
                "classification": "semantic_conflict",
                "summary": "The projects record different release targets.",
                "projects": ["project-a", "project-b"],
                "confidence": 0.91,
                "evidence": [
                    {
                        "project_id": "project-a",
                        "fact_id": "fact-a",
                        "revision_id": "revision-a",
                        "evidence_episode_ids": [PROJECT_EPISODE_A],
                    },
                    {
                        "project_id": "project-b",
                        "fact_id": "fact-b",
                        "revision_id": "revision-b",
                        "evidence_episode_ids": [PROJECT_EPISODE_B],
                    },
                ],
            }
        ],
    }


def consolidation_write(key: str) -> dict[str, object]:
    return {
        "claim_id": "claim-release-target",
        "namespace": "shared",
        "key": key,
        "value": {"content": "The projects target different release channels."},
        "observed_at": "2026-08-03T00:00:00Z",
        "valid_time": {"from": "2026-08-03T00:00:00Z"},
        "write_policy": {"id": "project-consolidation", "version": "1"},
        "confidence": 0.91,
        "sensitivity": "internal",
        "retention_policy_id": "standard",
    }


class FakeApi(BaseHTTPRequestHandler):
    requests: list[dict[str, object]] = []
    fail_fact: bool = False
    fail_fact_after: int | None = None
    fact_calls: int = 0
    deletion_calls: int = 0
    export_calls: int = 0

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        body = json.loads(raw_body) if raw_body else None
        self.requests.append(
            {
                "method": "POST",
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": body,
            }
        )
        if self.path.endswith("/episodes"):
            self._json(201, {"episode_id": EPISODE, "payload": body})
        elif self.path.endswith("/facts"):
            FakeApi.fact_calls += 1
            if self.fail_fact or (
                self.fail_fact_after is not None
                and FakeApi.fact_calls > self.fail_fact_after
            ):
                self._json(422, {"type": "invalid-request"})
            else:
                self._json(
                    201,
                    {"fact_id": FACT, "revision_id": REVISION, "value": body["value"]},
                )
        elif self.path.endswith("/retrievals"):
            self._json(201, {"status": "results", "items": [], "next_cursor": None})
        elif self.path.endswith("/deletions"):
            self._json(
                202,
                {
                    "operation_id": "019be000-0000-7000-8000-000000000070",
                    "lifecycle_state": "pending",
                },
            )
        elif self.path.endswith("/exports"):
            self._json(202, {"export_id": EXPORT, "lifecycle_state": "queued"})
        else:
            self._json(404, {"type": "resource-not-found"})

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        self.requests.append(
            {
                "method": "PUT",
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": body,
            }
        )
        if self.path.endswith("/checkpoint"):
            self._json_with_etag(
                201 if self.headers.get("If-None-Match") == "*" else 200,
                {"checkpoint_revision_id": CHECKPOINT_REVISION, "state": body["state"]},
                '"checkpoint-1"',
            )
        else:
            self._json(
                200,
                {
                    "fact_id": FACT,
                    "revision_id": "019be000-0000-7000-8000-000000000080",
                },
            )

    def do_GET(self) -> None:  # noqa: N802
        self.requests.append(
            {
                "method": "GET",
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": None,
            }
        )
        if "/deletions/" in self.path:
            FakeApi.deletion_calls += 1
            if FakeApi.deletion_calls == 2:
                self.send_response(304)
                self.send_header("ETag", '"pending-1"')
                self.end_headers()
                return
            if FakeApi.deletion_calls >= 3:
                self._json_with_etag(
                    200, {"lifecycle_state": "completed"}, '"completed-2"'
                )
                return
            self._json_with_etag(200, {"lifecycle_state": "pending"}, '"pending-1"')
            return
        if self.path.endswith("/content"):
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("ETag", '"export-content-1"')
            self.send_header("Content-Length", str(len(EXPORT_PACKAGE)))
            self.end_headers()
            self.wfile.write(EXPORT_PACKAGE)
            return
        if "/exports/" in self.path:
            FakeApi.export_calls += 1
            if FakeApi.export_calls == 2:
                self.send_response(303)
                self.send_header(
                    "Location",
                    f"/v1/tenants/{TENANT}/subjects/{SUBJECT}/exports/{EXPORT}/content",
                )
                self.send_header("ETag", '"export-2"')
                self.end_headers()
                return
            self._json_with_etag(
                200,
                {"export_id": EXPORT, "lifecycle_state": "materializing"},
                '"export-1"',
            )
            return
        if self.path.endswith("/checkpoint"):
            self._json_with_etag(
                200,
                {
                    "checkpoint_revision_id": CHECKPOINT_REVISION,
                    "state": {"step": "saved"},
                },
                '"checkpoint-1"',
            )
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("ETag", '"revision-1"')
        encoded = json.dumps(
            {"fact_id": FACT, "revision": {"revision_id": REVISION}}
        ).encode("utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _json_with_etag(self, status: int, value: object, etag: str) -> None:
        encoded = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _json(self, status: int, value: object) -> None:
        encoded = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class ClientTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeApi.requests = []
        FakeApi.fail_fact = False
        FakeApi.fail_fact_after = None
        FakeApi.fact_calls = 0
        FakeApi.deletion_calls = 0
        FakeApi.export_calls = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeApi)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = PalimpsestClient(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            bearer_token="test-token",
            tenant_id=TENANT,
            subject_id=SUBJECT,
            case_id=CASE,
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_remember_is_a_governed_episode_then_fact_write(self) -> None:
        result = self.client.remember(
            "The address is 10 Test Street.",
            key="shipping-address",
            metadata={"source": "user"},
            idempotency_key="remember-1",
        )

        self.assertEqual(result["episode"]["episode_id"], EPISODE)
        self.assertEqual(result["fact"]["fact_id"], FACT)
        self.assertEqual(
            [item["method"] for item in FakeApi.requests], ["POST", "POST"]
        )
        self.assertEqual(
            [item["headers"]["Idempotency-Key"] for item in FakeApi.requests],
            ["remember-1:episode", "remember-1:fact"],
        )
        self.assertTrue(
            all(
                item["headers"]["Authorization"] == "Bearer test-token"
                for item in FakeApi.requests
            )
        )

    def test_consolidation_writes_only_validated_episode_lineage(self) -> None:
        result = self.client.consolidate_project_review(
            project_comparison_result(),
            project_review(),
            [consolidation_write("release-target-difference")],
            consolidation_id="review-run-1",
        )

        self.assertEqual(
            result["profile"], "project-comparison-governed-consolidation-v1"
        )
        self.assertTrue(result["durable_write"])
        self.assertEqual(
            result["source_episode_ids"], [PROJECT_EPISODE_A, PROJECT_EPISODE_B]
        )
        self.assertEqual(result["writes"][0]["fact"]["fact_id"], FACT)
        self.assertEqual(len(FakeApi.requests), 1)
        request = FakeApi.requests[0]
        self.assertEqual(
            request["body"]["evidence_episode_ids"],
            [PROJECT_EPISODE_A, PROJECT_EPISODE_B],
        )
        self.assertEqual(request["body"]["namespace"], "shared")
        self.assertTrue(
            request["headers"]["Idempotency-Key"].startswith(
                "palimpsest-consolidation-v1:"
            )
        )

    def test_consolidation_failure_reports_completed_claims_for_retry(self) -> None:
        comparison = project_comparison_result()
        review = project_review()
        review["claims"].append(
            {**review["claims"][0], "claim_id": "claim-release-target-copy"}
        )
        writes = [
            consolidation_write("release-target-difference"),
            {
                **consolidation_write("release-target-difference-copy"),
                "claim_id": "claim-release-target-copy",
            },
        ]
        FakeApi.fail_fact_after = 1

        with self.assertRaises(PartialConsolidationError) as raised:
            self.client.consolidate_project_review(
                comparison,
                review,
                writes,
                consolidation_id="review-run-2",
            )

        self.assertEqual(len(raised.exception.completed), 1)
        self.assertEqual(
            raised.exception.completed[0]["claim_id"], "claim-release-target"
        )
        self.assertEqual(
            raised.exception.failed_write["claim_id"], "claim-release-target-copy"
        )
        self.assertIsInstance(raised.exception.cause, PalimpsestHttpError)

    def test_consolidation_preflight_errors_do_not_look_like_partial_writes(
        self,
    ) -> None:
        client_without_case = PalimpsestClient(
            base_url=f"http://127.0.0.1:{self.server.server_port}",
            bearer_token="test-token",
            tenant_id=TENANT,
            subject_id=SUBJECT,
        )

        with self.assertRaises(PalimpsestConfigurationError):
            client_without_case.consolidate_project_review(
                project_comparison_result(),
                project_review(),
                [consolidation_write("release-target-difference")],
                consolidation_id="review-run-preflight",
            )

        self.assertEqual(FakeApi.requests, [])

    def test_recall_sends_explicit_temporal_perspective_and_filters(self) -> None:
        self.client.recall(
            "shipping address",
            perspective={
                "kind": "as_of",
                "valid_at": "2026-01-01T00:00:00Z",
                "recorded_at": "2026-01-02T00:00:00Z",
            },
            page_size=5,
            filters={"namespaces": ["customer"]},
            idempotency_key="recall-1",
        )

        request = FakeApi.requests[0]
        self.assertEqual(
            request["path"].split("/v1")[1],
            f"/tenants/{TENANT}/subjects/{SUBJECT}/retrievals",
        )
        self.assertEqual(request["headers"]["Idempotency-Key"], "recall-1")
        self.assertEqual(request["body"]["perspective"]["kind"], "as_of")
        self.assertEqual(request["body"]["filters"], {"namespaces": ["customer"]})

    def test_recall_by_project_keeps_each_project_candidate_set_separate(self) -> None:
        results = self.client.recall_by_project(
            "release decision",
            ["project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"],
            idempotency_key_prefix="compare-1",
        )

        self.assertEqual(
            set(results), {"project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"}
        )
        self.assertEqual(len(FakeApi.requests), 2)
        self.assertEqual(
            [request["body"]["filters"] for request in FakeApi.requests],
            [
                {"namespaces": ["agent_session:project-aaaaaaaaaaaaaaaa"]},
                {"namespaces": ["agent_session:project-bbbbbbbbbbbbbbbb"]},
            ],
        )
        self.assertEqual(
            [request["headers"]["Idempotency-Key"] for request in FakeApi.requests],
            [
                "compare-1:project-aaaaaaaaaaaaaaaa",
                "compare-1:project-bbbbbbbbbbbbbbbb",
            ],
        )
        with self.assertRaisesRegex(
            PalimpsestConfigurationError, "owns the namespaces filter"
        ):
            self.client.recall_by_project(
                "release decision",
                ["project-aaaaaaaaaaaaaaaa"],
                filters={"namespaces": ["mixed"]},
            )

    def test_compare_project_bundles_marks_exact_and_different_values_without_semantic_claim(
        self,
    ) -> None:
        comparison = compare_project_bundles(
            {
                "project-a": {
                    "status": "results",
                    "items": [
                        {
                            "fact_id": "fact-a-1",
                            "revision_id": "revision-a-1",
                            "namespace": "project-a",
                            "key": "release-target",
                            "value": {"content": "v1", "metadata": {"source": "a"}},
                        },
                        {
                            "fact_id": "fact-a-2",
                            "revision_id": "revision-a-2",
                            "namespace": "project-a",
                            "key": "only-a",
                            "value": {"content": "local", "metadata": {}},
                        },
                    ],
                },
                "project-b": {
                    "status": "results",
                    "items": [
                        {
                            "fact_id": "fact-b-1",
                            "revision_id": "revision-b-1",
                            "namespace": "project-b",
                            "key": "release-target",
                            "value": {"content": "v2", "metadata": {"source": "b"}},
                        },
                        {
                            "fact_id": "fact-b-2",
                            "revision_id": "revision-b-2",
                            "namespace": "project-b",
                            "key": "only-b",
                            "value": {"content": "local", "metadata": {}},
                        },
                    ],
                },
            }
        )

        self.assertEqual(comparison["profile"], "project-comparison-structural-v1")
        self.assertFalse(comparison["semantic_inference"]["performed"])
        self.assertFalse(comparison["durable_write"])
        groups = {group["key"]: group for group in comparison["groups"]}
        self.assertEqual(
            groups["release-target"]["classification"], "same_key_different_value"
        )
        self.assertEqual(groups["only-a"]["classification"], "project_specific")
        self.assertEqual(groups["only-b"]["classification"], "project_specific")
        self.assertEqual(comparison["summary"]["same_key_different_value_groups"], 1)
        self.assertEqual(comparison["summary"]["exact_match_groups"], 0)
        self.assertEqual(comparison["summary"]["project_specific_groups"], 2)

    def test_compare_project_bundles_detects_exact_value_even_when_metadata_differs(
        self,
    ) -> None:
        comparison = compare_project_bundles(
            {
                "project-a": {
                    "items": [
                        {
                            "key": "same",
                            "value": {"content": "v1", "metadata": {"source": "a"}},
                        }
                    ]
                },
                "project-b": {
                    "items": [
                        {
                            "key": "same",
                            "value": {"metadata": {"source": "a"}, "content": "v1"},
                        }
                    ]
                },
            }
        )

        self.assertEqual(comparison["groups"][0]["classification"], "exact_match")
        self.assertEqual(comparison["summary"]["exact_match_groups"], 1)

    def test_compare_by_project_retrieves_isolated_bundles_before_comparing(
        self,
    ) -> None:
        result = self.client.compare_by_project(
            "release decision",
            ["project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"],
            idempotency_key_prefix="compare-2",
        )

        self.assertEqual(result["profile"], "project-comparison-structural-v1")
        self.assertEqual(result["comparison"]["summary"]["bundle_count"], 2)
        self.assertEqual(len(FakeApi.requests), 2)
        self.assertTrue(
            all(request["path"].endswith("/retrievals") for request in FakeApi.requests)
        )

    def test_compare_project_bundles_returns_bounded_lexical_review_candidates(
        self,
    ) -> None:
        comparison = compare_project_bundles(
            {
                "project-a": {
                    "items": [
                        {
                            "key": "decision-a",
                            "value": {
                                "content": "release target ships on stable channel"
                            },
                        }
                    ]
                },
                "project-b": {
                    "items": [
                        {
                            "key": "decision-b",
                            "value": {
                                "content": "release target ships on beta channel"
                            },
                        }
                    ]
                },
            }
        )

        self.assertEqual(comparison["lexical_review"]["profile"], "token-jaccard-v1")
        self.assertEqual(comparison["summary"]["lexical_review_candidate_count"], 1)
        self.assertGreaterEqual(
            comparison["lexical_review"]["candidates"][0]["similarity"], 0.5
        )
        candidate = comparison["lexical_review"]["candidates"][0]
        self.assertEqual(candidate["token_delta"]["only_in"]["project-a"], ["stable"])
        self.assertEqual(candidate["token_delta"]["only_in"]["project-b"], ["beta"])
        self.assertIn("release", candidate["token_delta"]["shared"])
        self.assertFalse(candidate["token_delta"]["truncated"])
        self.assertFalse(comparison["semantic_inference"]["performed"])

    def test_compare_project_bundles_exposes_observed_project_context(self) -> None:
        comparison = compare_project_bundles(
            {
                "project-a": {
                    "items": [
                        {
                            "key": "decision-a",
                            "value": {
                                "content": "release target",
                                "metadata": {
                                    "project_root": "/work/project-a",
                                    "branch": "main",
                                    "source": "codex",
                                    "role": "user",
                                    "session_id": "session-a",
                                },
                            },
                        },
                        {
                            "key": "decision-b",
                            "value": {
                                "content": "release target",
                                "metadata": {
                                    "project_root": "/work/project-a",
                                    "branch": "main",
                                    "source": "claude",
                                    "role": "assistant",
                                    "session_id": "session-b",
                                },
                            },
                        },
                    ]
                },
                "project-b": {
                    "items": [
                        {"key": "decision-c", "value": {"content": "release target"}}
                    ]
                },
            }
        )

        context = comparison["project_context"]["project-a"]
        self.assertEqual(context["project_roots"], ["/work/project-a"])
        self.assertEqual(context["branches"], ["main"])
        self.assertEqual(context["sources"], ["claude", "codex"])
        self.assertEqual(context["roles"], ["assistant", "user"])
        self.assertEqual(context["session_count"], 2)
        self.assertEqual(comparison["project_context"]["project-b"]["session_count"], 0)

    def test_correct_uses_strong_etag_and_forget_starts_deletion(self) -> None:
        self.client.correct(
            FACT,
            supersedes_revision_id=REVISION,
            value={"address": "20 New Street"},
            observed_at="2026-01-03T00:00:00Z",
            valid_time={"from": "2026-01-03T00:00:00Z"},
            evidence_episode_ids=[EPISODE],
            write_policy={"id": "direct-evidence", "version": "1"},
            confidence=1,
            sensitivity="internal",
            retention_policy_id="standard",
            if_match='"revision-1"',
            idempotency_key="correct-1",
        )
        self.client.forget(idempotency_key="forget-1")

        self.assertEqual(FakeApi.requests[0]["headers"]["If-Match"], '"revision-1"')
        self.assertEqual(FakeApi.requests[1]["headers"]["Idempotency-Key"], "forget-1")

    def test_checkpoint_creation_and_advance_use_exclusive_preconditions(self) -> None:
        created = self.client.save_checkpoint_response(
            AGENT,
            THREAD,
            state={"step": "start"},
            state_schema_version=1,
            effect_transitions=[],
            provenance={"source_type": "test", "source_uri": None, "external_id": None},
            sensitivity="internal",
            retention_policy_id="checkpoint-active-30d-v1",
            if_none_match="*",
            idempotency_key="checkpoint-1",
        )
        self.client.save_checkpoint(
            AGENT,
            THREAD,
            state={"step": "finish"},
            state_schema_version=1,
            effect_transitions=[],
            provenance={"source_type": "test", "source_uri": None, "external_id": None},
            sensitivity="internal",
            retention_policy_id="checkpoint-active-30d-v1",
            if_match=created.etag,
            idempotency_key="checkpoint-2",
        )
        current = self.client.get_checkpoint_response(AGENT, THREAD)

        checkpoint_requests = [
            item for item in FakeApi.requests if item["path"].endswith("/checkpoint")
        ]
        self.assertEqual(checkpoint_requests[0]["headers"]["If-None-Match"], "*")
        self.assertEqual(
            checkpoint_requests[1]["headers"]["If-Match"], '"checkpoint-1"'
        )
        self.assertEqual(current.etag, '"checkpoint-1"')

    def test_fact_response_exposes_etag_for_a_conditional_correction(self) -> None:
        response = self.client.get_fact_response(FACT)

        self.assertEqual(response.data["revision"]["revision_id"], REVISION)
        self.assertEqual(response.etag, '"revision-1"')

    def test_fact_promotion_failure_preserves_episode_evidence(self) -> None:
        FakeApi.fail_fact = True

        with self.assertRaises(PartialRememberError) as raised:
            self.client.remember(
                "Keep the episode.", key="partial", idempotency_key="partial-1"
            )

        self.assertEqual(raised.exception.episode["episode_id"], EPISODE)
        self.assertIsInstance(raised.exception.cause, PalimpsestHttpError)
        self.assertEqual(raised.exception.cause.status_code, 422)

    def test_wait_for_deletion_uses_conditional_polling_until_terminal(self) -> None:
        result = self.client.wait_for_deletion(
            "019be000-0000-7000-8000-000000000070",
            timeout_seconds=1,
            poll_interval_seconds=0.001,
        )

        self.assertEqual(result["lifecycle_state"], "completed")
        deletion_requests = [
            item for item in FakeApi.requests if "/deletions/" in item["path"]
        ]
        self.assertEqual(len(deletion_requests), 3)
        self.assertEqual(
            deletion_requests[1]["headers"]["If-None-Match"], '"pending-1"'
        )

    def test_export_lifecycle_preserves_ready_redirect_and_binary_content(self) -> None:
        created = self.client.start_export_response(idempotency_key="export-1")
        status = self.client.get_export_response(EXPORT)
        ready = self.client.get_export_response(EXPORT, if_none_match=status.etag)
        downloaded = self.client.download_export_response(EXPORT)

        self.assertEqual(created.status_code, 202)
        self.assertEqual(created.data["export_id"], EXPORT)
        self.assertEqual(status.data["lifecycle_state"], "materializing")
        self.assertEqual(ready.status_code, 303)
        self.assertTrue(ready.location.endswith(f"/exports/{EXPORT}/content"))
        self.assertEqual(ready.etag, '"export-2"')
        self.assertEqual(downloaded.content, EXPORT_PACKAGE)
        self.assertEqual(downloaded.etag, '"export-content-1"')

    def test_configuration_and_http_errors_are_typed(self) -> None:
        with self.assertRaises(PalimpsestConfigurationError):
            PalimpsestClient(
                base_url="file:///tmp/palimpsest",
                bearer_token="token",
                tenant_id=TENANT,
                subject_id=SUBJECT,
            )

        FakeApi.fail_fact = True
        with self.assertRaises(PartialRememberError) as raised:
            self.client.remember(
                "Not promoted.", key="error", idempotency_key="error-1"
            )
        self.assertEqual(raised.exception.cause.problem, {"type": "invalid-request"})


if __name__ == "__main__":
    unittest.main()
