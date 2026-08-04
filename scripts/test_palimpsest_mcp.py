#!/usr/bin/env python3

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
import palimpsest_mcp  # noqa: E402
from palimpsest import compare_project_bundles, validate_project_review  # noqa: E402


def make_comparison_result() -> dict[str, object]:
    bundles = {
        "project-a": {
            "items": [
                {
                    "fact_id": "fact-a",
                    "revision_id": "revision-a",
                    "key": "release-target",
                    "value": {"content": "ship version one"},
                    "evidence_episode_ids": ["episode-a"],
                }
            ]
        },
        "project-b": {
            "items": [
                {
                    "fact_id": "fact-b",
                    "revision_id": "revision-b",
                    "key": "release-target",
                    "value": {"content": "ship version two"},
                    "evidence_episode_ids": ["episode-b"],
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


def make_review_payload() -> dict[str, object]:
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
                        "evidence_episode_ids": ["episode-a"],
                    },
                    {
                        "project_id": "project-b",
                        "fact_id": "fact-b",
                        "revision_id": "revision-b",
                        "evidence_episode_ids": ["episode-b"],
                    },
                ],
            }
        ],
    }


class FakeClient:
    def __init__(self) -> None:
        self.retrievals: list[tuple[str, int]] = []
        self.project_retrievals: list[tuple[str, list[str], int]] = []
        self.project_comparisons: list[tuple[str, list[str], int]] = []
        self.project_reviews: list[tuple[dict[str, object], dict[str, object]]] = []
        self.project_consolidations: list[
            tuple[dict[str, object], dict[str, object], list[dict[str, object]], str]
        ] = []
        self.memories: list[dict[str, object]] = []

    def retrieve(self, query: str, page_size: int) -> dict[str, object]:
        self.retrievals.append((query, page_size))
        return {"status": "results", "items": [{"value": query}]}

    def recall_by_project(
        self, query: str, project_ids: list[str], page_size: int
    ) -> dict[str, object]:
        self.project_retrievals.append((query, project_ids, page_size))
        return {
            project_id: {"status": "results", "project_id": project_id}
            for project_id in project_ids
        }

    def compare_by_project(
        self, query: str, project_ids: list[str], page_size: int
    ) -> dict[str, object]:
        self.project_comparisons.append((query, project_ids, page_size))
        return {"profile": "project-comparison-structural-v1", "projects": project_ids}

    def validate_project_review(
        self, comparison_result: dict[str, object], review: dict[str, object]
    ) -> dict[str, object]:
        self.project_reviews.append((comparison_result, review))
        return validate_project_review(comparison_result, review)

    def consolidate_project_review(
        self,
        comparison_result: dict[str, object],
        review: dict[str, object],
        writes: list[dict[str, object]],
        consolidation_id: str,
    ) -> dict[str, object]:
        self.project_consolidations.append(
            (comparison_result, review, writes, consolidation_id)
        )
        return {
            "profile": "project-comparison-governed-consolidation-v1",
            "durable_write": True,
        }

    def remember(self, **kwargs: object) -> dict[str, object]:
        self.memories.append(kwargs)
        return {"episode": {"episode_id": "episode-1"}, "fact": {"fact_id": "fact-1"}}


class McpAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()

    def test_initialize_and_tool_listing(self) -> None:
        initialize = palimpsest_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            self.client,
        )
        self.assertEqual(initialize["result"]["protocolVersion"], "2025-11-25")
        listing = palimpsest_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            self.client,
        )
        self.assertEqual(
            [tool["name"] for tool in listing["result"]["tools"]],
            [
                "palimpsest_retrieve",
                "palimpsest_recall_by_project",
                "palimpsest_compare_by_project",
                "palimpsest_validate_project_review",
                "palimpsest_consolidate_project_review",
                "palimpsest_remember",
            ],
        )

    def test_retrieve_is_scoped_to_the_tool_arguments(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_retrieve",
                    "arguments": {"query": "  launch plan ", "page_size": 7},
                },
            },
            self.client,
        )
        self.assertEqual(self.client.retrievals, [("launch plan", 7)])
        self.assertNotIn("isError", response["result"])

    def test_remember_requires_content_and_promotes_evidence(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_remember",
                    "arguments": {
                        "content": "The release target is v1.",
                        "metadata": {"source": "user"},
                    },
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(len(self.client.memories), 1)
        self.assertEqual(
            self.client.memories[0]["content"], "The release target is v1."
        )
        self.assertEqual(self.client.memories[0]["namespace"], "codex")

    def test_recall_by_project_returns_separate_bundles(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_recall_by_project",
                    "arguments": {
                        "query": "  release decision ",
                        "project_ids": [" project-a ", "project-b", "project-b"],
                        "page_size": 7,
                    },
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(
            self.client.project_retrievals,
            [("release decision", ["project-a", "project-b"], 7)],
        )
        rendered = json.loads(response["result"]["content"][0]["text"])
        self.assertEqual(sorted(rendered), ["project-a", "project-b"])

    def test_recall_by_project_requires_distinct_projects(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_recall_by_project",
                    "arguments": {
                        "query": "release",
                        "project_ids": ["project-a", "project-a"],
                    },
                },
            },
            self.client,
        )
        self.assertTrue(response["result"]["isError"])
        self.assertIn("two distinct projects", response["result"]["content"][0]["text"])

    def test_compare_by_project_returns_structured_comparison(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_compare_by_project",
                    "arguments": {
                        "query": "  release decision ",
                        "project_ids": [" project-a ", "project-b"],
                        "page_size": 6,
                    },
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(
            self.client.project_comparisons,
            [("release decision", ["project-a", "project-b"], 6)],
        )
        rendered = json.loads(response["result"]["content"][0]["text"])
        self.assertEqual(rendered["profile"], "project-comparison-structural-v1")

    def test_validate_project_review_keeps_the_review_non_writing(self) -> None:
        comparison_result = make_comparison_result()
        review = make_review_payload()
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_validate_project_review",
                    "arguments": {
                        "comparison_result": comparison_result,
                        "review": review,
                    },
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(self.client.project_reviews, [(comparison_result, review)])
        rendered = json.loads(response["result"]["content"][0]["text"])
        self.assertFalse(rendered["durable_write"])

    def test_consolidate_project_review_routes_explicit_write_plan(self) -> None:
        comparison_result = make_comparison_result()
        review = make_review_payload()
        writes = [
            {
                "claim_id": "claim-release-target",
                "namespace": "shared",
                "key": "release-target-difference",
                "value": {"content": "The projects target different release channels."},
                "observed_at": "2026-08-03T00:00:00Z",
                "valid_time": {"from": "2026-08-03T00:00:00Z"},
                "write_policy": {"id": "project-consolidation", "version": "1"},
                "confidence": 0.91,
                "sensitivity": "internal",
                "retention_policy_id": "standard",
            }
        ]
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "palimpsest_consolidate_project_review",
                    "arguments": {
                        "comparison_result": comparison_result,
                        "review": review,
                        "writes": writes,
                        "consolidation_id": "review-run-1",
                    },
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(
            self.client.project_consolidations,
            [(comparison_result, review, writes, "review-run-1")],
        )
        rendered = json.loads(response["result"]["content"][0]["text"])
        self.assertTrue(rendered["durable_write"])

    def test_notifications_do_not_produce_output(self) -> None:
        incoming = io.StringIO(
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
        )
        outgoing = io.StringIO()
        palimpsest_mcp.serve(incoming, outgoing, self.client)
        self.assertEqual(outgoing.getvalue(), "")

    def test_invalid_arguments_are_tool_errors(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "palimpsest_retrieve", "arguments": {"query": ""}},
            },
            self.client,
        )
        self.assertTrue(response["result"]["isError"])


if __name__ == "__main__":
    unittest.main()
