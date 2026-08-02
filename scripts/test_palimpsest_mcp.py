#!/usr/bin/env python3

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
import palimpsest_mcp  # noqa: E402


class FakeClient:
    def __init__(self) -> None:
        self.retrievals: list[tuple[str, int]] = []
        self.memories: list[dict[str, object]] = []

    def retrieve(self, query: str, page_size: int) -> dict[str, object]:
        self.retrievals.append((query, page_size))
        return {"status": "results", "items": [{"value": query}]}

    def remember(self, **kwargs: object) -> dict[str, object]:
        self.memories.append(kwargs)
        return {"episode": {"episode_id": "episode-1"}, "fact": {"fact_id": "fact-1"}}


class McpAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()

    def test_initialize_and_tool_listing(self) -> None:
        initialize = palimpsest_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, self.client
        )
        self.assertEqual(initialize["result"]["protocolVersion"], "2025-11-25")
        listing = palimpsest_mcp.handle_message(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}, self.client
        )
        self.assertEqual(
            [tool["name"] for tool in listing["result"]["tools"]],
            ["palimpsest_retrieve", "palimpsest_remember"],
        )

    def test_retrieve_is_scoped_to_the_tool_arguments(self) -> None:
        response = palimpsest_mcp.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "palimpsest_retrieve", "arguments": {"query": "  launch plan ", "page_size": 7}},
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
                    "arguments": {"content": "The release target is v1.", "metadata": {"source": "user"}},
                },
            },
            self.client,
        )
        self.assertNotIn("isError", response["result"])
        self.assertEqual(len(self.client.memories), 1)
        self.assertEqual(self.client.memories[0]["content"], "The release target is v1.")
        self.assertEqual(self.client.memories[0]["namespace"], "codex")

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
