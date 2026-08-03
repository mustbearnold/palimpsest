from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from palimpsest import compare_project_bundles, validate_project_review


def comparison_result() -> dict[str, object]:
    bundles = {
        "project-a": {
            "status": "results",
            "items": [
                {
                    "fact_id": "fact-a",
                    "revision_id": "revision-a",
                    "namespace": "agent_session:project-a",
                    "key": "release-target",
                    "value": {"content": "ship version one", "metadata": {}},
                    "evidence_episode_ids": ["episode-a"],
                }
            ],
        },
        "project-b": {
            "status": "results",
            "items": [
                {
                    "fact_id": "fact-b",
                    "revision_id": "revision-b",
                    "namespace": "agent_session:project-b",
                    "key": "release-target",
                    "value": {"content": "ship version two", "metadata": {}},
                    "evidence_episode_ids": ["episode-b"],
                }
            ],
        },
    }
    comparison = compare_project_bundles(bundles)
    return {"profile": comparison["profile"], "bundles": bundles, "comparison": comparison}


def review_payload() -> dict[str, object]:
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


class ProjectReviewTests(unittest.TestCase):
    def test_valid_review_is_attributed_and_never_writes_memory(self) -> None:
        result = validate_project_review(comparison_result(), review_payload())

        self.assertEqual(result["profile"], "project-comparison-semantic-review-v1")
        self.assertTrue(result["contract_validation"]["passed"])
        self.assertFalse(result["contract_validation"]["semantic_truth_proven"])
        self.assertFalse(result["durable_write"])
        self.assertEqual(result["claims"][0]["evidence"][0]["key"], "release-target")
        self.assertEqual(result["claims"][0]["evidence"][1]["evidence_episode_ids"], ["episode-b"])

    def test_review_rejects_an_ungrounded_citation(self) -> None:
        review = review_payload()
        review["claims"][0]["evidence"][1]["revision_id"] = "revision-not-returned"

        with self.assertRaisesRegex(ValueError, "does not identify a returned retrieval item"):
            validate_project_review(comparison_result(), review)

    def test_review_rejects_a_comparison_not_matching_its_bundles(self) -> None:
        result = comparison_result()
        result["comparison"]["summary"]["item_count"] = 999

        with self.assertRaisesRegex(ValueError, "does not match its bundles"):
            validate_project_review(result, review_payload())

    def test_review_rejects_semantic_conflict_without_distinct_values(self) -> None:
        result = comparison_result()
        result["bundles"]["project-b"]["items"][0]["value"] = {
            "content": "ship version one",
            "metadata": {},
        }
        result["comparison"] = compare_project_bundles(result["bundles"])
        review = review_payload()

        with self.assertRaisesRegex(ValueError, "requires at least two distinct cited values"):
            validate_project_review(result, review)

    def test_comparison_normalizes_integral_json_numbers(self) -> None:
        bundles = {
            "project-a": {"items": [{"key": "attempts", "value": {"count": 1.0}}]},
            "project-b": {"items": [{"key": "attempts", "value": {"count": 1}}]},
        }

        comparison = compare_project_bundles(bundles)

        self.assertEqual(comparison["summary"]["exact_match_groups"], 1)


if __name__ == "__main__":
    unittest.main()
