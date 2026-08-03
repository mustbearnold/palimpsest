"""Deterministic, non-writing comparison of authorized project evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


COMPARISON_PROFILE = "project-comparison-structural-v1"


def compare_project_bundles(bundles: Mapping[str, Any]) -> dict[str, Any]:
    """Classify exact-key evidence across project retrieval responses.

    The comparison is intentionally structural. It groups visible items by a
    normalized fact key and compares canonical value digests; it does not use
    a model, infer semantic equivalence, or write a conflict/consolidation
    fact. The original authorized bundles remain the source material for a
    caller that needs semantic interpretation.
    """

    if not isinstance(bundles, Mapping) or isinstance(bundles, (str, bytes)):
        raise ValueError("bundles must be a mapping of project IDs to retrieval responses")
    normalized_bundles: dict[str, Any] = {}
    for raw_project_id, bundle in bundles.items():
        project_id = _project_id(raw_project_id)
        if project_id in normalized_bundles:
            raise ValueError("bundles must contain at least two distinct projects")
        normalized_bundles[project_id] = bundle
    project_ids = sorted(normalized_bundles)
    if len(project_ids) < 2 or len(set(project_ids)) != len(project_ids):
        raise ValueError("bundles must contain at least two distinct projects")

    grouped: dict[str, dict[str, Any]] = {}
    item_counts = {project_id: 0 for project_id in project_ids}
    for project_id in project_ids:
        bundle = normalized_bundles[project_id]
        if not isinstance(bundle, Mapping):
            raise ValueError(f"retrieval bundle for {project_id} must be an object")
        items = bundle.get("items", [])
        if not isinstance(items, list):
            raise ValueError(f"retrieval bundle for {project_id} must contain an items array")
        for item_index, item in enumerate(items):
            if not isinstance(item, Mapping):
                raise ValueError(f"retrieval item {item_index} for {project_id} must be an object")
            key = item.get("key")
            if isinstance(key, str) and key.strip():
                display_key = key.strip()
                comparison_key = display_key.casefold()
            else:
                display_key = None
                comparison_key = f"__unkeyed__:{project_id}:{item_index}"
            group = grouped.setdefault(
                comparison_key,
                {"key": display_key, "items_by_project": {}, "value_hashes": set()},
            )
            if group["key"] is None and display_key is not None:
                group["key"] = display_key
            value_sha256 = _value_sha256(item.get("value"))
            group["items_by_project"].setdefault(project_id, []).append(
                {
                    "fact_id": _optional_text(item.get("fact_id")),
                    "revision_id": _optional_text(item.get("revision_id")),
                    "namespace": _optional_text(item.get("namespace")),
                    "value_sha256": value_sha256,
                }
            )
            group["value_hashes"].add(value_sha256)
            item_counts[project_id] += 1

    result_groups: list[dict[str, Any]] = []
    for comparison_key in sorted(grouped):
        group = grouped[comparison_key]
        projects_present = sorted(group["items_by_project"])
        if len(projects_present) == 1:
            classification = "project_specific"
        elif len(group["value_hashes"]) == 1:
            classification = "exact_match"
        else:
            classification = "same_key_different_value"
        result_groups.append(
            {
                "comparison_key": comparison_key,
                "key": group["key"],
                "classification": classification,
                "projects": projects_present,
                "items_by_project": {
                    project_id: group["items_by_project"][project_id]
                    for project_id in projects_present
                },
            }
        )

    counts = {
        "exact_match_groups": sum(
            group["classification"] == "exact_match" for group in result_groups
        ),
        "same_key_different_value_groups": sum(
            group["classification"] == "same_key_different_value" for group in result_groups
        ),
        "project_specific_groups": sum(
            group["classification"] == "project_specific" for group in result_groups
        ),
    }
    return {
        "profile": COMPARISON_PROFILE,
        "projects": project_ids,
        "semantic_inference": {
            "performed": False,
            "method": "normalized-fact-key-and-value-sha256-v1",
            "same_key_different_value_is_review_candidate": True,
        },
        "durable_write": False,
        "summary": {
            "bundle_count": len(project_ids),
            "item_count": sum(item_counts.values()),
            "items_by_project": item_counts,
            "group_count": len(result_groups),
            **counts,
        },
        "groups": result_groups,
    }


def _project_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("bundles must use non-empty string project IDs")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _value_sha256(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("retrieval item values must be JSON-compatible") from exc
    return hashlib.sha256(encoded).hexdigest()
