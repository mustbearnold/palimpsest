"""Deterministic, non-writing comparison of authorized project evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any


COMPARISON_PROFILE = "project-comparison-structural-v1"
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_LEXICAL_OVERLAP_THRESHOLD = 0.5
_LEXICAL_OVERLAP_MINIMUM_SHARED_TOKENS = 3
_LEXICAL_OVERLAP_MAXIMUM_CANDIDATES = 100
_LEXICAL_REVIEW_MAXIMUM_TOKENS_PER_BUCKET = 20


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
    text_items: list[dict[str, Any]] = []
    item_counts = {project_id: 0 for project_id in project_ids}
    project_context = {
        project_id: {
            "project_roots": set(),
            "branches": set(),
            "sources": set(),
            "roles": set(),
            "sessions": set(),
        }
        for project_id in project_ids
    }
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
            item_ref = {
                "fact_id": _optional_text(item.get("fact_id")),
                "revision_id": _optional_text(item.get("revision_id")),
                "namespace": _optional_text(item.get("namespace")),
                "key": display_key,
                "value_sha256": value_sha256,
            }
            group["items_by_project"].setdefault(project_id, []).append(
                {
                    key: item_ref[key]
                    for key in ("fact_id", "revision_id", "namespace", "value_sha256")
                }
            )
            group["value_hashes"].add(value_sha256)
            _collect_project_context(project_context[project_id], item)
            content = _item_content(item)
            tokens = frozenset(_TOKEN_PATTERN.findall(content.casefold()))
            if tokens:
                text_items.append(
                    {
                        "project_id": project_id,
                        "item_index": item_index,
                        "tokens": tokens,
                        "ref": item_ref,
                    }
                )
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

    lexical_review = _lexical_review(text_items)
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
        "project_context": _serialize_project_context(project_context),
        "lexical_review": lexical_review,
        "summary": {
            "bundle_count": len(project_ids),
            "item_count": sum(item_counts.values()),
            "items_by_project": item_counts,
            "group_count": len(result_groups),
            "lexical_review_candidate_count": len(lexical_review["candidates"]),
            "lexical_review_truncated": lexical_review["truncated"],
            **counts,
        },
        "groups": result_groups,
    }


def _lexical_review(text_items: list[dict[str, Any]]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for left_index, left in enumerate(text_items):
        for right in text_items[left_index + 1 :]:
            if left["project_id"] == right["project_id"]:
                continue
            shared = left["tokens"] & right["tokens"]
            if len(shared) < _LEXICAL_OVERLAP_MINIMUM_SHARED_TOKENS:
                continue
            union = left["tokens"] | right["tokens"]
            score = len(shared) / len(union)
            if score < _LEXICAL_OVERLAP_THRESHOLD:
                continue
            ordered = sorted(
                (
                    (left["project_id"], left["item_index"], left),
                    (right["project_id"], right["item_index"], right),
                ),
                key=lambda value: (value[0], value[1]),
            )
            first_project, _, first_item = ordered[0]
            second_project, _, second_item = ordered[1]
            candidates.append(
                {
                    "similarity": round(score, 6),
                    "projects": [first_project, second_project],
                    "items": [
                        _lexical_item_ref(ordered[0][2]),
                        _lexical_item_ref(ordered[1][2]),
                    ],
                    "token_delta": _token_delta(
                        first_project,
                        first_item["tokens"],
                        second_project,
                        second_item["tokens"],
                    ),
                }
            )
    candidates.sort(
        key=lambda candidate: (
            -candidate["similarity"],
            candidate["projects"],
            [item.get("revision_id") or "" for item in candidate["items"]],
        )
    )
    truncated = len(candidates) > _LEXICAL_OVERLAP_MAXIMUM_CANDIDATES
    return {
        "profile": "token-jaccard-v1",
        "threshold": _LEXICAL_OVERLAP_THRESHOLD,
        "minimum_shared_tokens": _LEXICAL_OVERLAP_MINIMUM_SHARED_TOKENS,
        "maximum_tokens_per_bucket": _LEXICAL_REVIEW_MAXIMUM_TOKENS_PER_BUCKET,
        "truncated": truncated,
        "candidates": candidates[:_LEXICAL_OVERLAP_MAXIMUM_CANDIDATES],
    }


def _collect_project_context(context: dict[str, Any], item: Mapping[str, Any]) -> None:
    value = item.get("value")
    metadata = value.get("metadata") if isinstance(value, Mapping) else None
    if not isinstance(metadata, Mapping):
        return
    for metadata_key, context_key in (
        ("project_root", "project_roots"),
        ("branch", "branches"),
        ("source", "sources"),
        ("role", "roles"),
    ):
        text = _optional_text(metadata.get(metadata_key))
        if text is not None:
            context[context_key].add(text)
    session_id = _optional_text(metadata.get("session_id"))
    if session_id is not None:
        context["sessions"].add(session_id)


def _serialize_project_context(contexts: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        project_id: {
            "project_roots": sorted(context["project_roots"]),
            "branches": sorted(context["branches"]),
            "sources": sorted(context["sources"]),
            "roles": sorted(context["roles"]),
            "session_count": len(context["sessions"]),
        }
        for project_id, context in sorted(contexts.items())
    }


def _token_delta(
    first_project: str,
    first_tokens: frozenset[str],
    second_project: str,
    second_tokens: frozenset[str],
) -> dict[str, Any]:
    buckets = {
        "shared": sorted(first_tokens & second_tokens),
        "only_in": {
            first_project: sorted(first_tokens - second_tokens),
            second_project: sorted(second_tokens - first_tokens),
        },
    }
    truncated = any(
        len(tokens) > _LEXICAL_REVIEW_MAXIMUM_TOKENS_PER_BUCKET
        for tokens in [buckets["shared"], *buckets["only_in"].values()]
    )
    return {
        "shared": buckets["shared"][:_LEXICAL_REVIEW_MAXIMUM_TOKENS_PER_BUCKET],
        "only_in": {
            project_id: tokens[:_LEXICAL_REVIEW_MAXIMUM_TOKENS_PER_BUCKET]
            for project_id, tokens in buckets["only_in"].items()
        },
        "truncated": truncated,
    }


def _lexical_item_ref(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "fact_id": item["ref"]["fact_id"],
        "revision_id": item["ref"]["revision_id"],
        "namespace": item["ref"]["namespace"],
        "key": item["ref"]["key"],
        "value_sha256": item["ref"]["value_sha256"],
    }


def _item_content(item: Mapping[str, Any]) -> str:
    value = item.get("value")
    if isinstance(value, Mapping) and isinstance(value.get("content"), str):
        return value["content"]
    return value if isinstance(value, str) else ""


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
