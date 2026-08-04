"""Attributable, non-writing validation for semantic project reviews."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .comparison import compare_project_bundles


PROJECT_REVIEW_PROFILE = "project-comparison-semantic-review-v1"
PROJECT_CONSOLIDATION_PROFILE = "project-comparison-governed-consolidation-v1"
_STRUCTURAL_COMPARISON_PROFILE = "project-comparison-structural-v1"
_ALLOWED_CLASSIFICATIONS = frozenset(
    {
        "same_meaning",
        "semantic_difference",
        "semantic_conflict",
        "rekeyed_equivalent",
        "insufficient_evidence",
    }
)
_MAX_CLAIMS = 100
_MAX_EVIDENCE_PER_CLAIM = 20
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def prepare_project_consolidation(
    validated_review: Mapping[str, Any],
    writes: Sequence[Mapping[str, Any]],
    *,
    consolidation_id: str,
) -> dict[str, Any]:
    """Prepare explicit, attributable fact writes for validated review claims.

    The caller supplies the fact value and write policy. Episode lineage and
    per-claim idempotency keys are derived from the validated review so a
    caller cannot replace cited evidence or accidentally share a retry key
    between claims. This function never performs I/O.
    """

    if not isinstance(validated_review, Mapping):
        raise ValueError("validated_review must be an object")
    if validated_review.get("profile") != PROJECT_REVIEW_PROFILE:
        raise ValueError("validated_review must come from the semantic review profile")
    contract_validation = validated_review.get("contract_validation")
    if (
        not isinstance(contract_validation, Mapping)
        or contract_validation.get("passed") is not True
    ):
        raise ValueError("validated_review must have passed contract validation")
    normalized_consolidation_id = _bounded_text(
        consolidation_id, "consolidation_id", 255
    )
    if (
        not isinstance(writes, Sequence)
        or isinstance(writes, (str, bytes))
        or not writes
    ):
        raise ValueError("writes must be a non-empty array")
    if len(writes) > _MAX_CLAIMS:
        raise ValueError(f"writes must contain at most {_MAX_CLAIMS} entries")

    raw_claims = validated_review.get("claims")
    if not isinstance(raw_claims, list) or not raw_claims:
        raise ValueError("validated_review.claims must be a non-empty array")
    claims: dict[str, Mapping[str, Any]] = {}
    for claim in raw_claims:
        if not isinstance(claim, Mapping):
            raise ValueError("validated_review claims must be objects")
        claim_id = _bounded_text(
            claim.get("claim_id"), "validated_review claim_id", 128
        )
        if claim_id in claims:
            raise ValueError(f"validated_review claim ID is duplicated: {claim_id}")
        claims[claim_id] = claim

    normalized_writes: list[dict[str, Any]] = []
    claim_ids: set[str] = set()
    source_episode_ids: set[str] = set()
    for write_index, raw_write in enumerate(writes):
        if not isinstance(raw_write, Mapping):
            raise ValueError(f"writes[{write_index}] must be an object")
        claim_id = _bounded_text(
            raw_write.get("claim_id"), f"writes[{write_index}].claim_id", 128
        )
        if claim_id in claim_ids:
            raise ValueError(f"writes claim ID is duplicated: {claim_id}")
        claim_ids.add(claim_id)
        claim = claims.get(claim_id)
        if claim is None:
            raise ValueError(
                f"writes[{write_index}] references an unknown claim: {claim_id}"
            )
        classification = _bounded_text(
            claim.get("classification"),
            f"validated_review.claims[{claim_id}].classification",
            64,
        )
        if classification == "insufficient_evidence":
            raise ValueError(
                f"claim {claim_id} has insufficient_evidence and cannot be consolidated"
            )
        if "evidence_episode_ids" in raw_write:
            raise ValueError(
                "evidence_episode_ids are derived from the validated claim"
            )
        if "idempotency_key" in raw_write:
            raise ValueError(
                "idempotency_key is derived from consolidation_id and claim_id"
            )

        evidence_episode_ids = _claim_episode_ids(claim, claim_id)
        source_episode_ids.update(evidence_episode_ids)
        observed_at = _bounded_text(
            raw_write.get("observed_at"), f"writes[{write_index}].observed_at", 255
        )
        valid_time = _mapping(
            raw_write.get("valid_time"), f"writes[{write_index}].valid_time"
        )
        write_policy = _write_policy(
            raw_write.get("write_policy"), f"writes[{write_index}].write_policy"
        )
        confidence = raw_write.get("confidence")
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not 0 <= confidence <= 1
        ):
            raise ValueError(
                f"writes[{write_index}].confidence must be a number from 0 to 1"
            )
        if raw_write.get("value") is None:
            raise ValueError(f"writes[{write_index}].value must not be null")
        normalized_writes.append(
            {
                "claim_id": claim_id,
                "classification": classification,
                "claim_summary": _bounded_text(
                    claim.get("summary"),
                    f"validated_review.claims[{claim_id}].summary",
                    2000,
                ),
                "namespace": _bounded_text(
                    raw_write.get("namespace"), f"writes[{write_index}].namespace", 255
                ),
                "key": _bounded_text(
                    raw_write.get("key"), f"writes[{write_index}].key", 512
                ),
                "value": raw_write["value"],
                "observed_at": observed_at,
                "valid_time": valid_time,
                "evidence_episode_ids": evidence_episode_ids,
                "write_policy": write_policy,
                "confidence": float(confidence),
                "sensitivity": _bounded_text(
                    raw_write.get("sensitivity"),
                    f"writes[{write_index}].sensitivity",
                    255,
                ),
                "retention_policy_id": _bounded_text(
                    raw_write.get("retention_policy_id"),
                    f"writes[{write_index}].retention_policy_id",
                    255,
                ),
                "idempotency_key": _consolidation_idempotency_key(
                    normalized_consolidation_id, claim_id
                ),
            }
        )

    return {
        "profile": PROJECT_CONSOLIDATION_PROFILE,
        "consolidation_id": normalized_consolidation_id,
        "reviewer": dict(validated_review.get("reviewer", {})),
        "review_policy": dict(validated_review.get("review_policy", {})),
        "claim_ids": [write["claim_id"] for write in normalized_writes],
        "source_episode_ids": sorted(source_episode_ids),
        "writes": normalized_writes,
        "durable_write": False,
    }


def _claim_episode_ids(claim: Mapping[str, Any], claim_id: str) -> list[str]:
    raw_evidence = claim.get("evidence")
    if not isinstance(raw_evidence, list) or not raw_evidence:
        raise ValueError(f"validated_review claim {claim_id} has no evidence")
    episode_ids: set[str] = set()
    for evidence_index, citation in enumerate(raw_evidence):
        if not isinstance(citation, Mapping):
            raise ValueError(
                f"validated_review claim {claim_id} evidence must be objects"
            )
        raw_episode_ids = citation.get("evidence_episode_ids")
        if not isinstance(raw_episode_ids, list) or not raw_episode_ids:
            raise ValueError(
                f"validated_review claim {claim_id} evidence[{evidence_index}] has no episode IDs"
            )
        for episode_index, episode_id in enumerate(raw_episode_ids):
            episode_ids.add(
                _bounded_text(
                    episode_id,
                    f"validated_review claim {claim_id} evidence[{evidence_index}].evidence_episode_ids[{episode_index}]",
                    255,
                )
            )
    if not episode_ids:
        raise ValueError(
            f"validated_review claim {claim_id} has no evidence episode IDs"
        )
    return sorted(episode_ids)


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return dict(value)


def _write_policy(value: Any, name: str) -> dict[str, Any]:
    policy = _mapping(value, name)
    policy["id"] = _bounded_text(policy.get("id"), f"{name}.id", 255)
    policy["version"] = _bounded_text(policy.get("version"), f"{name}.version", 255)
    return policy


def _consolidation_idempotency_key(consolidation_id: str, claim_id: str) -> str:
    digest = hashlib.sha256(
        f"{consolidation_id}\x00{claim_id}".encode("utf-8")
    ).hexdigest()
    return f"palimpsest-consolidation-v1:{digest}"


def validate_project_review(
    comparison_result: Mapping[str, Any], review: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate an external semantic review against authorized comparison evidence.

    This module does not call a model, decide whether a claim is true, or write
    memory. It only checks that caller-supplied claims identify returned items,
    cite their source episodes, and carry reviewer/policy attribution before a
    caller decides whether to perform a separate governed write.
    """

    if not isinstance(review, Mapping):
        raise ValueError("review must be an object")
    bundles, comparison = _comparison_parts(comparison_result)
    projects = _string_list(comparison, "projects", minimum=2)
    project_set = set(projects)
    if set(bundles) != project_set:
        raise ValueError("comparison bundles and projects must name the same projects")
    reviewer = _attribution(
        review, "reviewer", ("principal_id", "provider", "model", "model_revision")
    )
    prompt_sha256 = _digest(
        review.get("reviewer", {}).get("prompt_sha256"), "reviewer.prompt_sha256"
    )
    reviewer["prompt_sha256"] = prompt_sha256
    review_policy = _attribution(review, "review_policy", ("id", "version"))
    review_policy["sha256"] = _digest(
        review.get("review_policy", {}).get("sha256"), "review_policy.sha256"
    )

    comparison_item_digests = _comparison_item_digests(comparison)
    items_by_ref = _returned_items(bundles, project_set)
    raw_claims = review.get("claims")
    if not isinstance(raw_claims, list) or not raw_claims:
        raise ValueError("review.claims must be a non-empty array")
    if len(raw_claims) > _MAX_CLAIMS:
        raise ValueError(f"review.claims must contain at most {_MAX_CLAIMS} claims")

    claims: list[dict[str, Any]] = []
    claim_ids: set[str] = set()
    source_episode_ids: set[str] = set()
    for claim_index, raw_claim in enumerate(raw_claims):
        if not isinstance(raw_claim, Mapping):
            raise ValueError(f"review claim {claim_index} must be an object")
        claim_id = _bounded_text(
            raw_claim.get("claim_id"), f"claims[{claim_index}].claim_id", 128
        )
        if claim_id in claim_ids:
            raise ValueError(f"review claim ID is duplicated: {claim_id}")
        claim_ids.add(claim_id)
        classification = _bounded_text(
            raw_claim.get("classification"), f"claims[{claim_index}].classification", 64
        )
        if classification not in _ALLOWED_CLASSIFICATIONS:
            allowed = ", ".join(sorted(_ALLOWED_CLASSIFICATIONS))
            raise ValueError(
                f"claims[{claim_index}].classification must be one of: {allowed}"
            )
        summary = _bounded_text(
            raw_claim.get("summary"), f"claims[{claim_index}].summary", 2000
        )
        claim_projects = _string_list(raw_claim, "projects", minimum=2)
        if not set(claim_projects).issubset(project_set):
            raise ValueError(
                f"claims[{claim_index}].projects contains an unknown project"
            )
        confidence = raw_claim.get("confidence")
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise ValueError(
                f"claims[{claim_index}].confidence must be a number from 0 to 1"
            )
        if not 0 <= confidence <= 1:
            raise ValueError(
                f"claims[{claim_index}].confidence must be a number from 0 to 1"
            )

        raw_evidence = raw_claim.get("evidence")
        if not isinstance(raw_evidence, list) or not raw_evidence:
            raise ValueError(
                f"claims[{claim_index}].evidence must be a non-empty array"
            )
        if len(raw_evidence) > _MAX_EVIDENCE_PER_CLAIM:
            raise ValueError(
                f"claims[{claim_index}].evidence must contain at most {_MAX_EVIDENCE_PER_CLAIM} items"
            )
        cited_projects: set[str] = set()
        cited_values: set[str] = set()
        citation_keys: set[tuple[str, str, str]] = set()
        evidence: list[dict[str, Any]] = []
        for evidence_index, raw_citation in enumerate(raw_evidence):
            if not isinstance(raw_citation, Mapping):
                raise ValueError(
                    f"claims[{claim_index}].evidence[{evidence_index}] must be an object"
                )
            project_id = _bounded_text(
                raw_citation.get("project_id"),
                f"claims[{claim_index}].evidence[{evidence_index}].project_id",
                255,
            )
            if project_id not in claim_projects:
                raise ValueError(
                    f"claims[{claim_index}] cites evidence outside its projects"
                )
            fact_id = _bounded_text(
                raw_citation.get("fact_id"),
                f"claims[{claim_index}].evidence[{evidence_index}].fact_id",
                255,
            )
            revision_id = _bounded_text(
                raw_citation.get("revision_id"),
                f"claims[{claim_index}].evidence[{evidence_index}].revision_id",
                255,
            )
            citation_key = (project_id, fact_id, revision_id)
            if citation_key in citation_keys:
                raise ValueError(f"claims[{claim_index}] repeats an evidence citation")
            citation_keys.add(citation_key)
            item = items_by_ref.get(citation_key)
            if item is None:
                raise ValueError(
                    f"claims[{claim_index}] evidence does not identify a returned retrieval item"
                )
            episode_ids = _string_list(
                raw_citation,
                "evidence_episode_ids",
                minimum=1,
                path=f"claims[{claim_index}].evidence[{evidence_index}]",
            )
            returned_episode_ids = {
                value
                for value in item.get("evidence_episode_ids", [])
                if isinstance(value, str) and value.strip()
            }
            if not returned_episode_ids or not set(episode_ids).issubset(
                returned_episode_ids
            ):
                raise ValueError(
                    f"claims[{claim_index}] evidence episode citation is not present on the returned item"
                )
            value_sha256 = comparison_item_digests.get(citation_key)
            if value_sha256 is None:
                raise ValueError(
                    f"claims[{claim_index}] evidence does not identify a comparison item"
                )
            cited_projects.add(project_id)
            cited_values.add(value_sha256)
            source_episode_ids.update(episode_ids)
            evidence.append(
                {
                    "project_id": project_id,
                    "fact_id": fact_id,
                    "revision_id": revision_id,
                    "namespace": _optional_text(item.get("namespace")),
                    "key": _optional_text(item.get("key")),
                    "value_sha256": value_sha256,
                    "evidence_episode_ids": sorted(set(episode_ids)),
                }
            )
        if cited_projects != set(claim_projects):
            raise ValueError(f"claims[{claim_index}] must cite every named project")
        if classification == "semantic_conflict" and len(cited_values) < 2:
            raise ValueError(
                f"claims[{claim_index}] semantic_conflict requires at least two distinct cited values"
            )
        claims.append(
            {
                "claim_id": claim_id,
                "classification": classification,
                "summary": summary,
                "projects": claim_projects,
                "confidence": confidence,
                "evidence": evidence,
            }
        )

    return {
        "profile": PROJECT_REVIEW_PROFILE,
        "source_comparison_profile": comparison.get("profile"),
        "projects": projects,
        "reviewer": reviewer,
        "review_policy": review_policy,
        "claims": claims,
        "contract_validation": {
            "passed": True,
            "evidence_citations_checked": sum(
                len(claim["evidence"]) for claim in claims
            ),
            "semantic_truth_proven": False,
        },
        "durable_write": False,
        "consolidation": {
            "allowed": False,
            "requires_explicit_governed_write": True,
            "source_episode_ids": sorted(source_episode_ids),
        },
    }


def _comparison_parts(
    comparison_result: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(comparison_result, Mapping):
        raise ValueError("comparison_result must be an object")
    if comparison_result.get("profile") != _STRUCTURAL_COMPARISON_PROFILE:
        raise ValueError(
            "comparison_result must come from the structural comparison profile"
        )
    bundles = comparison_result.get("bundles")
    comparison = comparison_result.get("comparison")
    if not isinstance(bundles, Mapping) or not isinstance(comparison, Mapping):
        raise ValueError(
            "comparison_result must contain bundles and comparison objects"
        )
    if comparison.get("profile") != _STRUCTURAL_COMPARISON_PROFILE:
        raise ValueError(
            "comparison_result must come from the structural comparison profile"
        )
    try:
        expected_comparison = compare_project_bundles(bundles)
    except ValueError as exc:
        raise ValueError(
            "comparison_result bundles are not a valid structural comparison"
        ) from exc
    if expected_comparison != comparison:
        raise ValueError("comparison_result comparison does not match its bundles")
    return bundles, comparison


def _returned_items(
    bundles: Mapping[str, Any], project_ids: set[str]
) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    items_by_ref: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for project_id in sorted(project_ids):
        bundle = bundles.get(project_id)
        if not isinstance(bundle, Mapping) or not isinstance(bundle.get("items"), list):
            raise ValueError(
                f"retrieval bundle for {project_id} must contain an items array"
            )
        for item in bundle["items"]:
            if not isinstance(item, Mapping):
                raise ValueError(
                    f"retrieval bundle for {project_id} contains a non-object item"
                )
            fact_id = _optional_text(item.get("fact_id"))
            revision_id = _optional_text(item.get("revision_id"))
            if fact_id is None or revision_id is None:
                continue
            key = (project_id, fact_id, revision_id)
            if key in items_by_ref:
                raise ValueError(
                    "comparison bundles contain duplicate fact/revision references"
                )
            items_by_ref[key] = item
    return items_by_ref


def _comparison_item_digests(
    comparison: Mapping[str, Any],
) -> dict[tuple[str, str, str], str]:
    groups = comparison.get("groups")
    if not isinstance(groups, list):
        raise ValueError("comparison.groups must be an array")
    digests: dict[tuple[str, str, str], str] = {}
    for group_index, group in enumerate(groups):
        if not isinstance(group, Mapping):
            raise ValueError(f"comparison group {group_index} must be an object")
        items_by_project = group.get("items_by_project")
        if not isinstance(items_by_project, Mapping):
            raise ValueError(
                f"comparison group {group_index} must contain items_by_project"
            )
        for raw_project_id, raw_items in items_by_project.items():
            project_id = _bounded_text(
                raw_project_id,
                f"comparison.groups[{group_index}].items_by_project project",
                255,
            )
            if not isinstance(raw_items, list):
                raise ValueError(
                    f"comparison group {group_index} items must be an array"
                )
            for item_index, raw_item in enumerate(raw_items):
                if not isinstance(raw_item, Mapping):
                    raise ValueError(
                        f"comparison.groups[{group_index}].items_by_project[{project_id}][{item_index}] must be an object"
                    )
                fact_id = _optional_text(raw_item.get("fact_id"))
                revision_id = _optional_text(raw_item.get("revision_id"))
                if fact_id is None or revision_id is None:
                    continue
                value_sha256 = _digest(
                    raw_item.get("value_sha256"),
                    f"comparison.groups[{group_index}].items_by_project[{project_id}][{item_index}].value_sha256",
                )
                key = (project_id, fact_id, revision_id)
                previous = digests.get(key)
                if previous is not None and previous != value_sha256:
                    raise ValueError(
                        "comparison contains conflicting value digests for an item"
                    )
                digests[key] = value_sha256
    return digests


def _attribution(
    parent: Mapping[str, Any], name: str, fields: tuple[str, ...]
) -> dict[str, str]:
    value = parent.get(name)
    if not isinstance(value, Mapping):
        raise ValueError(f"review.{name} must be an object")
    return {
        field: _bounded_text(value.get(field), f"review.{name}.{field}", 255)
        for field in fields
    }


def _string_list(
    parent: Mapping[str, Any],
    name: str,
    *,
    minimum: int,
    path: str | None = None,
) -> list[str]:
    value = parent.get(name)
    label = f"{path}.{name}" if path else f"review.{name}"
    if not isinstance(value, list) or len(value) < minimum:
        raise ValueError(f"{label} must contain at least {minimum} strings")
    result = [
        _bounded_text(item, f"{label}[{index}]", 255)
        for index, item in enumerate(value)
    ]
    if len(set(result)) != len(result):
        raise ValueError(f"{label} must not contain duplicates")
    return result


def _bounded_text(value: Any, name: str, maximum_bytes: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    result = value.strip()
    if len(result.encode("utf-8")) > maximum_bytes:
        raise ValueError(f"{name} must contain at most {maximum_bytes} UTF-8 bytes")
    return result


def _digest(value: Any, name: str) -> str:
    result = _bounded_text(value, name, 64)
    if not _DIGEST_PATTERN.fullmatch(result):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return result


def _optional_text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
