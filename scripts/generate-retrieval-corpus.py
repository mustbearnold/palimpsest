#!/usr/bin/env python3
"""Generate the frozen issue #22 synthetic retrieval corpus."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evaluations" / "retrieval-corpus-v1"
SEED = hashlib.sha256(b"palimpsest-retrieval-corpus-v1-seed").hexdigest()
BASE_VALID = "2026-06-30T00:00:00Z"


def case_uuid(number: int) -> str:
    return f"019fac90-0000-7000-8000-{number:012x}"


def fact(
    scenario_id: str,
    role: str,
    *,
    scope: str = "primary",
    namespace: str,
    key: str,
    text: str,
    embedding_fixture_role: str,
    sensitivity: str = "internal",
    retention_policy_id: str = "standard",
    observed_at: str = BASE_VALID,
    valid_from: str = BASE_VALID,
    write_policy_id: str = "temporal-stable-evidence",
    confidence: float = 1.0,
    supersedes: str | None = None,
    lifecycle: str = "active",
) -> dict:
    return {
        "id": f"{scenario_id}-{role}",
        "scope": scope,
        "namespace": namespace,
        "key": key,
        "text": text,
        "embedding_fixture_role": embedding_fixture_role,
        "sensitivity": sensitivity,
        "retention_policy_id": retention_policy_id,
        "observed_at": observed_at,
        "valid_from": valid_from,
        "write_policy_id": write_policy_id,
        "confidence": confidence,
        "supersedes": supersedes,
        "lifecycle": lifecycle,
    }


def split_for(index: int, calibration_count: int) -> str:
    return "calibration" if index <= calibration_count else "gate"


def scenario(category: str, index: int, ordinal: int, calibration_count: int) -> dict:
    sid = f"{category}-{index:03d}"
    namespace = f"corpus.{category.replace('-', '_')}"
    token = f"palimpsest-{category}-{index:03d}"
    case_id = case_uuid(ordinal)
    common = {
        "id": sid,
        "category": category,
        "split": split_for(index, calibration_count),
        "case_id": case_id,
        "perspective": "fixed",
        "query": token,
        "expected_disposition": "results",
        "relevant_ids": [f"{sid}-relevant"],
        "forbidden_ids": [],
        "expected_candidate_ids": [],
    }

    if category == "exact-name":
        key = token
        common["query"] = f"{namespace}:{key}"
        common["facts"] = [
            fact(sid, "relevant", namespace=namespace, key=key, text="canonical identity", embedding_fixture_role="near"),
            fact(sid, "distractor", namespace=namespace, key=f"other-{index:03d}", text=f"{token} {token}", embedding_fixture_role="distractor"),
        ]
        common["expected_candidate_ids"] = [f"{sid}-relevant", f"{sid}-distractor"]
    elif category == "temporal-contradiction":
        root_id = f"{sid}-root"
        expected = "root" if index % 2 else "relevant"
        common["perspective"] = "before-successor" if index % 2 else "after-successor"
        common["relevant_ids"] = [f"{sid}-{expected}"]
        common["facts"] = [
            fact(sid, "root", namespace=namespace, key=f"state-{index:03d}", text=f"{token} earlier evidence", embedding_fixture_role="near", observed_at="2026-04-01T00:00:00Z", valid_from="2026-04-01T00:00:00Z", confidence=0.9),
            fact(sid, "relevant", namespace=namespace, key=f"state-{index:03d}", text=f"{token} corrected evidence", embedding_fixture_role="relevant", observed_at="2026-03-01T00:00:00Z", valid_from="2026-03-01T00:00:00Z", confidence=1.0, supersedes=root_id),
            fact(sid, "distractor", namespace=namespace, key=f"stale-{index:03d}", text=f"{token} {token}", embedding_fixture_role="distractor", observed_at="2026-03-01T00:00:00Z", valid_from="2026-03-01T00:00:00Z", write_policy_id="temporal-active-case-evidence"),
        ]
        common["expected_candidate_ids"] = (
            [f"{sid}-root"]
            if index % 2
            else [f"{sid}-relevant", f"{sid}-distractor"]
        )
    elif category == "stale-distractor":
        common["facts"] = [
            fact(sid, "relevant", namespace=namespace, key=f"recent-{index:03d}", text=f"{token} current", embedding_fixture_role="relevant", observed_at=BASE_VALID, valid_from=BASE_VALID, write_policy_id="temporal-active-case-evidence"),
            fact(sid, "distractor", namespace=namespace, key=f"stale-{index:03d}", text=f"{token} {token} {token}", embedding_fixture_role="near", observed_at="2026-04-01T00:00:00Z", valid_from="2026-04-01T00:00:00Z", write_policy_id="temporal-active-case-evidence"),
        ]
        common["expected_candidate_ids"] = [f"{sid}-relevant", f"{sid}-distractor"]
    elif category == "stable-versus-decaying":
        common["facts"] = [
            fact(sid, "relevant", namespace=namespace, key=f"stable-{index:03d}", text=f"{token} durable", embedding_fixture_role="relevant", observed_at="2026-03-01T00:00:00Z", valid_from="2026-03-01T00:00:00Z"),
            fact(sid, "distractor", namespace=namespace, key=f"active-{index:03d}", text=f"{token} {token}", embedding_fixture_role="near", observed_at="2026-03-01T00:00:00Z", valid_from="2026-03-01T00:00:00Z", write_policy_id="temporal-active-case-evidence"),
        ]
        common["expected_candidate_ids"] = [f"{sid}-relevant", f"{sid}-distractor"]
    elif category == "abstention-conflict-ready":
        common["expected_disposition"] = "abstained"
        common["relevant_ids"] = []
        common["forbidden_ids"] = [f"{sid}-trap"]
        common["facts"] = [
            fact(sid, "trap", namespace=namespace, key=f"conflict-{index:03d}", text=f"{token} unresolved alternatives", embedding_fixture_role="trap", sensitivity="restricted"),
        ]
    elif category == "isolation-lifecycle":
        trap_group = (index - 1) // 8
        trap_scope = ["secondary-tenant", "secondary-subject", "primary", "primary"][trap_group]
        trap_sensitivity = "restricted" if trap_group in (0, 2) else "internal"
        lifecycle = "active" if trap_group < 3 else ("deleted" if index <= 28 else "expired")
        common["forbidden_ids"] = [f"{sid}-trap"]
        common["facts"] = [
            fact(sid, "relevant", namespace=namespace, key=f"allowed-{index:03d}", text=f"{token} authorized", embedding_fixture_role="relevant"),
            fact(sid, "trap", scope=trap_scope, namespace=namespace, key=f"trap-{index:03d}", text=f"{token} {token} private trap", embedding_fixture_role="trap", sensitivity=trap_sensitivity, retention_policy_id="retrieval-test-1s-v1" if lifecycle == "expired" else "standard", lifecycle=lifecycle),
        ]
        common["expected_candidate_ids"] = [f"{sid}-relevant"]
    else:
        raise AssertionError(category)
    return common


def main() -> None:
    plan = [
        ("exact-name", 24, 6),
        ("temporal-contradiction", 24, 6),
        ("stale-distractor", 16, 4),
        ("stable-versus-decaying", 16, 4),
        ("abstention-conflict-ready", 16, 4),
        ("isolation-lifecycle", 32, 8),
    ]
    scenarios = []
    ordinal = 1
    for category, count, calibration in plan:
        for index in range(1, count + 1):
            scenarios.append(scenario(category, index, ordinal, calibration))
            ordinal += 1
    corpus = {"version": "retrieval-corpus-v1", "seed": SEED, "scenarios": scenarios}
    OUTPUT.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(corpus, indent=2, sort_keys=True) + "\n").encode()
    (OUTPUT / "corpus.json").write_bytes(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    manifest = {
        "version": "retrieval-corpus-manifest-v1",
        "corpus_sha256": digest,
        "seed": SEED,
        "scenario_count": len(scenarios),
        "calibration_count": sum(item["split"] == "calibration" for item in scenarios),
        "gate_count": sum(item["split"] == "gate" for item in scenarios),
        "baselines": ["exact-fts-only", "exact-vector-only", "hybrid-without-temporal", "full-policy"],
        "embedder": {"id": "embedding-conformance-4d-v1", "version": "1", "kind": "deterministic-fixture"},
        "projection": {"schema_version": 1},
    }
    (OUTPUT / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"generated {len(scenarios)} scenarios; corpus sha256 {digest}")


if __name__ == "__main__":
    main()
