import assert from "node:assert/strict";
import { test } from "node:test";

import {
  compareProjectBundles,
  prepareProjectConsolidation,
  validateProjectReview,
} from "../src/index.js";

function comparisonResult() {
  const bundles = {
    "project-a": {
      status: "results",
      items: [{
        fact_id: "fact-a",
        revision_id: "revision-a",
        namespace: "agent_session:project-a",
        key: "release-target",
        value: { content: "ship version one", metadata: {} },
        evidence_episode_ids: ["episode-a"],
      }],
    },
    "project-b": {
      status: "results",
      items: [{
        fact_id: "fact-b",
        revision_id: "revision-b",
        namespace: "agent_session:project-b",
        key: "release-target",
        value: { content: "ship version two", metadata: {} },
        evidence_episode_ids: ["episode-b"],
      }],
    },
  };
  const comparison = compareProjectBundles(bundles);
  return { profile: comparison.profile, bundles, comparison };
}

function reviewPayload() {
  return {
    reviewer: {
      principal_id: "agent:project-review",
      provider: "openai",
      model: "gpt-5",
      model_revision: "2026-08-03",
      prompt_sha256: "a".repeat(64),
    },
    review_policy: { id: "project-review-v1", version: "1", sha256: "b".repeat(64) },
    claims: [{
      claim_id: "claim-release-target",
      classification: "semantic_conflict",
      summary: "The projects record different release targets.",
      projects: ["project-a", "project-b"],
      confidence: 0.91,
      evidence: [
        { project_id: "project-a", fact_id: "fact-a", revision_id: "revision-a", evidence_episode_ids: ["episode-a"] },
        { project_id: "project-b", fact_id: "fact-b", revision_id: "revision-b", evidence_episode_ids: ["episode-b"] },
      ],
    }],
  };
}

function consolidationWrite() {
  return {
    claim_id: "claim-release-target",
    namespace: "shared",
    key: "release-target-difference",
    value: { content: "The projects target different release channels." },
    observed_at: "2026-08-03T00:00:00Z",
    valid_time: { from: "2026-08-03T00:00:00Z" },
    write_policy: { id: "project-consolidation", version: "1" },
    confidence: 0.91,
    sensitivity: "internal",
    retention_policy_id: "standard",
  };
}

test("valid project review is attributed and non-writing", () => {
  const result = validateProjectReview(comparisonResult(), reviewPayload());

  assert.equal(result.profile, "project-comparison-semantic-review-v1");
  assert.equal(result.contract_validation.passed, true);
  assert.equal(result.contract_validation.semantic_truth_proven, false);
  assert.equal(result.durable_write, false);
  assert.equal(result.claims[0].evidence[0].key, "release-target");
});

test("project review rejects an ungrounded citation", () => {
  const review = reviewPayload();
  review.claims[0].evidence[1].revision_id = "revision-not-returned";

  assert.throws(
    () => validateProjectReview(comparisonResult(), review),
    /does not identify a returned retrieval item/,
  );
});

test("project review rejects a comparison not matching its bundles", () => {
  const result = comparisonResult();
  result.comparison.summary.item_count = 999;

  assert.throws(
    () => validateProjectReview(result, reviewPayload()),
    /does not match its bundles/,
  );
});

test("consolidation plan derives episode lineage and stable retry key", () => {
  const validated = validateProjectReview(comparisonResult(), reviewPayload());
  const plan = prepareProjectConsolidation(validated, [consolidationWrite()], { consolidationId: "review-run-1" });
  const repeat = prepareProjectConsolidation(validated, [consolidationWrite()], { consolidationId: "review-run-1" });

  assert.equal(plan.profile, "project-comparison-governed-consolidation-v1");
  assert.deepEqual(plan.claim_ids, ["claim-release-target"]);
  assert.deepEqual(plan.source_episode_ids, ["episode-a", "episode-b"]);
  assert.deepEqual(plan.writes[0].evidence_episode_ids, ["episode-a", "episode-b"]);
  assert.equal(plan.writes[0].classification, "semantic_conflict");
  assert.equal(plan.writes[0].idempotency_key, repeat.writes[0].idempotency_key);
  assert.match(plan.writes[0].idempotency_key, /^palimpsest-consolidation-v1:/);
});

test("consolidation plan rejects insufficient evidence and caller lineage", () => {
  const validated = validateProjectReview(comparisonResult(), reviewPayload());
  validated.claims[0].classification = "insufficient_evidence";
  assert.throws(
    () => prepareProjectConsolidation(validated, [{ ...consolidationWrite(), evidence_episode_ids: ["unrelated"] }], {
      consolidationId: "review-run-1",
    }),
    /insufficient_evidence/,
  );

  const validReview = validateProjectReview(comparisonResult(), reviewPayload());
  assert.throws(
    () => prepareProjectConsolidation(validReview, [{ ...consolidationWrite(), evidence_episode_ids: ["unrelated"] }], {
      consolidationId: "review-run-1",
    }),
    /evidence_episode_ids are derived/,
  );
});
