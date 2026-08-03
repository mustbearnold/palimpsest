import assert from "node:assert/strict";
import { test } from "node:test";

import { PalimpsestClient, compareProjectBundles } from "../src/index.js";

const TENANT = "019be000-0000-7000-8000-000000000010";
const SUBJECT = "019be000-0000-7000-8000-000000000020";
const CASE = "019be000-0000-7000-8000-000000000030";

test("remember uses separate governed episode and fact writes", async () => {
  const requests = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    requests.push({ url: String(url), init });
    const body = JSON.parse(init.body);
    const response = requests.length === 1
      ? { episode_id: "019be000-0000-7000-8000-000000000040" }
      : { fact_id: "019be000-0000-7000-8000-000000000050" };
    return new Response(JSON.stringify(response), {
      status: 201,
      headers: { "content-type": "application/json", etag: '"revision-1"' },
    });
  };

  try {
    const client = new PalimpsestClient({
      baseUrl: "http://127.0.0.1:8080",
      bearerToken: "test-token",
      tenantId: TENANT,
      subjectId: SUBJECT,
      caseId: CASE,
    });
    const result = await client.remember("The address is 10 Test Street.", {
      key: "shipping-address",
      idempotencyKey: "remember-1",
    });

    assert.equal(result.episode.episode_id, "019be000-0000-7000-8000-000000000040");
    assert.equal(result.fact.fact_id, "019be000-0000-7000-8000-000000000050");
    assert.deepEqual(requests.map(({ init }) => init.headers["Idempotency-Key"]), [
      "remember-1:episode",
      "remember-1:fact",
    ]);
    assert.equal(JSON.parse(requests[0].init.body).payload.content, "The address is 10 Test Street.");
    assert.equal(JSON.parse(requests[1].init.body).evidence_episode_ids[0], "019be000-0000-7000-8000-000000000040");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("configuration rejects credentials in the base URL", () => {
  assert.throws(
    () => new PalimpsestClient({
      baseUrl: "https://user:password@example.invalid",
      bearerToken: "token",
      tenantId: TENANT,
      subjectId: SUBJECT,
    }),
    /baseUrl must not contain credentials/,
  );
});

test("partial remember preserves the committed episode and typed HTTP cause", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls += 1;
    if (calls === 1) {
      return new Response(JSON.stringify({ episode_id: "019be000-0000-7000-8000-000000000040" }), {
        status: 201,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(JSON.stringify({ type: "invalid-request" }), {
      status: 422,
      headers: { "content-type": "application/problem+json" },
    });
  };

  try {
    const client = new PalimpsestClient({
      baseUrl: "http://127.0.0.1:8080",
      bearerToken: "test-token",
      tenantId: TENANT,
      subjectId: SUBJECT,
      caseId: CASE,
    });
    await assert.rejects(
      client.remember("Keep the evidence.", { key: "partial", idempotencyKey: "partial-1" }),
      (error) => {
        assert.equal(error.constructor.name, "PartialRememberError");
        assert.equal(error.episode.episode_id, "019be000-0000-7000-8000-000000000040");
        assert.equal(error.cause.statusCode, 422);
        assert.deepEqual(error.cause.problem, { type: "invalid-request" });
        return true;
      },
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("export ready redirects remain visible and are not followed", async () => {
  const originalFetch = globalThis.fetch;
  let request;
  globalThis.fetch = async (url, init) => {
    request = { url: String(url), init };
    return new Response(null, {
      status: 303,
      headers: {
        location: `/v1/tenants/${TENANT}/subjects/${SUBJECT}/exports/019be000-0000-7000-8000-0000000000c0/content`,
        etag: '"export-2"',
      },
    });
  };

  try {
    const client = new PalimpsestClient({
      baseUrl: "http://127.0.0.1:8080",
      bearerToken: "test-token",
      tenantId: TENANT,
      subjectId: SUBJECT,
    });
    const response = await client.getExportResponse("019be000-0000-7000-8000-0000000000c0", {
      ifNoneMatch: '"export-1"',
    });
    assert.equal(response.statusCode, 303);
    assert.equal(response.etag, '"export-2"');
    assert.match(response.location, /\/content$/);
    assert.equal(request.init.redirect, "manual");
    assert.equal(request.init.headers["If-None-Match"], '"export-1"');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("recallByProject keeps each project candidate set separate", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, init) => {
    requests.push({ url: String(url), init, body: JSON.parse(init.body) });
    return new Response(JSON.stringify({ status: "results", items: [] }), {
      status: 201,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    const client = new PalimpsestClient({
      baseUrl: "http://127.0.0.1:8080",
      bearerToken: "test-token",
      tenantId: TENANT,
      subjectId: SUBJECT,
    });
    const result = await client.recallByProject(
      "release decision",
      ["project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"],
      { idempotencyKeyPrefix: "compare-1" },
    );

    assert.deepEqual(Object.keys(result), ["project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"]);
    assert.deepEqual(requests.map(({ body }) => body.filters), [
      { namespaces: ["agent_session:project-aaaaaaaaaaaaaaaa"] },
      { namespaces: ["agent_session:project-bbbbbbbbbbbbbbbb"] },
    ]);
    assert.deepEqual(requests.map(({ init }) => init.headers["Idempotency-Key"]), [
      "compare-1:project-aaaaaaaaaaaaaaaa",
      "compare-1:project-bbbbbbbbbbbbbbbb",
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("compareProjectBundles marks key differences without semantic overclaim", () => {
  const comparison = compareProjectBundles({
    "project-a": {
      items: [
        { fact_id: "fact-a-1", revision_id: "revision-a-1", key: "release-target", value: { content: "v1" } },
        { fact_id: "fact-a-2", revision_id: "revision-a-2", key: "only-a", value: { content: "local" } },
      ],
    },
    "project-b": {
      items: [
        { fact_id: "fact-b-1", revision_id: "revision-b-1", key: "release-target", value: { content: "v2" } },
        { fact_id: "fact-b-2", revision_id: "revision-b-2", key: "only-b", value: { content: "local" } },
      ],
    },
  });

  assert.equal(comparison.profile, "project-comparison-structural-v1");
  assert.equal(comparison.semantic_inference.performed, false);
  assert.equal(comparison.durable_write, false);
  const groups = Object.fromEntries(comparison.groups.map((group) => [group.key, group]));
  assert.equal(groups["release-target"].classification, "same_key_different_value");
  assert.equal(groups["only-a"].classification, "project_specific");
  assert.equal(groups["only-b"].classification, "project_specific");
  assert.equal(comparison.summary.same_key_different_value_groups, 1);
});

test("compareProjectBundles recognizes exact canonical values", () => {
  const comparison = compareProjectBundles({
    "project-a": { items: [{ key: "same", value: { content: "v1", metadata: { source: "a" } } }] },
    "project-b": { items: [{ key: "same", value: { metadata: { source: "a" }, content: "v1" } }] },
  });

  assert.equal(comparison.groups[0].classification, "exact_match");
  assert.equal(comparison.summary.exact_match_groups, 1);
});

test("compareByProject retrieves isolated bundles and adds a structural summary", async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, init) => {
    requests.push({ url: String(url), init, body: JSON.parse(init.body) });
    return new Response(JSON.stringify({ status: "results", items: [] }), {
      status: 201,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    const client = new PalimpsestClient({
      baseUrl: "http://127.0.0.1:8080",
      bearerToken: "test-token",
      tenantId: TENANT,
      subjectId: SUBJECT,
    });
    const result = await client.compareByProject(
      "release decision",
      ["project-aaaaaaaaaaaaaaaa", "project-bbbbbbbbbbbbbbbb"],
      { idempotencyKeyPrefix: "compare-2" },
    );

    assert.equal(result.profile, "project-comparison-structural-v1");
    assert.equal(result.comparison.summary.bundle_count, 2);
    assert.equal(requests.length, 2);
    assert.deepEqual(requests.map(({ body }) => body.filters), [
      { namespaces: ["agent_session:project-aaaaaaaaaaaaaaaa"] },
      { namespaces: ["agent_session:project-bbbbbbbbbbbbbbbb"] },
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("compareProjectBundles returns bounded lexical review candidates", () => {
  const comparison = compareProjectBundles({
    "project-a": {
      items: [{ key: "decision-a", value: { content: "release target ships on stable channel" } }],
    },
    "project-b": {
      items: [{ key: "decision-b", value: { content: "release target ships on beta channel" } }],
    },
  });

  assert.equal(comparison.lexical_review.profile, "token-jaccard-v1");
  assert.equal(comparison.summary.lexical_review_candidate_count, 1);
  assert.ok(comparison.lexical_review.candidates[0].similarity >= 0.5);
  assert.equal(comparison.semantic_inference.performed, false);
});
