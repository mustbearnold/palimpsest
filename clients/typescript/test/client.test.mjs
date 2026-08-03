import assert from "node:assert/strict";
import { test } from "node:test";

import { PalimpsestClient } from "../src/index.js";

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
