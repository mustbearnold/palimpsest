# Palimpsest TypeScript client

This is a dependency-free TypeScript/JavaScript client for Palimpsest's versioned `/v1` HTTP API. It uses the platform `fetch` implementation and ships runtime JavaScript plus TypeScript declarations, so no generator or bundler is required. The server remains responsible for authorization, temporal correctness, write-policy validation, and deletion state.

Install it from a checkout:

```bash
npm install ./clients/typescript
```

The high-level helpers mirror the Python client:

```ts
import { PalimpsestClient } from "@palimpsest/client";

const client = new PalimpsestClient({
  baseUrl: "http://127.0.0.1:8080",
  bearerToken: "operator-issued-token",
  tenantId: "019be000-0000-7000-8000-000000000010",
  subjectId: "019be000-0000-7000-8000-000000000020",
  caseId: "019be000-0000-7000-8000-000000000030",
});

const saved = await client.remember("The customer moved to 20 New Street.", {
  key: "shipping-address",
  idempotencyKey: "case-123-address-1",
});
const current = await client.recall("shipping address", {
  idempotencyKey: "case-123-recall-1",
});
const projectBundles = await client.recallByProject(
  "release decision",
  ["project-0123456789abcdef", "project-fedcba9876543210"],
  { idempotencyKeyPrefix: "case-123-compare-1" },
);
const projectComparison = await client.compareByProject(
  "release decision",
  ["project-0123456789abcdef", "project-fedcba9876543210"],
  { idempotencyKeyPrefix: "case-123-compare-2" },
);
const fact = await client.getFactResponse(saved.fact.fact_id);
await client.correct(saved.fact.fact_id, {
  supersedesRevisionId: fact.data.revision.revision_id,
  value: { address: "22 New Street" },
  observedAt: "2026-08-03T00:00:00Z",
  validTime: { from: "2026-08-03T00:00:00Z" },
  evidenceEpisodeIds: [saved.episode.episode_id],
  writePolicy: { id: "direct-evidence", version: "1" },
  confidence: 1,
  sensitivity: "internal",
  retentionPolicyId: "standard",
  ifMatch: fact.etag,
  idempotencyKey: "case-123-address-2",
});
```

Mutations generate an idempotency key when one is omitted. Callers that retry should supply a stable key. `remember` intentionally performs two durable requests; if the episode succeeds and fact promotion fails, it throws `PartialRememberError` with the committed episode and typed cause.

Ready export status is represented as a `303` response with its download `Location`; redirects are not followed implicitly. `waitForDeletion` uses conditional requests and stops only at a server-reported terminal state.

`recallByProject` returns separate retrieval responses with exact project namespaces. `compareByProject` returns those bundles plus deterministic exact-key/value-digest classifications and bounded token-overlap candidates for differently keyed session messages. The result also reports project-root, branch, source, role, and unique-session context observed in returned ingestion metadata. Lexical candidates include a bounded shared/only-in token delta so wording changes are visible. Same-key/different-value and lexical-overlap groups are review candidates only: no model inference, semantic conflict claim, or durable write occurs.

An external model or human can interpret the isolated evidence, after which the client can validate the bounded, attributed claims:

```js
import { validateProjectReview } from "palimpsest";

const validated = validateProjectReview(comparison, review);
```

Validation checks returned fact/revision and source-episode citations plus reviewer and policy digests. An explicitly approved consolidation can then write caller-supplied facts while deriving source episode lineage from the validated claims:

```js
const result = await client.consolidateProjectReview(comparison, review, [{
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
}], { consolidationId: "review-run-1" });
```

Reuse the consolidation ID and all write inputs to retry. Each claim is a separate idempotent fact write, so a later failure raises `PartialConsolidationError` with the committed prefix. It is not an atomic batch and does not prove semantic truth.
