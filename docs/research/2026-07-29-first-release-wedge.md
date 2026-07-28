# First-release wedge and proof strategy

Status: research recommendation for Wayfinder issues #10 and #11
Evidence cutoff: 2026-07-29
Decision horizon: first public development release through 2027

## Recommendation

Palimpsest should be **the governed, bitemporal case-memory layer for teams that
run multi-tenant operational agents**.

The first customer is the platform or security engineer at a 10-100 person B2B
software company deploying a support, success, or casework agent across customer
accounts. Their urgent job is:

> Let an agent reuse changing account and case facts across sessions without
> showing the wrong principal stale, superseded, unsupported, or deleted
> information, and produce evidence of what the system knew, when it knew it,
> and why it returned it.

The first release should own one complete **authorized case-memory lifecycle**:
ingest an immutable episode, commit an attributable fact revision under an
explicit write policy, supersede or conflict that revision, retrieve the current
or as-of authorized view, and complete scoped deletion. Every returned item must
carry its provenance, temporal perspective, authorization decision, and
retrieval-policy version.

This is deliberately not “better vector memory,” a general agent runtime, an
autonomous knowledge graph, or another conversation-history store. The durable
position is **memory correctness and governance as an executable public
contract**.

## Evidence: why this wedge exists now

### 1. The ecosystem has validated persistent memory, but generic recall is crowded

As a rough demand signal rather than a market-size estimate, the first-party
GitHub repositories for [Mem0](https://github.com/mem0ai/mem0),
[Graphiti](https://github.com/getzep/graphiti),
[LangGraph](https://github.com/langchain-ai/langgraph), and
[Letta](https://github.com/letta-ai/letta) had approximately 61,900, 29,300,
38,300, and 24,000 stars respectively when queried through GitHub's API on
2026-07-29. Each has an active memory surface:

- [OpenAI Agents SDK sessions](https://openai.github.io/openai-agents-python/sessions/)
  retain conversation history and offer SQLite, Redis, SQLAlchemy, MongoDB,
  Dapr, hosted, compacted, and encrypted implementations.
- [LangGraph](https://docs.langchain.com/oss/python/concepts/memory) separates
  thread-scoped checkpoints from cross-thread semantic, episodic, and
  procedural memory; its store persists namespaced JSON documents and can add
  semantic search.
- [Mem0](https://docs.mem0.ai/open-source/features/rest-api) exposes add, list,
  search, update, delete, and per-memory history through an OSS REST server.
- [Letta](https://docs.letta.com/guides/core-concepts/memory/memory-blocks)
  gives agents persistent, editable, shareable blocks that remain in context.
- [Graphiti](https://github.com/getzep/graphiti) already provides temporal fact
  validity, raw episodes, provenance, automatic invalidation, and hybrid
  semantic, keyword, and graph retrieval.

**Evidence-backed conclusion:** persistence, search, personalization, and even
temporal graphs are no longer an unoccupied position. A release pitched as
“memory for agents” or “temporal knowledge graph” enters a mature feature race
without a crisp reason to switch.

### 2. 2026 evaluation work exposes a governance and temporal-correctness gap

The strongest new evidence is not another recall leaderboard:

- [GateMem](https://arxiv.org/abs/2606.18829) evaluates legitimate utility,
  contextual access control, and active forgetting in multi-principal medical,
  office, education, and household settings. Its authors report that none of
  the tested methods simultaneously achieved strong utility, robust access
  control, and reliable forgetting. The public
  [artifact](https://rzhub.github.io/GateMem/project.html) provides code, data,
  leak-target annotations, a leaderboard, and the multiplicative governance
  score `MGS = U * (1 - A) * (1 - F)`.
- [GroupMemBench](https://arxiv.org/abs/2605.14498) tests group dynamics,
  speaker-grounded belief, knowledge update, temporal reasoning, and
  abstention. The strongest reported system averaged only 46.0%; knowledge
  update reached 27.1%, and a simple BM25 baseline matched or exceeded most
  memory systems.
- [LongMemEval](https://github.com/xiaowu0162/LongMemEval) contributes 500
  questions spanning extraction, multi-session reasoning, knowledge updates,
  temporal reasoning, and abstention, with timestamped and scalable histories.
- [LongMemEval-V2](https://github.com/xiaowu0162/LongMemEval-V2), released in
  2026, moves from chat recollection to whether agents learn static state,
  dynamic state, workflows, environment gotchas, and invalid premises from up
  to 500 multimodal trajectories and 115 million tokens. It scores the
  accuracy-latency frontier, not accuracy alone.
- [PASB](https://arxiv.org/abs/2607.10526) traces 1,600 cases across a cleared
  session boundary. Its authors report downstream failure rising from 45.0% in
  session-only episodes to 71.9% after commitment, with stored claims showing
  status promotion, attribution removal, and scope broadening.
- [StateFuse](https://arxiv.org/abs/2607.05844) finds that conflict-preserving
  surfaces do not automatically beat strong flat baselines on answer accuracy,
  but do expose contradictions and enable safer abstention and correction than
  collapsed last-write-wins state. That is evidence for a stronger contract,
  not for graph complexity.

**Evidence-backed conclusion:** the frontier is moving from “can it recall?” to
“can it govern durable writes and return current, authorized, attributable
truth under change?” Palimpsest's existing invariants align unusually well with
that frontier.

### 3. Runtime checkpointing is important, but it is not the winning product wedge

[LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
already saves graph state at each step, supports pending-write recovery, replay,
time travel, human approval, and fault-tolerant resume. The
[OpenAI Agents SDK `RunState`](https://openai.github.io/openai-agents-python/ref/run_state/)
is a serializable pause/resume boundary, while its sessions manage stored
conversation history.

**Evidence-backed conclusion:** Palimpsest should retain a clean checkpoint
resource for interoperability, but should not lead with a replacement workflow
runtime. Existing runtimes should call Palimpsest for governed long-term memory.

## Inference: exact customer and job

No customer interviews or production telemetry exist in this repository. The
following choice is therefore a product inference, not discovered demand.

### Initial customer

The buyer/user pair is:

- **Buyer:** engineering or security lead accountable for a multi-tenant B2B
  agent and its data boundary.
- **Primary user:** platform/backend engineer integrating one support or casework
  agent with an existing PostgreSQL deployment.
- **Deployment:** one organization, multiple customer accounts, multiple human
  principals and agent workers, single region, self-hosted or customer VPC.
- **Trigger:** the team has moved beyond a prototype and now needs corrections,
  deletion, auditability, and isolation without replaying full transcripts.

Customer support/casework is the best initial domain because its unit of work is
bounded, facts change, several principals participate, stale information causes
observable harm, and account isolation is mandatory. The first release should
not claim medical, legal, or financial production readiness; GateMem's domains
are evaluation stressors, not authorization to market regulated compliance.

### Job to be done

“When our agent learns or corrects a fact about a customer case, give every
future run the authorized current truth and its evidence; when we investigate,
reconstruct the valid-time and recorded-time view; when the subject deletes it,
make it unavailable to the agent.”

### Why this is stronger through 2027

Model quality, embedding models, rerankers, and framework APIs will continue to
change. Tenant/subject scope, provenance, bitemporal revision semantics,
conflict visibility, explicit write policy, and deletion completion are durable
data-contract properties. They can survive a change from pgvector to another
index or from LangGraph to another runtime. This is the basis of the 2027 moat.

## The narrowest complete vertical slice

The first release should expose one versioned HTTP lifecycle and one Python
client. The stable public behavior is:

1. **Observe:** append a tenant-, subject-, case-, principal-, and source-scoped
   episode with observed time, recorded time, sensitivity, retention, content
   hash, and idempotency key.
2. **Commit or correct:** create a fact revision only through an attributable
   write policy; link evidence; use optimistic concurrency; supersede an exact
   predecessor or surface an unresolved conflict rather than overwrite it.
3. **Read:** return either the current view or an explicit valid-time and/or
   recorded-time as-of view. Apply authorization, deletion, retention, and
   sensitivity filters before lexical/vector candidate generation.
4. **Explain:** return source episode identifiers, revision chain, temporal
   interpretation, component scores, retrieval-policy version, and a redacted
   authorization decision receipt.
5. **Forget:** run scoped deletion as an idempotent state machine across
   canonical records, indexes, caches, and artifact access; retain only the
   documented audit-safe tombstone.

Keep these outside the wedge: UI, autonomous free-form consolidation, graph
database, general workflow orchestration, multi-region writes, procedure
learning, artifact bodies, TypeScript SDK, hosted control plane, and a claim of
regulatory compliance. Automated extraction can be an experimental adapter,
but the release claim must also work with explicit caller-proposed facts so a
model is not the authority at the commit boundary.

## Differentiated outcome

The product promise should be:

> **No stale, forbidden, unsupported, or deleted case fact silently enters an
> agent's context. Every returned fact can be traced and reconstructed as of the
> relevant time.**

This is stronger and more testable than “higher memory accuracy.” It joins four
properties competitors tend to expose separately:

| Surface | First-party evidence | Gap Palimpsest should own |
| --- | --- | --- |
| OpenAI Agents SDK / LangGraph | Strong session and durable-execution primitives; flexible namespaced stores | Governed cross-runtime fact revisions, not another runtime |
| Mem0 | Simple memory CRUD, search, history, decay, and entity linking | Explicit bitemporal views, authorization-before-candidates, conflict surface, deletion proof |
| Letta | Agent-managed persistent blocks and archival memory | The docs warn concurrent block updates are last-write-wins; Palimpsest preserves evidence and correction history |
| Graphiti / Zep | The closest competitor: temporal facts, episodes, provenance, hybrid graph retrieval | A PostgreSQL-first service whose public contract and conformance suite make multi-principal authorization, deletion, and recorded-time audit release gates |

The Graphiti gap must be treated carefully. A user-filed
[Graphiti issue #1383](https://github.com/getzep/graphiti/issues/1383) describes
missing scoped write authority and downstream derivation tracking, but it is an
external proposal, not a maintainer-confirmed security finding. Graphiti also
states that its OSS core requires users to build surrounding user/conversation
management and production controls, while managed Zep supplies governance and
scale. Palimpsest must win by proof and operational simplicity, not by claiming
Graphiti has no temporal model.

## Proof strategy

### Release corpus

Create a versioned **Palimpsest Governed Temporal Memory Suite** with a public
generator, fixed seeds, expected candidate sets, and machine-readable judgments.
Start with at least 400 black-box scenarios across four equal families:

- temporal updates: late-arriving evidence, future-valid changes, corrections,
  overlapping intervals, clock skew, and valid-time versus recorded-time asks;
- multi-principal traps: cross-tenant, cross-subject, delegated, expired grant,
  indirect confirmation, error/log/export leakage, and cache/index leakage;
- write governance: unsupported assertions, attribution loss, scope broadening,
  repeated pressure, concurrent correction, retry, and unresolved conflict;
- active forgetting and recovery: partial deletion, retry, reindex, cache loss,
  backup/restore, tombstone behavior, and attempts to reconstruct deleted facts.

Use [GateMem's public artifact](https://github.com/rzhub/GateMem) as the external
governance evaluation, [LongMemEval](https://github.com/xiaowu0162/LongMemEval)
for update/temporal/abstention comparability, and the public
[PASB artifact](https://github.com/henrymao2004/agent-sycophancy) for commit-gate
stress tests. Add LongMemEval-V2 Small only after the first release to test the
2027 “experienced operational colleague” expansion; its 115-million-token upper
tier is not a sensible first-release gate.

### Hard release metrics

Report every metric with the corpus version, model and prompt version, embedding
version, hardware, database size, warm/cold state, confidence interval, and raw
predictions. Do not publish one blended “memory score” alone.

| Property | Development-release gate |
| --- | --- |
| Authorization | 0 unauthorized records enter candidate sets, outputs, explanations, logs, exports, or errors in the owned suite; 0 GateMem access-control violations on the selected public split |
| Forgetting | 0 deleted facts are returned, confirmed, or reconstructed after completion; all partial failures converge or remain explicitly incomplete; 0 GateMem active-forgetting failures on the selected split |
| Temporal correctness | 100% exact current, valid-time as-of, and recorded-time as-of answers on deterministic temporal scenarios |
| Provenance and conflict | 100% of returned facts link to source episodes and policy version; 100% of injected contradictions are either resolved by explicit evidence/policy or surfaced, never silently overwritten |
| Useful retrieval | At least 90% evidence Recall@10 overall and 85% on update/temporal subsets; fixed-reader answer accuracy at least 10 percentage points above both BM25-only and vector-only baselines on those subsets, with no category regressing by more than 2 points |
| Governance utility | GateMem `U >= 0.80`, `A = 0`, `F = 0`, hence `MGS >= 0.80`, reported both overall and by domain; zero observed violations must include an upper confidence bound, not be described as proof of universal safety |
| Latency and context | On a published 1-million-revision single-node profile, authorized current retrieval p95 <= 200 ms and p99 <= 400 ms at 20 concurrent clients; <= 5,000 retrieved-context tokens per query and at least 80% fewer tokens than full-history input |
| Recovery | After backup/restore and complete index rebuild, canonical hashes, current/as-of answers, deletion states, and authorization results match the pre-failure oracle exactly |

The retrieval thresholds are proposed release targets, not measured Palimpsest
results. If the initial baselines already exceed them, ratchet the gates upward;
never tune the corpus or reader after seeing held-out answers.

### Baselines and ablations

Every report should compare the same fixed reader and verification budget across:

1. full history with no memory service;
2. PostgreSQL BM25 only;
3. pgvector only;
4. hybrid retrieval without temporal/governance policy;
5. full Palimpsest policy.

Ablate temporal filters, pre-candidate authorization, provenance, conflict
surfacing, and deletion propagation one at a time. This shows which contract
property changes outcomes and avoids attributing model gains to the memory
service.

## Adoption path

1. Ship a pinned Docker Compose development profile using the customer's
   existing PostgreSQL mental model, one migration command, health/readiness
   checks, and one black-box conformance command.
2. Offer four Python operations—`remember`, `recall`, `correct`, and `forget`—as
   middleware/tools for LangGraph and OpenAI Agents SDK. Do not require replacing
   their checkpoints, runner, model, or message history.
3. Import episodes first in shadow mode, compare Palimpsest's authorized context
   with the incumbent memory output, and block production reads until tenant,
   temporal, and deletion gates pass on customer fixtures.
4. Land with one agent and one case namespace; expand to shared agents and
   organizational memory only after access policies and audit receipts are
   validated. Keep HTTP/OpenAPI stable and MCP/framework adapters replaceable.

The first useful demo should be a support-case correction: a customer changes
shipping address after an earlier order, the new fact becomes current at an
explicit valid time, an auditor reconstructs what was known before the delayed
update, another tenant cannot retrieve either value, and deletion removes both
from agent-facing memory while preserving an audit-safe tombstone.

## Risks and falsification criteria

This recommendation should be reversed or narrowed if any of these occur during
discovery:

- fewer than 3 of 10 qualified platform/security interviews report a current
  production pain involving correction, isolation, deletion, or audit;
- three design partners prefer a managed context graph and will not operate a
  PostgreSQL service even with a one-command profile;
- Palimpsest cannot reach GateMem's zero-violation gates without collapsing
  legitimate utility below `U = 0.80`;
- explicit or policy-gated fact commits create enough integration work that
  teams choose unsafe autonomous extraction instead;
- the 1-million-revision profile misses the published latency gate by more than
  2x after query-plan and index tuning.

Until customer interviews are complete, call this the **strongest evidence-backed
wedge hypothesis**, not validated product-market fit. The immediate decision for
issue #11 should adopt the customer, job, slice, outcome, and gates above, then
schedule interviews and three design-partner shadow deployments as the next
validation step.
