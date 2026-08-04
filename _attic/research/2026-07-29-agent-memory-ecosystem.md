# Agent-memory ecosystem and interoperability surface, 2026–2027

**Research date:** 2026-07-29 **Question:** Which ecosystem developments, public protocols, evaluation practices, and interoperability expectations should constrain Palimpsest so its first release remains relevant through 2027?

## Executive recommendation

Palimpsest should remain a **versioned temporal memory service with its own HTTP/OpenAPI contract**. It should not make MCP, A2A, an agent framework, an embedding store, or an observability backend its public domain model.

Adopt these durable seams:

1. Publish the canonical API as HTTP described initially by [OpenAPI 3.1.2](https://spec.openapis.org/oas/v3.1.2.html), using a 3.2-compatible design subset, explicit JSON Schema dialects, generated-client compatibility tests, idempotency, cursors, conditional writes, and additive evolution within `/v1`. Promote the description to [OpenAPI 3.2.0](https://spec.openapis.org/oas/v3.2.0.html) only after the complete validator, mock, diff, renderer, and client-generator corpus passes.
2. Ship an MCP server adapter for model-facing retrieval and controlled writes; expose read-only addressable views as resources and operations as tools.
3. Support A2A 1.0 identifiers and message/artifact ingestion at the boundary, but do not equate an A2A task, context, or history with durable memory.
4. Emit OpenTelemetry traces using stable core APIs and W3C propagation. Pin any GenAI semantic-convention version behind a translation layer.
5. Preserve provenance internally in a model that can export to W3C PROV; retain raw episodes and derivation links rather than storing only model-made summaries or embeddings.

**Inference:** No reviewed standard defines a portable, trustworthy canonical record for agent memory. MCP standardizes context and tool exchange, while A2A standardizes opaque-agent collaboration. Major frameworks and products expose incompatible session, store, fact, graph, or memory-record abstractions. Palimpsest's defensible interoperability surface is therefore a precise memory contract plus adapters—not adoption of a vendor's storage schema.

## What is stable enough to adopt

| Surface | Source-backed state on 2026-07-29 | Palimpsest decision |
| --- | --- | --- |
| HTTP API descriptions | OpenAPI 3.2.0 is the latest published OAS, dated 2025-09-19; 3.1.2 remains a published version. OAS is language-agnostic and intended for documentation, generation, and testing. [Published versions](https://spec.openapis.org/oas/) | **Adopt 3.1.2 initially; gate 3.2.0.** Keep `/v1` behavior independent of document syntax and promote only when the complete toolchain corpus passes. Avoid implementation-defined constructs even when valid OAS. |
| MCP core | The 2025-11-25 revision is a stateful JSON-RPC client-host-server protocol with version and capability negotiation. Servers expose resources, tools, and prompts; connections are isolated by the host. [Architecture](https://modelcontextprotocol.io/specification/2025-11-25/architecture/index) [Lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) | **Adopt as an adapter.** Implement negotiated resources/tools against the canonical service. Do not make JSON-RPC or session lifecycle the storage contract. |
| MCP authorization | Remote MCP authorization uses OAuth protected-resource metadata and requires RFC 8707 resource indicators; the specification emphasizes minimal scopes and step-up authorization. [Authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization) | **Adopt for the remote adapter.** Map external scopes to Palimpsest authorization, never the reverse, and re-run record-level authorization before every retrieval stage. |
| A2A core | A2A 1.0.0 is the latest released version and is described by its project as stable and production-ready. It defines discovery, version negotiation, tasks, messages, parts, artifacts, streaming, and extensions without exposing an agent's internal memory or tools. [1.0 announcement](https://a2a-protocol.org/latest/announcing-1.0/) [Specification](https://a2a-protocol.org/latest/specification/) | **Adopt boundary identifiers and an ingestion/export adapter.** Preserve protocol version, agent/task/context/message/artifact IDs, timestamps, and media types as external provenance. |
| Distributed tracing | OpenTelemetry's trace API and context are stable; its propagator requirements carry W3C `traceparent`/`tracestate`. [Trace API](https://opentelemetry.io/docs/specs/otel/trace/api/) [Propagators](https://opentelemetry.io/docs/specs/otel/context/api-propagators/) | **Adopt.** Store optional trace/span correlation on writes and emit spans for authorization, candidates, reranking, consolidation, and deletion. Telemetry is evidence correlation, not canonical memory. |
| Provenance concepts | W3C PROV is a Recommendation for interoperable provenance. Its starting model is Entity, Activity, and Agent, with derivation, generation, attribution, revision, and primary-source relations. [PROV-O](https://www.w3.org/TR/prov-o/) [PROV-DM](https://www.w3.org/TR/prov-dm/) | **Adopt semantically; export on demand.** Keep a relational internal model with enough lineage to map records, derivations, write policies, and responsible actors to PROV. Do not require RDF in the transactional path. |

## What is volatile and must stay behind adapters

### MCP and A2A evolution

**Fact:** MCP tasks were introduced in the 2025-11-25 revision and remain explicitly experimental; they are durable state machines for deferred request results. [MCP tasks](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)

**Decision:** Do not make MCP task IDs Palimpsest checkpoint IDs or promise MCP task durability as memory durability. An adapter may correlate them. MCP tool definitions should use structured output schemas, but tools and annotations remain untrusted inputs at the service boundary. [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)

**Fact:** A2A 1.0 uses explicit `Major.Minor` protocol negotiation and supports URI-identified extensions. Tasks hold current status, optional message history, and artifacts; the core protocol deliberately permits opaque internal state. [A2A specification](https://a2a-protocol.org/latest/specification/) [Extension governance](https://a2a-protocol.org/latest/topics/extension-and-binding-governance/)

**Decision:** Do not publish a Palimpsest-specific A2A extension in v1. First prove the HTTP contract and adapters. If multiple independent implementations need temporal memory exchange, propose a versioned extension URI later, with a reference implementation and conformance suite.

### Observability vocabulary

**Fact:** OpenTelemetry core tracing is stable, but the GenAI semantic conventions moved to a separate repository and continue to change. The official repository covers agent, MCP, model, tool, metric, and event conventions; the project's 2026 ecosystem note still calls GenAI instrumentation fast-moving. [GenAI repository](https://github.com/open-telemetry/semantic-conventions-genai) [2026 ecosystem note](https://opentelemetry.io/blog/2026/introducing-the-ecosystem-explorer/)

**Decision:** Use stable OTel span/context primitives and internal low-cardinality attributes. Translate to a pinned GenAI schema at export. Default to metadata, counts, timings, policy IDs, and hashes; never emit raw private memories, model prompts, tool payloads, or retrieved content merely because a convention permits content capture.

### Framework and vendor APIs

The market is converging on common *capabilities*, not common record shapes:

| First-party surface | What it establishes | Constraint for Palimpsest |
| --- | --- | --- |
| OpenAI Agents SDK | Sessions maintain conversation history and provide interchangeable SQLite, Redis, SQLAlchemy, MongoDB, Dapr, encrypted, and hosted implementations. Session memory cannot be combined indiscriminately with server-managed response continuation. [Sessions](https://openai.github.io/openai-agents-python/sessions/) | Provide a session backend/adapter eventually, but distinguish thread checkpoints from cross-thread durable facts. Do not couple to one continuation mechanism. |
| LangGraph | Checkpointers persist thread state at graph steps; a separate Store supports cross-thread memory. Replay can re-trigger later model calls and side effects. [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence) | Model checkpoint import separately from facts and episodes. Require idempotency receipts so replay does not duplicate durable writes or successful side effects. |
| Google ADK / Vertex Memory Bank | ADK separates session state from a `MemoryService` that ingests and searches cross-session information. Memory Bank generates or directly ingests scoped memories; its public cloud documentation still identifies Memory Bank as Preview. [ADK memory](https://adk.dev/sessions/memory/) [Memory Bank retrieval](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/fetch-memories) | Target the small `add/search` integration seam, not Google's extracted-memory schema. Treat preview APIs as volatile and keep scope mapping explicit. |
| Amazon Bedrock AgentCore Memory | Short-term memory stores raw session events; asynchronous strategies extract and consolidate long-term records, organized by actor/session/namespace. [Memory types](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/memory-types.html) [Organization](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/memory-organization.html) | Preserve the useful episode-versus-derived-record distinction, but retain attributable derivation and temporal history beyond the managed API's abstraction. |
| Microsoft Agent Framework | Sessions, history providers, and context providers compose conversation state, memory integrations, and audit storage. [Memory and persistence](https://learn.microsoft.com/en-us/agent-framework/get-started/memory) | Expose a provider adapter; avoid making any language framework's provider interface canonical. |
| Mem0 | The self-hosted REST surface provides create/get/search/update/delete/history scoped by user, agent, or run; its inference path extracts and resolves conflicts before storage. [REST API](https://docs.mem0.ai/open-source/features/rest-api) [Add pipeline](https://docs.mem0.ai/core-concepts/memory-operations/add) | Offer migration/import compatibility, not behavioral equivalence. An overwrite-style update is insufficient for Palimpsest's as-of and supersession invariants. |
| Letta | Letta separates in-context memory blocks, files, archival memory, and external RAG; AgentFile serializes agent configuration and editable memory but states that archival passages are planned. [Context hierarchy](https://docs.letta.com/guides/core-concepts/memory/context-hierarchy) [AgentFile](https://docs.letta.com/guides/core-concepts/agent-file) | Consider `.af` import/export only after its memory coverage stabilizes. Keep procedure revisions distinct from agent state and factual memory. |
| Zep / Graphiti | Zep ingests thread messages into a user-level temporal knowledge graph and preserves changing facts in graph context. [Threads](https://help.getzep.com/threads) [Concepts](https://help.getzep.com/concepts) | Treat temporal graphs as evidence that time-aware retrieval matters, not as proof PostgreSQL must be replaced by a graph database. Benchmark before adding one. |

**Inference:** Adapters should target behavioral seams—append event, save/load checkpoint, put/get/search scoped memory, retrieve current/as-of, export, and delete—not mirror SDK class trees. This is the narrowest common surface that can survive framework churn through 2027.

## Recommended public interoperability contract

### Canonical HTTP API

The first stable contract should expose these concepts explicitly:

- tenant, subject, actor, thread, and authorization scope as separate fields;
- immutable episodes with observed time, recorded time, provenance, content type, source identifiers, and idempotency key;
- fact/procedure revision chains with valid-time intervals, recorded time, confidence, sensitivity, derivation links, and supersession reason;
- checkpoints with optimistic concurrency, parent checkpoint, and receipts for completed side effects;
- retrieval with current/as-of mode, exact filters, policy version, bounded candidates, explanation/attribution, and stable pagination;
- scoped export and deletion jobs with status, tombstones, audit evidence, and explicit treatment of regenerated indexes and immutable artifacts.

Every response record should carry its schema version. Unknown additive fields must round-trip where practical. External IDs should be namespaced by source system and protocol version. Content bodies should use explicit media types; large objects should remain integrity-checked artifact references.

### MCP mapping

- **Resources:** schemas, current/as-of read views, retrieval results stored by immutable result ID, and export manifests. Use opaque Palimpsest URIs rather than filesystem or database locations.
- **Tools:** write episode, supersede revision, save/load checkpoint, retrieve, request export, and request deletion. Mutations require explicit scopes, idempotency keys, and structured results.
- **Do not expose:** unrestricted SQL, cross-tenant search, raw embeddings, arbitrary policy bypass, or a generic “remember everything” operation.

### A2A mapping

Ingest A2A Messages and Artifacts as source episodes or artifact references. Store Task and Context IDs as correlation, never as ownership or authorization. Record the negotiated A2A version and extension URIs. On export, preserve the original payload/media type where retention and authorization allow it.

### Provenance and telemetry mapping

Internally, a source episode or fact revision maps to a PROV Entity; ingestion, consolidation, retrieval, and deletion map to Activities; human, agent, model, service, and write-policy identities map to Agents. A derived fact must identify the activity and evidence entities that produced it. OTel trace/span IDs can correlate the runtime activity, but they are optional references because traces may be sampled or expired.

## Evaluation requirements

Public benchmark scores are useful signals, not release evidence.

**Fact:** LongMemEval isolates five abilities: information extraction, multi-session reasoning, temporal reasoning, knowledge updates, and abstention. It contains 500 curated questions and reports a substantial accuracy drop under sustained interaction. [LongMemEval paper](https://arxiv.org/abs/2410.10813)

**Fact:** LoCoMo evaluates question answering, event summarization, and multimodal dialogue across long, multi-session conversations; the original work found long-context and RAG approaches still below human performance. [LoCoMo paper](https://aclanthology.org/2024.acl-long.747/)

**Fact:** Product-authored papers report gains for extraction/consolidation and temporal graph designs, but their configurations, judges, and baselines differ. [Mem0 paper](https://arxiv.org/abs/2504.19413) [Zep paper](https://arxiv.org/abs/2501.13956)

Palimpsest's release gate should therefore include:

1. LongMemEval and LoCoMo-compatible retrieval/answering runs with pinned data, models, prompts, embedders, rerankers, seeds, costs, and latency percentiles.
2. Native bitemporal scenarios: corrections, future-valid facts, late-arriving evidence, backdated writes, simultaneous contradictions, and as-of queries.
3. Security scenarios: cross-tenant and cross-subject non-interference at every retrieval stage, including caches, lexical/vector indexes, graph expansion, reranking, exports, and deletion.
4. Recovery scenarios: crash/retry at each consolidation and checkpoint seam, duplicate delivery, replayed agent runs, cache/index loss, backup/restore, and rebuilding all derived indexes from canonical records.
5. Attribution and abstention: every returned derived claim links to authorized evidence; absent, deleted, expired, low-confidence, or conflicting evidence yields a calibrated empty/conflict result rather than invented certainty.

Report retrieval recall separately from final model answer quality. Also report p50/p95/p99 latency, storage growth, ingest-to-availability delay, tokens, model cost, stale-fact rate, unauthorized-candidate count (which must be zero), and recovery point/time results. Never compare vendor headline scores as if they were measured under one harness.

## Adopt, adapt, exclude

### Adopt now

- OpenAPI 3.1.2 HTTP contract using a 3.2-compatible design subset, explicit JSON Schemas, conformance fixtures, and generated Python/TypeScript client gates; promote the document syntax only after the complete 3.2 toolchain corpus passes.
- MCP 2025-11-25 resource/tool adapter with version/capability negotiation and standards-based remote authorization.
- A2A 1.0 correlation and lossless episode/artifact ingestion.
- Stable OpenTelemetry tracing/context plus W3C-compatible propagation.
- W3C PROV-compatible lineage semantics and deterministic export.

### Adapt behind versioned boundaries

- MCP tasks and future MCP revisions; A2A extensions and alternate bindings; GenAI semantic conventions; provider-specific session/memory interfaces; embedding, reranking, graph, and consolidation implementations.
- Each adapter records protocol/package version, has contract fixtures, and can be disabled without changing canonical records.

### Deliberately exclude from v1

- A universal agent runtime, prompt orchestrator, or model gateway.
- A proprietary “memory protocol,” A2A memory extension, or AgentFile dialect before independent interoperability demand exists.
- Raw conversation mirroring by default, automatic durable writes without an attributable policy, or telemetry content capture by default.
- A graph database as source of truth, vector-only canonical storage, silent overwrite of contradicted facts, or retrieval filters applied after ranking.
- Claims of protocol compatibility without published conformance fixtures and tests against at least one independent implementation.

## 2027 watchlist and decision triggers

Review quarterly, but change the core only when evidence crosses a trigger:

| Watch | Trigger for action |
| --- | --- |
| OpenAPI 3.2 tooling | Promote the description syntax when the selected validator, mock server, diff checker, Rust renderer, and Python/TypeScript generators all pass the same contract corpus. |
| MCP revisions and experimental tasks | Add support when two major hosts implement the same released behavior and the official SDK/TCK path is stable; keep older negotiated revisions in tests. |
| A2A extensions | Propose or implement memory exchange only after two independent consumers require semantics the HTTP API cannot express through ordinary messages/artifacts. |
| OTel GenAI conventions | Promote translated attributes into the default exporter only after the specific spans/attributes used by Palimpsest are marked stable; retain internal names meanwhile. |
| Framework/provider adapters | Prioritize by demonstrated user demand and contract-test maintainability, not repository popularity or benchmark marketing. |
| Graph and learned retrieval | Write an ADR only if reproducible Palimpsest evaluations show PostgreSQL hybrid retrieval misses a release target and the candidate improves it without violating temporal or isolation gates. |

The architecture is future-proof when protocol changes require adapter updates, not memory migrations; when new retrievers can be rebuilt from canonical data; and when every answer remains reproducible as of a time, policy, schema, and authorized evidence set.
