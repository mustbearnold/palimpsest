# Palimpsest Domain Context

Use these terms consistently in code, issues, tests, and documentation.

| Term | Meaning |
| --- | --- |
| **MemoryService** | The public behavioral boundary for writing, reading, retrieving, superseding, exporting, and deleting memory. |
| **Tenant** | The top-level administrative and isolation boundary that owns memory policy and contains subjects, principals, agents, cases, and threads. Records from different tenants never share an authorization or retrieval candidate set. |
| **Subject** | The person, organization, system, or other entity that a memory is about or retained on behalf of. A subject is data scope, not necessarily the identity making a request. |
| **Principal** | An authenticated human, service, or agent identity whose grants determine which scoped operations and memories are authorized. Request payloads cannot grant a principal authority. |
| **Agent** | A software principal that reads or writes through MemoryService. An agent has no authority beyond the grants of its authenticated principal identity. |
| **Case** | A tenant-scoped operational matter, such as a support request, that groups related threads, episodes, and revisions. Case membership organizes work but does not replace subject scope or principal authorization. |
| **Thread** | One resumable sequence of agent interactions scoped by tenant, subject, and thread identifier. |
| **Checkpoint** | A durable snapshot or delta that allows an interrupted thread to resume without replaying successful side effects. |
| **Episode** | An immutable, timestamped observation or experience such as a message, tool result, action, or outcome. |
| **Fact revision** | A versioned semantic claim derived from attributable evidence, with validity and confidence metadata. |
| **Procedure revision** | A versioned rule, prompt, skill, or workflow that may be discovered semantically but is selected by exact policy. |
| **Artifact reference** | Metadata and an integrity-checked pointer to a large object stored outside the relational database. |
| **Export operation** | A durable, independently authorized operation that freezes an immutable membership manifest and materializes a versioned canonical-history package. It is not a legal-rights determination. |
| **Deletion operation** | A durable, independently authorized workflow that fences a subject, purges configured live targets, verifies absence, and records a minimal content-free tombstone. |
| **Subject lifecycle fence** | The monotonic subject-wide boundary that prevents new content leases during deletion and never returns to active. |
| **Content lease** | A bounded, subject-scoped grant held by a content-producing response so deletion can drain or revoke in-flight disclosure before purge. It stores no response content. |
| **Deletion tombstone** | Retention-governed, content-free evidence used for idempotency, restore suppression, and lifecycle audit; it never contains raw subject or memory identifiers or deleted payload digests. |
| **Observed time** | When the event happened in the source domain. |
| **Recorded time** | When Palimpsest committed the record. |
| **Valid time** | The interval during which a fact or procedure is considered true or applicable. |
| **Bitemporal** | Queryable by both valid time and recorded time, preserving what was believed and when it was learned. |
| **Supersession** | Linking a newer revision to the older revision it replaces without deleting history. |
| **Consolidation** | An attributable, idempotent process that derives facts or summaries from episodes. |
| **Retrieval policy** | The versioned rules for exact filters, lexical/vector candidates, temporal decay, importance, confidence, and reranking. |
| **Current view** | The active, non-deleted revisions valid at query time. |
| **As-of view** | The revisions valid or recorded at an explicitly requested historical instant. |
| **Autonomy frontier** | Open `ready-for-agent` issues with no open blockers and no active assignee. |

Avoid using **memory** to mean only an embedding. An embedding is a derived
retrieval representation of a canonical memory record.
