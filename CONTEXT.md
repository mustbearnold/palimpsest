# Palimpsest Domain Context

Use these terms consistently in code, issues, tests, and documentation.

| Term | Meaning |
| --- | --- |
| **MemoryService** | The public behavioral boundary for writing, reading, retrieving, superseding, exporting, and deleting memory. |
| **Thread** | One resumable sequence of agent interactions scoped by tenant, subject, and thread identifier. |
| **Checkpoint** | A durable snapshot or delta that allows an interrupted thread to resume without replaying successful side effects. |
| **Episode** | An immutable, timestamped observation or experience such as a message, tool result, action, or outcome. |
| **Fact revision** | A versioned semantic claim derived from attributable evidence, with validity and confidence metadata. |
| **Procedure revision** | A versioned rule, prompt, skill, or workflow that may be discovered semantically but is selected by exact policy. |
| **Artifact reference** | Metadata and an integrity-checked pointer to a large object stored outside the relational database. |
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
