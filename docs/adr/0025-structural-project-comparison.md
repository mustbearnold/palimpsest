# ADR-0025: Structural project comparison boundary

Status: accepted

Date: 2026-08-03

## Context

Project-aware ingestion and isolated recall make it safe to place evidence
from several repositories side by side, but a caller still has to inspect all
of the returned items to find likely differences. Automatically calling every
different message a semantic conflict would be unsafe: agent transcripts are
raw episodes, keys may be unrelated, and no model output is allowed to become
durable memory without an attributable write policy.

## Decision

Add a dependency-free structural comparison to the Python and TypeScript
clients and the local MCP adapter. It performs one authorized recall per
project, groups returned items by normalized fact key, and compares canonical
SHA-256 digests of JSON values. It reports exact matches, project-specific
keys, and same-key/different-value review candidates together with references
to the visible fact and revision IDs.

The comparison returns the original project-keyed bundles, performs no model
inference, writes no memory, and labels same-key/different-value results as
review candidates rather than semantic conflicts. Semantic interpretation and
any governed consolidation remain a separate future boundary.

## Consequences

Agents get a stable, privacy-conscious shortlist for cross-project review while
authorization and temporal correctness remain owned by the HTTP service. The
summary is deterministic and reproducible, but it cannot explain intent or
establish that two differently worded memories mean the same thing.
