# ADR-0025: Structural project comparison boundary

Status: accepted

Date: 2026-08-03

## Context

Project-aware ingestion and isolated recall make it safe to place evidence from several repositories side by side, but a caller still has to inspect all of the returned items to find likely differences. Automatically calling every different message a semantic conflict would be unsafe: agent transcripts are raw episodes, keys may be unrelated, and no model output is allowed to become durable memory without an attributable write policy.

## Decision

Add a dependency-free structural comparison to the Python and TypeScript clients and the local MCP adapter. It performs one authorized recall per project, groups returned items by normalized fact key, and compares canonical SHA-256 digests of JSON values. It reports exact matches, project-specific keys, and same-key/different-value review candidates together with references to the visible fact and revision IDs. It also reports at most 100 token-Jaccard overlap candidates across differently keyed content items, using a 0.5 similarity threshold and at least three shared tokens. Each candidate includes at most 20 shared tokens and 20 tokens that occur only in each project, with a truncation flag. The response also carries project-root, branch, source, role, and unique-session labels observed in returned ingestion metadata; absent labels remain absent rather than being inferred.

The comparison returns the original project-keyed bundles, performs no model inference, writes no memory, and labels both same-key/different-value and lexical-overlap results as review candidates rather than semantic conflicts. Semantic interpretation and any governed consolidation remain a separate future boundary.

## Consequences

Agents get a stable, privacy-conscious shortlist for cross-project review while authorization and temporal correctness remain owned by the HTTP service. The summary is deterministic and reproducible, but it cannot explain intent or establish that two differently worded memories mean the same thing. The token delta is a wording hint, not a semantic explanation. Transcript event keys remain unique by source/event, so lexical candidates are hints for an agent to inspect, not automatic alignments.
