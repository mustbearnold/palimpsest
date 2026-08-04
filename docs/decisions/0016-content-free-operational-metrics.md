# ADR-0016: Content-free operational metrics

Status: accepted

Date: 2026-08-03

## Context

Self-hosted operators need a scrapeable signal for background cleanup without making ordinary telemetry another authorization or privacy surface. The service already tracks bounded content-lease cleanup outcomes, but exposing those counters through logs would make collection dependent on log parsing and would invite accidental scope data in labels.

## Decision

Expose `GET /metrics` from the unauthenticated probe router. It returns Prometheus text format 0.0.4 with `Cache-Control: no-store` and a fixed metric vocabulary:

- build identity and the latest migration version in the binary;
- content-lease release retries and runtime-unavailable deferrals;
- outstanding cleanup releases; and
- releases deferred to lease expiry.

The endpoint does not query PostgreSQL, require bearer authentication, or include tenant IDs, subject IDs, memory IDs, payloads, credentials, trace values, raw errors, or unbounded labels. The only label is the fixed package version on the build identity metric. This is a minimal operational seam, not a claim that the service yet exposes the complete HTTP, database, worker, retrieval, backup, or tracing telemetry described by the operations baseline.

## Consequences

Scrapers can observe the existing response-cleanup recovery boundary even when the database is unavailable, and the metric surface is safe to expose beside the content-free liveness and readiness probes. Additional metrics require a separate contract decision and must preserve bounded names and privacy-safe dimensions.
