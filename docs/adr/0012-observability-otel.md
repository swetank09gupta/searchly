# ADR 0012: OpenTelemetry for Tracing, Prometheus for Metrics

**Status:** Accepted
**Date:** 2026-06-03

## Context

Distributed systems with async pipelines (Gateway → API → Kafka → Indexer → OpenSearch) are impossible to debug without end-to-end traces. We also need per-tenant RED metrics for SLOs and noisy-neighbor detection.

## Decision

- **Tracing:** **OpenTelemetry SDK** with auto-instrumentation for Spring, JDBC, OpenSearch client, Kafka, and Redis. Export to **Jaeger** (local) / Tempo / vendor (prod). Trace context propagated through Kafka headers (W3C `traceparent`) so end-to-end traces span the async pipeline.
- **Metrics:** **Micrometer** → **Prometheus** scrape endpoint. Standard RED + USE metrics; custom business metrics tagged with `tenant_id` and `tier`.
- **Logs:** structured JSON via Logback; `trace_id`, `span_id`, `tenant_id`, `user_id` on every line for correlation. Shipped to Loki or ELK in production.
- **Sampling:** head 10% + tail-based for error and slow traces (configurable).

## Consequences

**Positive**
- Vendor-neutral instrumentation — OTel is the de-facto standard; can switch backends without re-instrumenting.
- Per-tenant tags enable fairness monitoring and per-tenant cost attribution.
- Three pillars (metrics, logs, traces) correlate by `trace_id`.

**Negative**
- High-cardinality tags (`tenant_id` × `endpoint`) can explode Prometheus storage — mitigated by capping cardinality, dropping `tenant_id` from histograms (keep on counters only), and rolling up small tenants into an "other" bucket above N tenants.
- Trace storage cost — controlled by sampling.
