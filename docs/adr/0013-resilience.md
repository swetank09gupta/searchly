# ADR 0013: Resilience4j for Circuit Breakers, Retries, Bulkheads

**Status:** Accepted
**Date:** 2026-06-03

## Context

Every call to OpenSearch, Postgres, Redis, MinIO, Keycloak, and Kafka is a potential failure mode. Without explicit resilience patterns, a slow downstream becomes a thread-pool exhaustion outage that takes the whole service down.

## Decision

Use **Resilience4j** for:
- **Circuit breakers** around every external dependency.
- **Retries with exponential backoff and jitter** for idempotent operations only (GET, blob download, Kafka offset commit).
- **Timeouts** on every external call (search 400ms, blob 5s, Tika 30s).
- **Bulkheads** — separate thread pools per downstream AND per tenant tier, so one slow dependency or one noisy tier cannot consume all threads.
- **Rate limiter** (in addition to the Redis rate limit) as a last-line defense at the client side.

## Consequences

**Positive**
- Failures degrade gracefully: cache stale results on OpenSearch outage; metadata-only search from Postgres as a degraded mode.
- Per-tier bulkheads prevent ENTERPRISE bulk imports from starving STANDARD search traffic.
- Standard Spring integration via annotations or programmatic API.

**Negative**
- Configuration sprawl (one set of thresholds per dependency × per tier) — mitigated by sensible defaults in `application.yaml` and per-environment overrides.
- Retries on non-idempotent operations cause duplicates — we restrict retries to operations with `Idempotency-Key` or natural idempotency.

**Critical rule**
- **Never retry a non-idempotent operation without an idempotency key.** Producer-side `Idempotency-Key` header on `POST /documents` makes the create-document path retry-safe (duplicate submissions return the same `doc_id`).
