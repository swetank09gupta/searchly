# ADR 0007: Redis Sliding-Window Rate Limiting

**Status:** Accepted
**Date:** 2026-06-03

## Context

Per-tenant rate limiting is mandatory. Algorithm options:
- **Fixed window** — simple but suffers from boundary bursts (2x intended rate at the window edge).
- **Token bucket** — smooth, but limits a sustained rate, not a strict window.
- **Sliding window log / counter** — accurate rolling window; slightly more storage/compute.

Distributed enforcement requires a shared state store.

## Decision

Implement a **sliding-window counter** in **Redis** using sorted sets:
- Key: `rl:{tenant_id}:{endpoint_class}`
- On each request: `ZADD` request timestamp, `ZREMRANGEBYSCORE` to expire entries older than the window, `ZCARD` to count, reject if > limit.
- Wrapped in a single Lua script for atomicity.

Apply limits at multiple layers:
- **Per-tenant** (primary) — limits derived from tier (FREE/STANDARD/PREMIUM/ENTERPRISE).
- **Per-IP** at the gateway — defends authN endpoints from credential stuffing.
- **Per-user within tenant** — prevents one bad user from exhausting tenant quota.

## Consequences

**Positive**
- True rolling window avoids fixed-window edge bursts.
- Centralized in Redis → consistent enforcement across gateway and API instances.
- Atomic Lua script avoids race conditions under high concurrency.
- Cheap, well-understood, easy to monitor.

**Negative**
- Redis is now on the request critical path (mitigated by short TTLs, connection pooling, and circuit-breaker fallback to fail-open with alert).
- Sorted-set memory grows with QPS × window — tuned by capping window size and expiring keys eagerly.

**Rejected alternatives**
- **Bucket4j + Redis token bucket:** great library, but token bucket smooths bursts rather than strictly enforcing a window — wrong shape for tier-based fairness.
- **In-memory per-instance limiter:** fails for horizontally scaled gateway/API.
