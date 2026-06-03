# ADR 0011: Multi-Layer Caching Strategy

**Status:** Accepted
**Date:** 2026-06-03

## Context

To meet <500ms p95 at 1000+ QPS economically, we cannot hit OpenSearch on every request. Caching must be safe across tenants and roles, and must invalidate correctly on mutation.

## Decision

Use a **multi-layer cache**:

| Layer | Tech | Key | TTL | Notes |
|---|---|---|---|---|
| Edge (prod) | CDN | URL + Authorization hash | seconds | Optional |
| App (distributed) | Redis | `cache:{tenant_id}:{role}:q:{sha256}` | 60s | Shared across pods |
| App (in-process) | Caffeine | tenant config, JWKS keys, parsed JWTs | 5 min / 1 h | Per pod |
| OpenSearch | built-in | filter cache, request cache | LRU | Auto |

Cache invalidation on `POST/DELETE /documents` deletes the tenant's query-cache key prefix; tenant config invalidation broadcasts via Redis pub/sub.

## Consequences

**Positive**
- Aggressive cache absorbs hot queries; target ≥70% hit rate on search.
- In-process caches eliminate Redis round-trips on the hottest items (config, JWKS).
- TTL bounds staleness — search index lag (~1s) means a 60s TTL on results is acceptable.

**Negative**
- Invalidation is best-effort across pods (mitigated by short TTLs).
- Cache stampede risk on key expiry (mitigated by request coalescing / single-flight).

**Critical correctness rule**
- Cache keys **must include `tenant_id` AND role** — otherwise a VIEWER might receive results visible only to an EDITOR (e.g., a draft document). This is enforced in `QueryCacheKey.build()` and covered by an integration test.
