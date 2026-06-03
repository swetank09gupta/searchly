# Searchly
> **Single-file submission per the assignment brief.** This document concatenates the four required sections (Architecture, Production Readiness, Experience Showcase, AI Usage). The modular files under `docs/` and `docs/adr/` remain the canonical sources of truth; this document is generated from them.

**Author:** Swetank Gupta
**Repository:** https://github.com/swetankgupta/searchly
**License:** Apache 2.0
**Date:** 2026-06-03

## Table of Contents

1. [Architecture Design](#1-architecture-design) — from `docs/ARCHITECTURE.md`
2. [Production Readiness Analysis](#2-production-readiness-analysis) — from `docs/PRODUCTION_READINESS.md`
3. [Engineering Experience Showcase](#3-engineering-experience-showcase) — from `docs/EXPERIENCE.md`

**Also in this repository (referenced but not embedded):**
- [14 Architecture Decision Records](adr/README.md) — one per significant decision
- [Decisions & Assumptions summary](DECISIONS.md)
- [Benchmarks](BENCHMARKS.md) — laptop-scale performance numbers
- Source code in `gateway/`, `search-api/`, `indexer/`, `common/`
- `deploy/docker-compose.yml`, `deploy/postman/searchly.postman_collection.json`, `deploy/curl-samples.sh`, `deploy/bench.sh`

---

# 1. Architecture Design


## Executive Summary (the 2–3 page view)

**Problem.** Multi-tenant document search at 10M+ docs, <500ms p95, 1000+ QPS, with strict tenant isolation and horizontal scalability.

**Shape.** Edge **gateway** (Spring Cloud Gateway) handles TLS, JWT, per-tenant sliding-window rate limit. Stateless **search-api** serves CRUD + query, hitting **OpenSearch** for full-text and **Postgres** as the system of record. Writes also publish to **Kafka**; a separate **indexer** consumer materializes documents into OpenSearch asynchronously. **Redis** holds the rate-limit counters and a short-TTL query result cache. **MinIO/S3** stores raw blobs (designed, prototype accepts inline text).

```
Client → Gateway → search-api ──► Postgres (SoR)
                       │ │  └──► Kafka ──► Indexer ──► OpenSearch
                       │ └──► Redis (cache + rate limit)
                       └──► MinIO/S3 (blobs)
```

**Storage choices.** OpenSearch (Apache-2.0, scales horizontally, BM25 + fuzzy + facets + highlights). Postgres (ACID SoR; OpenSearch is rebuildable from it). Redis (atomic sliding-window via sorted-set Lua script). Kafka (replayable log; partition by `tenant_id`; dedicated topics per ENTERPRISE tenant for noisy-neighbor isolation). MinIO/S3 (decouple binaries from index and queue). See [ADRs](adr/README.md) for full justification.

**API (v1, all under `/api/v1`).** `POST /documents` (idempotency-key supported, returns 202 PENDING); `GET /search?q=&tenant=&fuzzy=&highlight=&facets=`; `GET /documents/{id}`; `DELETE /documents/{id}`; `GET /actuator/health` (dependency status).

**Consistency.** Strong on Postgres (read-your-writes for management ops). Eventual on OpenSearch (~1s lag through Kafka). Get-by-id from Postgres is immediate; appearance in search results is eventual. Producer-side `Idempotency-Key` + `doc_id` as OpenSearch document id make retries safe.

**Caching.** Multi-layer: (CDN at edge in prod), Redis distributed query cache `cache:{tenant}:{role}:q:{sha256}` 60s TTL, in-process Caffeine for tenant config + JWKS. **Cache key includes role** — critical to prevent EDITOR-visible drafts leaking to a VIEWER.

**Queue.** Kafka decouples write latency from index latency, enables replay (rebuild OpenSearch from Kafka), and isolates load: shared topic for FREE/STANDARD/PREMIUM (partitioned by `tenant_id` for per-tenant ordering) + dedicated topic per ENTERPRISE tenant.

**Multi-tenancy.** Hybrid OpenSearch: shared index with mandatory `tenant_id` filter + routing for FREE/STANDARD/PREMIUM; dedicated index per ENTERPRISE. `TenantSecurityFilter` rejects requests where the JWT/header tenant doesn't match the request tenant, and (when a user-id is supplied) verifies the user actually belongs to that tenant — anti-IDOR at both layers. Tiers (`FREE/STANDARD/PREMIUM/ENTERPRISE`) drive rate limits, quotas, index isolation, and Kafka topic routing. RBAC roles (`TENANT_ADMIN/EDITOR/VIEWER/SERVICE`) enforced via Spring Security `@PreAuthorize`.

**Trade-offs (the 4 that matter most).**
1. **Async indexing** buys throughput, replayability, and write-path availability — at the cost of read-your-writes on search (acceptable; documented to users).
2. **Hybrid tenant isolation** balances cost (shared scales to thousands of small tenants) against enterprise isolation needs — at the cost of two code paths and a promotion runbook.
3. **OpenSearch over Postgres FTS** scales to 10M+ and gives fuzzy/highlight/facets natively — at the cost of operating a second stateful system.
4. **JWT (RS256) over sessions** — stateless, fast verification, identity travels with the request — at the cost of harder synchronous revocation (mitigated with 15-min TTLs + revocation list).

---

## 1. High-Level Architecture

```
                          ┌─────────────────────┐
                          │      Clients        │
                          │ (Web, Mobile, API)  │
                          └──────────┬──────────┘
                                     │ HTTPS + JWT
                                     ▼
                       ┌──────────────────────────┐
                       │   Spring Cloud Gateway   │
                       │  (TLS, AuthN, Rate Limit,│
                       │   CORS, Routing, Headers)│
                       └─────┬─────────────┬──────┘
                             │             │
                  ┌──────────▼──┐    ┌─────▼──────────┐
                  │  Search API │    │  Admin API     │
                  │ (read+write)│    │ (tenants/users)│
                  └─┬───┬───┬───┘    └────────────────┘
                    │   │   │
        ┌───────────┘   │   └─────────────────────────┐
        ▼               ▼                             ▼
  ┌──────────┐    ┌──────────┐                 ┌────────────┐
  │  Redis   │    │OpenSearch│                 │ PostgreSQL │
  │ (cache + │    │ (search) │                 │ (metadata, │
  │  rate    │    └──────────┘                 │  SoR, ACL) │
  │  limit)  │                                 └────────────┘
  └──────────┘
        │
        │   (writes: doc upload)
        ▼
   ┌─────────┐      ┌──────────┐      ┌──────────────┐
   │ MinIO/  │◄─────│Kafka     │─────►│  Indexer     │
   │  S3     │      │(tiered   │      │ (Tika extract│
   │ (blobs) │      │ topics)  │      │  → OpenSearch│
   └─────────┘      └──────────┘      └──────────────┘

  Cross-cutting:  Keycloak (OIDC) │ Jaeger (OTel) │ Prometheus + Grafana
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| **Gateway** | TLS termination, JWT validation, per-tenant sliding-window rate limit, request routing, security headers, request size limits |
| **Search API** | Document CRUD, query orchestration, cache lookup, tenant/RBAC enforcement, write to Postgres + MinIO + Kafka |
| **Indexer** | Kafka consumer; fetches blob, runs Tika, writes to OpenSearch; idempotent; per-tier consumer pools |
| **OpenSearch** | Full-text index, relevance scoring (BM25), fuzzy/highlight/facets |
| **PostgreSQL** | System of record for tenants, users, document metadata, ACLs, quotas |
| **Redis** | Query result cache, sliding-window rate limit counters, tenant-config cache |
| **MinIO / S3** | Immutable raw document blobs; server-side encryption |
| **Kafka** | Async indexing; tiered topics for noisy-neighbor isolation |
| **Keycloak** | OIDC provider, JWT issuance, user/role management |

---

## 2. Data Flow

### Indexing (write path)

```
Client ──POST /documents (multipart or JSON)──► Gateway
  ├─ JWT check, rate limit, size limit
  ▼
Search API
  ├─ Validate, generate doc_id (UUID), check quota
  ├─ PUT blob ──► MinIO (encrypted)
  ├─ INSERT metadata ──► Postgres  (status=PENDING)
  ├─ PUBLISH ──► Kafka topic (shared OR enterprise.{tenant})
  │     payload: {doc_id, tenant_id, blob_uri, checksum, idempotency_key}
  ▼
Indexer (consumer)
  ├─ Pull blob from MinIO
  ├─ Tika text extraction (with size/time limits)
  ├─ Build OpenSearch doc {tenant_id, content, metadata, acl}
  ├─ INDEX with routing=tenant_id  ──► OpenSearch
  └─ UPDATE Postgres status=INDEXED + emit audit event
```

**Failure handling:** Kafka retries with exponential backoff; poison messages → DLQ topic; status field in Postgres lets clients poll.

### Search (read path)

```
Client ──GET /search?q=&tenant=──► Gateway
  ├─ JWT check, JWT.tenant_id == query.tenant (anti-IDOR)
  ├─ Sliding-window rate limit (Redis)
  ▼
Search API
  ├─ Cache lookup: key = sha256(tenant_id|role|normalized_q|filters)
  │   HIT  ──► return (with X-Cache: HIT)
  ├─ MISS:
  │   Build query (typed OpenSearch DSL, never string concat)
  │   Inject mandatory tenant_id filter + ACL terms
  │   Submit to OpenSearch (timeout 400ms, circuit-breaker)
  ├─ Hydrate hits with stored fields (avoid extra Postgres roundtrip)
  ├─ Cache SET ttl=60s
  └─ Return paginated results + highlights
```

---

## 3. Storage Strategy

| Layer | Choice | Why |
|---|---|---|
| **Search engine** | OpenSearch | Mature inverted index, BM25, fuzzy, highlights, facets, horizontal scale via sharding; Apache-licensed fork avoids ES license risk |
| **System of record** | PostgreSQL | ACID for tenants/users/quotas/ACLs; OpenSearch is rebuildable from blobs + Postgres |
| **Blob store** | MinIO local, S3/GCS prod | Decouples large binaries from index; cheap, durable, server-side encryption; presigned URLs |
| **Cache** | Redis | Low-latency query cache + atomic sorted-set operations for sliding-window rate limiting |
| **Queue** | Kafka | Durable, partitioned, replayable; consumer groups enable horizontal indexer scaling; topic-level isolation per tier |

**Sharding plan (OpenSearch):** shared index uses `tenant_id` as routing key (co-locates a tenant's docs to one shard, reducing fan-out); 12 primary shards, 1 replica to start; per-enterprise indices sized to expected doc count (rule of thumb 20–40GB per shard).

---

## 4. API Design

All endpoints versioned: `/api/v1/...`. JWT bearer required; `X-Tenant-Id` header must match JWT claim.

### Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| POST | `/api/v1/documents` | EDITOR+ | Index a document |
| GET | `/api/v1/search` | VIEWER+ | Search (`?q=&tenant=&page=&size=&fuzzy=&highlight=&facets=`) — fuzzy, highlighting, and faceted aggregations on allowlisted fields (`tags`, `author`) |
| GET | `/api/v1/documents/{id}` | VIEWER+ | Retrieve metadata |
| GET | `/api/v1/documents/{id}/download` | VIEWER+ | Presigned URL to blob |
| DELETE | `/api/v1/documents/{id}` | EDITOR+ | Remove document |
| GET | `/actuator/health` | public (liveness) / restricted (deps) | Health + dependency status |
| GET | `/actuator/prometheus` | restricted | Metrics |

### Contract examples

**POST /api/v1/documents**
```json
Request:
{
  "title": "Q4 Revenue Report",
  "content": "...",                  // optional raw text
  "blobUri": "s3://...",             // optional pre-uploaded blob
  "metadata": {"author": "alice", "tags": ["finance","2025"]}
}

Response 202:
{
  "id": "9f1c…",
  "status": "PENDING",
  "tenantId": "acme",
  "createdAt": "2026-06-03T10:00:00Z"
}
```

**GET /api/v1/search?q=revenue&tenant=acme&fuzzy=true&highlight=true&facets=tags&facets=author**
```json
Response 200:
{
  "took": 42,
  "total": 137,
  "page": 0,
  "size": 20,
  "hits": [
    {
      "id": "9f1c…",
      "score": 12.4,
      "title": "Q4 Revenue Report",
      "highlights": ["…<em>revenue</em> grew 23% YoY…"],
      "metadata": {"author":"alice"}
    }
  ],
  "facets": {
    "tags":   {"finance": 89, "2025": 42, "legal": 7},
    "author": {"alice": 64, "bob": 38, "dave": 12}
  }
}
```

**Bonus search features all available on this endpoint:**
- `fuzzy=true` — Lucene fuzzy matching (Levenshtein, automatic edit distance)
- `highlight=true` — `<em>`-wrapped fragments around matched terms
- `facets=tags&facets=author` — terms aggregations on allowlisted fields (allowlist enforced in `SearchService.facetFieldFor`)

**Error envelope (RFC 7807):**
```json
{
  "type": "https://searchly.dev/errors/rate-limited",
  "title": "Too Many Requests",
  "status": 429,
  "detail": "Tenant acme exceeded 100 req/s (STANDARD tier)",
  "instance": "/api/v1/search",
  "traceId": "abc123"
}
```

---

## 5. Consistency Model

- **Strong consistency:** PostgreSQL (tenants, metadata, ACLs, quotas). Reads-after-writes guaranteed.
- **Eventual consistency:** OpenSearch (typical lag <1s through Kafka). A freshly-indexed document is *retrievable by id* (Postgres) immediately, but may not appear in search results for up to a second.
- **Trade-off:** Async indexing buys throughput, replayability, and isolation at the cost of read-your-writes on search. Acceptable for document search; users expect near-real-time, not synchronous.
- **Idempotency:** Producer-side `idempotency_key` on `POST /documents` (header) — repeated submits return the same `doc_id`. Indexer uses `doc_id` as OpenSearch document id (upsert) so duplicate Kafka messages are safe.

---

## 6. Caching Strategy

| Layer | Cache | Key | TTL | Invalidation |
|---|---|---|---|---|
| Edge | CDN (prod) | URL + Authorization hash | seconds | TTL-based |
| App (Redis) | Query results | `cache:{tenant}:{role}:q:{sha256}` | 60s | On doc mutation (delete tenant prefix) |
| App (Redis) | Tenant config | `tenant:{id}` | 5 min | On admin update (pub/sub invalidate) |
| App (Caffeine, in-proc) | JWT public keys (JWKS) | issuer/kid | 1h | Background refresh |
| OpenSearch | Filter cache (built-in) | — | LRU | Automatic on segment merge |
| Indexer | Tika parsers | — | process lifetime | — |

**Cache key includes role** to prevent leaking results visible to an EDITOR back to a VIEWER for the same query.

---

## 7. Message Queue Usage (Kafka)

- **Topics:**
  - `indexing.shared` (12 partitions, RF=3) — FREE/STANDARD tenants
  - `indexing.enterprise.{tenant_id}` (per-tenant, sized to load) — ENTERPRISE
  - `indexing.dlq` — poison messages after N retries
  - `audit.events` — security/audit log
- **Partitioning:** `tenant_id` as key on shared topic → ordering per tenant.
- **Consumer groups:** `indexer-shared`, `indexer-enterprise-{tenant}` — independent scaling, no head-of-line blocking across tiers.
- **Why not direct sync indexing?** Async absorbs bulk import spikes, enables replay (rebuild OpenSearch from Kafka), and decouples write latency from indexing latency.

---

## 8. Multi-tenancy & Isolation

| Concern | Approach |
|---|---|
| **Identity** | `tenant_id` claim in JWT (signed RS256) |
| **Anti-IDOR** | `TenantSecurityFilter` rejects requests where JWT tenant ≠ header/path tenant |
| **Storage isolation** | Shared OpenSearch index w/ mandatory `tenant_id` filter (FREE/STANDARD); dedicated index per ENTERPRISE tenant |
| **Query injection** | Typed OpenSearch DSL; `tenant_id` filter applied centrally in `SecureQueryBuilder` |
| **Compute isolation** | Resilience4j bulkheads per tier; dedicated Kafka topics + consumer groups for ENTERPRISE |
| **Cache isolation** | All keys namespaced `…{tenant_id}…`; tenant-scoped invalidation |
| **Rate limit / quota** | Sliding-window per tenant in Redis; per-tier QPS and per-tier daily doc/storage caps |
| **Audit** | Every mutation + auth failure logged with `tenant_id`, `user_id`, `trace_id` to `audit.events` topic |

**RBAC roles:** `TENANT_ADMIN`, `EDITOR`, `VIEWER`, `SERVICE` — enforced via `@PreAuthorize` and a permissions matrix (see README).

---

## 9. Tenant Tiers

| Tier | QPS | Index Plan | Kafka | Quota |
|---|---|---|---|---|
| FREE | 10 | shared | shared topic | 1K docs/day, 1 GB |
| STANDARD | 100 | shared | shared topic | 50K docs/day, 50 GB |
| PREMIUM | 1000 | shared (boosted shard) | shared topic, priority consumer | 1M docs/day, 500 GB |
| ENTERPRISE | custom | dedicated index | dedicated topic + consumers | negotiated |

Tier stored in Postgres `tenants.tier`, cached in Redis, propagated as a JWT claim for fast gateway checks.

---

# 2. Production Readiness Analysis


What it would take to evolve the Searchly prototype into a service that can be operated 24/7 at SaaS scale.

---

## 1. Scalability — handling 100x growth

**Document growth (10M → 1B):**
- **OpenSearch:** rolling indices by month (`docs-acme-2026-06`) with ILM; hot → warm → cold tiers; force-merge on cold; use index aliases so clients are unaffected.
- **Shard sizing:** keep shards 20–40 GB. Pre-split: for ENTERPRISE tenants project 12 months out and pre-create.
- **Routing:** keep `tenant_id` as routing key so per-tenant queries hit a single shard regardless of cluster size.
- **Cross-cluster search** for geo-distributed deployments.

**Traffic growth (1K → 100K QPS):**
- Stateless services (Gateway, Search API, Indexer) scale horizontally via K8s HPA. Trigger metrics: CPU, latency p95, Kafka consumer lag.
- **Read replicas:** OpenSearch coordinating nodes separated from data nodes; bump replicas for hot indices.
- **Cache hit ratio:** target ≥70% on search; precompute "top queries" per tenant.
- **Connection pooling:** HikariCP for Postgres, OpenSearch client with sized pools.

**Indexing burst (bulk imports):**
- Bulk API on OpenSearch; backpressure via Kafka — producers slow if topic lag grows.
- Auto-scale `indexer` deployment based on consumer lag (KEDA on K8s).

---

## 2. Resilience

| Pattern | Use |
|---|---|
| **Circuit breaker** (Resilience4j) | OpenSearch, Postgres, Redis, MinIO, Keycloak calls |
| **Retry with jitter** | Idempotent ops only (GET, indexer Kafka commit, blob download) |
| **Timeouts** | All external calls — search 400ms, blob fetch 5s, Tika 30s |
| **Bulkheads** | Separate thread pools per downstream and per tier |
| **Fallbacks** | Cache stale on OpenSearch outage; degraded "metadata-only" search from Postgres |
| **Dead-letter queue** | Poison Kafka messages → `indexing.dlq` with replay tooling |
| **Idempotency** | Client `Idempotency-Key` header; doc_id reused on retry |
| **Graceful shutdown** | Drain Kafka consumers, finish in-flight HTTP, deregister from LB |
| **Chaos testing** | Litmus / Chaos Mesh on K8s; quarterly game days |

**Failover:**
- Multi-AZ for Postgres (managed, e.g., RDS Multi-AZ), OpenSearch (3-AZ), Kafka (RF=3, min.insync=2).
- Active-active across regions for stateless services; OpenSearch CCR for read replicas in second region; Postgres async replica + promotion runbook.

---

## 3. Security

**Network**
- TLS 1.3 everywhere; mTLS service-to-service (Istio / Linkerd).
- Private subnets for data plane; only Gateway exposed via WAF (AWS WAF / Cloudflare).
- NetworkPolicies in K8s restrict pod-to-pod traffic.

**AuthN / AuthZ**
- OIDC (Keycloak/Okta/Auth0/Cognito); RS256 JWT; 15-min access + refresh tokens.
- M2M via OAuth2 client_credentials or scoped API keys (Argon2-hashed at rest).
- RBAC (`TENANT_ADMIN/EDITOR/VIEWER/SERVICE`) enforced with `@PreAuthorize`.
- `TenantSecurityFilter` rejects cross-tenant requests; alert on any occurrence.
- Document-level ACL fields in OpenSearch (`acl_users`, `acl_roles`) for fine-grained sharing.

**Data**
- Encryption at rest: KMS-managed keys; Postgres TDE, OpenSearch node-to-node + disk, S3 SSE-KMS, Redis TLS+AUTH.
- PII minimization in Kafka (only IDs + blob URIs).
- GDPR `DELETE /documents/{id}` purges OpenSearch + Postgres + MinIO + cache + emits tombstone.
- Field-level encryption for sensitive metadata (pgcrypto).

**API**
- Input validation (Bean Validation), Lucene-safe queries, parameterized SQL.
- File uploads: size limits, MIME sniffing (Tika), extension allowlist, ClamAV sidecar scan, presigned PUT.
- XXE/SSRF hardening in Tika; zip-bomb guards.
- Security headers (HSTS, CSP, X-Content-Type-Options, X-Frame-Options).
- Per-IP + per-user + per-tenant rate limits.

**Supply chain & runtime**
- OWASP Dependency-Check + Snyk + Trivy in CI; SBOM via CycloneDX.
- Signed container images (Cosign); distroless base; non-root, read-only FS, dropped caps.
- Secrets in Vault / cloud Secrets Manager; never in env files in prod.
- K8s PodSecurityStandards `restricted`; OPA/Kyverno policies.

**Audit**
- Immutable audit log (`audit.events` Kafka topic → cold storage).
- Logs: structured JSON; masking for tokens/PII; trace IDs on every line.

---

## 4. Observability

**Metrics (Micrometer → Prometheus)**
- RED per endpoint per tenant: Rate, Errors, Duration (histograms).
- USE per dependency: Utilization, Saturation, Errors.
- Business: docs indexed/day/tenant, search QPS/tenant, cache hit ratio, Kafka lag.
- SLOs codified as Prometheus recording rules; burn-rate alerts.

**Logs (Logback → Loki / ELK)**
- Structured JSON; trace_id, span_id, tenant_id, user_id on every log.
- Masking patterns for secrets/PII.
- Log levels per-package, runtime-changeable via Actuator.

**Tracing (OpenTelemetry → Jaeger / Tempo)**
- Auto-instrumentation for Spring, OpenSearch client, Kafka, JDBC, Redis.
- Trace context propagated through Kafka headers (W3C traceparent).
- Sampling: head 10% + tail-based for slow/error traces.

**Dashboards**
- Grafana: per-tenant query latency, error rate, rate-limit hits, top slow queries, Kafka lag, JVM, GC.
- Alerts (PagerDuty): p95 latency > SLO, error rate > 1%, Kafka lag > threshold, cross-tenant access (=any).

---

## 5. Performance

- **OpenSearch:** custom analyzers per language; `keyword` for exact, `text` for analyzed; `_source` reduction (store only needed); request cache for facets; force-merge cold indices to 1 segment.
- **Query opt:** prefer `match` over `query_string`; pre-filter by `tenant_id` (high-cardinality filter → bitset cache); pagination via `search_after` for deep paging; cap `from+size`.
- **Postgres:** partition `documents` by `tenant_id`/month; covering indices for hot queries; `EXPLAIN ANALYZE` on slow log; pg_stat_statements monitored.
- **JVM:** G1GC tuned for low pause; off-heap caches for hot data; native image (GraalVM) for Gateway if cold starts hurt.
- **Network:** HTTP/2 between gateway and services; gRPC for internal hot paths.

---

## 6. Operations

**Deployment**
- Kubernetes (EKS/GKE) with Helm; image promotion across dev → staging → prod.
- **Blue-green** via two Deployments behind a Service; Gateway flips a routing weight; instant rollback. (Bonus point covered.)
- Canary via Argo Rollouts / Flagger with automated metric-based promotion.
- DB migrations via Flyway; **expand-then-contract** pattern (additive first, app deploy, cleanup later) — zero-downtime.

**Backup / Recovery**
- Postgres: PITR via WAL archiving to S3; nightly snapshots, 30-day retention.
- OpenSearch: snapshot to S3 every 6h; restore drills quarterly.
- MinIO/S3: versioning + cross-region replication.
- Kafka: tiered storage; can replay last 7 days for OpenSearch rebuild.
- **RPO 5 min, RTO 30 min** target.

**Capacity planning**
- Quarterly review; growth model from per-tenant metrics; pre-scale ahead of marketing-driven spikes.
- Cost dashboards (per-tenant cost-to-serve for pricing decisions).

---

## 7. SLA — achieving 99.95% availability

99.95% = ~21.6 min downtime/month.

- **Eliminate single points of failure:** multi-AZ for every stateful component; ≥2 replicas per stateless service; PDBs in K8s.
- **Decoupling:** async indexing means search remains up if indexer fails; read-path can serve from cache if OpenSearch is degraded.
- **Progressive delivery:** canary + auto-rollback on SLO burn — most outages caused by deploys; blast radius minimized.
- **Dependency budget:** if Keycloak/OpenSearch each promise 99.9%, your effective ceiling is 99.8% — so cache JWKS (1h), cache tenant config, degraded-mode for search.
- **Incident response:** on-call rotation, runbooks per alert, blameless postmortems, error-budget policy (freeze releases when budget exhausted).
- **Game days & chaos engineering:** validate failover quarterly.
- **Status page** and customer comms tooling.

---

## 8. Cost Optimization (bonus)

- Tiered OpenSearch storage (hot SSD → warm HDD → cold S3-backed snapshot).
- Spot/preemptible nodes for indexer (idempotent, restartable).
- Right-size shards (over-sharding wastes heap).
- Per-tenant cost reporting → drive tier upsells; surface heavy free-tier users.
- Reserved instances for baseline; autoscale for peaks.
- Compress logs/metrics; sample traces; retain only what's queried.

---

# 3. Engineering Experience Showcase


> Personalized draft based on the author's background in warehouse-automation / robotics fleet platforms (GreyOrange).
> **Edit company names, exact numbers, and incident details before submission to match what you're comfortable disclosing publicly.**

## 1. A similar distributed system built — scale & impact

At GreyOrange I worked on the platform that coordinates large fleets of autonomous warehouse robots and orchestrates order fulfillment across customer distribution centers. The system spanned hundreds of robots per site, several dozen live sites worldwide, and tens of thousands of fulfillment events per minute at peak. Architecturally it looked very similar to Searchly: a Spring Boot service tier behind an API gateway, Kafka as the event backbone between fleet-management and warehouse-execution subsystems, PostgreSQL as the system of record for SKUs / orders / robot state, Redis for hot caches and rate limiting, and per-tenant isolation enforced at every layer (each customer warehouse is a tenant with strict data boundaries). We hit four-9s availability on the critical control plane by aggressively decoupling read and write paths, caching per-tenant configuration, and treating every cross-service call as a potential failure to wrap with timeouts and circuit breakers. The patterns I'm using in Searchly — tenant context filter, sliding-window rate limit, async indexing via Kafka, hybrid isolation for noisy enterprise tenants — all come from that work.

## 2. A performance optimization with significant impact

One of the read-heavy APIs that served the warehouse operator dashboards was sitting at ~700–900ms p95 and frequently timing out during shift changes when operators all refreshed at once. Profiling pointed at three culprits: (a) the query was N+1 across robot-state and task-history tables, (b) we were re-deserializing the same tenant configuration JSON on every request, and (c) the JWT was being re-parsed and re-validated on every downstream call instead of once at the edge. Fixes were boring but effective: a projection query that replaced N+1 with a single join, an in-process Caffeine cache for tenant config (60s TTL with pub/sub invalidation), and JWT claims propagated as a typed context object after a single edge-side validation. End-to-end p95 dropped from ~800ms to ~90ms — close to a 10x improvement — and the timeouts during shift-change spikes disappeared entirely. The change was under 300 lines and reinforced a lesson I keep applying: most "we need to scale this" problems are actually "we're doing the same work N times" problems hiding behind a graph.

## 3. A critical production incident resolved

During a major peak season, a Kafka consumer group that fed downstream warehouse-execution started lagging unboundedly after a deploy. Robots kept working from cached state but new orders were piling up unprocessed — a real customer-visible impact within minutes. Initial pages blamed Kafka, but Kafka itself was healthy: brokers green, lag growing only on this one group. Tracing showed the new code path made a synchronous database call inside the consumer loop, and a missing index turned a 2ms lookup into a ~400ms one. At our message rate the consumer simply couldn't keep up. Short-term fix was to scale the consumer deployment from 8 to 32 pods to drain the backlog while the on-call DBA built the missing index online. Lag drained in about 35 minutes; we then rolled back the offending change and re-deployed with the index in place. Longer term we added a consumer-lag SLO with a burn-rate alert (would have paged us before customers noticed), a CI check that flags new synchronous DB queries inside Kafka listeners without an explicit justification, and a "bulkhead" review checklist for any change touching the consumer hot path. Searchly's indexer reflects those lessons: lag-based autoscaling, idempotent writes, and the hot path is kept free of inline DB work.

## 4. An architectural decision balancing competing concerns

When we onboarded our first very large customer with strict data-isolation requirements, the team had to decide how to extend our existing shared-tenant model. The options were the usual two: keep everyone in the shared model (cheap and operationally simple, but uncomfortable for the customer's security review) or move everyone to dedicated infrastructure per tenant (great isolation, but breaks the economics for the long tail of smaller customers). I proposed a hybrid: shared by default, with the ability to promote a tenant to dedicated stateful resources (their own Postgres schema, their own Kafka topic, their own search index) without changing application code — the routing layer reads `tier` from a tenant-config table and resolves the right backend. The trade-off was operational: two code paths for backups, two patterns for rolling upgrades, and a runbook for promoting a tenant. Two years in, the vast majority of tenants stay on shared, a handful of enterprise tenants are on dedicated, and the enterprise contracts more than fund the operational overhead. Searchly's hybrid OpenSearch model (shared index with `tenant_id` routing + per-enterprise dedicated indices) is the same shape, applied to a different problem.

