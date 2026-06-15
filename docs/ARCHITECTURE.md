# Searchly — Architecture Design

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
                       └─────────────┬────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │      Search API      │
                          │   (read + write)     │
                          └──┬────┬────┬─────────┘
                             │    │    │
             ┌───────────────┘    │    └────────────────────┐
             ▼                    ▼                         ▼
  ┌──────────────┐      ┌──────────────┐          ┌──────────────────┐
  │    Redis     │      │  OpenSearch  │          │   PostgreSQL     │
  │ (cache +     │      │ documents-*  │          │ tenants, users,  │
  │  rate limit) │      │ chunks-*     │          │ doc metadata,    │
  └──────────────┘      │ (BM25+kNN)   │          │ ACLs, quotas,    │
                        └──────────────┘          │ kg_entities,     │
                                                  │ kg_relationships │
                                                  └──────────────────┘
        │
        │   (writes: doc upload)
        ▼
   ┌─────────┐      ┌──────────┐      ┌──────────────────────────┐
   │ MinIO/  │◄─────│Kafka     │─────►│  Indexer                 │
   │  S3     │      │(tiered   │      │ (Tika → chunk → embed    │
   │ (blobs) │      │ topics)  │      │  via Embedding Svc :8083 │
   └─────────┘      └──────────┘      │  → documents-*+chunks-*) │
                                      └──────────────────────────┘
                                               │ POST /embed
                                               ▼
                                      ┌──────────────────────────┐
                                      │   Embedding Service      │
                                      │  :8083                   │
                                      │  POST /embed  (BGE)      │
                                      │  POST /rerank (reranker) │
                                      └──────────────────────────┘

  Cross-cutting:  Keycloak (OIDC) │ Jaeger (OTel) │ Prometheus + Grafana
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| **Gateway** | TLS termination, JWT validation, per-tenant sliding-window rate limit, request routing, security headers, request size limits |
| **Search API** | Document CRUD, query orchestration, cache lookup, tenant/RBAC enforcement, write to Postgres + MinIO + Kafka |
| **Indexer** | Kafka consumer; fetches blob, runs Tika, chunks text (1500 chars, 200-char overlap, sentence-aware), calls Embedding Service to embed chunks (50/request), writes full doc to `documents-*` and chunk vectors + `embedding_version` to `chunks-*`; idempotent (content-fingerprint SHA-256 check); per-tier consumer pools |
| **Embedding Service** | FastAPI at `:8083`; `POST /embed` serves `BAAI/bge-small-en-v1.5` (384-dim, asymmetric query prefix); `POST /rerank` serves `BAAI/bge-reranker-base` cross-encoder for RAG reranking |
| **OpenSearch** | Full-text BM25 index (`documents-*`), kNN HNSW vector index (`chunks-*`, 384-dim), fuzzy/highlight/facets/aggregations |
| **PostgreSQL** | System of record for tenants, users, document metadata, ACLs, quotas, and knowledge graph (`kg_entities`, `kg_relationships`) |
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
  ├─ Content fingerprint check (SHA-256) — skip chunk re-embed if unchanged
  ├─ Write full doc ──► OpenSearch documents-* (routing=tenant_id)
  ├─ Chunk text (1500 chars, 200-char overlap, sentence-aware)
  │     ADR/architecture files kept whole if < 12K chars
  ├─ Batch embed chunks (50/request) ──► Embedding Service POST /embed
  ├─ Write chunk vectors ──► OpenSearch chunks-* (embedding + embedding_version)
  └─ UPDATE Postgres status=INDEXED + emit audit event
```

**Failure handling:** Kafka retries with exponential backoff; `indexing.dlq` topic is defined for poison messages but **DLQ routing is not yet implemented in the Indexer** — a poison message currently crashes the consumer (known gap, Sprint 3.1); status field in Postgres lets clients poll.

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
  │   Inject mandatory tenant_id filter
  │   NOTE: acl_users/acl_roles fields exist in OpenSearch but are NOT enforced here (known gap, Sprint 2.1)
  │   Submit to OpenSearch (timeout 400ms, circuit-breaker)
  ├─ Hydrate hits with stored fields (avoid extra Postgres roundtrip)
  ├─ Cache SET ttl=60s
  └─ Return paginated results + highlights
```

---

## 3. Storage Strategy

| Layer | Choice | Why |
|---|---|---|
| **Search engine** | OpenSearch | Mature inverted index (BM25, fuzzy, highlights, facets) + HNSW kNN vector index for semantic search; horizontal scale via sharding; Apache-licensed fork avoids ES license risk |
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
  - `indexing.dlq` — designed for poison messages after N retries; **Indexer DLQ routing not yet implemented** (known gap, Sprint 3.1)
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

## 9. Intelligence Layer (built on top of the search platform)

The search platform powers an organisation intelligence agent that answers natural-language
questions from your engineering knowledge base. Full design: [INTELLIGENCE_ARCHITECTURE.md](INTELLIGENCE_ARCHITECTURE.md).

### RAG pipeline (post P0–P3)

```
User query
  → metadata extraction (regex: env, service) — ~0ms
  → cache check (Redis) — ~1ms
  → query rewrite (Ollama llama3.2:3b) — ~600ms
  → embed ×2 (BAAI/bge-small-en-v1.5, 384-dim, query prefix) — ~25ms each
  → 6 retrieval legs (currently sequential, target: CompletableFuture.allOf()):
      knnOrig×1.0, knnRew×0.7, bm25Orig×1.0, bm25Rew×0.7
      custKnn×2.0, custBm25×2.0 (customer= context only)
      each: top-50, with env/service term filters
  → RRF merge: score = Σ (listWeight × authorityWeight) / (60 + rank)
      authority: live_logs=1.0 → deployment=0.9 → code=0.8 → jira=0.7 → confluence=0.5
  → top-30 → cross-encoder rerank (BAAI/bge-reranker-base) — ~300ms
  → source budget: warehouse_logs=2, deployment/jira/code/confluence=1 each
  → top-6 → Ollama generate (llama3.2:3b) — ~4s
  → SearchResponse + retrievalTraces[30] (see ADR 0023)
```

### Knowledge Graph (P3.1 — storage only, extraction not yet wired)

Two Postgres tables (`kg_entities`, `kg_relationships`, V4 migration) with JSONB properties.
BFS traversal via recursive CTE (max depth 5). REST API at `/api/v1/kg`.

**The graph is empty** — `connectors/sync.py` was not updated. Wiring plan (ordered by ROI):
1. Jira remote links → `jira_issue --[fixed_by]--> pull_request` (Sprint 2.2)
2. GitHub PR/commits API → `pull_request --[contains]--> commit` (Sprint 2.3)
3. File path heuristics → `commit --[touches]--> service`
4. k8s label `app=<service>` → `deployment --[runs]--> service`

### Retrieval Tracing (P3.2)

Every `SearchResponse` carries a `retrievalTraces` list. Each trace records knnRank, bm25Rank,
rrfScore, rrfRank, rerankerScore, finalRank, included, embeddingVersion for one RRF candidate.
Enables per-stage regression root-cause without re-running queries in a debugger.

### Continuous Evaluation (P3.4)

`eval_scheduler.py` runs nightly at 02:00 UTC (APScheduler). Writes results to
`eval_history/YYYY-MM-DD_HH-MM.json`. Detects >10% regression in avg_answer_score,
source_recall, retrieval_recall@20, keyword_hit_rate, pass_rate. Eval dataset is 5 sample
questions today — target 200+ production-derived cases.

---

## 10. Tenant Tiers

| Tier | QPS | Index Plan | Kafka | Quota |
|---|---|---|---|---|
| FREE | 10 | shared | shared topic | 1K docs/day, 1 GB |
| STANDARD | 100 | shared | shared topic | 50K docs/day, 50 GB |
| PREMIUM | 1000 | shared (boosted shard) | shared topic, priority consumer | 1M docs/day, 500 GB |
| ENTERPRISE | custom | dedicated index | dedicated topic + consumers | negotiated |

Tier stored in Postgres `tenants.tier`, cached in Redis, propagated as a JWT claim for fast gateway checks.
