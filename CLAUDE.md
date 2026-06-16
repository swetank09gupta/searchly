# Searchly — Project Context for Claude

## What This Is
Searchly is a multi-tenant enterprise operational intelligence platform. It combines hybrid BM25 + semantic (kNN) search, a RAG pipeline, and an agentic warehouse assistant that queries live Kubernetes clusters to answer questions like "why is the allocator crashing for sams-club-atlanta in prod?".

---

## Services
| Service | Stack | Port | Role |
|---|---|---|---|
| `gateway` | Spring Cloud Gateway | 8080 | TLS, JWT auth, per-tenant rate limiting |
| `search-api` | Java/Spring Boot 3 | 8081 | Search + document management + KG API |
| `indexer` | Java/Spring Boot | 8082 | Kafka consumer → OpenSearch writer |
| `embedding-service` | FastAPI | 8083 | BGE `bge-small-en-v1.5` (384-dim) + cross-encoder reranker |
| `warehouse-agent` | FastAPI | 8084 | Org intelligence + live k8s cluster access |
| `connectors` | Python | — | Cron sync: Jira, Confluence, GitHub |
| `py-indexer` | Python | — | Alternate Kafka consumer (backup) |

---

## Storage
- **PostgreSQL** — tenants, users, doc metadata, quotas, ACLs, knowledge graph (`kg_entities`, `kg_relationships`)
- **OpenSearch** — BM25 + k-NN indices; shared (`documents-shared`, `chunks-shared`) or per-tenant (`documents-{id}`, `chunks-{id}`)
- **Redis** — 60s query result cache + sliding-window rate limit counters
- **Kafka** — async indexing queue; `indexing.shared` (12 partitions) + `indexing.enterprise.{tenantId}`
- **MinIO/S3** — raw document blob storage

---

## Multi-Tenancy
- **FREE/STANDARD/PREMIUM** → shared indices; mandatory `tenant_id` filter + routing key on every query
- **ENTERPRISE** → dedicated per-tenant indices; dedicated Kafka topic
- Anti-IDOR: `TenantSecurityFilter` validates JWT tenant == request tenant == user membership
- **Gap:** `acl_users`/`acl_roles` fields stored in OpenSearch but never enforced at query time (sub-tenant ACL not yet active)

---

## Search + RAG Pipeline (post P0–P3)

```
User Query
  → Metadata extraction (regex: env, service, source_type) — ~0ms
  → Cache check (Redis) — ~1ms
  → Query rewrite (Ollama llama3.2:3b) — ~600ms
  → Embed ×2 (BGE bge-small-en-v1.5, query prefix) — ~25ms each
  → 6 retrieval legs (currently sequential, parallelism is a known gap):
      knnOrig×1.0, knnRew×0.7, bm25Orig×1.0, bm25Rew×0.7
      custKnn×2.0, custBm25×2.0 — top-50 each, with env/service filters
  → RRF merge: score = Σ (listWeight × authorityWeight) / (60 + rank)
      Authority: LIVE_LOGS=1.0, DEPLOYMENT=0.9, CODE=0.8, JIRA=0.7, CONFLUENCE=0.5
  → Top-30 → cross-encoder rerank (bge-reranker-base) — ~300ms
  → Source budget: warehouse_logs=2, deployment_state/jira/git/confluence=1
  → Top-6 chunks → Ollama generate — ~4s
  → Return answer + sources + RetrievalTrace[30] per chunk
```

**Query**: typed OpenSearch Java DSL; `match(content, q)` + `term(tenant_id)` + optional `term(metadata.env)` / `term(metadata.service)` filters.

**BM25**: OpenSearch defaults (k1=1.2, b=0.75). Recency boost (Gauss decay, 30d scale, 0.5 decay) applied to `documents-*` BM25. Origin is `System.currentTimeMillis()` (numeric epoch ms) because `created_at` is mapped as `long`, not `date` — date-math (`now/d`) fails on `long` fields. **Not yet applied to chunk retrieval (RagService.bm25Internal)** — still a gap.

**Highlighting**: `content` field, 150 chars/fragment, 2 fragments/hit.

**Pagination**: cursor-based (`search_after`) via base64-encoded sort values. `from+size` still supported for backward compat.

---

## Embedding & Reranking

- **Model**: `BAAI/bge-small-en-v1.5` (384-dim, CPU, ~22 MB)
- **Query prefix**: `"Represent this sentence: "` applied to query vectors only
- **Passage encoding**: no prefix
- **Version constant**: `EmbeddingClient.EMBEDDING_VERSION = "bge-small-en-v1.5-v1"` written into every chunk as `embedding_version`
- **Reranker**: `BAAI/bge-reranker-base` at `/rerank` on embedding-service port 8083
- **Index remapping**: NOT required — same 384 dimensions as previous model

---

## Indexing Pipeline
1. Search API: generate UUID → Postgres `PENDING` → MinIO → Kafka publish → 202 response
2. Indexer: content fingerprint check (SHA-256) → skip chunk re-embed if unchanged → write full doc to `documents-*` → chunk (1500 chars, 200 overlap, sentence-aware) → batch embed (50/request) → write to `chunks-*` with `embedding_version`
3. `doc_id` = OpenSearch `_id` → upsert semantics → safe Kafka replay
4. Bulk indexing: up to 500 docs per `/_bulk` request

**Chunking special case**: files under `adr/`, `decisions/`, `architecture/` kept whole if <12K chars.

---

## OpenSearch Mappings
**documents-***: `tenant_id` (keyword), `title` (text), `content` (text), `metadata` (object), `created_at` (**long — epoch millis**, NOT `date`), `content_fingerprint` (keyword). 3 shards, 0 replicas (dev). The `long` mapping for `created_at` means Gauss decay must use numeric epoch origin, not date-math strings.

**chunks-***: above fields + `chunk_text` (text), `chunk_index` (integer), `embedding` (knn_vector, dim=384, HNSW M=16 ef=128, Lucene engine), `embedding_version` (keyword). `knn: true`.

---

## Knowledge Graph (P3.1 — Storage Layer Complete, Extraction NOT Wired)

**Tables (Postgres)**: `kg_entities` (entity_type, entity_id, tenant_id, name, properties jsonb), `kg_relationships` (from_type, from_id, relation, to_type, to_id, tenant_id, properties jsonb). V4 migration.

**API**: `POST /api/v1/kg/entity`, `POST /api/v1/kg/relationship`, `GET /api/v1/kg/entity/{type}/{id}/neighbors`, `GET /api/v1/kg/traverse/{type}/{id}?depth=3`

**Traversal**: recursive CTE BFS (outbound), max depth configurable (capped at 5).

**CRITICAL**: `connectors/sync.py` is NOT updated. The graph is currently empty. Extraction must be wired before the graph has any value. Priority order:
1. Jira remote links → `jira_issue --[fixed_by]--> pull_request` (API-based, authoritative, implement first)
2. GitHub PR commits → `pull_request --[contains]--> commit`
3. File path heuristics → `commit --[modifies]--> service`
4. k8s deployment labels → `deployment --[runs]--> service`

---

## Retrieval Tracing (P3.2)

Every `SearchResponse` includes `retrieval_traces: List<RetrievalTrace>` for the top-30 rerank candidates:
```json
{
  "chunk_id": "...",
  "doc_id": "...",
  "source": "jira",
  "knn_rank": 3,
  "bm25_rank": 1,
  "rrf_score": 0.0412,
  "rrf_rank": 2,
  "reranker_score": 0.87,
  "final_rank": 1,
  "included": true,
  "embedding_version": "bge-small-en-v1.5-v1"
}
```

---

## AI / RAG
- **Embedding**: `BAAI/bge-small-en-v1.5` (384-dim, CPU), FastAPI port 8083
- **Reranker**: `BAAI/bge-reranker-base`, same service, `/rerank` endpoint
- **Vector search**: HNSW in OpenSearch (`chunks-*`)
- **Hybrid**: 6-list RRF fusion with SourceAuthority weights
- **LLM**: Ollama `llama3.2:3b` (self-hosted, no data egress, 120s timeout)
- **RAG context**: system prompt + top-6 chunks + customer header; max 5 agentic tool rounds
- **Warehouse Agent tools**: `search_knowledge`, `get_logs`, `get_deployment_state`, `get_pod_status`, `list_log_indices`
- **Agent loop**: Planner (JSON tool plan) → Execution (parallel tool calls) → Synthesis. **Knowledge-only shortcut**: when no live cluster is configured (`operational=False`), the LLM planner is bypassed and `search_knowledge` is called directly — `llama3.2:3b` is too small to reliably emit JSON tool arrays under open-ended choice.
- **`rag_only` flag**: `search_knowledge` sends `rag_only=true` to the gateway. `RagService` skips the warehouse-agent path when this is set, breaking the circular routing loop (search_knowledge → gateway → RagService → warehouseAgent → search_knowledge → ∞).
- **Session memory**: rolling summary + structured_memory {customer, environment, active_issue, investigation_state, known_findings}; 5 verbatim recent turns kept
- **Credential access (Mode A)**: fetches ES password at runtime from k8s Secret, execs in filebeat pod — zero stored credentials
- **Entity resolution**: `resolver.py` slides a 1–4 word window over the full question and scores each phrase against the customer registry (max of hint-score and scan-score). Any phrasing resolves — "solution numbers for sodimac colombia", "GMI's prod cluster", etc. — without depending on the entity extractor correctly isolating the customer substring. Matched phrase is learned as an alias for instant future resolution.

---

## Evaluation (P2.5 + P3.4)
- **Metrics**: source_recall, retrieval_recall@20, MRR, keyword_hit_rate, LLM judge (0–2)
- **Runner**: `warehouse-agent/eval.py` — runs against any dataset, prints summary
- **Scheduler**: `warehouse-agent/eval_scheduler.py` — APScheduler nightly at 02:00 UTC
- **History**: `eval_history/YYYY-MM-DD_HH-MM.json`, regression detection at >10% metric drop
- **Dataset**: `warehouse-agent/eval_dataset.json` — currently 5 questions; target 200+ production-derived cases

---

## Known Gaps / Top Priority Next Work

### Security (Critical)
- `acl_users`/`acl_roles` in OpenSearch never enforced — add `bool.should` filter to all chunk + document queries

### Correctness (High)
- KG extraction not wired — graph empty, wire Jira remote links first
- Retrieval legs sequential — use `CompletableFuture.allOf()`, ~250ms free
- Recency boost missing from chunk BM25 — add `function_score` to `RagService.bm25Internal()`. Use numeric epoch origin (`System.currentTimeMillis()`) and scale in ms — NOT date-math `now/d`, which requires `date` type but `created_at` is `long`.

### Reliability (Medium)
- Sessions in-memory — move `SessionStore` to Redis (30-line change)
- Redis failure → 429 — catch `RedisException`, degrade to allow-through
- `Kafka max.poll.records` unbounded — set to 10, add concurrency semaphore

### Long-Term
- Embedding version migration path (versioned index aliases)
- Production query log → eval dataset feedback loop
- Ollama async queue + streaming for tail latency

---

## RBAC
Roles are JWT claims, enforced via Spring `@PreAuthorize` + `TenantSecurityFilter`.

| Action | VIEWER | EDITOR | TENANT_ADMIN | SERVICE |
|---|---|---|---|---|
| GET /search | ✓ | ✓ | ✓ | ✓ |
| GET /documents/{id} | ✓ | ✓ | ✓ | ✓ |
| POST /documents | ✗ | ✓ | ✓ | ✓ |
| DELETE /documents/{id} | ✗ | ✓ | ✓ | ✗ |
| Manage tenant config | ✗ | ✗ | ✓ | ✗ |
| POST /kg/entity | ✗ | ✓ | ✓ | ✓ |

**GDPR delete**: `DELETE /documents/{id}` purges OpenSearch + Postgres + MinIO + cache + emits Kafka tombstone.

---

## Observability (designed, partially wired)
- **Tracing**: OpenTelemetry → Jaeger (local) / Tempo (prod); W3C `traceparent` propagated through Kafka headers
- **Metrics**: Micrometer → Prometheus; RED per endpoint per tenant — **instrumentation not fully wired**
- **Logs**: structured JSON (Logback); `trace_id`, `span_id`, `tenant_id`, `user_id` → Loki / ELK
- **Retrieval traces**: per-chunk pipeline provenance returned in every `SearchResponse`

---

## Connectors
- **Jira**: JQL cursor pagination, 150ms pacing, max 1000 issues/project; re-fetches all on each cycle (no delta — gap)
- **Confluence**: recursive page fetch (depth ≤8), HTML stripped
- **GitHub**: language-aware chunking (Python=AST, Java=regex, fallback=2000-char text); ADR/architecture files kept whole (<12K)
- **Sync schedule**: Track A (60min) — deployment state; Track B (4h) — Jira + Confluence + repos
- **State**: `.sync_state.json` on Docker volume

---

## Index Size Math
| Scale | Disk | RAM (HNSW) |
|---|---|---|
| 100K chunks | ~380 MB | 4 GB |
| 1M chunks | ~3.8 GB | 16 GB |
| 10M chunks | ~38 GB | 64 GB |

---

## Deploy
- Push to git → pull on VM → `docker compose up -d --build` (never scp files directly)
- See `deploy/` directory for compose files
- DB migrations: Flyway, expand-then-contract pattern
- Build tool: **Maven** (explicit preference — do not suggest Gradle)
- Prod target: K8s (blue-green via two Deployments; Argo Rollouts for canary)

---

## Caching
| Layer | TTL | Key |
|---|---|---|
| Redis query results | 60s | `cache:{tenantId}:{roles_csv}:{sha256(query)}` |
| Caffeine tenant config | 5m | in-process |
| Caffeine JWKS | 1h | in-process |

Invalidation: any document mutation triggers `cache.invalidateTenant(tenantId)`.
Conversational queries (session_id set) and cursor-paginated queries are never cached.
