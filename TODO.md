# Searchly — Next Work

Last updated: 2026-06-16 after P0–P3 completion.

---

## Sprint 1 — Quick Wins (Low effort, High impact)

These three are independent, can be done in parallel, and require no schema changes.

### S1.1 — Parallel Retrieval Legs  `search-api/RagService.java`
**Why:** 6 retrieval legs run sequentially today (~300ms wasted). Each is an independent HTTP call.  
**Fix:** Wrap all 6 in `CompletableFuture.allOf()` with a shared fixed thread pool (8 threads). Wall-clock time drops to max(slowest leg) ≈ 60ms instead of sum(all legs) ≈ 300ms. ~250ms free latency gain.  
**Files:** `RagService.java` — replace sequential calls in `answer()` with parallel futures.

### S1.2 — Recency Boost on Chunk BM25  `search-api/RagService.java`
**Why:** `SearchService` applies Gauss decay to `documents-*` BM25. `RagService.bm25Internal()` uses plain `match` — a 3-year-old Confluence page competes equally with yesterday's deployment log.  
**Fix:** Add `function_score` wrapper to `bm25Internal()` matching the pattern in `SearchService`. **Important:** `created_at` is mapped as `long` (epoch millis), NOT `date`. Use numeric epoch origin (`System.currentTimeMillis()`) and numeric scale (`30L * 24 * 60 * 60 * 1000` ms). Do NOT use `"now/d"` date-math — it only works on `date` type fields and will produce "all shards failed".  
**Files:** `RagService.bm25Internal()` — 15-line change, pattern identical to `SearchService` lines 90–104.

### S1.3 — Redis Session Store for Intelligence Agent  `intelligence-agent/session.py`
**Why:** `SessionStore` is an in-memory dict. Horizontal scaling of intelligence-agent is architecturally impossible.  
**Fix:** Replace the dict with Redis hash keys (`session:{id}`) + TTL expiry. `Session` dataclass serializes to JSON already. The public interface (`get_or_create`, `get`) stays identical — only the storage backend changes.  
**Files:** `session.py` (~30 lines), `requirements.txt` (redis-py already in ecosystem).

---

## Sprint 2 — Security + Correctness

### S2.1 — Enforce ACL Fields at Query Time  `search-api`  ⚠️ Security
**Why:** `acl_users` and `acl_roles` fields exist in OpenSearch documents but are never applied as filters. Any authenticated user within a tenant sees all documents regardless of intended sharing restrictions.  
**Fix:** Add a mandatory `bool.should` clause to every OpenSearch query in `SearchService.search()` and `RagService.bm25Internal()`:
```
(acl_roles contains any(userRoles)) OR (acl_users contains userId) OR (NOT exists acl_roles)
```
`TenantContext` already carries `roles()` and a user ID field is available from the JWT.  
**Files:** `SearchService.java`, `RagService.java`, `TenantContext.java` (add userId field), `KnnSearchClient.java` (add acl filter to kNN queries).

### S2.2 — Wire KG Extraction: Jira Remote Links  `connectors/sync.py`
**Why:** The knowledge graph storage layer exists but `connectors/sync.py` was never updated. The graph is empty and provides zero value.  
**Fix:** In the Jira connector sync loop, after indexing each issue: call `POST /api/v1/kg/entity` for the Jira issue, then `GET /rest/api/3/issue/{key}/remotelink` and for each GitHub PR URL call `POST /api/v1/kg/relationship` with relation `fixed_by`.  
**Start with this because:** Jira remote links are authoritative (no regex heuristics), highest signal, and the Jira connector already fetches issues in a loop.  
**Files:** `connectors/sync.py` — add `_write_jira_kg_entries(issue, kg_base_url, tenant_id)` helper called after existing index call.

### S2.3 — Wire KG Extraction: GitHub PR → Commits  `connectors/sync.py`
**Dependency:** Do after S2.2.  
**Fix:** In GitHub sync, for each PR: `GET /repos/{owner}/{repo}/pulls/{n}/commits` → upsert commit entities + `pull_request --[contains]--> commit` relationships.  
**Files:** `connectors/sync.py`

---

## Sprint 3 — Reliability

### S3.1 — Kafka Dead-Letter Queue  `indexer/`
**Why:** A single malformed Kafka message crashes the consumer loop for that partition. All subsequent messages are blocked indefinitely.  
**Fix:** Catch exceptions in `IndexingConsumer.process()` per message. After 3 retries (exponential backoff), publish the message to `indexing.dlq` topic and commit the offset. Add Prometheus counter `indexing_dlq_total`.  
**Files:** `IndexingConsumer.java`, `RetryConsumer.java` (already exists — wire it to DLQ sink), Kafka topic config.

### S3.2 — Bound Kafka max.poll.records  ✅ Done
Set `max.poll.records=10` in `indexer/src/main/resources/application.yml`.

### S3.3 — Redis Failure Degradation in Rate Limiter  `gateway/`
**Why:** Redis failure causes every request to return 429. The rate limiter fails closed.  
**Fix:** In the Spring Cloud Gateway rate limit filter, catch `RedisException` and allow the request through (or fall back to a permissive in-memory counter). Log the Redis outage as an alert.  
**Files:** Gateway rate limit filter (Spring Cloud Gateway's Redis rate limiter has an `onError` hook).

---

## Sprint 4 — Observability + Eval Quality

### S4.1 — Production Query Logging  `search-api/`
**Why:** The eval dataset has 5 questions. We have no idea if retrieval quality is good on real queries.  
**Fix:** Log every search query to a `query_log` Postgres table (query_text, tenant_id, answer_snippet, sources, had_live_data, latency_ms, timestamp). Add a nightly job that exports the last 24h of queries to a staging eval dataset. Human review → promote to golden set.  
**Tables:** V5 migration — `query_log (id, tenant_id, query_text, answer_snippet, sources jsonb, latency_ms, created_at)`.

### S4.2 — Wire Micrometer Timers in RagService  `search-api/RagService.java`
**Why:** No per-stage latency metrics exist. Ollama slowdowns are invisible until users complain.  
**Fix:** Inject `MeterRegistry` into `RagService`. Add `Timer.record()` around: query_rewrite, embed, knn_search, bm25_search, rerank, generate. Tag with `tenant_id`, `has_customer`, `has_live_data`.  
**Files:** `RagService.java`, `application.yml` (enable Prometheus endpoint).

---

## Backlog (Future, No Sprint Assigned)

| Item | Why deferred |
|---|---|
| Embedding version migration path (versioned index aliases) | High effort, no urgent need until model upgrade |
| Commit ↔ Service path heuristics in KG | Depends on consistent monorepo layout — validate with team first |
| Deployment ↔ Service KG from k8s labels | Needs intelligence-agent to call KG API on each deployment scan |
| Ollama async queue + streaming | High effort, major architecture change |
| HNSW quantization (fp16) for >5M chunks | Not yet near this scale |
| Jira incremental sync (updatedDate filter) | 150ms pacing handles it for now; revisit at >50K issues |
| User thumbs-up/down on answers | UX dependency; product decision needed |

---

## Done (P0–P3 + session fixes, 2026-06-16)
- [x] Cursor-based pagination (search_after)
- [x] GDPR delete pipeline
- [x] Redis sliding-window rate limiting
- [x] Bulk indexing API
- [x] Content fingerprinting (skip re-embed on unchanged docs)
- [x] BGE embeddings (bge-small-en-v1.5) with query prefix
- [x] Cross-encoder reranker (bge-reranker-base)
- [x] Dual-query retrieval (6 legs, RRF with authority weights)
- [x] Source budget context selection
- [x] Metadata-aware retrieval (env/service extraction)
- [x] Recency boost on documents-* BM25 (Gauss decay, numeric epoch origin — `created_at` is `long` not `date`)
- [x] Resilience4j circuit breakers (5 instances)
- [x] Planner agent loop (plan → execute → synthesize)
- [x] Rolling session memory compression with structured_memory
- [x] Evaluation framework (source_recall, retrieval_recall@20, MRR, keyword_hit_rate, LLM judge)
- [x] Knowledge graph storage + traversal + API (KgController, KnowledgeGraphService, V4 migration)
- [x] Retrieval tracing (RetrievalTrace per chunk in SearchResponse)
- [x] Embedding version lineage (embedding_version on every chunk)
- [x] Nightly eval scheduler (APScheduler, regression detection)
- [x] `rag_only=true` flag — breaks circular routing loop (search_knowledge → gateway → RagService → warehouseAgent → ∞)
- [x] HTTP/1.1 forced on all 5 Java HTTP clients (EmbeddingClient, WarehouseAgentClient, RerankClient, KnnSearchClient, OllamaClient) — Java 11 defaults to HTTP/2 but FastAPI/uvicorn and Ollama only support HTTP/1.1
- [x] Knowledge-only planner bypass — `llama3.2:3b` unreliable for JSON tool arrays; search_knowledge auto-called when no live cluster configured
- [x] Window-scan entity resolution in `resolver.py` — 1–4 word sliding window over full question; any phrasing resolves customer regardless of sentence structure or prepositions
- [x] DevOps repo branch sync — signal branches hardcoded; `feature/*`, `dev/*` etc permanently excluded; `greymatter-deployment`, `pick-assist-helm-charts`, `jenkins` configured
- [x] Customer auto-registration from DevOps repo branches — `{customer-id}-{env}` branch convention parsed on each sync; idempotent POST to intelligence-agent `/api/v1/customers` + `/api/v1/customers/{id}/environments/{env}`
- [x] Container restart resilience — scheduler checks `last_shared_completed_at` at startup; skips initial full sync if completed within `SYNC_FULL_INTERVAL_HOURS`
- [x] Incremental Jira sync — delta JQL `AND updated >= last_completed_at` on every run after first; `--force` bypasses
- [x] Incremental Confluence sync — delta CQL search `lastModified >= last_completed_at` on every run after first; switches from recursive content API to flat search API for delta mode
- [x] Parallel sync workers — Jira/Confluence via `SYNC_ATLASSIAN_WORKERS` (default 3), GitHub repos/KG via `SYNC_GITHUB_WORKERS` (default 4); thread-safe state via `_STATE_LOCK` + `_update_state()`; shared rate limiters `_ATLASSIAN_RL` (7 req/s) + `_GITHUB_RL` (5 req/s)
- [x] Kafka `max.poll.records=10` — prevents OOM burst when parallel sync fills Kafka faster
