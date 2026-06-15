# Intelligence Agent — Architecture Design

> This document covers the **organisation intelligence layer** built on top of the Searchly
> search platform. For the underlying search engine (multi-tenant BM25 + kNN, OpenSearch, Kafka
> indexing pipeline) see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Problem Statement

Engineering knowledge is spread across silos that people context-switch between constantly:

| Silo | Typical volume | Access today |
|---|---|---|
| Jira (tickets, epics, bugs, specs) | Tens of thousands of issues | Jira search, tribal knowledge |
| Confluence (ADRs, LLDs, runbooks, design docs) | Thousands of pages | Confluence search, tribal knowledge |
| GitHub (source code, inline docs, configs) | Many repos, millions of lines | grep + blame |
| Elasticsearch logs (live, per-environment) | Real-time, ephemeral | Kibana, SSH |

This layer collapses all four into one natural-language interface: ask a question, get a
synthesised answer with citations, optionally enriched with live log data from any registered
environment.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     User (browser / curl)                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ HTTPS
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│               Intelligence Agent  (FastAPI)                         │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────────────┐  │
│  │  Chat UI    │   │   RAG Core   │   │   Live Tools             │  │
│  │ (chat.html) │   │  (agent.py)  │   │  (tools.py)              │  │
│  └─────────────┘   │              │   │  ├─ get_logs()           │  │
│                    │ 1. embed Q   │   │  ├─ get_pod_status()     │  │
│                    │ 2. BM25+kNN  │   │  ├─ list_customers()     │  │
│                    │ 3. rerank    │   │  └─ describe_product()   │  │
│                    │ 4. LLM gen   │   └──────────────────────────┘  │
│                    └──────────────┘                                  │
└────────┬──────────────────────┬──────────────────────────┬──────────┘
         │                      │                          │
         ▼                      ▼                          ▼
┌─────────────────┐  ┌───────────────────┐  ┌─────────────────────────┐
│  OpenSearch          │  │  Ollama (LLM)     │  │  Elasticsearch (ECK)    │
│  BM25 + kNN (HNSW)  │  │  runs on CPU      │  │  Filebeat → Logstash    │
│  documents-* (BM25) │  │  ~4-5s / query    │  │  live application logs  │
│  chunks-*   (kNN)   │  └───────────────────┘  └──────────┬──────────────┘
└─────────────────┘                                     │ Mode A (bastion-kubectl)
         ▲                                              │ fetches ES password at runtime
         │ index writes                                 ▼
┌────────┴────────────────────────────────────────────────────────────┐
│                    Sync Cron  (connectors/sync.py)                   │
│  ┌───────────────┐  ┌──────────────────┐  ┌────────────────────┐    │
│  │  JiraFetcher  │  │ ConfluenceFetcher │  │   RepoIndexer      │    │
│  │  10k issues   │  │  recursive depth │  │  SHA-based incr.   │    │
│  │  per project  │  │  8, 2k pages/sp. │  │  any GitHub org    │    │
│  └───────────────┘  └──────────────────┘  └────────────────────┘    │
│                                                                       │
│  scheduler.py:  Track A (environment state, 60 min)                  │
│                 Track B (Jira + Confluence + repos, 4 h)             │
└─────────────────────────────────────────────────────────────────────┘
```

### Port map (docker-compose defaults)

| Port | Service |
|---|---|
| 8084 | Intelligence Agent (FastAPI) — chat UI + REST API |
| 8081 | Search API (Java) — BM25 + kNN endpoint + Knowledge Graph API |
| 9200 | OpenSearch — document store |
| 8083 | Embedding service — `BAAI/bge-small-en-v1.5` (`POST /embed`) + `BAAI/bge-reranker-base` (`POST /rerank`) |
| 11434 | Ollama — LLM (`llama3.2:3b`) |
| 5432 | PostgreSQL — Search API metadata + Knowledge Graph tables |
| 6379 | Redis — query cache + rate limiting |

---

## 3. Components

### 3.1 Intelligence Agent

FastAPI application. Entry point: `main.py`. All endpoints are under `/api/v1/agent/`.

| File | Responsibility |
|---|---|
| `main.py` | FastAPI app, route declarations, lifespan (starts eval scheduler) |
| `agent.py` | RAG pipeline: query rewrite → embed × 2 → 6-leg retrieval → RRF → rerank → LLM |
| `chat_handler.py` | Planner → Execution → Synthesis loop; rolling structured session memory |
| `tools.py` | Live tools: `get_logs()`, `get_pod_status()`, `get_deployment_state()`, `list_log_indices()`, `search_knowledge()` |
| `elastic_logs.py` | ES log queries — Mode A (bastion-kubectl) and Mode B (direct HTTP) |
| `customer_registry.py` | Thread-safe, file-backed lifecycle-aware customer/environment registry |
| `entity_extractor.py` | Regex-based entity extraction: env, service, customer, intent |
| `resolver.py` | Fuzzy match → RESOLVED / NEEDS_CONFIRM; unknown customer → offers to register |
| `session.py` | In-memory conversation session store with TTL expiry (horizontal scaling gap — see §8) |
| `eval_scheduler.py` | APScheduler nightly eval at 02:00 UTC; writes to `eval_history/`; detects >10% metric regression |
| `auth.py` | API key authentication middleware |
| `products_config.py` | Product catalogue loaded from products.yml |
| `static/chat.html` | Browser chat UI (no build step required) |

#### RAG Pipeline (agent.py + RagService.java)

```
Query
  │
  ├─ 1. Metadata extraction (QueryMetadataExtractor — regex, ~0ms)
  │       └─ env name? service? customer? ticket ID? error code?
  │           → OpenSearch term filters applied to subsequent legs
  │
  ├─ 2. Intent detection (chat_handler.py)
  │       └─ operational keywords regex → tool loop OR knowledge-only
  │
  ├─ 3. Planner (Ollama call 1 of N)
  │       └─ produces an ordered tool execution plan (plan → execute → synthesise)
  │           replaces the old reactive N-round loop
  │
  ├─ 4. Query rewriting (Ollama, ~600ms)
  │       └─ alternative phrasing of the query for dual-query retrieval
  │
  ├─ 5. Embedding ×2 (BGE bge-small-en-v1.5, ~25ms each)
  │       └─ POST /embed → embedding-service
  │           query prefix: "Represent this sentence: " (asymmetric retrieval encoding)
  │           original query + rewritten query → two 384-dim vectors
  │
  ├─ 6. 6-leg retrieval (currently sequential; target: parallel via CompletableFuture)
  │       ├─ knnOrig  (weight 1.0) — kNN on original vector, top-50
  │       ├─ knnRew   (weight 0.7) — kNN on rewritten vector, top-50
  │       ├─ bm25Orig (weight 1.0) — BM25 on original query, top-50
  │       ├─ bm25Rew  (weight 0.7) — BM25 on rewritten query, top-50
  │       ├─ custKnn  (weight 2.0) — kNN filtered to customer chunks (if customer= set)
  │       └─ custBm25 (weight 2.0) — BM25 filtered to customer chunks (if customer= set)
  │
  ├─ 7. RRF merge with source authority weighting
  │       └─ score = Σ (listWeight × authorityWeight) / (60 + rank)
  │           authority: live_logs=1.0 → deployment=0.9 → code=0.8 → jira=0.7 → confluence=0.5
  │           → top-30 rerank candidates
  │
  ├─ 8. Cross-encoder reranking (bge-reranker-base, ~300ms for 30 pairs)
  │       └─ POST /rerank → embedding-service
  │           → top-6 chunks by reranker score
  │           source budget: warehouse_logs=2, deployment/jira/code/confluence=1 each
  │
  ├─ 9. Live tool calls (Execution phase — if plan includes live tools)
  │       └─ tools.py → ES logs / k8s state / pod status
  │
  ├─ 10. Context assembly (Synthesis phase)
  │        └─ top-6 chunks + live tool output + structured session memory
  │            rolling memory: {customer, environment, active_issue, investigation_state,
  │                             known_findings, resolved} + 5 verbatim recent turns
  │
  └─ 11. LLM generation (Ollama, ~4s p50)
           └─ POST http://ollama:11434/api/generate
```

### 3.2 Sync Cron (`connectors/sync.py`)

Incremental knowledge-base builder. Runs on a two-track schedule:

**Track A — Environment state (every 60 min)**
- SSHes to bastion → `kubectl` to gather live deployment versions, pod health, recent events
- Writes structured state docs to OpenSearch with `source=customer_state` tag

**Track B — Shared knowledge (every 4 hours)**
- `JiraFetcher` — pulls issues from configured projects (JQL: `project IN (...)`, updated in last sync window). Stores title, description, comments, resolution.
- `ConfluenceFetcher` — fetches top-level pages + children recursively (depth ≤ 8). Strips HTML, preserves structure.
- `RepoIndexer` — fully automatic, two-level incremental sync:
  - **Repo discovery** — if `github_org` is set, all repos in the org are fetched from the GitHub API automatically. No manual listing in `products.yml` required.
  - **SHA check** — `git ls-remote HEAD` per repo before cloning; repos with no new commits are skipped in milliseconds.
  - **Multi-branch** — set `GIT_BRANCHES=develop,release/*` to index additional branches. Glob patterns expand server-side; new branches matching a pattern are picked up automatically on the next cycle with no config change.
  - **ADR-aware chunking** — files in `adr/`, `decisions/`, `architecture/` paths kept whole if < 12,000 chars (see §4).
  - State key: `repo_name` for default branch, `repo_name:branch` for named branches.
- All chunks written to OpenSearch with `source`, `product`, `repo`, `branch`, `doc_type` metadata.

**Incremental state** — `.sync_state.json` persists last-seen SHAs and timestamps across restarts.

**Rate limiting** — `_api_get()` helper: 150ms inter-request pacing, honours `Retry-After` on 429, exponential backoff on 5xx.

### 3.3 Elasticsearch Log Access

Two transport modes — auto-selected by `tools.py` based on environment config:

#### Mode A — Bastion-kubectl (DEFAULT for ECK deployments)

No ES credential stored in config. Password fetched at runtime from the k8s Secret that ECK
manages automatically:

```
Agent → SSH bastion
    → kubectl get secret <es-secret> -n <es-namespace>
    → base64-decode → ES password (RAM only, never persisted)
    → kubectl exec <filebeat-pod> — curl http://<es-clusterip>:9200/<index>/_search
    → return JSON hits
```

Zero-hit namespace retry: if the configured `k8s_namespace` returns 0 hits, the query is
automatically retried without the namespace filter, and a warning is surfaced to update the config.

Config fields (stored in `customers.yml` per environment):

| Field | Default | Meaning |
|---|---|---|
| `elastic_k8s_ns` | `elastic-system` | Namespace where ECK pods run |
| `elastic_k8s_secret` | `<release>-es-elastic-user` | Secret containing ES credentials |
| `elastic_k8s_svc` | `<release>-es-http` | ES ClusterIP Service |
| `elastic_index` | `filebeat-*` | Index pattern for log queries |
| `elastic_fields` | `{}` | Field name overrides if Logstash schema differs |

#### Mode B — Direct HTTP (secondary)

Used when `elastic_url` is set in the environment config. Supports API key or basic auth. Use for
non-ECK deployments or when an external ES endpoint is accessible from the agent machine.

### 3.4 Customer Registry

Thread-safe, file-backed lifecycle registry. Environments progress through:

```
solution → dev → testing → staging → prod
```

Each stage stores independent k8s access details and ES config. `resolve_env()` returns the
highest configured environment when no specific env is requested.

Data stored in `customers_db.json` (Docker volume — survives restarts). Can be bootstrapped from
`connectors/customers.yml` via `import_yaml()`.

---

## 4. Key Design Decisions

Full rationale in [docs/adr/](adr/README.md). Summary of decisions specific to this layer:

| # | Decision | Choice | Rationale |
|---|---|---|---|
| [0015](adr/0015-self-hosted-llm-ollama.md) | LLM runtime | Ollama on CPU | No external API cost, no data egress; internal knowledge is confidential |
| [0016](adr/0016-hybrid-bm25-knn-rag.md) | Initial retrieval | BM25 + kNN hybrid (RRF) | Keyword catches ticket IDs; semantic catches intent — **superseded by ADR 0021** |
| [0017](adr/0017-bastion-kubectl-es-access.md) | ES log access | Mode A: bastion-kubectl | ES passwords are env-specific; no central credential store is feasible |
| [0018](adr/0018-adr-aware-chunking.md) | Chunking strategy | ADR/LLD files kept whole | Splitting architecture docs loses cross-section context |
| [0019](adr/0019-incremental-repo-indexing.md) | Repo sync | SHA-check before clone | Re-cloning all repos on each cycle saturates disk and bandwidth |
| [0020](adr/0020-multi-source-knowledge-index.md) | Knowledge sources | Jira + Confluence + GitHub | Three silos cover the majority of institutional knowledge |
| [0021](adr/0021-bge-embeddings-cross-encoder-reranking.md) | Retrieval pipeline | BGE + dual-query + reranker | Improves recall (dual-query) and precision (cross-encoder) over ADR 0016 |
| [0022](adr/0022-knowledge-graph-postgres.md) | Knowledge graph | Postgres flat tables + JSONB | Shallow traversal; avoids adding a graph database to the operational stack |
| [0023](adr/0023-retrieval-tracing.md) | Pipeline observability | RetrievalTrace per chunk in SearchResponse | Enables per-stage regression root-cause without re-running queries in a debugger |

---

## 5. Data Flows

### 5.1 Query with live log context

```
User: "why is service X failing in the staging environment?"

1. Metadata extraction: environment=acme-staging, product=backend
2. Intent detection:    operational keywords match → tool loop path
3. Planner:             Ollama produces plan: [search_knowledge, get_logs, synthesise]
4. Query rewriting:     Ollama rewrites query → alternative phrasing
5. Embedding ×2:        embed original + rewritten query → two 384-dim vectors
                        query prefix: "Represent this sentence: "
6. 6-leg retrieval:     knnOrig, knnRew, bm25Orig, bm25Rew + custKnn, custBm25 (customer=acme-staging)
                        each top-50, with env=acme-staging term filter
7. RRF merge:           score = Σ (weight × authority) / (60 + rank)
                        → top-30 candidates
8. Reranking:           bge-reranker-base scores 30 (query, chunk) pairs
                        → top-6 chunks from Confluence/GitHub/Jira
9. Live tool:           get_logs(customer=acme-staging, query="error", tail=200)
                        → Mode A: SSH bastion → kubectl exec filebeat-pod → ES query
                        → recent log lines
10. Context:            top-6 chunks + log lines + structured session memory
11. LLM:                generates answer with citations
12. Response:           streamed to browser
    + retrievalTraces:  per-chunk knn/bm25/rrf/reranker ranks in SearchResponse
```

### 5.2 Incremental sync cycle

```
scheduler.py wakes (4h)
    │
    ├── JiraFetcher.fetch(projects=JIRA_PROJECTS)
    │       └── JQL: project IN (...) AND updated >= <last_sync>
    │           stores title+description+comments as chunks
    │
    ├── ConfluenceFetcher.fetch(spaces=CONFLUENCE_SPACES)
    │       └── GET /wiki/rest/api/content/{id}/child/page (depth 8)
    │           strips HTML; tags with doc_type=confluence
    │
    └── RepoIndexer
            ├── discover repos
            │       GitHub API → all repos in org (auto, no listing needed)
            │       + any explicit repos: from products.yml
            │       - skip_repos list
            │
            └── for each repo:
                    [default branch]
                    SHA = git ls-remote HEAD
                    if SHA == .sync_state["repo"] → skip
                    else → git clone --depth 1
                           walk + chunk → index → .sync_state["repo"] = SHA

                    [each GIT_BRANCHES pattern, e.g. "develop", "release/*"]
                    git ls-remote refs/heads/<pattern>
                    → expands glob → returns all matching branches + SHAs
                    → new branches auto-discovered (no config change needed)
                    for each branch_name:
                        if SHA == .sync_state["repo:branch"] → skip
                        else → git clone --depth 1 --branch <branch_name>
                               walk + chunk (metadata.branch=branch_name) → index
                               .sync_state["repo:branch"] = SHA
```

---

## 6. Security Model

| Concern | Approach |
|---|---|
| **ES credentials** | Never stored. Fetched at runtime via `kubectl get secret`. Exist only in RAM for the duration of one query, then discarded. |
| **Atlassian token** | Stored in `connectors/.env` (Docker volume, not in git). Used only by sync cron. |
| **GitHub PAT** | Stored in `connectors/.env`. Read-only (`repo` scope). |
| **Environment DB** | `customers_db.json` mounted as Docker volume. Contains bastion hostnames and kubectl context names only — no passwords. |
| **Agent API** | API key auth (`auth.py`). Key set via `AGENT_API_KEY` env var. |
| **LLM** | Runs locally inside Docker network. No data sent to external APIs. |
| **Log data** | Never persisted. ES query results exist only in agent RAM for one request lifecycle. |
| **Git** | Repos cloned read-only (`--depth 1`). Clones deleted after indexing. |

---

## 7. Operational Runbook

### Start / stop

```bash
./start.sh              # start everything + auto-sync
./start.sh --rebuild    # rebuild images (after code changes)
./start.sh --stop       # stop (data preserved in volumes)
./start.sh --status     # health + doc counts
./start.sh --logs       # stream sync-cron logs
./start.sh --force-sync # manual full sync now
```

### Targeted sync

```bash
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only jira
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only confluence
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only repos --force
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only customer --customer <id>
```

### Register a new environment

```bash
# POST /api/v1/customers
curl -X POST http://localhost:8084/api/v1/customers \
  -H "X-Api-Key: <key>" \
  -H "Content-Type: application/json" \
  -d '{"id": "acme-prod", "name": "Acme Corp — Prod", "products": ["backend", "platform"]}'

# Add environment with Mode A ES access (no password needed)
curl -X POST http://localhost:8084/api/v1/customers/acme-prod/environments/prod \
  -H "X-Api-Key: <key>" \
  -H "Content-Type: application/json" \
  -d '{
    "k8s_bastion":   "deploy@your-bastion-host",
    "k8s_context":   "acme-prod",
    "k8s_namespace": "production",
    "pod_map": {"api-server": "backend", "worker": "backend"}
  }'
```

### Inspect indexed document counts

```bash
curl "http://localhost:9200/_cat/indices?v&h=index,docs.count,store.size"
```

---

## 8. Scalability Notes

The current design is single-node CPU-only to enable zero-cost self-hosting.

| Bottleneck | Current | Scale path |
|---|---|---|
| LLM throughput | ~4–5s/query p50 including rewrite, 1 concurrent | Add GPU node; or add second Ollama instance + round-robin |
| Embedding + reranking | ~50ms/embed, ~300ms/rerank (30 pairs) | Multi-worker FastAPI; GPU for sub-50ms |
| Retrieval legs | Sequential HTTP calls, ~300ms total | `CompletableFuture.allOf()` → max(slowest leg) ≈ 60ms (Sprint 1.1) |
| Session memory | In-memory dict — single instance only | Redis backend swap, ~30-line change (Sprint 1.3) |
| OpenSearch | Single node, no replicas | 3-node cluster, 1 replica, dedicated master |
| Sync cron | Sequential, 4h cycle | Parallelize per-repo; shorter cycle for Jira only |
| ES log latency | SSH + kubectl exec, ~2-4s | Cache indices list; keep SSH connection warm |

### Known architectural gaps

1. **ACL fields not enforced** — `acl_users`/`acl_roles` stored in OpenSearch but never applied as filters (Sprint 2.1).
2. **Knowledge graph is empty** — KG storage layer exists (`kg_entities`, `kg_relationships`) but `connectors/sync.py` was not updated. Jira remote links wiring is Sprint 2.2.
3. **Retrieval legs sequential** — 6 HTTP calls run in series; ~300ms recoverable (Sprint 1.1).
4. **Sessions in-memory** — warehouse-agent cannot scale horizontally (Sprint 1.3).
5. **No production query log** — eval dataset has 5 sample questions; no feedback loop from real queries (Sprint 4.1).
