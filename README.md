# Searchly — Organisation Intelligence

> Ask anything about your engineering organisation — architecture decisions, tickets, live logs,
> source code, runbooks — in natural language. Self-hosted, zero data egress.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen)](https://spring.io/projects/spring-boot)

---

## What is this?

An **organisation intelligence system** that answers questions from your entire engineering
knowledge base. It combines:

- **Hybrid RAG** (BM25 + semantic kNN) over your organisation's knowledge — Jira tickets,
  Confluence docs, ADRs, GitHub repos
- **Live log access** — real-time queries to Elasticsearch clusters via bastion-kubectl
  (Mode A: zero credential storage, password fetched at runtime from k8s Secret)
- **Self-hosted LLM** — Ollama running locally in Docker, no external API calls, no data egress
- **Customer / environment registry** — track deployments across dev → staging → prod

### Example questions it can answer

| Question | Sources used |
|---|---|
| "Why is service X failing in production?" | Live ES logs + tickets |
| "What does ticket ENG-2466 change?" | Jira ticket + comments |
| "How does our caching strategy work?" | Confluence ADR + source code |
| "What alternatives were considered for the queue design?" | ADRs + design docs |
| "What version is deployed in the staging environment?" | k8s state (via bastion) |

---

## Quick start

```bash
git clone <this-repo>
cd searchly

# 1. Set your credentials in connectors/.env
cp connectors/.env.example connectors/.env
nano connectors/.env          # fill in JIRA_URL, JIRA_EMAIL, JIRA_TOKEN, GIT_TOKEN

# 2. Configure what to index
nano connectors/products.yml  # set github_org, add your repos
nano connectors/customers.yml # add your environments (optional for live log queries)

# 3. Start everything
./start.sh

# 4. Open the chat UI
open http://localhost:8084

# 5. Watch the first sync (takes 10-60 min depending on repo/ticket volume)
./start.sh --logs
```

---

## Architecture overview

```
Browser / curl
    │
    ▼
Intelligence Agent (FastAPI :8084)
    ├── RAG: query rewrite → embed (BGE) → 6-leg BM25+kNN → RRF → reranker → LLM
    │         ↑ BAAI/bge-small-en-v1.5 (384-dim, asymmetric query prefix)
    │         ↑ BAAI/bge-reranker-base (cross-encoder, top-30 → top-6)
    ├── Live: SSH bastion → kubectl exec → Elasticsearch query
    │         ↑ ES password fetched from k8s Secret at runtime, never stored
    └── Knowledge Graph (Postgres) — Jira↔PR↔commit↔service relationships

Sync Cron (every 4h):
    Jira projects       → OpenSearch  [+ kg_entities / kg_relationships via remote links]
    Confluence spaces   → OpenSearch  [recursive child pages]
    GitHub repos        → OpenSearch  [SHA-incremental, only changed repos]
    ADRs / design docs  → OpenSearch  [kept whole, not split]
```

Full design: [docs/INTELLIGENCE_ARCHITECTURE.md](docs/INTELLIGENCE_ARCHITECTURE.md)

---

## Commands

```bash
./start.sh                  # start everything
./start.sh --rebuild        # rebuild images after code changes
./start.sh --stop           # stop all containers (data preserved)
./start.sh --status         # health check + indexed doc counts
./start.sh --logs           # stream sync-cron logs
./start.sh --force-sync     # trigger full sync right now

# Targeted sync
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only jira
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only confluence
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only repos --force
docker compose -f deploy/docker-compose.yml run --rm connectors python sync.py --only customer --customer <id>
```

---

## Configuration at a glance

| File | What to configure |
|---|---|
| `connectors/.env` | Jira/Confluence/GitHub credentials; which Jira projects and Confluence spaces to index |
| `connectors/products.yml` | GitHub org name; which repos to clone and index; pod-name → product mapping |
| `connectors/customers.yml` | Customer environments: bastion SSH, kubectl context, namespace, pod map |
| `.env` (root) | LLM model, SSH key path, sync intervals |

---

## Documentation

| Document | Purpose |
|---|---|
| **[Intelligence Architecture](docs/INTELLIGENCE_ARCHITECTURE.md)** | Full system design: RAG pipeline, ES log access, sync cron, customer registry |
| **[Setup Guide](docs/SETUP_INTELLIGENCE.md)** | Prerequisites, start/stop, verification, adding customers, troubleshooting |
| [ADRs 0015–0023](docs/adr/README.md) | Architecture Decision Records (platform + intelligence + retrieval quality) |
| [Search Platform Architecture](docs/ARCHITECTURE.md) | Underlying platform: BM25+kNN, OpenSearch, Kafka indexing, multi-tenancy |
| [Production Readiness](docs/PRODUCTION_READINESS.md) | Scalability, resilience, security, observability, known gaps |
| [Decisions & Trade-offs](docs/DECISIONS.md) | Key decisions and assumptions summary |
| [Benchmarks](docs/BENCHMARKS.md) | Benchmark numbers and caveats |

---

## Underlying Platform

Built on the **Searchly** multi-tenant document search platform — a reference implementation of
enterprise-grade document search demonstrating multi-tenancy, fault tolerance, horizontal
scalability, and security patterns suitable for SaaS-scale workloads.

Stack: **Java 21** · **Spring Boot 3** · **OpenSearch** · **Kafka** · **PostgreSQL** · **Redis**

## License

Apache License 2.0 — see [LICENSE](LICENSE).
