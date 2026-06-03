# Searchly — Setup Guide

Step-by-step instructions to build, run, and exercise Searchly locally.

## Prerequisites

| Tool | Version | Why |
|---|---|---|
| Docker | 24+ | Runs the full stack |
| Docker Compose | v2+ | Orchestrates services (bundled with Docker Desktop) |
| JDK | 21 (Temurin recommended) | Only needed if you build the jars locally; Docker build does this for you |
| Maven | 3.9+ | Only needed if you build locally |
| `jq` | any | Pretty-prints JSON in sample scripts |
| `curl` | any | API calls |
| Free RAM | ~4 GB | OpenSearch + Kafka + Postgres + Redis + 3 Java apps |
| Free ports | 5432, 6379, 8080, 8081, 9090, 9092, 9200, 16686 | Host-mapped by compose |

Verify Docker is running:

```bash
docker info >/dev/null && echo "Docker OK"
```

## Quickstart (one command)

```bash
git clone https://github.com/swetankgupta/searchly.git
cd searchly/deploy
docker compose up -d --build
```

First boot takes ~2–3 minutes (image pulls + first-time Maven builds inside the search-api/indexer/gateway Dockerfiles). Subsequent boots are ~30 seconds.

### What boots up

| Service | Port | Purpose |
|---|---|---|
| `gateway` | 8080 | Spring Cloud Gateway — JWT, rate limit, routing |
| `search-api` | 8081 | REST API, OpenSearch reads, Postgres writes, Kafka producer, **auto-seeds data on first boot** |
| `indexer` | 8082 | Kafka consumer → OpenSearch writer |
| `postgres` | 5432 | System of record (tenants, users, document metadata) |
| `redis` | 6379 | Query cache + sliding-window rate limit |
| `opensearch` | 9200 | Search index |
| `kafka` | 9092 | Async indexing pipeline |
| `prometheus` | 9090 | Metrics scraper |
| `jaeger` | 16686 | Distributed traces UI |

### Wait for the stack to be healthy

```bash
until curl -sf http://localhost:8081/actuator/health >/dev/null; do sleep 3; done && echo "READY"
```

## Verify it's working

### 1. Health check

```bash
curl -s http://localhost:8080/actuator/health | jq .
# expect: {"status":"UP"}

curl -s http://localhost:8081/actuator/health | jq .
# expect db + redis components UP
```

### 2. Hit pre-seeded data (no manual indexing needed)

```bash
curl -s "http://localhost:8080/api/v1/search?q=revenue&tenant=acme" \
  -H "X-Tenant-Id: acme" -H "X-User-Id: alice" | jq '{total, titles:[.hits[].title]}'
# expect: {"total":1,"titles":["Q4 2025 Revenue Report"]}
```

### 3. Run the full sample script

```bash
cd ..   # back to repo root
bash deploy/curl-samples.sh
```

Walks through: health → pre-seeded search → faceted search → index a new doc → search for it → get by id → delete.

### 4. Postman

Import `deploy/postman/searchly.postman_collection.json`. Variables `base`, `tenant`, `user`, `adminToken` are pre-set.

### 5. Benchmark

```bash
bash deploy/bench.sh
# or against the PREMIUM tier with more headroom:
TENANT=globex APP_USER=dave bash deploy/bench.sh
```

Numbers from a recent run are in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Seeded data

On the very first boot, the search-api auto-creates:

- **4 tenants** (Flyway migration V1):
  - `acme` (STANDARD, 100 QPS), `globex` (PREMIUM, 1000 QPS), `initech` (FREE, 10 QPS), `umbrella` (ENTERPRISE)
- **13 users** (Flyway migration V2): one admin/editor/viewer per tenant + a service account
- **13 sample documents** (DataSeeder): pushed through the real Kafka → indexer → OpenSearch path

Re-seeding is idempotent: if any document already exists, seeding is skipped. Disable in production with `SEARCHLY_SEED_DATA=false`.

See [README §What you get on first boot](README.md#what-you-get-on-first-boot) for the full user table.

## Authentication in dev mode

Two headers:

| Header | Required | Purpose |
|---|---|---|
| `X-Tenant-Id` | yes (or `?tenant=` query param) | Which tenant the request is for |
| `X-User-Id` | recommended | Look up the user in DB; roles come from the DB row; enforces user belongs to claimed tenant |
| `X-User-Roles` | only if no `X-User-Id` | Override roles for ad-hoc testing; defaults to `VIEWER` |

Production replaces these with an OIDC JWT (RS256) — same `TenantSecurityFilter` extension point. See [ADR 0008](docs/adr/0008-authn-jwt-oidc.md).

## Adding your own tenants and users (no SQL)

```bash
ADMIN='-H X-Admin-Token:dev-admin-token -H Content-Type:application/json'

# Create tenant
curl -X POST http://localhost:8080/api/v1/admin/tenants $ADMIN \
  -d '{"id":"mycorp","name":"My Corp","tier":"PREMIUM"}'

# Create user
curl -X POST http://localhost:8080/api/v1/admin/tenants/mycorp/users $ADMIN \
  -d '{"id":"tester","displayName":"Test","email":"t@mycorp.test","roles":"EDITOR,VIEWER"}'

# Use it
curl -X POST http://localhost:8080/api/v1/documents \
  -H "X-Tenant-Id: mycorp" -H "X-User-Id: tester" -H "Content-Type: application/json" \
  -d '{"title":"hi","content":"new tenant"}'
```

Default admin token is `dev-admin-token`. Override via `SEARCHLY_ADMIN_TOKEN` env var (set on the `search-api` service in docker-compose, or in your K8s secret).

## Building from source (without Docker)

Only needed if you want to iterate on code without rebuilding container images.

```bash
mvn -DskipTests package
# starts each service against the running infra (postgres/redis/opensearch/kafka from compose):
java -jar search-api/target/search-api.jar
java -jar indexer/target/indexer.jar
java -jar gateway/target/gateway.jar
```

You'll need the supporting services up. Easiest: start the whole stack with compose, then `docker compose stop search-api indexer gateway`, and run the Java apps locally — they default to `localhost` for all backends.

## Common operations

### Reset everything

```bash
cd deploy
docker compose down -v   # -v drops volumes (Postgres, Kafka, OpenSearch data)
docker compose up -d --build
```

### Tail logs

```bash
docker compose logs -f search-api
docker compose logs -f indexer
```

### Inspect Postgres directly

```bash
docker compose exec postgres psql -U searchly -c "SELECT id, tier FROM tenants;"
docker compose exec postgres psql -U searchly -c "SELECT id, tenant_id, roles FROM users;"
docker compose exec postgres psql -U searchly -c "SELECT id, tenant_id, title, status FROM documents;"
```

### Inspect OpenSearch directly

```bash
curl -s "http://localhost:9200/_cat/indices?v"
curl -s "http://localhost:9200/documents-shared/_search?pretty" | head -50
```

### Inspect Kafka topics

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic indexing.shared --from-beginning --max-messages 5
```

### Open the tracing UI

[http://localhost:16686](http://localhost:16686) — Jaeger UI. (Tracing is wired in the architecture; the prototype doesn't ship OTel exporters by default — that's a docked TODO.)

### Open Prometheus

[http://localhost:9090](http://localhost:9090) — query `up`, `http_server_requests_seconds_count`, `kafka_consumer_lag_records`, etc.

## Troubleshooting

### `docker compose up` says "manifest for bitnami/kafka:3.7 not found"

You're on an old `docker-compose.yml`. The current file uses `apache/kafka:3.7.1`. Pull latest from the repo.

### Search returns 0 hits right after indexing

Indexing is asynchronous (Kafka → indexer → OpenSearch). Typical lag is ~1 second. Sleep and retry. Verify with:
```bash
docker compose logs indexer --tail 20 | grep "Indexed doc"
```

### 401 Unauthorized: "Missing X-Tenant-Id"

Add the header or `?tenant=...` query param. Admin endpoints (`/api/v1/admin/*`) use `X-Admin-Token` instead.

### 403 Forbidden: "User not authorized for this tenant"

You sent `X-User-Id` for a user that doesn't belong to the `X-Tenant-Id` in the request. Use a matching pair (e.g., `alice` belongs to `acme`).

### 429 Too Many Requests

Per-tenant rate limit kicked in. Tiers and budgets:
- FREE: 10 QPS
- STANDARD: 100 QPS
- PREMIUM: 1000 QPS
- ENTERPRISE: 10000 QPS

Switch to a higher-tier tenant (e.g., `globex`) or wait one second.

### `search-api` container restarts repeatedly

Check Postgres/Redis are healthy first:
```bash
docker compose ps
docker compose logs postgres redis | tail -30
```

### Builds taking forever

Docker layer cache is fine — the Maven `.m2` dir is **not** cached between builds in the multi-stage Dockerfile (intentional, keeps images reproducible). For faster local iteration, build with Maven outside Docker:
```bash
mvn -DskipTests package
docker compose up -d --no-build   # uses existing images; only restarts containers
```

(You'd need to update the Dockerfiles to COPY a pre-built jar instead of running mvn for true incremental builds; left as a follow-up.)

### Port conflict

Either stop the conflicting local service or change the host-side port mapping in `deploy/docker-compose.yml`.

### Clean slate

```bash
cd deploy
docker compose down -v
docker system prune -f                 # remove dangling images/volumes
docker compose up -d --build           # fresh start, re-seeds data
```

## Where things live

```
searchly/
├── README.md                        ← project overview + doc index
├── SETUP.md                         ← THIS FILE — run-it-yourself guide
├── LICENSE                          ← Apache 2.0
├── docs/
│   ├── SUBMISSION.md                ← single-file submission (per assessment brief)
│   ├── ARCHITECTURE.md
│   ├── PRODUCTION_READINESS.md
│   ├── EXPERIENCE.md
│   ├── DECISIONS.md
│   ├── BENCHMARKS.md
│   ├── AI_USAGE.md
│   └── adr/                         ← 14 ADRs
├── common/                          ← shared DTOs, TenantContext, Tier
├── search-api/                      ← REST API + DataSeeder + Admin API
├── indexer/                         ← Kafka consumer → OpenSearch
├── gateway/                         ← Spring Cloud Gateway
├── deploy/
│   ├── docker-compose.yml
│   ├── Dockerfile.{search-api,indexer,gateway}
│   ├── prometheus.yml
│   ├── curl-samples.sh              ← end-to-end demo
│   ├── bench.sh                     ← ab-based benchmarks
│   └── postman/searchly.postman_collection.json
└── pom.xml                          ← Maven parent
```

## Next steps after the prototype runs

1. Open [`docs/SUBMISSION.md`](docs/SUBMISSION.md) for the full architecture + production readiness narrative.
2. Skim [`docs/adr/`](docs/adr/) — 14 short Architecture Decision Records cover the *why* behind every major choice.
3. Run [`deploy/bench.sh`](deploy/bench.sh) and compare to [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).
4. Explore the admin API to spin up your own tenant; see ["Adding your own tenants and users"](#adding-your-own-tenants-and-users-no-sql) above.
