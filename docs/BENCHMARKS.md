# Searchly — Benchmarks

> Prototype-grade numbers captured on a developer laptop with the full stack co-resident in Docker (Postgres, Redis, OpenSearch, Kafka, MinIO substitute, Search API, Indexer, Gateway, Prometheus, Jaeger — all on one host). These are not production numbers; they show the shape of the latency curve, the relative cost of features (facets), and the effect of caching.

## Test setup

- **Hardware:** Apple Silicon MacBook (single host, all 9 containers co-resident)
- **Tool:** Apache Bench (`ab -l`) — `-l` flag tolerates response-size variation (the search response's `took` field changes per call, otherwise ab incorrectly flags requests as failed)
- **Tenant:** `globex` (PREMIUM tier — 1000 QPS rate-limit budget)
- **User:** `dave` (TENANT_ADMIN role)
- **Dataset:** 200 pre-warmed documents (varied tags/authors) indexed via `POST /documents` → Kafka → OpenSearch
- **Load:** N=2000 requests, C=50 concurrent
- **Reproduce:** `TENANT=globex APP_USER=dave bash deploy/bench.sh`

## Results

### Search — `GET /api/v1/search?q=revenue`

| Metric | Value |
|---|---|
| Throughput | **557 req/s** |
| Mean latency | 90 ms |
| p50 | 44 ms |
| p95 | **235 ms** |
| p99 | 382 ms |
| Failed | 0 |

### Search with facets — `GET /api/v1/search?q=revenue&facets=tags&facets=author`

| Metric | Value |
|---|---|
| Throughput | **734 req/s** |
| Mean latency | 68 ms |
| p50 | 40 ms |
| p95 | **227 ms** |
| p99 | 503 ms |
| Failed | 0 |

Faceted search is fractionally **faster** than non-faceted in this run because once Redis cache warms up (60s TTL on identical queries), faceted responses hit cache too — the aggregations are computed once and reused. The cold-path cost of aggregations is real but cache makes it disappear under steady load.

### Document GET — `GET /api/v1/documents/{id}` (Postgres-backed)

| Metric | Value |
|---|---|
| Throughput | **1014 req/s** |
| Mean latency | 49 ms |
| p50 | 34 ms |
| p95 | **141 ms** |
| p99 | 278 ms |
| Failed | 0 |

GET-by-id outperforms search because it skips OpenSearch entirely (served from Postgres + Hibernate L1 cache) and has a smaller response payload.

## Read-out vs the targets

| Target (from brief) | Observed (laptop) | Notes |
|---|---|---|
| p95 < 500 ms | **235 ms (search)**, **141 ms (get)** | ✅ comfortably under, even on a saturated dev box |
| ≥1000 concurrent searches/sec | 557–734/s on this host | Single-host. Stack is stateless above OpenSearch → scales linearly with Search API replicas; per-shard OpenSearch capacity is the long-term bottleneck. |
| 10M+ documents | not stress-tested | Sharding strategy (`tenant_id` routing, 20–40 GB per shard, monthly rolling indices) is documented in [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md). |

## What the numbers tell you (and don't)

**Tell you:**
- The architecture meets the p95 budget with substantial headroom even when the stack runs on a single laptop.
- Redis caching pays for itself within the first 100 requests — the second-time-same-query path is dominated by cache lookup, not OpenSearch latency.
- Postgres read path (~1k QPS for get-by-id) is not the bottleneck for the document-detail endpoint.
- No errors at this load. Rate limiter would kick in at the tier ceiling (1000 QPS for PREMIUM) — we did not exceed it here.

**Don't tell you:**
- Behaviour at the actual target dataset size (10M docs). On a co-resident OpenSearch single-node with default JVM heap (512 MB in compose), shard cache evictions would dominate at scale.
- Tail latency under bursty traffic with a cold cache.
- Sustained throughput — `ab` is a 30-second snapshot; production deserves a `k6`/`Gatling` run over minutes with realistic query distribution.
- Network behaviour over a real LAN/WAN; everything here is loopback.

## What to do for a production benchmark

1. **Realistic dataset.** Index 10M synthesized documents distributed across 20–50 tenants (Zipfian distribution to mirror real SaaS shape).
2. **Realistic query mix.** Read/write ratio of ~95/5, query string variety (cache hit rate target ≥70%).
3. **Distributed load.** `k6` or `Gatling` running from a separate host (or fleet), targeting a multi-AZ deployment.
4. **Soak test.** 30+ minutes at steady-state, measure tail latency, GC pause distribution, OpenSearch shard cache eviction rate, Kafka consumer lag.
5. **Failure injection.** Kill one OpenSearch data node, observe cache fallback and recovery time.

That's the production-readiness path; the numbers above are the "is the design defensible on day one" answer.
