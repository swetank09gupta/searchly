# Production Readiness Analysis

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
| **Dead-letter queue** | `indexing.dlq` topic defined; **Indexer DLQ routing not yet implemented** — poison messages crash the consumer (known gap, Sprint 3.1); replay tooling designed |
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
- Document-level ACL fields in OpenSearch (`acl_users`, `acl_roles`) for fine-grained sharing — **fields exist but are not yet enforced at query time; this is a known gap (Sprint 2.1)**.

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

## 8. Known Architectural Gaps (post P0–P3 review)

These are architectural weaknesses that are understood and scheduled, ordered by severity.

| # | Gap | Severity | Sprint | Notes |
|---|---|---|---|---|
| 1 | `acl_users`/`acl_roles` stored in OpenSearch but never enforced at query time | **Critical** | 2.1 | Any authenticated user within a tenant sees all documents regardless of intended sharing |
| 2 | Knowledge graph extraction not wired — graph is empty | High | 2.2–2.3 | KG tables + API exist; `connectors/sync.py` never updated; start with Jira remote links (authoritative) |
| 3 | 6 retrieval legs run as sequential HTTP calls | High | 1.1 | ~300ms recoverable; fix: `CompletableFuture.allOf()` in `RagService.answer()` |
| 4 | Recency boost missing from chunk BM25 | Medium | 1.2 | `SearchService` applies Gauss decay to `documents-*` BM25; `RagService.bm25Internal()` uses plain `match` |
| 5 | Sessions in-memory — blocks horizontal scaling of intelligence-agent | Medium | 1.3 | `SessionStore` is a dict; swap to Redis hashes + TTL (~30-line change) |
| 6 | No embedding version migration path | Medium | Backlog | Model upgrades require full re-embed; need versioned index aliases strategy |
| 7 | No production query log → eval feedback loop | Medium | 4.1 | Eval dataset has 5 questions; no loop from real user queries |
| 8 | `Kafka max.poll.records` unbounded | Medium | 3.2 | Default 500; a burst of large documents can OOM the indexer; set to 10 + semaphore |
| 9 | Redis failure fails closed (429 all requests) | Low | 3.3 | Rate limiter has no degradation path; catch `RedisException`, allow through |
| 10 | Ollama is synchronous, no partial degradation | Low | Backlog | No async queue; tail latency (120s timeout) blocks the request thread |

---

## 9. Cost Optimization (bonus)

- Tiered OpenSearch storage (hot SSD → warm HDD → cold S3-backed snapshot).
- Spot/preemptible nodes for indexer (idempotent, restartable).
- Right-size shards (over-sharding wastes heap).
- Per-tenant cost reporting → drive tier upsells; surface heavy free-tier users.
- Reserved instances for baseline; autoscale for peaks.
- Compress logs/metrics; sample traces; retain only what's queried.
