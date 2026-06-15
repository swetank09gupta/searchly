# Decisions & Assumptions

Summary of choices and assumptions behind Searchly. **For full context, alternatives, and consequences, see the [ADRs](adr/README.md).**

## Assumptions

1. **Document model:** mostly text-bearing files (PDF, DOCX, HTML, plain text) up to ~50 MB; binary-heavy media (video, images) out of scope.
2. **Tenancy:** hundreds of small tenants + a handful of large/enterprise tenants — drives the hybrid isolation strategy.
3. **Read-heavy workload:** search QPS ≫ index QPS; cache + read replicas pay off.
4. **Eventual consistency on search is acceptable** (typical lag < 1s). Get-by-id is strongly consistent (served from Postgres metadata).
5. **Tenants do not run custom code** — no per-tenant query DSL or stored procedures; reduces attack surface.
6. **Identity provider exists** (Keycloak in this prototype, Okta/Auth0/Cognito in customer deployments).
7. **Network:** services run in a private VPC; only the Gateway is internet-exposed.
8. **Compliance scope:** GDPR (right-to-erasure), SOC 2 controls; HIPAA-grade is out of scope for the prototype but the patterns (encryption, audit, RBAC) support it.

## Key Trade-offs

| Decision | Picked | Rejected | Why |
|---|---|---|---|
| Search engine | OpenSearch | Postgres FTS | Postgres FTS fine to ~1M docs/tenant but loses at 10M+; OpenSearch wins on fuzzy/highlight/facets and horizontal scale |
| License-clean fork | OpenSearch | Elasticsearch | ES SSPL license risk for SaaS redistribution; OpenSearch is Apache-2.0 |
| Tenant isolation | Hybrid (shared + per-ENTERPRISE indices) | Pure shared / pure per-tenant | Shared scales to many tenants; per-tenant isolates noisy enterprises; hybrid balances both |
| Rate limit | Redis sliding window (sorted set) | Fixed window / token bucket | True rolling window avoids fixed-window boundary bursts; sorted set gives exact counts |
| Queue | Kafka | RabbitMQ / SQS | Replayable log enables OpenSearch rebuild from Kafka; partitioning by tenant; topic-per-tier isolation |
| Blob storage | MinIO/S3, not in Kafka or index | Inline in messages / OpenSearch `_source` | Large binaries bloat index heap & Kafka; S3 is cheap and durable |
| AuthN | OIDC + JWT (RS256) | Sessions, opaque tokens | Stateless, scalable; asymmetric verification at gateway without IdP roundtrip |
| RBAC enforcement | Spring `@PreAuthorize` + `TenantSecurityFilter` | Custom interceptor | Standard, declarative; filter handles tenant identity uniformly |
| Cache key | Includes `tenant_id` + role | Just query hash | Prevents cross-role result leak |
| Consistency | Strong on Postgres, eventual on OpenSearch | Synchronous dual-write | Sync writes couple availability and double-write inconsistency is worse; async w/ Kafka replay is more robust |
| Build tool | Maven (user choice) | Gradle | Explicit user preference |
| Container orchestration | Docker Compose (local), K8s (prod) | Plain JVM/systemd | Brief encourages compose; K8s is standard prod target |
| API security | JWT validated at Gateway AND service | Only at Gateway | Defense in depth — a misconfigured Gateway shouldn't bypass auth |
| Embedding model | `BAAI/bge-small-en-v1.5` (asymmetric, 384-dim) | `all-MiniLM-L6-v2` (symmetric) | BGE asymmetric encoding designed for retrieval; same 384-dim avoids index remapping |
| Query expansion | Dual-query via Ollama rewrite | Single query / HyDE | Rewrite adds recall without changing query semantics; HyDE risks hallucinating the hypothetical doc |
| Retrieval precision | Cross-encoder reranker (`bge-reranker-base`, 30→6) | Expand k in first-stage retrieval | Bi-encoder recall is cheap; cross-encoder precision is expensive — stage the cost |
| Source authority | RRF weight × authority multiplier per source tag | Boost in LLM prompt | Retrieval quality is a retrieval problem; prompting the LLM about source rank conflates retrieval and generation |
| Knowledge graph store | PostgreSQL flat tables + JSONB | Neo4j / Neptune | Queries never exceed depth 3; dedicated graph DB doubles operational complexity for shallow traversal |
| Retrieval observability | `RetrievalTrace` per chunk in `SearchResponse` | Log-side only | Eval tooling and debugging need the trace co-located with the response, not reconstructed via trace_id join |
| Session memory | Rolling summary + structured `session_memory` dict | Full conversation history | Full history OOMs Ollama context window at ~8 turns; structured dict preserves entities across compressions |
| Eval scheduling | APScheduler nightly (process-internal) | External cron + CI | Keeps dependency count low; nightly is sufficient; move to CI if warehouse-agent scales to multiple replicas |

## Out of Scope for Prototype (documented in PRODUCTION_READINESS.md)

- Helm charts, K8s manifests beyond design discussion
- Real WAF / DDoS protection
- ClamAV virus scanning
- Cross-region replication
- Full IdP integration with external providers
- Comprehensive load test suite (basic benchmark only)
- UI / Admin console
