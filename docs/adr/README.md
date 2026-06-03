# Architecture Decision Records

ADRs capture significant architectural decisions, their context, and consequences.
Format adapted from [Michael Nygard's template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-language-and-framework.md) | Java 21 + Spring Boot 3 + Maven | Accepted |
| [0002](0002-search-engine.md) | OpenSearch as search engine | Accepted |
| [0003](0003-system-of-record.md) | PostgreSQL as system of record | Accepted |
| [0004](0004-async-indexing-kafka.md) | Async indexing via Kafka | Accepted |
| [0005](0005-blob-storage.md) | S3-compatible blob store (MinIO local) | Accepted |
| [0006](0006-multi-tenancy-isolation.md) | Hybrid tenant isolation (shared + per-enterprise) | Accepted |
| [0007](0007-rate-limiting.md) | Redis sliding-window rate limiting | Accepted |
| [0008](0008-authn-jwt-oidc.md) | OIDC + JWT (RS256) via Keycloak | Accepted |
| [0009](0009-rbac.md) | Role-based access control with tenant scoping | Accepted |
| [0010](0010-api-gateway.md) | Spring Cloud Gateway as edge | Accepted |
| [0011](0011-caching-strategy.md) | Multi-layer caching (Redis + Caffeine) | Accepted |
| [0012](0012-observability-otel.md) | OpenTelemetry for tracing, Prometheus for metrics | Accepted |
| [0013](0013-resilience.md) | Resilience4j for circuit breakers, retries, bulkheads | Accepted |
| [0014](0014-deployment-strategy.md) | Docker Compose for local, Kubernetes for production | Accepted |

## Status values

- **Proposed** — under discussion
- **Accepted** — decided and in effect
- **Deprecated** — no longer relevant but kept for history
- **Superseded by [#]** — replaced by another ADR
