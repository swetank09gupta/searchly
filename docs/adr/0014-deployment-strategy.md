# ADR 0014: Docker Compose for Local, Kubernetes for Production

**Status:** Accepted
**Date:** 2026-06-03

## Context

The assignment encourages docker-compose for the prototype. The production target is a multi-service, horizontally scalable system requiring autoscaling, rolling deploys, secret management, and resource isolation.

## Decision

- **Local / demo / CI integration tests:** **Docker Compose** (`deploy/docker-compose.yml`) — single-command boot of Gateway, Search API, Indexer, OpenSearch, Postgres, Redis, Kafka, MinIO, Keycloak, Jaeger, Prometheus, Grafana.
- **Production:** **Kubernetes** (EKS / GKE / AKS) with Helm charts. Stateful components (OpenSearch, Postgres, Kafka) use their respective operators (OpenSearch Operator, Zalando postgres-operator, Strimzi for Kafka) OR managed services (RDS, MSK, OpenSearch Service).

**Progressive delivery:**
- Blue-green via two Deployments behind a Service, Gateway flips routing weight — instant rollback.
- Canary via Argo Rollouts / Flagger with automated SLO-based promotion.
- DB migrations via Flyway, expand-then-contract pattern for zero-downtime.

**Autoscaling:**
- HPA on Gateway and Search API (CPU + latency p95).
- KEDA on Indexer (Kafka consumer lag).
- Cluster autoscaler for node pools.

## Consequences

**Positive**
- Local-to-prod parity is high: same container images, same configuration patterns.
- Compose accelerates onboarding and integration testing.
- Kubernetes provides the autoscaling, self-healing, and progressive delivery primitives needed for 99.95%.

**Negative**
- Two deployment surfaces to maintain (Compose for dev, Helm for prod) — mitigated by keeping container images and configuration formats identical and treating compose as a non-production tool.
- Kubernetes operational complexity is real; mitigated by using managed control planes and managed stateful services where possible.

**What the prototype includes**
- `deploy/docker-compose.yml` for local
- Helm chart skeleton and K8s manifests are **out of scope for the 3–4h prototype** but the design is K8s-ready (12-factor, stateless services, externalized config).
