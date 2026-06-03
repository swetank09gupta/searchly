# Searchly

> Distributed, multi-tenant document search service — find the needle in 10M+ documents in under 500ms.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen)](https://spring.io/projects/spring-boot)

Searchly is a reference implementation of an enterprise-grade document search service built as a technical assessment. It demonstrates multi-tenancy, fault tolerance, horizontal scalability, and security patterns suitable for SaaS-scale workloads.

---

## Targets

| Concern | Target |
|---|---|
| Documents | 10M+ across tenants |
| Latency (p95) | < 500ms |
| Throughput | 1000+ concurrent searches/sec |
| Tenancy | Multi-tenant with strict isolation |
| Availability (prod) | 99.95% |

## Stack

- **Java 21**, **Maven** (multi-module), **Spring Boot 3**
- **Spring Cloud Gateway** — edge routing, authN, rate limit
- **Spring Security 6 + Keycloak** (OIDC/JWT RS256)
- **OpenSearch** — full-text search, relevance, fuzzy, highlighting, facets
- **PostgreSQL** — system-of-record for metadata
- **Redis** — query cache + sliding-window rate limit
- **Apache Kafka** — async indexing pipeline (tiered topics)
- **MinIO** (S3-compatible) — raw document blob storage
- **Apache Tika** — text extraction (PDF, DOCX, etc.)
- **Resilience4j** — circuit breakers, retries, bulkheads
- **OpenTelemetry → Jaeger** — distributed tracing
- **Prometheus + Grafana** — metrics
- **Testcontainers + JUnit 5** — integration tests
- **Docker Compose** — local orchestration (K8s for prod)

## Repository Layout

```
searchly/
├── docs/
│   ├── ARCHITECTURE.md          # System design, diagrams, data flow
│   ├── PRODUCTION_READINESS.md  # Scalability, resilience, security, ops
│   ├── EXPERIENCE.md            # Engineering experience showcase
│   ├── DECISIONS.md             # Assumptions and trade-offs
│   └── AI_USAGE.md              # Note on AI tool usage
├── gateway/                     # Spring Cloud Gateway module
├── search-api/                  # REST + query service
├── indexer/                     # Kafka consumer + Tika + OpenSearch writes
├── common/                      # Shared DTOs, tenant context, security
├── deploy/
│   ├── docker-compose.yml
│   └── postman/                 # Sample API requests
├── pom.xml
├── LICENSE
└── README.md
```

## Quickstart

> Full setup, troubleshooting, and operator commands are in **[SETUP.md](SETUP.md)**.

```bash
git clone https://github.com/swetankgupta/searchly.git
cd searchly/deploy
docker compose up -d --build
# wait ~2-3 min on first boot; ~30s after
until curl -sf http://localhost:8081/actuator/health >/dev/null; do sleep 3; done
bash ../deploy/curl-samples.sh
```

That's it — the stack auto-seeds tenants, users, and sample documents on first boot, so you can search immediately. See [SETUP.md](SETUP.md) for prerequisites, verification steps, admin operations, and troubleshooting.

### What you get on first boot

When you run `docker compose up`, the search-api auto-seeds (idempotent):
- **4 tenants** (one per tier) — Flyway migration `V1`
- **13 users** including a service account — Flyway migration `V2`
- **13 sample documents** distributed across tenants — `DataSeeder` runs through the full Kafka → indexer → OpenSearch path on startup

So immediately after boot you can search without indexing anything yourself — try `?q=revenue&tenant=acme`, `?q=audit&tenant=umbrella&facets=tags`, etc. Set `SEARCHLY_SEED_DATA=false` to disable in production. Seeding is skipped if any document already exists.

### Seeded tenants and users

Flyway seeds 4 tenants (one per tier) and 12 users + 1 service account on first boot.

**Tenants:**

| Tenant | Tier | QPS limit | Daily quota |
|---|---|---|---|
| `acme` | STANDARD | 100 | 50,000 |
| `globex` | PREMIUM | 1,000 | 1,000,000 |
| `initech` | FREE | 10 | 1,000 |
| `umbrella` | ENTERPRISE | 10,000 | unlimited |

**Users** (use `X-User-Id` header to authenticate; roles resolved from DB):

| User | Tenant | Roles |
|---|---|---|
| `alice` | acme | TENANT_ADMIN, EDITOR, VIEWER |
| `bob` | acme | EDITOR, VIEWER |
| `carol` | acme | VIEWER |
| `dave` | globex | TENANT_ADMIN, EDITOR, VIEWER |
| `eve` | globex | EDITOR, VIEWER |
| `frank` | globex | VIEWER |
| `grace` | initech | TENANT_ADMIN, EDITOR, VIEWER |
| `heidi` | initech | EDITOR, VIEWER |
| `ivan` | initech | VIEWER |
| `judy` | umbrella | TENANT_ADMIN, EDITOR, VIEWER |
| `mallory` | umbrella | EDITOR, VIEWER |
| `nia` | umbrella | VIEWER |
| `svc-indexer` | acme | SERVICE, EDITOR |

`TenantSecurityFilter` enforces: (a) the tenant exists, (b) if `X-User-Id` is supplied, the user must belong to the claimed tenant (anti-IDOR), and (c) roles come from the DB row (the header `X-User-Roles` is only consulted when no user id is sent — useful for quick exploratory calls).

### Adding your own tenants and users (self-service admin API)

If you want to test with a tenant or user outside the seeded set, use the admin API instead of editing SQL. Auth is by token (`X-Admin-Token` header, default `dev-admin-token` — override with `SEARCHLY_ADMIN_TOKEN` env var in production). All admin endpoints bypass tenant-scoped auth and rate-limiting.

```bash
ADMIN='-H X-Admin-Token:dev-admin-token -H Content-Type:application/json'

# Create a tenant
curl -X POST http://localhost:8080/api/v1/admin/tenants $ADMIN \
  -d '{"id":"mycorp","name":"My Corp","tier":"PREMIUM"}'

# Create a user inside it
curl -X POST http://localhost:8080/api/v1/admin/tenants/mycorp/users $ADMIN \
  -d '{"id":"tester","displayName":"Test User","email":"tester@mycorp.test","roles":"TENANT_ADMIN,EDITOR,VIEWER"}'

# List tenants / users
curl -H "X-Admin-Token: dev-admin-token" http://localhost:8080/api/v1/admin/tenants
curl -H "X-Admin-Token: dev-admin-token" http://localhost:8080/api/v1/admin/tenants/mycorp/users

# Use it
curl -X POST http://localhost:8080/api/v1/documents \
  -H "X-Tenant-Id: mycorp" -H "X-User-Id: tester" -H "Content-Type: application/json" \
  -d '{"title":"hello","content":"first doc"}'
```

Admin endpoints:
| Method | Path |
|---|---|
| `POST` | `/api/v1/admin/tenants` |
| `GET`  | `/api/v1/admin/tenants` |
| `POST` | `/api/v1/admin/tenants/{tenantId}/users` |
| `GET`  | `/api/v1/admin/tenants/{tenantId}/users` |

> Production note: the `X-Admin-Token` scheme is a prototype convenience. In production this controller should be guarded by a `SUPER_ADMIN` role on a JWT (see [ADR 0009](docs/adr/0009-rbac.md)) or moved entirely to a separate admin service on a private network.

### Sample requests

> **Dev-mode auth:** the prototype uses headers for tenant + user resolution. **Production replaces this with an OIDC JWT (RS256)** — same `TenantSecurityFilter` extension point. See [ADR 0008](docs/adr/0008-authn-jwt-oidc.md).

```bash
H='-H X-Tenant-Id:acme -H X-User-Id:alice -H Content-Type:application/json'

# Index
curl -X POST http://localhost:8080/api/v1/documents $H \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"title":"Q4 report","content":"Revenue grew 23% YoY","metadata":{"tags":["finance"]}}'

# Search
curl "http://localhost:8080/api/v1/search?q=revenue&tenant=acme&fuzzy=true&highlight=true" $H

# Retrieve / Delete
curl http://localhost:8080/api/v1/documents/{id} $H
curl -X DELETE http://localhost:8080/api/v1/documents/{id} $H
```

A Postman collection is provided in [deploy/postman/](deploy/postman/) and a runnable shell script in [deploy/curl-samples.sh](deploy/curl-samples.sh).

## Documentation

> [`docs/SUBMISSION.md`](docs/SUBMISSION.md) is a single-file document containing Architecture + Production Readiness + Experience + AI Usage (per the assignment brief's "single PDF or Markdown file" requirement). All the other docs below are the canonical modular versions.

| Document | Purpose |
|---|---|
| **[Submission (single file)](docs/SUBMISSION.md)** | **All four required sections in one document** |
| [SETUP.md](SETUP.md) | Run-it-yourself guide: prerequisites, quickstart, verification, troubleshooting |
| [Architecture](docs/ARCHITECTURE.md) | System design, diagrams, data flow, API contracts |
| [Production Readiness](docs/PRODUCTION_READINESS.md) | Scalability, resilience, security, observability, SLA |
| [Decisions](docs/DECISIONS.md) | Assumptions and key trade-offs (summary) |
| [ADRs](docs/adr/README.md) | Architecture Decision Records (one per decision, full context) |
| [Benchmarks](docs/BENCHMARKS.md) | Laptop-scale benchmark numbers and caveats |
| [Experience](docs/EXPERIENCE.md) | Author's relevant experience |
| [AI Usage](docs/AI_USAGE.md) | How AI tools were used in this assessment |

## Multi-tenancy at a glance

- **Identity:** JWT (RS256) carries `tenant_id`, `tier`, and roles. Every request validates JWT-tenant ≡ header-tenant.
- **RBAC:** `TENANT_ADMIN`, `EDITOR`, `VIEWER`, `SERVICE` enforced via `@PreAuthorize`.
- **Isolation:** Shared OpenSearch index with mandatory `tenant_id` filter + routing for FREE/STANDARD; dedicated index for ENTERPRISE.
- **Fairness:** Sliding-window rate limit (Redis) per tenant + per tier; dedicated Kafka topics for ENTERPRISE prevent backlog starvation.
- **Quotas:** Per-tier document count and storage limits.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
