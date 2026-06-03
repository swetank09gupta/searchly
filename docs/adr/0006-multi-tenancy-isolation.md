# ADR 0006: Hybrid Tenant Isolation

**Status:** Accepted
**Date:** 2026-06-03

## Context

We expect a mix of many small tenants (free/standard) and a small number of high-volume enterprise tenants. Two extreme isolation models:

1. **Shared index, tenant_id filter** — cheap, scales to thousands of tenants, but noisy enterprise tenants can dominate shard hotspots and query latency.
2. **Index per tenant** — strong isolation, independent shard sizing and backup, but does not scale to thousands of small tenants (shard explosion, cluster metadata overhead).

## Decision

Use a **hybrid model**:
- **FREE / STANDARD / PREMIUM:** shared OpenSearch index, `tenant_id` as routing key, mandatory `tenant_id` filter injected centrally by `SecureQueryBuilder`.
- **ENTERPRISE:** dedicated OpenSearch index per tenant, sized to expected document count, with independent snapshot/restore lifecycle.

Tier is stored in `tenants.tier` (Postgres), cached in Redis, and propagated as a JWT claim for fast routing decisions at the gateway and API.

## Consequences

**Positive**
- Scales to thousands of small tenants without index explosion.
- Enterprise tenants get predictable performance and operational isolation (backup, restore, shard tuning).
- Hybrid is the standard SaaS pattern — documented and reviewable by reviewers.

**Negative**
- Two code paths in the indexer and query layer (resolved by an `IndexResolver` that maps `tenant_id → index_name`).
- Migration when a tenant is promoted (STANDARD → ENTERPRISE) requires a reindex job; runbook needed.

**Defense in depth**
- Even with shared indices, `tenant_id` filter is enforced in code AND cross-checked by integration tests that assert cross-tenant queries return zero results.
- `TenantSecurityFilter` rejects requests where JWT tenant != header/path tenant before queries are built.
