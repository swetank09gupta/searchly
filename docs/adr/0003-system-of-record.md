# ADR 0003: PostgreSQL as System of Record

**Status:** Accepted
**Date:** 2026-06-03

## Context

We need a durable, transactional store for tenants, users, roles, document metadata, ACLs, quotas, and audit references. The search index (OpenSearch) is not authoritative — it must be rebuildable from the source of truth and the blob store.

## Decision

Use **PostgreSQL** as the system of record (SoR).

## Consequences

**Positive**
- ACID guarantees for tenant/user/quota mutations; we can rely on read-your-writes for management operations.
- Mature partitioning (`PARTITION BY LIST(tenant_id)` or by month) supports per-tenant data lifecycle and large-table performance.
- Strong ecosystem: managed offerings (RDS/Aurora/CloudSQL), Flyway/Liquibase for migrations, `pg_stat_statements` for query observability.
- pgcrypto enables column-level encryption for sensitive PII.

**Negative**
- Operational complexity for HA (Multi-AZ, replication, failover); mitigated by using a managed service in production.

**How OpenSearch and Postgres stay aligned**
- Writes go: Postgres first (status=PENDING) → Kafka event → indexer → OpenSearch (status=INDEXED).
- If OpenSearch loses data, we replay from Kafka or rebuild from Postgres + MinIO blobs.
