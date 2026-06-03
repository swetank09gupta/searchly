# ADR 0004: Async Indexing via Kafka

**Status:** Accepted
**Date:** 2026-06-03

## Context

Indexing involves blob fetch, Tika text extraction, and OpenSearch write — collectively too slow and too failure-prone to run synchronously on the request thread. We also need bulk-import resilience, replayability for index rebuilds, and isolation between tenant tiers.

## Decision

Use **Apache Kafka** as the asynchronous indexing pipeline, with **tiered topics**:
- `indexing.shared` — partitioned by `tenant_id` for FREE/STANDARD tiers
- `indexing.enterprise.{tenant_id}` — dedicated topic per ENTERPRISE tenant
- `indexing.dlq` — poison message dead-letter queue
- `audit.events` — security/audit log stream

## Consequences

**Positive**
- Durable, replayable log: OpenSearch can be rebuilt by replaying from a Kafka offset.
- Partitioning by `tenant_id` gives per-tenant ordering on the shared topic.
- Dedicated enterprise topics prevent backlog from a bulk import starving other tenants (noisy-neighbor isolation).
- Consumer groups scale horizontally; KEDA can autoscale indexers on consumer lag.
- Decouples write latency from indexing latency — search remains available even if indexers are degraded.

**Negative**
- Eventual consistency on search results (typical lag <1s; users see indexed docs in a moment, not synchronously).
- Operational footprint: Kafka brokers, schema management, monitoring.
- Idempotency required: indexer must tolerate duplicate messages (we use `doc_id` as the OpenSearch document id → upsert).

**Rejected alternatives**
- **RabbitMQ / SQS:** no replayable log; harder to rebuild the index from scratch.
- **Synchronous indexing:** couples search write availability to OpenSearch availability; bulk imports starve interactive traffic.
