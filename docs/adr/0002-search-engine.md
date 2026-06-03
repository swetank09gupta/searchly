# ADR 0002: OpenSearch as Search Engine

**Status:** Accepted
**Date:** 2026-06-03

## Context

The service must support full-text search with relevance ranking over 10M+ documents across tenants, with <500ms p95 latency and 1000+ QPS. Required features include fuzzy search, highlighting, and faceted aggregations. Options considered: PostgreSQL FTS, Elasticsearch, OpenSearch, Typesense, Meilisearch.

## Decision

Use **OpenSearch** (Apache-2.0 licensed fork of Elasticsearch) as the search engine.

## Consequences

**Positive**
- Mature inverted index with BM25 relevance, fuzzy queries, highlighters, and aggregations out of the box.
- Horizontal scaling via shards and replicas; cross-cluster search for geo-distribution.
- Apache 2.0 license avoids the SSPL redistribution risk that affects Elasticsearch 7.11+ for SaaS use cases.
- Tooling parity with Elasticsearch ecosystem (Kibana → OpenSearch Dashboards, Logstash, Beats compatibility).

**Negative**
- Operationally heavier than Postgres FTS; requires JVM tuning, shard sizing discipline, and snapshot management.
- Eventually consistent with our write path (acceptable — see [ADR 0004](0004-async-indexing-kafka.md)).

**Rejected alternatives**
- **PostgreSQL FTS:** fine to ~1M docs/tenant but degrades on the 10M-scale facets/fuzzy use cases.
- **Elasticsearch:** SSPL license risk for SaaS distribution.
- **Typesense/Meilisearch:** great DX, but less proven at the 10M+ multi-tenant scale and weaker ecosystem.
