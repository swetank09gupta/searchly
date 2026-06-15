# ADR 0022: Knowledge Graph in PostgreSQL

**Status:** Accepted (storage layer complete; extraction not yet wired)
**Date:** 2026-06-16
**Layer:** Search API + Connectors

## Context

The intelligence agent answers questions from flat document chunks. Some questions require
understanding *relationships* between entities: "which PR fixed ticket ENG-2466?", "which
services does deployment X run?", "what Confluence page describes this Jira epic?". These
traversal queries cannot be answered from chunk similarity alone — they need a graph.

## Decision

Store the knowledge graph in **PostgreSQL** using two flat tables with a JSONB properties column,
not a dedicated graph database.

```sql
CREATE TABLE kg_entities (
    id          BIGSERIAL PRIMARY KEY,
    entity_type VARCHAR(64)  NOT NULL,   -- jira_issue, pull_request, commit, service, deployment
    entity_id   VARCHAR(255) NOT NULL,   -- e.g. "ENG-2466", "github.com/org/repo/pull/42"
    name        TEXT,
    properties  JSONB,
    tenant_id   VARCHAR(64)  NOT NULL,
    UNIQUE (tenant_id, entity_type, entity_id)
);

CREATE TABLE kg_relationships (
    id            BIGSERIAL PRIMARY KEY,
    source_id     BIGINT REFERENCES kg_entities(id),
    target_id     BIGINT REFERENCES kg_entities(id),
    relation_type VARCHAR(64) NOT NULL,  -- fixed_by, contains, runs, references
    properties    JSONB,                 -- {"confidence": 0.95, "extraction_method": "api"}
    tenant_id     VARCHAR(64) NOT NULL
);
```

Traversal (BFS up to depth N) via recursive CTE in `KnowledgeGraphService`:

```sql
WITH RECURSIVE traverse AS (
    SELECT target_id, 1 AS depth FROM kg_relationships WHERE source_id = :startId
    UNION ALL
    SELECT r.target_id, t.depth + 1 FROM kg_relationships r
    JOIN traverse t ON r.source_id = t.target_id
    WHERE t.depth < :maxDepth
) SELECT DISTINCT e.* FROM kg_entities e JOIN traverse t ON e.id = t.target_id;
```

REST API at `/api/v1/kg`: POST /entity, POST /relationship, GET /neighbors, GET /traverse.

### Why not a dedicated graph database (Neo4j, Neptune)?

This knowledge graph has three distinctive properties that make a relational implementation
preferable:
1. **Shallow traversal** — queries rarely go beyond depth 3 (ticket → PR → commit → service).
   Deep recursive traversals where graph databases excel never appear in practice.
2. **Existing operational database** — PostgreSQL is already the system of record, already
   backed up, already monitored. Adding Neo4j doubles operational complexity for a feature that
   holds a few hundred thousand edges at full adoption.
3. **Unified transaction** — writing an entity and its Postgres metadata in the same transaction
   eliminates a class of partial-failure inconsistency.

### Extraction design (not yet wired — Sprint 2.2 / 2.3)

Connectors will populate the graph incrementally:

| Relationship | Source | Method | Confidence |
|---|---|---|---|
| `jira_issue --[fixed_by]--> pull_request` | Jira remote links API | API-based (authoritative) | 0.95 |
| `pull_request --[contains]--> commit` | GitHub pull/commits API | API-based | 1.0 |
| `commit --[touches]--> service` | Changed file paths + SERVICE_ROOTS config | Path heuristic | 0.80 |
| `deployment --[runs]--> service` | k8s label `app=<service>` | Label-based | 0.90 |
| `confluence_page --[references]--> jira_issue` | Regex `[A-Z]{2,10}-\d+` on page body | Regex | 0.70 |

Jira remote links are the highest-confidence source (API-authoritative) and are the priority for
first wiring (Sprint 2.2).

Confidence is stored in `kg_relationships.properties` JSONB and used to filter traversal:
`(r.properties->>'confidence')::float > 0.7`.

## Consequences

**Positive**
- Zero additional infrastructure (already have Postgres).
- JSONB properties column handles schema-free entity attributes without migrations.
- Recursive CTE handles the required shallow traversal efficiently.
- UNIQUE constraints make upsert-based sync idempotent.

**Negative**
- Deep traversal (>5 hops) degrades — fine for our use case; documented as limit.
- Graph is currently empty — extraction not yet wired to connectors (Sprint 2.2 + 2.3).
- No native graph query language; complex path patterns require SQL CTEs.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| Neo4j | Separate service, backup, monitoring; not justified for shallow traversal |
| Amazon Neptune | Cloud-specific; violates self-hosted constraint |
| OpenSearch graph plugin | Not available in OpenSearch without commercial plugin |
| Adjacency list in application memory | Lost on restart; can't be queried by other services |
