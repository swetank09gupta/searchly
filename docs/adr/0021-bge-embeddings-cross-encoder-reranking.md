# ADR 0021: BGE Embeddings + Cross-Encoder Reranking + Dual-Query Retrieval

**Status:** Accepted
**Date:** 2026-06-16
**Supersedes:** [ADR 0016](0016-hybrid-bm25-knn-rag.md)
**Layer:** Intelligence Agent + Search API + Indexer + Embedding Service

## Context

ADR 0016 established hybrid BM25 + kNN with `all-MiniLM-L6-v2` as the retrieval strategy. After
running the system with a real engineering knowledge base, three retrieval quality gaps emerged:

1. **Recall gap** — a single BM25 + kNN call over the literal query misses results that a
   paraphrase of the same query would find. This is particularly visible on operational queries
   where users phrase the same question differently each time.
2. **Precision gap** — RRF returns up to 20 candidates to the LLM context window. Most queries
   only need 3–5 chunks. The LLM receives irrelevant context, increasing hallucination risk and
   token cost.
3. **Source authority gap** — chunks from live Elasticsearch logs about the current incident
   should be weighted higher than a 2-year-old Confluence page. Equal RRF weights don't reflect
   that operational sources are authoritative for operational queries.

## Decision

Replace the ADR 0016 retrieval pipeline with a three-stage system.

### Stage 1 — Dual-query embedding

Every user query generates **two** query vectors:
- **Original** — embed the raw query directly
- **Rewritten** — rewrite the query via Ollama (`llama3.2:3b`) to a semantically equivalent
  alternative phrasing, then embed that

Query rewriting is **additive** — the rewrite adds recall via alternative vocabulary, not a
replacement that might change query semantics.

### Stage 2 — 6-leg retrieval with source authority weighting (sequential today; target: parallel via CompletableFuture — Sprint 1.1)

Run six retrieval legs:

| Leg | Type | Weight |
|---|---|---|
| knnOrig | kNN on original query | 1.0 |
| knnRew | kNN on rewritten query | 0.7 |
| bm25Orig | BM25 on original query | 1.0 |
| bm25Rew | BM25 on rewritten query | 0.7 |
| custKnnOrig | kNN filtered to customer chunks | 2.0 |
| custBm25Orig | BM25 filtered to customer chunks | 2.0 |

Customer-specific legs only run when the query has a resolved customer context. They are weighted
2× to ensure customer-specific operational data beats generic documentation.

RRF fusion formula per chunk:
```
rrfScore = Σ (listWeight × authorityWeight) / (60 + rank)
```

Source authority weights (applied at score time, never surfaced in the LLM prompt):

| Source tag | Authority weight |
|---|---|
| `live_logs` | 1.0 |
| `deployment_state` | 0.9 |
| `code` / `adr` | 0.8 |
| `jira` | 0.7 |
| `confluence` | 0.5 |

Each leg retrieves top-50 candidates. RRF merge produces top-30 rerank candidates.

### Stage 3 — Cross-encoder reranking + source budget

A cross-encoder (`BAAI/bge-reranker-base`) scores each of the 30 (query, chunk) pairs jointly,
producing a scalar relevance score. The top-6 chunks by reranker score are passed to the LLM.

After reranking, a **source budget** limits context saturation:

| Source | Max chunks |
|---|---|
| `warehouse_logs` | 2 |
| `deployment_state`, `jira`, `code`, `confluence` | 1 each |

### Embedding model change

Replace `sentence-transformers/all-MiniLM-L6-v2` with `BAAI/bge-small-en-v1.5`.

BGE uses an asymmetric encoding strategy — queries are prefixed with
`"Represent this sentence: "` at embed time; passage encoding uses no prefix. This asymmetry is
specifically designed for retrieval (query ↔ document matching) rather than semantic similarity
(sentence ↔ sentence). The original `all-MiniLM-L6-v2` was trained symmetrically.

Both models produce 384-dimensional vectors — the existing HNSW index is reused without
remapping. `EMBEDDING_VERSION = "bge-small-en-v1.5-v1"` is written into every chunk document
for lineage tracking (see ADR 0023).

## Consequences

**Positive**
- Dual-query substantially improves recall on operational queries where phrasing varies.
- Reranker improves precision: LLM context shrinks from 20 chunks to 6, reducing irrelevant
  context and token cost.
- Source authority weighting ensures live operational data is prioritised over stale docs.
- Customer-specific legs (2×) ensure the right-customer context is never crowded out.
- BGE asymmetric encoding is designed for retrieval; measurable MRR improvement in evaluation.

**Negative**
- Query rewriting adds ~600ms median latency (Ollama call). Total RAG path grows from ~1s to ~5s.
- Cross-encoder `bge-reranker-base` is ~280 MB on disk; adds ~300ms/30 pairs at CPU inference (p50).
- Model change requires a full re-embed of all existing chunks (one-time migration; no index
  schema change since 384-dim is unchanged).

**Known gaps at acceptance**
- Retrieval legs are currently sequential HTTP calls, not parallel — ~300ms recoverable. Fix:
  `CompletableFuture.allOf()` (Sprint 1.1).
- Recency boost (Gauss decay) applied to `documents-*` BM25 in SearchService but not to
  `RagService.bm25Internal()` (Sprint 1.2).

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| HyDE (hypothetical document embedding) | Can hallucinate the hypothetical document; dual-query with actual rewriting is safer |
| Larger embedding model (768-dim) | Requires HNSW index remapping; 2× memory for marginal gain; deferred |
| Sparse retrieval (SPLADE) | Not natively supported in OpenSearch without custom plugins |
| Proprietary reranker API (Cohere) | External API call; violates zero-egress requirement (ADR 0015) |
