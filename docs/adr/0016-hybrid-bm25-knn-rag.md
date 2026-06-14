# ADR 0016: Hybrid BM25 + kNN Search for RAG Retrieval

**Status:** Accepted
**Date:** 2026-06-14
**Layer:** Intelligence Agent + Search API

## Context

The RAG pipeline must retrieve the most relevant chunks (tickets, docs, code files, ADRs) given
a user query. The choice of retrieval strategy directly affects answer quality.

A typical engineering knowledge base has two types of queries simultaneously:

1. **Keyword-heavy** — "what does ticket ENG-2466 fix?", "error code 504 in payment service",
   "how is the LRU cache invalidated" — exact tokens matter. BM25 (TF-IDF variant) is naturally
   strong here.
2. **Semantic / intent** — "why is the deployment failing?", "explain our rate limiting
   approach", "how does the queue handle backpressure" — intent matters more than exact words.
   Dense vector (kNN) is naturally strong here.

Both types appear in the same user sessions.

## Decision

Use **hybrid BM25 + kNN retrieval with Reciprocal Rank Fusion (RRF)** as the default retrieval
strategy.

**Implementation:**
- Embedding model: `sentence-transformers/all-MiniLM-L6-v2` (384-dim, ~22 MB, CPU-fast)
- Served by: `embedding-service/` (FastAPI, `POST /embed`)
- Vector index: OpenSearch HNSW (`knn_vector` field in the `chunks` index)
- Fusion: RRF with equal weights (BM25 rank + kNN rank → combined score)
- Top-K: retrieve 20 candidates (BM25 top-10 + kNN top-10), re-rank by RRF, return top-8

**Query flow:**
```
user_query
    ├── BM25: POST /api/v1/search?q=<query>  (OpenSearch full-text)
    └── kNN:  POST /api/v1/search (with knn_vector from /embed)
              → RRF fusion → top-8 chunks → LLM context
```

## Consequences

**Positive**
- Ticket IDs, error codes, and symbol names are caught by BM25 where pure semantic search
  misses them (no exact token overlap with docs that describe the same concept differently).
- Intent queries are caught by kNN where pure BM25 misses them.
- RRF is robust to score scale differences — no normalisation tuning needed.
- `all-MiniLM-L6-v2` is fast (<50ms CPU), small (22 MB), and well-tested for retrieval.

**Negative**
- Two retrieval calls per query instead of one (~80ms overhead; negligible vs 20-40s LLM time).
- HNSW index requires additional OpenSearch memory (~1.1 × dim × vectors × 4 bytes).
  For 500k chunks at 384 dim: ~830 MB — fits comfortably in a 4 GB heap.
- Embedding model must be consistent between index time and query time. Changing the model
  requires re-indexing everything.

**Neutral**
- RRF weights (BM25 vs kNN) can be tuned if retrieval quality drifts.
- Cross-encoder reranking is a natural next step if precision needs improving — deferred.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| BM25 only | Misses intent queries; poor on paraphrased concepts |
| kNN only | Misses exact identifier matches (ticket IDs, error codes, function names) |
| Cross-encoder reranking | Requires separate model; 50–200ms additional latency per candidate; deferred |
| Larger embedding model (`all-mpnet-base-v2`, 768 dim) | Marginal quality gain at 2× memory; not justified yet |
