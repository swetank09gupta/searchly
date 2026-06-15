# ADR 0023: Per-Chunk Retrieval Tracing in SearchResponse

**Status:** Accepted
**Date:** 2026-06-16
**Layer:** Search API + Common

## Context

The multi-stage retrieval pipeline (6 legs → RRF → reranker → source budget) is a black box
from the caller's perspective. When retrieval quality regresses, there is no way to diagnose
*why* without re-running the query in a debugger. Specifically:

- Did a chunk rank high on kNN but not BM25 (semantic match only)?
- Was it included in RRF top-30 but dropped by the reranker?
- Which embedding version produced the vector that found it?
- Why did chunk A end up in the LLM context but chunk B didn't?

Without this data, the nightly eval scheduler can detect *that* quality regressed but cannot
identify *where in the pipeline* the regression occurred.

## Decision

Attach a `retrievalTraces` list to every `SearchResponse`. Each entry is a `RetrievalTrace`
record covering all stages for one candidate chunk:

```java
public record RetrievalTrace(
    String  chunkId,
    String  docId,
    String  source,
    Integer knnRank,         // best rank across all kNN legs (null if not in any kNN leg)
    Integer bm25Rank,        // best rank across all BM25 legs (null if not in any BM25 leg)
    double  rrfScore,        // RRF merged score
    int     rrfRank,         // rank in RRF merged list (among top-30)
    Double  rerankerScore,   // cross-encoder score (null if not sent to reranker)
    Integer finalRank,       // rank after reranking (null if not included)
    boolean included,        // true if this chunk was in the final LLM context
    String  embeddingVersion // e.g. "bge-small-en-v1.5-v1"
) {}
```

Traces cover **all RRF candidates** (up to 30 chunks), not just the included ones. This allows
callers to inspect why a specific chunk was ranked 31st rather than appearing in context.

The `embeddingVersion` field per trace enables post-hoc analysis of whether chunks indexed with
an older model behave differently than those with the current model.

### Implementation

`RagService` was refactored to propagate rank data through the pipeline via two auxiliary records:
- `MergeResult` — holds the sorted candidate list and per-chunk `knnRanks` + `bm25Ranks` maps
- `RerankResult` — holds the reranked list and per-chunk `rerankerScores` map

These records are local to `RagService` and do not cross the service boundary. The final
`SearchResponse` field is the only public surface.

## Consequences

**Positive**
- Retrieval pipeline is fully observable without code changes.
- Nightly eval can log traces alongside scores, enabling regression root-cause from logs alone.
- Model migration evaluation: compare `included=true` rate for chunks with old vs new
  `embeddingVersion` to measure real-world impact before committing to a model upgrade.
- Debug tooling can surface: "this chunk had knnRank=2 but rerankerScore=-1.2 (dropped by
  reranker)" — immediately identifies a reranker calibration problem.

**Negative**
- Response payload grows: 30 traces × ~200 bytes ≈ 6 KB added to every search response.
  Acceptable for internal API; add `?traces=false` opt-out if bandwidth becomes a concern.
- Cache hit responses carry stale traces (they reflect the pipeline at cache-write time).
  This is by design: the trace represents the actual pipeline run that produced the answer.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| Log traces to a separate log stream | Correlation with the response requires trace_id join; harder for eval tooling to consume |
| Store traces in a separate Postgres table | High write volume for a diagnostic feature; complicates the response contract |
| Opt-in `?debug=true` parameter | Retrieval quality tooling needs traces on every call, not just when debug is on |
