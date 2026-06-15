package dev.searchly.common;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;

public class DocumentDto {

    public record CreateRequest(
            @NotBlank @Size(max = 500) String title,
            @NotBlank @Size(max = 1_000_000) String content,
            Map<String, Object> metadata
    ) {}

    public record CreateResponse(
            String id,
            String tenantId,
            String status,
            Instant createdAt
    ) {}

    public record DocumentView(
            String id,
            String tenantId,
            String title,
            String content,
            Map<String, Object> metadata,
            String status,
            Instant createdAt
    ) {}

    public record SearchHit(
            String id,
            double score,
            String title,
            java.util.List<String> highlights,
            Map<String, Object> metadata
    ) {}

    /**
     * Per-chunk provenance trace through the retrieval pipeline (P3.2).
     * Returned alongside the answer so clients can inspect why each chunk was selected.
     */
    public record RetrievalTrace(
            String  chunkId,
            String  docId,
            String  source,           // metadata.source field (jira | git | confluence | warehouse_logs | ...)
            Integer knnRank,          // 1-based best rank across kNN legs; null if not retrieved by kNN
            Integer bm25Rank,         // 1-based best rank across BM25 legs; null if not retrieved by BM25
            double  rrfScore,         // merged RRF score
            int     rrfRank,          // 1-based position in RRF merged list (before reranking)
            Double  rerankerScore,    // cross-encoder score; null if reranker unavailable
            Integer finalRank,        // 1-based position in final LLM context window; null if not selected
            boolean included,         // true if this chunk was sent to the LLM
            String  embeddingVersion  // model version used to embed this chunk (P3.3)
    ) {}

    public record SearchResponse(
            long took,
            long total,
            int page,
            int size,
            java.util.List<SearchHit> hits,
            Map<String, Map<String, Long>> facets,
            String  answer,
            java.util.List<String> sources,
            // ── Conversational / multi-tenant fields ──────────────────────────
            String  sessionId,
            boolean needsClarification,
            String  resolvedCustomer,
            String  resolvedEnv,
            String  lifecycleStage,
            String  lifecycleLabel,
            boolean hasLiveData,
            // ── Cursor-based pagination ────────────────────────────────────────
            String  nextCursor,
            // ── Retrieval traces (P3.2) ────────────────────────────────────────
            java.util.List<RetrievalTrace> retrievalTraces
    ) {
        // Backward-compatible constructor for callers that predate RAG
        public SearchResponse(long took, long total, int page, int size,
                              java.util.List<SearchHit> hits,
                              Map<String, Map<String, Long>> facets) {
            this(took, total, page, size, hits, facets,
                 null, null, null, false, null, null, null, null, false, null,
                 java.util.List.of());
        }

        // Constructor for static RAG answers (no live data)
        public SearchResponse(long took, long total, int page, int size,
                              java.util.List<SearchHit> hits,
                              Map<String, Map<String, Long>> facets,
                              String answer, java.util.List<String> sources) {
            this(took, total, page, size, hits, facets,
                 answer, sources, null, false, null, null, null, null, false, null,
                 java.util.List.of());
        }
    }
}
