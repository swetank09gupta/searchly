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

    public record SearchResponse(
            long took,
            long total,
            int page,
            int size,
            java.util.List<SearchHit> hits,
            Map<String, Map<String, Long>> facets,
            String  answer,               // LLM-generated answer (always present)
            java.util.List<String> sources,  // chunk IDs or "live:tool_name"
            // ── Conversational / multi-tenant fields ──────────────────────────
            String  sessionId,            // pass this back in subsequent requests
            boolean needsClarification,   // if true, answer IS a question for the user
            String  resolvedCustomer,     // which customer was identified
            String  resolvedEnv,          // which env was queried (dev/prod/etc.)
            String  lifecycleStage,       // solution | dev | testing | staging | prod
            String  lifecycleLabel,       // human-readable lifecycle label
            boolean hasLiveData           // true if live cluster was queried
    ) {
        // Backward-compatible constructor for callers that predate RAG
        public SearchResponse(long took, long total, int page, int size,
                              java.util.List<SearchHit> hits,
                              Map<String, Map<String, Long>> facets) {
            this(took, total, page, size, hits, facets,
                 null, null, null, false, null, null, null, null, false);
        }

        // Constructor for static RAG answers (no live data)
        public SearchResponse(long took, long total, int page, int size,
                              java.util.List<SearchHit> hits,
                              Map<String, Map<String, Long>> facets,
                              String answer, java.util.List<String> sources) {
            this(took, total, page, size, hits, facets,
                 answer, sources, null, false, null, null, null, null, false);
        }
    }
}
