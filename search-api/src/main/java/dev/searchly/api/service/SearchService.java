package dev.searchly.api.service;

import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.common.DocumentDto;
import dev.searchly.common.TenantContext;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified hybrid search:
 *  1. BM25 keyword search on the documents index  → ranked document hits
 *  2. RagService runs kNN + BM25 on the chunks index, merges via RRF, calls Ollama
 *
 * One endpoint, always semantic + keyword + LLM answer.
 */
@Service
public class SearchService {
    private static final Logger log = LoggerFactory.getLogger(SearchService.class);

    private final OpenSearchClient os;
    private final CacheService cache;
    private final RagService rag;

    public SearchService(OpenSearchClient os, CacheService cache, RagService rag) {
        this.os = os;
        this.cache = cache;
        this.rag = rag;
    }

    /**
     * @param customer  optional customer ID — adds their live logs + deployment state to context
     * @param product   optional product filter — narrows to one product's docs
     * @param env       optional env filter — prod / staging / dev
     */
    public DocumentDto.SearchResponse search(String q, int page, int size,
                                             boolean fuzzy, boolean highlight,
                                             List<String> facets,
                                             String customer, String product,
                                             String env, String sessionId) throws IOException {
        TenantContext ctx = TenantContextHolder.require();
        String roleKey  = ctx.roles().stream().sorted().reduce((a, b) -> a + "," + b).orElse("");
        String facetKey = facets == null ? "" : String.join(",", facets);
        // Don't cache conversational requests (session state makes each turn unique)
        boolean conversational = sessionId != null && !sessionId.isBlank();
        String cacheKey = conversational ? null : cache.key(ctx.tenantId(), roleKey,
                q + ":" + fuzzy + ":" + highlight + ":" + facetKey
                + ":" + (customer != null ? customer : "")
                + ":" + (product  != null ? product  : "")
                + ":" + (env      != null ? env      : ""), page, size);

        DocumentDto.SearchResponse cached = cacheKey != null ? cache.get(cacheKey) : null;
        if (cached != null) return cached;

        // --- leg 1: BM25 keyword search on full documents (for the hits list) ---
        Query tenantFilter = Query.of(b -> b.term(t -> t.field("tenant_id")
                .value(v -> v.stringValue(ctx.tenantId()))));
        Query textQuery = fuzzy
                ? Query.of(b -> b.match(m -> m.field("content")
                        .query(v -> v.stringValue(q)).fuzziness("AUTO")))
                : Query.of(b -> b.match(m -> m.field("content")
                        .query(v -> v.stringValue(q))));
        Query combined = Query.of(b -> b.bool(bool -> bool.must(textQuery).filter(tenantFilter)));

        Map<String, Aggregation> aggs = new LinkedHashMap<>();
        if (facets != null) {
            for (String f : facets) {
                String field = facetFieldFor(f);
                if (field != null) aggs.put(f, Aggregation.of(a -> a.terms(t -> t.field(field).size(20))));
            }
        }

        SearchRequest req = SearchRequest.of(s -> s
                .index(docIndexName(ctx))
                .routing(ctx.tenantId())
                .from(page * size)
                .size(size)
                .query(combined)
                .aggregations(aggs)
                .highlight(h -> h.fields("content", f -> f.fragmentSize(150).numberOfFragments(2))));

        SearchResponse<Map> res;
        try {
            res = os.search(req, Map.class);
        } catch (org.opensearch.client.opensearch._types.OpenSearchException ose) {
            if (ose.getMessage() != null && ose.getMessage().contains("index_not_found")) {
                return emptyResponse(page, size);
            }
            throw ose;
        }

        List<DocumentDto.SearchHit> hits = new ArrayList<>();
        for (Hit<Map> h : res.hits().hits()) {
            Map<String, Object> src = h.source() != null ? h.source() : new HashMap<>();
            List<String> highlights = h.highlight().getOrDefault("content", List.of());
            hits.add(new DocumentDto.SearchHit(
                    h.id(),
                    h.score() == null ? 0.0 : h.score(),
                    String.valueOf(src.getOrDefault("title", "")),
                    highlights,
                    (Map<String, Object>) src.getOrDefault("metadata", Map.of())));
        }

        Map<String, Map<String, Long>> facetResults = new LinkedHashMap<>();
        if (res.aggregations() != null) {
            res.aggregations().forEach((name, agg) -> {
                if (agg.isSterms()) {
                    Map<String, Long> buckets = new LinkedHashMap<>();
                    agg.sterms().buckets().array().forEach(b -> buckets.put(b.key(), b.docCount()));
                    facetResults.put(name, buckets);
                }
            });
        }

        long total = res.hits().total() != null ? res.hits().total().value() : 0;

        // --- leg 2: warehouse agent chat (with fuzzy resolution + live data) or static RAG ---
        RagService.RagResult ragResult;
        try {
            ragResult = rag.answer(q, ctx, customer, product, env, sessionId);
        } catch (Exception e) {
            log.warn("RAG pipeline failed, returning keyword hits only: {}", e.getMessage());
            ragResult = new RagService.RagResult(null, List.of(),
                    null, false, null, null, null, null, false);
        }

        DocumentDto.SearchResponse response = new DocumentDto.SearchResponse(
                res.took(), total, page, size, hits, facetResults,
                ragResult.answer(), ragResult.sources(),
                ragResult.sessionId(), ragResult.needsClarification(),
                ragResult.resolvedCustomer(), ragResult.resolvedEnv(),
                ragResult.lifecycleStage(), ragResult.lifecycleLabel(),
                ragResult.hasLiveData());
        if (cacheKey != null) cache.put(cacheKey, response);
        return response;
    }

    private DocumentDto.SearchResponse emptyResponse(int page, int size) {
        return new DocumentDto.SearchResponse(0, 0, page, size, List.of(), Map.of());
    }

    private static String facetFieldFor(String facet) {
        return switch (facet) {
            case "tags"   -> "metadata.tags.keyword";
            case "author" -> "metadata.author.keyword";
            default -> null;
        };
    }

    private String docIndexName(TenantContext ctx) {
        return ctx.tier().name().equals("ENTERPRISE")
                ? "documents-" + ctx.tenantId()
                : "documents-shared";
    }
}
