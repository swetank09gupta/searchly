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
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class SearchService {

    private final OpenSearchClient os;
    private final CacheService cache;

    public SearchService(OpenSearchClient os, CacheService cache) {
        this.os = os;
        this.cache = cache;
    }

    public DocumentDto.SearchResponse search(String q, int page, int size, boolean fuzzy, boolean highlight, List<String> facets) throws IOException {
        TenantContext ctx = TenantContextHolder.require();
        String roleKey = ctx.roles().stream().sorted().reduce((a, b) -> a + "," + b).orElse("");
        String facetKey = facets == null ? "" : String.join(",", facets);
        String cacheKey = cache.key(ctx.tenantId(), roleKey, q + ":" + fuzzy + ":" + highlight + ":" + facetKey, page, size);
        DocumentDto.SearchResponse hit = cache.get(cacheKey);
        if (hit != null) return hit;

        // Tenant filter is mandatory — built centrally here (see ADR 0006).
        Query tenantFilter = Query.of(b -> b.term(t -> t.field("tenant_id").value(v -> v.stringValue(ctx.tenantId()))));
        Query textQuery = fuzzy
                ? Query.of(b -> b.match(m -> m.field("content").query(v -> v.stringValue(q)).fuzziness("AUTO")))
                : Query.of(b -> b.match(m -> m.field("content").query(v -> v.stringValue(q))));

        Query combined = Query.of(b -> b.bool(bool -> bool.must(textQuery).filter(tenantFilter)));

        Map<String, Aggregation> aggs = new LinkedHashMap<>();
        if (facets != null) {
            for (String f : facets) {
                String field = facetFieldFor(f);
                if (field != null) {
                    aggs.put(f, Aggregation.of(a -> a.terms(t -> t.field(field).size(20))));
                }
            }
        }

        SearchRequest req = SearchRequest.of(s -> s
                .index(indexName(ctx))
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
                DocumentDto.SearchResponse empty = new DocumentDto.SearchResponse(0, 0, page, size, List.of(), Map.of());
                cache.put(cacheKey, empty);
                return empty;
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
        DocumentDto.SearchResponse response = new DocumentDto.SearchResponse(
                res.took(), total, page, size, hits, facetResults);
        cache.put(cacheKey, response);
        return response;
    }

    private static String facetFieldFor(String facet) {
        // Allowlist of facetable fields. Free-form facet names rejected to avoid injection.
        return switch (facet) {
            case "tags"   -> "metadata.tags.keyword";
            case "author" -> "metadata.author.keyword";
            default -> null;
        };
    }

    private String indexName(TenantContext ctx) {
        return ctx.tier().name().equals("ENTERPRISE")
                ? "documents-" + ctx.tenantId()
                : "documents-shared";
    }
}
