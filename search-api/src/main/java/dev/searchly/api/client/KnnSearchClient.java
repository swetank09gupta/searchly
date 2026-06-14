package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Raw-HTTP client for k-NN vector search on the chunks-* OpenSearch indices.
 * Supports optional metadata filters: customer, product, env.
 */
@Component
public class KnnSearchClient {
    private static final Logger log = LoggerFactory.getLogger(KnnSearchClient.class);

    private final String osBase;
    private final ObjectMapper mapper;
    private final HttpClient http;

    public KnnSearchClient(
            @Value("${searchly.opensearch.scheme:http}") String scheme,
            @Value("${searchly.opensearch.host:localhost}") String host,
            @Value("${searchly.opensearch.port:9200}") int port,
            ObjectMapper mapper) {
        this.osBase = scheme + "://" + host + ":" + port;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public record ChunkHit(String chunkId, String docId, String title, String chunkText,
                           int chunkIndex, Map<String, Object> metadata) {}

    /** Shared knowledge search with optional metadata filters. */
    public List<ChunkHit> search(String chunkIndex, List<Double> queryVector,
                                 String tenantId, int k,
                                 String customer, String product, String env) {
        List<Map<String, Object>> filters = new ArrayList<>();
        filters.add(termFilter("tenant_id", tenantId));
        if (customer != null && !customer.isBlank())
            filters.add(termFilter("metadata.customer", customer));
        if (product != null && !product.isBlank())
            filters.add(termFilter("metadata.product", product));
        if (env != null && !env.isBlank())
            filters.add(termFilter("metadata.env", env));
        return runKnn(chunkIndex, queryVector, tenantId, k, filters);
    }

    /** Customer-specific search — scoped strictly to one customer's logs + deployment. */
    public List<ChunkHit> searchByCustomer(String chunkIndex, List<Double> queryVector,
                                            String tenantId, String customer, int k) {
        return runKnn(chunkIndex, queryVector, tenantId, k, List.of(
                termFilter("tenant_id", tenantId),
                termFilter("metadata.customer", customer)));
    }

    @SuppressWarnings("unchecked")
    private List<ChunkHit> runKnn(String chunkIndex, List<Double> queryVector,
                                   String tenantId, int k,
                                   List<Map<String, Object>> filters) {
        try {
            Map<String, Object> knnClause = Map.of(
                    "embedding", Map.of("vector", queryVector, "k", k));
            Map<String, Object> query = new LinkedHashMap<>();
            query.put("bool", Map.of("must", List.of(Map.of("knn", knnClause)),
                                     "filter", filters));

            Map<String, Object> body = Map.of(
                    "query", query,
                    "size", k,
                    "_source", List.of("doc_id", "chunk_index", "title",
                                       "chunk_text", "metadata"));

            String url = osBase + "/" + chunkIndex + "/_search?routing=" + tenantId;
            String respBody = post(url, mapper.writeValueAsString(body));
            if (respBody == null) return List.of();

            Map<String, Object> parsed = mapper.readValue(respBody, Map.class);
            Map<String, Object> hits = (Map<String, Object>) parsed.get("hits");
            List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");

            List<ChunkHit> results = new ArrayList<>();
            for (Map<String, Object> h : hitList) {
                String id = (String) h.get("_id");
                Map<String, Object> src = (Map<String, Object>) h.get("_source");
                if (src == null) continue;
                results.add(new ChunkHit(
                        id,
                        (String) src.get("doc_id"),
                        (String) src.getOrDefault("title", ""),
                        (String) src.getOrDefault("chunk_text", ""),
                        (int) src.getOrDefault("chunk_index", 0),
                        (Map<String, Object>) src.getOrDefault("metadata", Map.of())));
            }
            return results;
        } catch (Exception e) {
            log.warn("kNN search failed on {}: {}", chunkIndex, e.getMessage());
            return List.of();
        }
    }

    private Map<String, Object> termFilter(String field, String value) {
        return Map.of("term", Map.of(field, value));
    }

    private String post(String url, String jsonBody) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() >= 300) {
            log.warn("kNN HTTP {}: {}", resp.statusCode(), resp.body());
            return null;
        }
        return resp.body();
    }
}
