package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Cross-encoder reranker backed by BAAI/bge-reranker-base (served by embedding-service).
 * Returns a relevance score per passage; higher = more relevant.
 */
@Component
public class RerankClient {
    private static final Logger log = LoggerFactory.getLogger(RerankClient.class);

    private final String baseUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;

    public RerankClient(
            @Value("${searchly.embedding.url:http://localhost:8083}") String baseUrl,
            ObjectMapper mapper) {
        this.baseUrl = baseUrl;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build();
    }

    /**
     * Score each passage against the query. Returns empty list on circuit open or error.
     */
    @CircuitBreaker(name = "reranker", fallbackMethod = "rerankFallback")
    @SuppressWarnings("unchecked")
    public List<Double> rerank(String query, List<String> passages) {
        try {
            String body = mapper.writeValueAsString(Map.of("query", query, "passages", passages));
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/rerank"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(15))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("Reranker HTTP " + resp.statusCode());
            }
            Map<String, Object> result = mapper.readValue(resp.body(), Map.class);
            return (List<Double>) result.get("scores");
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("Reranker call failed", e);
        }
    }

    @SuppressWarnings("unused")
    private List<Double> rerankFallback(String query, List<String> passages, Throwable t) {
        log.warn("Reranker circuit open or failed — skipping rerank: {}", t.getMessage());
        return List.of();
    }
}
