package dev.searchly.indexer;

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
import java.util.List;
import java.util.Map;

@Component
public class EmbeddingClient {
    private static final Logger log = LoggerFactory.getLogger(EmbeddingClient.class);

    private final String baseUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;

    public EmbeddingClient(
            @Value("${searchly.embedding.url:http://localhost:8083}") String baseUrl,
            ObjectMapper mapper) {
        this.baseUrl = baseUrl;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Returns one embedding vector per input text, in the same order.
     * Returns an empty list on any failure so the caller can skip embedding
     * without failing the whole Kafka message (document is still keyword-searchable).
     */
    @SuppressWarnings("unchecked")
    public List<List<Double>> embed(List<String> texts) {
        if (texts == null || texts.isEmpty()) return List.of();
        try {
            String body = mapper.writeValueAsString(Map.of("texts", texts));
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/embed"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                log.warn("Embedding service HTTP {}: {}", resp.statusCode(), resp.body());
                return List.of();
            }
            Map<String, Object> result = mapper.readValue(resp.body(), Map.class);
            return (List<List<Double>>) result.get("vectors");
        } catch (Exception e) {
            log.warn("Embedding call failed (doc will be keyword-only): {}", e.getMessage());
            return List.of();
        }
    }
}
