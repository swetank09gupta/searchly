package dev.searchly.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Calls the embedding service to produce dense vectors for text chunks.
 *
 * Uses HttpURLConnection (not Java's HttpClient) — HttpClient creates a
 * background SelectorManager thread that can OOM and die under load, making the
 * client permanently broken for the rest of the container's lifetime.
 * HttpURLConnection is plain blocking I/O with no background threads.
 *
 * Returns empty list on any failure so the caller can skip embedding
 * without failing the whole Kafka message (document remains keyword-searchable).
 */
@Component
public class EmbeddingClient {
    private static final Logger log = LoggerFactory.getLogger(EmbeddingClient.class);

    private final String embedUrl;
    private final ObjectMapper mapper;

    public EmbeddingClient(
            @Value("${searchly.embedding.url:http://localhost:8083}") String baseUrl,
            ObjectMapper mapper) {
        this.embedUrl = baseUrl + "/embed";
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public List<List<Double>> embed(List<String> texts) {
        if (texts == null || texts.isEmpty()) return List.of();
        try {
            byte[] body = mapper.writeValueAsBytes(Map.of("texts", texts));

            HttpURLConnection conn = (HttpURLConnection) new URL(embedUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(60_000);   // embedding can be slow for large batches
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.setRequestProperty("Content-Length", String.valueOf(body.length));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }

            int status = conn.getResponseCode();
            if (status != 200) {
                log.warn("Embedding service HTTP {}: {}", status,
                        new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
                return List.of();
            }

            byte[] responseBytes = conn.getInputStream().readAllBytes();
            Map<String, Object> result = mapper.readValue(responseBytes, Map.class);
            return (List<List<Double>>) result.get("vectors");

        } catch (Exception e) {
            log.warn("Embedding call failed (doc will be keyword-only): {}", e.getMessage());
            return List.of();
        }
    }
}
