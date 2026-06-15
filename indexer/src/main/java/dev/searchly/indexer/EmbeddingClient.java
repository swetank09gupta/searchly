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
 * Chunks are sent in small batches (EMBED_BATCH_SIZE) to avoid G1GC humongous
 * object allocation failures. A single request for a large document (e.g. 667
 * chunks from a 1 MB Kafka message) produces a ~3.8 MB embedding response byte
 * array. G1GC (default with 4 GB heap, 2 MB regions) treats objects > 1 MB as
 * "humongous" and must place them in consecutive free regions — if the heap is
 * fragmented this fails even when total free space is ample. At 50 chunks/batch
 * the response is ~288 KB, well below the 1 MB humongous threshold.
 *
 * Returns empty list on any failure so the caller can skip embedding
 * without failing the whole Kafka message (document remains keyword-searchable).
 */
@Component
public class EmbeddingClient {
    private static final Logger log = LoggerFactory.getLogger(EmbeddingClient.class);

    // Keep batches small so embedding response byte[] stays under G1GC's
    // humongous-object threshold (half the region size = 1 MB for a 4 GB heap).
    // 50 chunks × 384 floats × ~12 chars/float ≈ 230 KB per response — safe.
    private static final int EMBED_BATCH_SIZE = 50;

    private final String embedUrl;
    private final ObjectMapper mapper;

    public EmbeddingClient(
            @Value("${searchly.embedding.url:http://localhost:8083}") String baseUrl,
            ObjectMapper mapper) {
        this.embedUrl = baseUrl + "/embed";
        this.mapper = mapper;
    }

    /**
     * Returns one embedding vector per input text, in the same order.
     * Sends in batches of EMBED_BATCH_SIZE to avoid large humongous allocations.
     */
    public List<List<Double>> embed(List<String> texts) {
        if (texts == null || texts.isEmpty()) return List.of();
        List<List<Double>> all = new java.util.ArrayList<>(texts.size());
        for (int start = 0; start < texts.size(); start += EMBED_BATCH_SIZE) {
            List<String> batch = texts.subList(start, Math.min(start + EMBED_BATCH_SIZE, texts.size()));
            List<List<Double>> batchVecs = embedBatch(batch);
            if (batchVecs.isEmpty()) return List.of();   // fail fast: caller skips chunk index
            all.addAll(batchVecs);
        }
        return all;
    }

    @SuppressWarnings("unchecked")
    private List<List<Double>> embedBatch(List<String> texts) {
        try {
            byte[] body = mapper.writeValueAsBytes(Map.of("texts", texts));

            HttpURLConnection conn = (HttpURLConnection) new URL(embedUrl).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(30_000);
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
