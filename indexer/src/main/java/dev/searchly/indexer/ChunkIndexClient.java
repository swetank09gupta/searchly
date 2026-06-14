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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the chunks-* OpenSearch indices (k-NN enabled) and writes chunk documents.
 * Uses raw HTTP because opensearch-java's typed DSL does not expose the knn_vector
 * mapping type or the index-level knn:true setting.
 */
@Component
public class ChunkIndexClient {
    private static final Logger log = LoggerFactory.getLogger(ChunkIndexClient.class);
    private static final int VECTOR_DIM = 384;

    private final String osBase;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final Set<String> ensuredChunkIndices = ConcurrentHashMap.newKeySet();

    public ChunkIndexClient(
            @Value("${searchly.opensearch.scheme:http}") String scheme,
            @Value("${searchly.opensearch.host:localhost}") String host,
            @Value("${searchly.opensearch.port:9200}") int port,
            ObjectMapper mapper) {
        this.osBase = scheme + "://" + host + ":" + port;
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public void indexChunk(String chunkIndex, String tenantId, String docId, int chunkIdx,
                           String title, String chunkText, List<Double> embedding,
                           Map<String, Object> metadata, String createdAt) {
        try {
            ensureChunkIndex(chunkIndex);

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("doc_id", docId);
            doc.put("chunk_index", chunkIdx);
            doc.put("tenant_id", tenantId);
            doc.put("title", title);
            doc.put("chunk_text", chunkText);
            doc.put("embedding", embedding);
            doc.put("metadata", metadata);
            doc.put("created_at", createdAt);

            String id = docId + "-chunk-" + chunkIdx;
            String url = osBase + "/" + chunkIndex + "/_doc/" + id + "?routing=" + tenantId;
            put(url, mapper.writeValueAsString(doc));
            log.debug("Indexed chunk {}/{} for doc {}", chunkIdx, chunkIndex, docId);
        } catch (Exception e) {
            log.warn("Failed to index chunk {}/{} for doc {}: {}", chunkIdx, chunkIndex, docId, e.getMessage());
        }
    }

    private void ensureChunkIndex(String index) throws Exception {
        if (ensuredChunkIndices.contains(index)) return;

        String checkUrl = osBase + "/" + index;
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(checkUrl))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(5))
                .build();
        int status = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();

        if (status == 404) {
            String mapping = buildKnnIndexMapping();
            put(osBase + "/" + index, mapping);
            log.info("Created k-NN chunk index: {}", index);
        }
        ensuredChunkIndices.add(index);
    }

    private String buildKnnIndexMapping() throws Exception {
        Map<String, Object> embeddingField = Map.of(
                "type", "knn_vector",
                "dimension", VECTOR_DIM,
                "method", Map.of(
                        "name", "hnsw",
                        "engine", "lucene",
                        "parameters", Map.of("m", 16, "ef_construction", 128)
                )
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("embedding", embeddingField);
        properties.put("chunk_text", Map.of("type", "text"));
        properties.put("tenant_id", Map.of("type", "keyword"));
        properties.put("doc_id", Map.of("type", "keyword"));
        properties.put("title", Map.of("type", "text"));
        properties.put("chunk_index", Map.of("type", "integer"));
        properties.put("created_at", Map.of("type", "date"));

        Map<String, Object> body = Map.of(
                "settings", Map.of(
                        "index", Map.of(
                                "knn", true,
                                "number_of_shards", "3",
                                "number_of_replicas", "1"
                        )
                ),
                "mappings", Map.of("properties", properties)
        );
        return mapper.writeValueAsString(body);
    }

    private void put(String url, String jsonBody) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            log.warn("OpenSearch PUT {} → {}: {}", url, resp.statusCode(), resp.body());
        }
    }
}
