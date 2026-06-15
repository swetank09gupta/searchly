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
 * Manages all OpenSearch indices and documents using raw HTTP (Java built-in HttpClient).
 *
 * Handles both full-document indices (documents-*) for keyword search and
 * k-NN chunk indices (chunks-*) for semantic/RAG search.
 *
 * Raw HTTP is used throughout to avoid the Apache HttpClient5 IO reactor
 * (ApacheHttpClient5TransportBuilder) which OOMs under load due to channel
 * accumulation in validateActiveChannels, even with small documents.
 */
@Component
public class ChunkIndexClient {
    private static final Logger log = LoggerFactory.getLogger(ChunkIndexClient.class);
    private static final int VECTOR_DIM = 384;

    private final String osBase;
    private final ObjectMapper mapper;
    // Force HTTP/1.1 — OpenSearch 2.x speaks HTTP/1.1; HTTP/2 upgrade causes body-null issues
    // and the selector/dispatch threads can OOM under sustained load.
    private final HttpClient http;
    private final Set<String> ensuredDocIndices   = ConcurrentHashMap.newKeySet();
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
                .version(HttpClient.Version.HTTP_1_1)   // avoids HTTP/2 IO reactor OOMs
                .build();
    }

    // ── Full-document indexing (documents-* indices, BM25 keyword search) ──────

    /**
     * Upserts a full document into the documents-* index for keyword search.
     * doc_id is used as the OpenSearch document id (idempotent / upsert semantics).
     */
    public void indexDocument(String index, String tenantId, String docId,
                              String title, String content,
                              Map<String, Object> metadata, String createdAt) throws Exception {
        ensureDocumentIndex(index);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("tenant_id", tenantId);
        doc.put("title",     title);
        doc.put("content",   content);
        doc.put("metadata",  metadata != null ? metadata : Map.of());
        doc.put("created_at", createdAt);

        String url = osBase + "/" + index + "/_doc/" + docId + "?routing=" + tenantId;
        put(url, mapper.writeValueAsString(doc));
        log.info("Indexed doc {} for tenant {} in {}", docId, tenantId, index);
    }

    private void ensureDocumentIndex(String index) throws Exception {
        if (ensuredDocIndices.contains(index)) return;
        if (indexExists(index)) { ensuredDocIndices.add(index); return; }

        String body = mapper.writeValueAsString(Map.of(
                "settings", Map.of(
                        "index", Map.of(
                                "number_of_shards",   "3",
                                "number_of_replicas", "0"   // single-node: 0 replicas → green
                        )
                )
        ));
        put(osBase + "/" + index, body);
        log.info("Created document index: {}", index);
        ensuredDocIndices.add(index);
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
        if (!indexExists(index)) {
            put(osBase + "/" + index, buildKnnIndexMapping());
            log.info("Created k-NN chunk index: {}", index);
        }
        ensuredChunkIndices.add(index);
    }

    private boolean indexExists(String index) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(osBase + "/" + index))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(5))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
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
                                "number_of_replicas", "0"   // single-node: 0 replicas → green
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
