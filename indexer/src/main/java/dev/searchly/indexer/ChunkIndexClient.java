package dev.searchly.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages all OpenSearch indices and documents using plain HttpURLConnection.
 *
 * Handles both full-document indices (documents-*) for keyword search and
 * k-NN chunk indices (chunks-*) for semantic/RAG search.
 *
 * HttpURLConnection is used throughout to avoid selector-manager and IO-reactor
 * threads (Java HttpClient / Apache HttpClient5) that OOM and permanently die
 * under sustained load. Plain blocking I/O has no background threads and
 * cannot get into a permanently-broken state from a GC event.
 */
@Component
public class ChunkIndexClient {
    private static final Logger log = LoggerFactory.getLogger(ChunkIndexClient.class);
    private static final int VECTOR_DIM = 384;

    private final String osBase;
    private final ObjectMapper mapper;
    private final Set<String> ensuredDocIndices   = ConcurrentHashMap.newKeySet();
    private final Set<String> ensuredChunkIndices = ConcurrentHashMap.newKeySet();

    public ChunkIndexClient(
            @Value("${searchly.opensearch.scheme:http}") String scheme,
            @Value("${searchly.opensearch.host:localhost}") String host,
            @Value("${searchly.opensearch.port:9200}") int port,
            ObjectMapper mapper) {
        this.osBase = scheme + "://" + host + ":" + port;
        this.mapper = mapper;
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
        put(url, mapper.writeValueAsBytes(doc));
        log.info("Indexed doc {} for tenant {} in {}", docId, tenantId, index);
    }

    private void ensureDocumentIndex(String index) throws Exception {
        if (ensuredDocIndices.contains(index)) return;
        if (!indexExists(index)) {
            Map<String, Object> body = Map.of(
                    "settings", Map.of(
                            "index", Map.of(
                                    "number_of_shards",   "3",
                                    "number_of_replicas", "0"  // single-node → green
                            )
                    )
            );
            put(osBase + "/" + index, mapper.writeValueAsBytes(body));
            log.info("Created document index: {}", index);
        }
        ensuredDocIndices.add(index);
    }

    // ── Chunk indexing (chunks-* indices, k-NN vector search) ────────────────

    public void indexChunk(String chunkIndex, String tenantId, String docId, int chunkIdx,
                           String title, String chunkText, List<Double> embedding,
                           Map<String, Object> metadata, String createdAt) {
        try {
            ensureChunkIndex(chunkIndex);

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("doc_id",      docId);
            doc.put("chunk_index", chunkIdx);
            doc.put("tenant_id",   tenantId);
            doc.put("title",       title);
            doc.put("chunk_text",  chunkText);
            doc.put("embedding",   embedding);
            doc.put("metadata",    metadata);
            doc.put("created_at",  createdAt);

            String id  = docId + "-chunk-" + chunkIdx;
            String url = osBase + "/" + chunkIndex + "/_doc/" + id + "?routing=" + tenantId;
            put(url, mapper.writeValueAsBytes(doc));
            log.debug("Indexed chunk {}/{} for doc {}", chunkIdx, chunkIndex, docId);
        } catch (Exception e) {
            log.warn("Failed to index chunk {}/{} for doc {}: {}", chunkIdx, chunkIndex, docId, e.getMessage());
        }
    }

    private void ensureChunkIndex(String index) throws Exception {
        if (ensuredChunkIndices.contains(index)) return;
        if (!indexExists(index)) {
            put(osBase + "/" + index, mapper.writeValueAsBytes(buildKnnIndexMapping()));
            log.info("Created k-NN chunk index: {}", index);
        }
        ensuredChunkIndices.add(index);
    }

    private Map<String, Object> buildKnnIndexMapping() {
        Map<String, Object> embeddingField = Map.of(
                "type",      "knn_vector",
                "dimension", VECTOR_DIM,
                "method",    Map.of(
                        "name",       "hnsw",
                        "engine",     "lucene",
                        "parameters", Map.of("m", 16, "ef_construction", 128)
                )
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("embedding",   embeddingField);
        properties.put("chunk_text",  Map.of("type", "text"));
        properties.put("tenant_id",   Map.of("type", "keyword"));
        properties.put("doc_id",      Map.of("type", "keyword"));
        properties.put("title",       Map.of("type", "text"));
        properties.put("chunk_index", Map.of("type", "integer"));
        properties.put("created_at",  Map.of("type", "date"));

        return Map.of(
                "settings", Map.of(
                        "index", Map.of(
                                "knn",                true,
                                "number_of_shards",   "3",
                                "number_of_replicas", "0"   // single-node → green
                        )
                ),
                "mappings", Map.of("properties", properties)
        );
    }

    // ── Shared HTTP helpers (HttpURLConnection — no background threads) ───────

    private boolean indexExists(String index) throws Exception {
        HttpURLConnection conn = open(osBase + "/" + index, "HEAD");
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(5_000);
        int status = conn.getResponseCode();
        conn.disconnect();
        return status == 200;
    }

    private void put(String url, byte[] jsonBody) throws Exception {
        HttpURLConnection conn = open(url, "PUT");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);
        conn.setRequestProperty("Content-Type",   "application/json; charset=utf-8");
        conn.setRequestProperty("Content-Length", String.valueOf(jsonBody.length));

        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonBody);
        }

        int status = conn.getResponseCode();
        if (status >= 300) {
            String body = "";
            try (InputStream es = conn.getErrorStream()) {
                if (es != null) body = new String(es.readAllBytes(), StandardCharsets.UTF_8);
            }
            log.warn("OpenSearch PUT {} → {}: {}", url, status, body);
        } else {
            // drain response to allow connection reuse
            try (InputStream is = conn.getInputStream()) { is.transferTo(OutputStream.nullOutputStream()); }
        }
        conn.disconnect();
    }

    private static HttpURLConnection open(String url, String method) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        return conn;
    }
}
