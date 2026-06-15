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

    // ── Bulk entry records (P0.6) ─────────────────────────────────────────────
    public record BulkDocEntry(String index, String tenantId, String docId,
                                String title, String content, Map<String, Object> metadata,
                                String createdAt) {}

    public record BulkChunkEntry(String index, String tenantId, String docId, int chunkIdx,
                                  String title, String chunkText, List<Double> embedding,
                                  Map<String, Object> metadata, String createdAt,
                                  String embeddingVersion) {}

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
        log.info("idx-step1 ensureDocIdx doc={}", docId);
        ensureDocumentIndex(index);

        log.info("idx-step2 buildMap doc={} content_len={}", docId, content != null ? content.length() : 0);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("tenant_id", tenantId);
        doc.put("title",     title);
        doc.put("content",   content);
        doc.put("metadata",  metadata != null ? metadata : Map.of());
        doc.put("created_at", createdAt);
        doc.put("content_fingerprint", ContentFingerprinter.fingerprint(title, content));

        log.info("idx-step3 serialize doc={}", docId);
        byte[] body = mapper.writeValueAsBytes(doc);
        log.info("idx-step4 PUT doc={} body_len={}", docId, body.length);
        String url = osBase + "/" + index + "/_doc/" + docId + "?routing=" + tenantId;
        put(url, body);
        log.info("Indexed doc {} for tenant {} in {}", docId, tenantId, index);
    }

    /**
     * Returns the stored content_fingerprint for a document, or null if the document
     * does not yet exist or the field is absent. Used to skip chunk re-embedding
     * when content has not changed between sync cycles.
     */
    @SuppressWarnings("unchecked")
    public String getDocumentFingerprint(String index, String docId) {
        try {
            String url = osBase + "/" + index + "/_doc/" + docId
                    + "?_source_includes=content_fingerprint";
            HttpURLConnection conn = open(url, "GET");
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(5_000);
            int status = conn.getResponseCode();
            if (status == 404) return null;
            if (status >= 300) return null;
            try (InputStream is = conn.getInputStream()) {
                Map<String, Object> resp = mapper.readValue(is, Map.class);
                Map<String, Object> src = (Map<String, Object>) resp.get("_source");
                return src != null ? (String) src.get("content_fingerprint") : null;
            }
        } catch (Exception e) {
            log.debug("getDocumentFingerprint failed for {}/{}: {}", index, docId, e.getMessage());
            return null;
        }
    }

    // ── Bulk indexing (P0.6) ─────────────────────────────────────────────────

    /**
     * Bulk-indexes up to 500 full documents in a single /_bulk request.
     * 10-50× throughput improvement over per-document PUT.
     */
    public void bulkIndexDocuments(List<BulkDocEntry> entries) throws Exception {
        if (entries.isEmpty()) return;
        // Ensure all required indices exist first
        Set<String> indices = new java.util.LinkedHashSet<>();
        for (BulkDocEntry e : entries) indices.add(e.index());
        for (String idx : indices) ensureDocumentIndex(idx);

        StringBuilder ndjson = new StringBuilder();
        for (BulkDocEntry e : entries) {
            Map<String, Object> meta = Map.of(
                    "_index", e.index(),
                    "_id",    e.docId(),
                    "routing", e.tenantId());
            ndjson.append(mapper.writeValueAsString(Map.of("index", meta))).append('\n');

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("tenant_id",  e.tenantId());
            doc.put("title",      e.title());
            doc.put("content",    e.content());
            doc.put("metadata",   e.metadata());
            doc.put("created_at", e.createdAt());
            doc.put("content_fingerprint", ContentFingerprinter.fingerprint(e.title(), e.content()));
            ndjson.append(mapper.writeValueAsString(doc)).append('\n');
        }
        bulk(ndjson.toString());
        log.info("Bulk indexed {} documents", entries.size());
    }

    /**
     * Bulk-indexes chunk vectors in a single /_bulk request.
     */
    public void bulkIndexChunks(List<BulkChunkEntry> entries) throws Exception {
        if (entries.isEmpty()) return;
        Set<String> indices = new java.util.LinkedHashSet<>();
        for (BulkChunkEntry e : entries) indices.add(e.index());
        for (String idx : indices) ensureChunkIndex(idx);

        StringBuilder ndjson = new StringBuilder();
        for (BulkChunkEntry e : entries) {
            String id = e.docId() + "-chunk-" + e.chunkIdx();
            Map<String, Object> meta = Map.of(
                    "_index", e.index(),
                    "_id",    id,
                    "routing", e.tenantId());
            ndjson.append(mapper.writeValueAsString(Map.of("index", meta))).append('\n');

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("doc_id",           e.docId());
            doc.put("chunk_index",       e.chunkIdx());
            doc.put("tenant_id",         e.tenantId());
            doc.put("title",             e.title());
            doc.put("chunk_text",        e.chunkText());
            doc.put("embedding",         e.embedding());
            doc.put("metadata",          e.metadata());
            doc.put("created_at",        e.createdAt());
            doc.put("embedding_version", e.embeddingVersion());
            ndjson.append(mapper.writeValueAsString(doc)).append('\n');
        }
        bulk(ndjson.toString());
        log.debug("Bulk indexed {} chunks", entries.size());
    }

    private void bulk(String ndjson) throws Exception {
        byte[] body = ndjson.getBytes(StandardCharsets.UTF_8);
        HttpURLConnection conn = open(osBase + "/_bulk", "POST");
        conn.setDoOutput(true);
        conn.setConnectTimeout(15_000);
        conn.setReadTimeout(60_000);
        conn.setRequestProperty("Content-Type", "application/x-ndjson; charset=utf-8");
        conn.setRequestProperty("Content-Length", String.valueOf(body.length));
        try (OutputStream os = conn.getOutputStream()) { os.write(body); }
        int status = conn.getResponseCode();
        if (status >= 300) {
            String err = "";
            try (InputStream es = conn.getErrorStream()) {
                if (es != null) err = new String(es.readAllBytes(), StandardCharsets.UTF_8);
            }
            log.warn("Bulk /_bulk → {}: {}", status, err.substring(0, Math.min(300, err.length())));
        } else {
            try (InputStream is = conn.getInputStream()) { is.transferTo(OutputStream.nullOutputStream()); }
        }
        conn.disconnect();
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
                           Map<String, Object> metadata, String createdAt,
                           String embeddingVersion) {
        try {
            ensureChunkIndex(chunkIndex);

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("doc_id",            docId);
            doc.put("chunk_index",        chunkIdx);
            doc.put("tenant_id",          tenantId);
            doc.put("title",              title);
            doc.put("chunk_text",         chunkText);
            doc.put("embedding",          embedding);
            doc.put("metadata",           metadata);
            doc.put("created_at",         createdAt);
            doc.put("embedding_version",  embeddingVersion);

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
        log.info("put-a open {}", url);
        HttpURLConnection conn = open(url, "PUT");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);
        conn.setRequestProperty("Content-Type",   "application/json; charset=utf-8");
        conn.setRequestProperty("Content-Length", String.valueOf(jsonBody.length));

        log.info("put-b write body_len={}", jsonBody.length);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonBody);
        }

        log.info("put-c getResponseCode");
        int status = conn.getResponseCode();
        log.info("put-d status={}", status);
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
        log.info("put-e disconnect");
        conn.disconnect();
    }

    private static HttpURLConnection open(String url, String method) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        return conn;
    }
}
