package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import dev.searchly.common.Tier;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes indexing events and writes to OpenSearch.
 * Idempotent: doc_id is the OpenSearch document id (upsert semantics).
 *
 * After writing the full document (keyword search), the content is also chunked,
 * embedded, and written to the chunks-* index for RAG / semantic search.
 * Embedding failures are non-fatal — the document remains keyword-searchable.
 */
@Component
public class IndexingConsumer {
    private static final Logger log = LoggerFactory.getLogger(IndexingConsumer.class);

    private final OpenSearchClient os;
    private final ChunkingService chunker;
    private final EmbeddingClient embedder;
    private final ChunkIndexClient chunkIndex;
    private final Map<String, Boolean> ensuredIndices = new ConcurrentHashMap<>();

    public IndexingConsumer(OpenSearchClient os,
                            ChunkingService chunker,
                            EmbeddingClient embedder,
                            ChunkIndexClient chunkIndex) {
        this.os = os;
        this.chunker = chunker;
        this.embedder = embedder;
        this.chunkIndex = chunkIndex;
    }

    @KafkaListener(topicPattern = "indexing\\.shared|indexing\\.enterprise\\..+", groupId = "indexer")
    public void onMessage(IndexingEvent event) {
        try {
            String index = docIndexName(event);
            ensureIndex(index);
            indexFullDocument(index, event);
            indexChunks(event);
        } catch (OutOfMemoryError oom) {
            // A single massive document (e.g. Confluence space with thousands of child pages)
            // can exhaust the heap even before truncation runs. Log and skip — the doc is
            // already written to the keyword index; only vector chunks are lost for this one doc.
            // Committing the offset lets Kafka advance past it so the container stays alive.
            log.error("OOM processing doc {} (content too large for heap) — skipping chunk index, doc is keyword-searchable",
                    event.docId());
            System.gc();
        } catch (IOException e) {
            log.error("Failed to index {}: {}", event.docId(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void indexFullDocument(String index, IndexingEvent event) throws IOException {
        Map<String, Object> doc = new HashMap<>();
        doc.put("tenant_id", event.tenantId());
        doc.put("title", event.title());
        doc.put("content", event.content());
        doc.put("metadata", event.metadata());
        doc.put("created_at", event.createdAt().toString());

        os.index(IndexRequest.of(i -> i
                .index(index)
                .id(event.docId())
                .routing(event.tenantId())
                .document(doc)));
        log.info("Indexed doc {} for tenant {} in {}", event.docId(), event.tenantId(), index);
    }

    private void indexChunks(IndexingEvent event) {
        String fullText = buildTextForEmbedding(event);
        List<String> chunks = chunker.chunk(fullText);
        if (chunks.isEmpty()) return;

        List<List<Double>> vectors = embedder.embed(chunks);
        if (vectors.isEmpty()) {
            log.warn("Embedding unavailable for doc {} — skipping chunk index", event.docId());
            return;
        }

        String chunkIdx = chunkIndexName(event);
        for (int i = 0; i < chunks.size(); i++) {
            List<Double> vec = i < vectors.size() ? vectors.get(i) : null;
            if (vec == null) continue;
            chunkIndex.indexChunk(
                    chunkIdx,
                    event.tenantId(),
                    event.docId(),
                    i,
                    event.title(),
                    chunks.get(i),
                    vec,
                    event.metadata() != null ? event.metadata() : Map.of(),
                    event.createdAt().toString());
        }
        log.info("Indexed {} chunks for doc {} in {}", chunks.size(), event.docId(), chunkIdx);
    }

    // Max chars to embed — Confluence spaces with thousands of child pages can produce
    // multi-MB strings that OOM the JVM during substring chunking. Cap at ~750 KB
    // (~500k tokens worth), which covers even the most detailed technical pages.
    private static final int MAX_EMBED_CHARS = 750_000;

    // Embed title + content together so the title context carries into each chunk's vector.
    // IMPORTANT: truncate content BEFORE concatenation — concatenating two multi-MB strings
    // doubles memory pressure and can OOM before we even reach the truncation check.
    private String buildTextForEmbedding(IndexingEvent event) {
        String title = event.title() != null ? event.title() : "";
        String content = event.content() != null ? event.content() : "";
        // Truncate content first so concatenation never creates a huge intermediate string
        if (content.length() > MAX_EMBED_CHARS) {
            log.warn("Doc {} content truncated from {} to {} chars for chunking",
                    event.docId(), content.length(), MAX_EMBED_CHARS);
            content = content.substring(0, MAX_EMBED_CHARS);
        }
        return title.isBlank() ? content : title + "\n\n" + content;
    }

    private String docIndexName(IndexingEvent e) {
        return e.tier() == Tier.ENTERPRISE ? "documents-" + e.tenantId() : "documents-shared";
    }

    private String chunkIndexName(IndexingEvent e) {
        return e.tier() == Tier.ENTERPRISE ? "chunks-" + e.tenantId() : "chunks-shared";
    }

    private void ensureIndex(String index) throws IOException {
        if (ensuredIndices.containsKey(index)) return;
        boolean exists = os.indices().exists(ExistsRequest.of(e -> e.index(index))).value();
        if (!exists) {
            os.indices().create(CreateIndexRequest.of(c -> c
                    .index(index)
                    .settings(s -> s.numberOfShards("3").numberOfReplicas("1"))));
            log.info("Created OpenSearch index {}", index);
        }
        ensuredIndices.put(index, true);
    }
}
