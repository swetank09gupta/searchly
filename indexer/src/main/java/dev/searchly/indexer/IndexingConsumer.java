package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import dev.searchly.common.Tier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Consumes indexing events and writes to OpenSearch.
 * Idempotent: doc_id is the OpenSearch document id (upsert semantics).
 *
 * All OpenSearch I/O goes through ChunkIndexClient which uses Java's built-in
 * HttpClient with HTTP/1.1. This avoids the Apache HttpClient5 IO reactor
 * (ApacheHttpClient5TransportBuilder) which suffered from repeated
 * OutOfMemoryError in validateActiveChannels even on tiny documents.
 *
 * After writing the full document (keyword search), the content is also chunked,
 * embedded, and written to the chunks-* index for RAG / semantic search.
 * Embedding failures are non-fatal — the document remains keyword-searchable.
 */
@Component
public class IndexingConsumer {
    private static final Logger log = LoggerFactory.getLogger(IndexingConsumer.class);

    private final ChunkIndexClient osClient;
    private final ChunkingService chunker;
    private final EmbeddingClient embedder;

    public IndexingConsumer(ChunkIndexClient osClient,
                            ChunkingService chunker,
                            EmbeddingClient embedder) {
        this.osClient = osClient;
        this.chunker  = chunker;
        this.embedder = embedder;
    }

    @KafkaListener(topicPattern = "indexing\\.shared|indexing\\.enterprise\\..+", groupId = "indexer")
    public void onMessage(IndexingEvent event) {
        try {
            String docIdx   = docIndexName(event);
            String chunkIdx = chunkIndexName(event);

            osClient.indexDocument(
                    docIdx,
                    event.tenantId(),
                    event.docId(),
                    event.title(),
                    event.content(),
                    event.metadata(),
                    event.createdAt().toString());

            indexChunks(event, chunkIdx);

        } catch (OutOfMemoryError oom) {
            // Rare: fires when a doc's content is so large that chunking/embedding
            // exhausts the heap even after the MAX_EMBED_CHARS guard.
            // Committing the Kafka offset (by not rethrowing) lets the container
            // advance past this message instead of crashing and restarting.
            log.error("OOM processing doc {} — skipping chunk index, doc is keyword-searchable",
                    event.docId());
            System.gc();
        } catch (Exception e) {
            log.error("Failed to index {}: {}", event.docId(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void indexChunks(IndexingEvent event, String chunkIdx) {
        String fullText = buildTextForEmbedding(event);
        List<String> chunks = chunker.chunk(fullText);
        if (chunks.isEmpty()) return;

        List<List<Double>> vectors = embedder.embed(chunks);
        if (vectors.isEmpty()) {
            log.warn("Embedding unavailable for doc {} — skipping chunk index", event.docId());
            return;
        }

        Map<String, Object> metadata = event.metadata() != null ? event.metadata() : Map.of();
        for (int i = 0; i < chunks.size(); i++) {
            List<Double> vec = i < vectors.size() ? vectors.get(i) : null;
            if (vec == null) continue;
            osClient.indexChunk(
                    chunkIdx,
                    event.tenantId(),
                    event.docId(),
                    i,
                    event.title(),
                    chunks.get(i),
                    vec,
                    metadata,
                    event.createdAt().toString());
        }
        log.info("Indexed {} chunks for doc {} in {}", chunks.size(), event.docId(), chunkIdx);
    }

    // Max chars to embed — caps embedding payload size. Documents are split upstream
    // (split_doc in sync.py) so this truncation should rarely fire, but kept as
    // a last-resort guard against unexpectedly large content.
    private static final int MAX_EMBED_CHARS = 750_000;

    // Embed title + content together so the title context carries into each chunk's vector.
    // Truncate content BEFORE concatenation to avoid huge intermediate strings.
    private String buildTextForEmbedding(IndexingEvent event) {
        String title   = event.title()   != null ? event.title()   : "";
        String content = event.content() != null ? event.content() : "";
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
}
