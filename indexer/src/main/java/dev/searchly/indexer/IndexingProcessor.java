package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import dev.searchly.common.Tier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Core indexing logic, shared between the main consumer and retry consumer.
 * Kept separate so retry paths call identical processing code.
 */
@Component
public class IndexingProcessor {
    private static final Logger log = LoggerFactory.getLogger(IndexingProcessor.class);
    private static final int MAX_EMBED_CHARS = 750_000;

    private final ChunkIndexClient osClient;
    private final ChunkingService chunker;
    private final EmbeddingClient embedder;

    public IndexingProcessor(ChunkIndexClient osClient,
                             ChunkingService chunker,
                             EmbeddingClient embedder) {
        this.osClient = osClient;
        this.chunker  = chunker;
        this.embedder = embedder;
    }

    public void process(IndexingEvent event) throws Exception {
        String docIdx   = docIndexName(event);
        String chunkIdx = chunkIndexName(event);

        String newFingerprint = ContentFingerprinter.fingerprint(event.title(), event.content());
        String existingFingerprint = osClient.getDocumentFingerprint(docIdx, event.docId());

        osClient.indexDocument(
                docIdx, event.tenantId(), event.docId(),
                event.title(), event.content(),
                event.metadata(), String.valueOf(event.createdAt()));

        if (newFingerprint.equals(existingFingerprint)) {
            log.debug("Content unchanged for doc {} — skipping chunk re-embed", event.docId());
            return;
        }
        indexChunks(event, chunkIdx);
    }

    /** Bulk-process a batch: index all documents first, then chunk+embed+index all chunks. */
    public void processBatch(List<IndexingEvent> events) throws Exception {
        // Deduplicate within batch: keep last event per (docId+tenantId) combination
        Map<String, IndexingEvent> dedupedMap = new java.util.LinkedHashMap<>();
        for (IndexingEvent e : events) dedupedMap.put(e.tenantId() + ":" + e.docId(), e);
        List<IndexingEvent> deduped = new java.util.ArrayList<>(dedupedMap.values());
        if (deduped.size() < events.size()) {
            log.info("Deduped batch: {} → {} events", events.size(), deduped.size());
        }

        // Phase 1: bulk-index full documents
        List<ChunkIndexClient.BulkDocEntry> docEntries = deduped.stream()
                .map(e -> new ChunkIndexClient.BulkDocEntry(
                        docIndexName(e), e.tenantId(), e.docId(),
                        e.title(), e.content(),
                        e.metadata() != null ? e.metadata() : Map.of(),
                        String.valueOf(e.createdAt())))
                .toList();
        osClient.bulkIndexDocuments(docEntries);

        // Phase 2: chunk + embed + bulk-index chunks (skip unchanged content)
        List<ChunkIndexClient.BulkChunkEntry> chunkEntries = new java.util.ArrayList<>();
        for (IndexingEvent event : deduped) {
            String chunkIdx = chunkIndexName(event);
            String fullText = buildTextForEmbedding(event);
            List<String> chunks = chunker.chunk(fullText);
            if (chunks.isEmpty()) continue;

            Runtime rt = Runtime.getRuntime();
            log.info("heap before embed: free={}MB total={}MB max={}MB chunks={} doc={}",
                    rt.freeMemory() / 1024 / 1024, rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024, chunks.size(), event.docId());

            List<List<Double>> vectors = embedder.embed(chunks);
            if (vectors.isEmpty()) {
                log.warn("Embedding unavailable for doc {} — skipping chunk index", event.docId());
                continue;
            }

            Map<String, Object> metadata = event.metadata() != null ? event.metadata() : Map.of();
            for (int i = 0; i < chunks.size(); i++) {
                List<Double> vec = i < vectors.size() ? vectors.get(i) : null;
                if (vec == null) continue;
                chunkEntries.add(new ChunkIndexClient.BulkChunkEntry(
                        chunkIdx, event.tenantId(), event.docId(), i,
                        event.title(), chunks.get(i), vec, metadata,
                        String.valueOf(event.createdAt()),
                        EmbeddingClient.EMBEDDING_VERSION));
            }
        }
        if (!chunkEntries.isEmpty()) {
            osClient.bulkIndexChunks(chunkEntries);
            log.info("Bulk indexed {} chunks across {} docs", chunkEntries.size(), events.size());
        }
    }

    private void indexChunks(IndexingEvent event, String chunkIdx) {
        String fullText = buildTextForEmbedding(event);
        List<String> chunks = chunker.chunk(fullText);
        if (chunks.isEmpty()) return;

        Runtime rt = Runtime.getRuntime();
        log.info("heap before embed: free={}MB total={}MB max={}MB chunks={} doc={}",
                rt.freeMemory() / 1024 / 1024, rt.totalMemory() / 1024 / 1024,
                rt.maxMemory() / 1024 / 1024, chunks.size(), event.docId());

        List<List<Double>> vectors = embedder.embed(chunks);
        if (vectors.isEmpty()) {
            log.warn("Embedding unavailable for doc {} — skipping chunk index", event.docId());
            return;
        }

        Map<String, Object> metadata = event.metadata() != null ? event.metadata() : Map.of();
        for (int i = 0; i < chunks.size(); i++) {
            List<Double> vec = i < vectors.size() ? vectors.get(i) : null;
            if (vec == null) continue;
            osClient.indexChunk(chunkIdx, event.tenantId(), event.docId(), i,
                    event.title(), chunks.get(i), vec, metadata, String.valueOf(event.createdAt()),
                    EmbeddingClient.EMBEDDING_VERSION);
        }
        log.info("Indexed {} chunks for doc {} in {}", chunks.size(), event.docId(), chunkIdx);
    }

    // Max chars to embed — last-resort guard against unexpectedly large content.
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
