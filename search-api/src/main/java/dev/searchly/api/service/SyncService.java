package dev.searchly.api.service;

import dev.searchly.api.model.DocumentEntity;
import dev.searchly.api.model.SourceDocumentEntity;
import dev.searchly.api.repository.DocumentRepository;
import dev.searchly.api.repository.SourceDocumentRepository;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles source document tracking and stale-doc purge (P0.3 Tombstone).
 *
 * Tracking is written by DocumentService.create() whenever a document includes
 * metadata.source_id + metadata.source.  The connector records sync start time,
 * runs the sync (which refreshes last_seen_at for every seen doc), then calls
 * purgeStale() to delete anything not updated since sync began.
 */
@Service
public class SyncService {
    private static final Logger log = LoggerFactory.getLogger(SyncService.class);

    private final SourceDocumentRepository sourceDocRepo;
    private final DocumentRepository docRepo;
    private final OpenSearchClient os;
    private final CacheService cache;

    public SyncService(SourceDocumentRepository sourceDocRepo,
                       DocumentRepository docRepo,
                       OpenSearchClient os,
                       CacheService cache) {
        this.sourceDocRepo = sourceDocRepo;
        this.docRepo       = docRepo;
        this.os            = os;
        this.cache         = cache;
    }

    /**
     * Called from DocumentService after a document with source_id metadata is created.
     * Upserts the tracking row so last_seen_at reflects this sync cycle.
     */
    @Transactional
    public void trackSourceDocument(String sourceId, String sourceType,
                                     String tenantId, String docId) {
        Optional<SourceDocumentEntity> existing =
                sourceDocRepo.findBySourceIdAndSourceTypeAndTenantId(sourceId, sourceType, tenantId);
        if (existing.isPresent()) {
            SourceDocumentEntity e = existing.get();
            e.setLastSeenAt(Instant.now());
            e.setDocId(docId);
            sourceDocRepo.save(e);
        } else {
            sourceDocRepo.save(new SourceDocumentEntity(sourceId, sourceType, tenantId, docId));
        }
    }

    /**
     * Deletes all documents of the given source type whose last_seen_at is earlier
     * than syncStartedAt (i.e. they were not touched during the most recent sync).
     *
     * Deleted from: source_documents tracking table, documents (Postgres), and
     * both documents-* and chunks-* OpenSearch indices.
     *
     * @return number of documents purged
     */
    @Transactional
    public int purgeStale(String tenantId, String sourceType, Instant syncStartedAt) {
        List<SourceDocumentEntity> stale = sourceDocRepo
                .findByTenantIdAndSourceTypeAndLastSeenAtBefore(tenantId, sourceType, syncStartedAt);

        if (stale.isEmpty()) {
            log.info("purgeStale: no stale {} docs for tenant={}", sourceType, tenantId);
            return 0;
        }

        String docIndex   = "documents-shared";
        String chunkIndex = "chunks-shared";

        int purged = 0;
        for (SourceDocumentEntity entry : stale) {
            String docId = entry.getDocId();
            if (docId == null) continue;

            // Delete full doc from OpenSearch documents-* index
            try {
                os.delete(DeleteRequest.of(d -> d.index(docIndex).id(docId).routing(tenantId)));
            } catch (Exception ex) {
                log.debug("OS doc delete for {} skipped: {}", docId, ex.getMessage());
            }

            // Delete all chunks for this doc from chunks-* index
            try {
                Query docIdFilter = Query.of(q -> q.term(t -> t.field("doc_id")
                        .value(v -> v.stringValue(docId))));
                os.deleteByQuery(DeleteByQueryRequest.of(d -> d
                        .index(chunkIndex)
                        .routing(tenantId)
                        .query(docIdFilter)));
            } catch (Exception ex) {
                log.debug("OS chunk deleteByQuery for {} skipped: {}", docId, ex.getMessage());
            }

            // Delete from Postgres documents table
            try {
                docRepo.findById(UUID.fromString(docId)).ifPresent(docRepo::delete);
            } catch (Exception ex) {
                log.debug("Postgres delete for {} skipped: {}", docId, ex.getMessage());
            }

            purged++;
        }

        int removed = sourceDocRepo.deleteStale(tenantId, sourceType, syncStartedAt);
        cache.invalidateTenant(tenantId);
        log.info("purgeStale: purged {} stale {} docs for tenant={} (tracking rows removed: {})",
                purged, sourceType, tenantId, removed);
        return purged;
    }
}
