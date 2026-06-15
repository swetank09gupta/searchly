package dev.searchly.api.service;

import dev.searchly.api.model.DocumentEntity;
import dev.searchly.api.repository.DocumentRepository;
import dev.searchly.api.repository.TenantRepository;
import dev.searchly.api.security.TenantContextHolder;
import dev.searchly.common.DocumentDto;
import dev.searchly.common.IndexingEvent;
import dev.searchly.common.TenantContext;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class DocumentService {

    private final DocumentRepository docRepo;
    private final TenantRepository tenantRepo;
    private final KafkaTemplate<String, IndexingEvent> kafka;
    private final OpenSearchClient os;
    private final CacheService cache;

    @Value("${searchly.kafka.topic-shared}") String sharedTopic;
    @Value("${searchly.kafka.topic-enterprise-prefix}") String enterprisePrefix;

    public DocumentService(DocumentRepository docRepo, TenantRepository tenantRepo,
                           KafkaTemplate<String, IndexingEvent> kafka, OpenSearchClient os,
                           CacheService cache) {
        this.docRepo = docRepo;
        this.tenantRepo = tenantRepo;
        this.kafka = kafka;
        this.os = os;
        this.cache = cache;
    }

    @Transactional
    public DocumentDto.CreateResponse create(DocumentDto.CreateRequest req, String idempotencyKey) {
        TenantContext ctx = TenantContextHolder.require();

        // Quota check
        long count = docRepo.countByTenantId(ctx.tenantId());
        long quota = tenantRepo.findById(ctx.tenantId()).map(t -> t.getQuotaDocs()).orElse(0L);
        if (count >= quota) {
            throw new IllegalStateException("Tenant " + ctx.tenantId() + " exceeded document quota: " + quota);
        }

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        DocumentEntity e = new DocumentEntity();
        e.setId(id);
        e.setTenantId(ctx.tenantId());
        e.setTitle(req.title());
        e.setContent(req.content());
        e.setMetadata(req.metadata());
        e.setStatus("PENDING");
        e.setCreatedAt(now);
        e.setUpdatedAt(now);
        docRepo.save(e);

        IndexingEvent event = new IndexingEvent(
                id.toString(), ctx.tenantId(), ctx.tier(),
                req.title(), req.content(), req.metadata(), now.toEpochMilli(), idempotencyKey);

        String topic = ctx.tier().name().equals("ENTERPRISE")
                ? enterprisePrefix + ctx.tenantId()
                : sharedTopic;
        kafka.send(topic, ctx.tenantId(), event);

        cache.invalidateTenant(ctx.tenantId());
        return new DocumentDto.CreateResponse(id.toString(), ctx.tenantId(), "PENDING", now);
    }

    public DocumentDto.DocumentView get(UUID id) {
        TenantContext ctx = TenantContextHolder.require();
        DocumentEntity e = docRepo.findByIdAndTenantId(id, ctx.tenantId())
                .orElseThrow(() -> new NoSuchElementException("Document not found"));
        return new DocumentDto.DocumentView(
                e.getId().toString(), e.getTenantId(), e.getTitle(), e.getContent(),
                e.getMetadata(), e.getStatus(), e.getCreatedAt());
    }

    @Transactional
    public void delete(UUID id) {
        TenantContext ctx = TenantContextHolder.require();
        DocumentEntity e = docRepo.findByIdAndTenantId(id, ctx.tenantId())
                .orElseThrow(() -> new NoSuchElementException("Document not found"));
        docRepo.delete(e);
        try {
            os.delete(DeleteRequest.of(d -> d.index(indexName(ctx)).id(id.toString()).routing(ctx.tenantId())));
        } catch (Exception ex) {
            // Best-effort: index/doc may not yet exist (eventual consistency); tombstone in prod
        }
        cache.invalidateTenant(ctx.tenantId());
    }

    private String indexName(TenantContext ctx) {
        return ctx.tier().name().equals("ENTERPRISE")
                ? "documents-" + ctx.tenantId()
                : "documents-shared";
    }
}
