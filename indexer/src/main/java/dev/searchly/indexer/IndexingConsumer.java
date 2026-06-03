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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes indexing events and writes to OpenSearch.
 * Idempotent: doc_id is the OpenSearch document id (upsert semantics).
 * See ADR 0004.
 */
@Component
public class IndexingConsumer {
    private static final Logger log = LoggerFactory.getLogger(IndexingConsumer.class);

    private final OpenSearchClient os;
    private final Map<String, Boolean> ensuredIndices = new ConcurrentHashMap<>();

    public IndexingConsumer(OpenSearchClient os) {
        this.os = os;
    }

    @KafkaListener(topicPattern = "indexing\\.shared|indexing\\.enterprise\\..+", groupId = "indexer")
    public void onMessage(IndexingEvent event) {
        try {
            String index = indexName(event);
            ensureIndex(index);

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
        } catch (IOException e) {
            log.error("Failed to index {}: {}", event.docId(), e.getMessage(), e);
            throw new RuntimeException(e); // triggers Kafka retry; DLQ after N attempts in prod
        }
    }

    private String indexName(IndexingEvent e) {
        return e.tier() == Tier.ENTERPRISE
                ? "documents-" + e.tenantId()
                : "documents-shared";
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
