package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes indexing events from the main topics and writes to OpenSearch.
 *
 * Batch mode (P0.6): up to 500 records per poll are bulk-indexed in a single
 * OpenSearch _bulk request for 10-50× throughput improvement.
 *
 * Failure handling (P0.1): a failed batch is retried per-document; each
 * failed document is routed to indexing.retry1/2/3 then indexing.dlq via
 * RetryPublisher — no poison message loops.
 *
 * Offset is always committed (no rethrow) so a bad document cannot wedge
 * the consumer.  Retry topics provide persistent, delay-gated re-delivery.
 */
@Component
public class IndexingConsumer {
    private static final Logger log = LoggerFactory.getLogger(IndexingConsumer.class);

    private final IndexingProcessor processor;
    private final RetryPublisher retryPublisher;

    public IndexingConsumer(IndexingProcessor processor, RetryPublisher retryPublisher) {
        this.processor     = processor;
        this.retryPublisher = retryPublisher;
    }

    @KafkaListener(
            topicPattern    = "indexing\\.shared|indexing\\.enterprise\\..+",
            groupId         = "indexer",
            containerFactory = "batchKafkaListenerContainerFactory")
    public void onMessages(List<ConsumerRecord<String, IndexingEvent>> records) {
        if (records.isEmpty()) return;

        List<IndexingEvent> events = records.stream()
                .map(ConsumerRecord::value)
                .filter(e -> e != null)
                .toList();

        try {
            processor.processBatch(events);
            log.info("Bulk-indexed {} docs", events.size());
            return;
        } catch (OutOfMemoryError oom) {
            Runtime rt = Runtime.getRuntime();
            log.error("OOM during batch index — heap: free={}MB total={}MB max={}MB. Falling back to per-doc.",
                    rt.freeMemory() / 1024 / 1024,
                    rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024);
            System.gc();
        } catch (Exception e) {
            log.warn("Batch indexing failed ({}), falling back to per-doc: {}",
                    records.size(), e.getMessage());
        }

        // Batch failed — process each document individually so only bad docs go to retry
        for (ConsumerRecord<String, IndexingEvent> record : records) {
            IndexingEvent event = record.value();
            if (event == null) continue;
            try {
                processor.process(event);
            } catch (OutOfMemoryError oom) {
                Runtime rt = Runtime.getRuntime();
                log.error("OOM processing doc={} heap: free={}MB total={}MB max={}MB",
                        event.docId(),
                        rt.freeMemory() / 1024 / 1024,
                        rt.totalMemory() / 1024 / 1024,
                        rt.maxMemory() / 1024 / 1024);
                System.gc();
                retryPublisher.handleFailure(record, new RuntimeException("OOM", oom));
            } catch (Exception ex) {
                log.error("Failed to index doc={}: {}", event.docId(), ex.getMessage(), ex);
                retryPublisher.handleFailure(record, ex);
            }
            // No rethrow — offset always committed; retry topics handle re-delivery.
        }
    }
}
