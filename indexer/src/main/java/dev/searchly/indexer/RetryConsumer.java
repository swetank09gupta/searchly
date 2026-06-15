package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes retry topics and re-attempts indexing after the required delay.
 *
 * Delay enforcement: if the message's X-Retry-After timestamp is still in the
 * future by more than REQUEUE_THRESHOLD_MS, the message is re-queued to the
 * same topic (offset advances, other messages processed first).  If the wait
 * is short, the thread sleeps briefly before processing.
 *
 * This approach keeps the consumer group alive with no partition-pausing
 * complexity.  Retry topics are low-volume (poison messages are rare), so
 * occasional re-queuing is acceptable.
 */
@Component
public class RetryConsumer {
    private static final Logger log = LoggerFactory.getLogger(RetryConsumer.class);

    // Re-queue instead of sleeping when remaining wait > 30 s
    private static final long REQUEUE_THRESHOLD_MS = 30_000;

    private final IndexingProcessor processor;
    private final RetryPublisher retryPublisher;

    public RetryConsumer(IndexingProcessor processor, RetryPublisher retryPublisher) {
        this.processor     = processor;
        this.retryPublisher = retryPublisher;
    }

    @KafkaListener(
            topics = {
                "${searchly.kafka.retry1-topic:indexing.retry1}",
                "${searchly.kafka.retry2-topic:indexing.retry2}",
                "${searchly.kafka.retry3-topic:indexing.retry3}"
            },
            groupId = "indexer-retry")
    public void onRetryMessage(ConsumerRecord<String, IndexingEvent> record) {
        long retryAfter = RetryPublisher.getLongHeader(record, RetryPublisher.HEADER_RETRY_AFTER, 0L);
        long waitMs = retryAfter - System.currentTimeMillis();

        if (waitMs > REQUEUE_THRESHOLD_MS) {
            // Still too early — requeue and advance past this message
            retryPublisher.requeue(record);
            log.debug("Requeued {} — not ready for {}s", record.topic(), waitMs / 1000);
            return;
        }

        if (waitMs > 0) {
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        IndexingEvent event = record.value();
        if (event == null) {
            log.error("Null event on retry topic {}, discarding", record.topic());
            return;
        }

        int retryCount = RetryPublisher.getIntHeader(record, RetryPublisher.HEADER_RETRY_COUNT, 0);
        try {
            processor.process(event);
            log.info("Retry success: doc={} on attempt {}", event.docId(), retryCount);
        } catch (OutOfMemoryError oom) {
            Runtime rt = Runtime.getRuntime();
            log.error("OOM on retry for doc={} heap: free={}MB total={}MB max={}MB",
                    event.docId(),
                    rt.freeMemory() / 1024 / 1024,
                    rt.totalMemory() / 1024 / 1024,
                    rt.maxMemory() / 1024 / 1024);
            System.gc();
            retryPublisher.handleFailure(record, new RuntimeException("OOM", oom));
        } catch (Exception e) {
            log.error("Retry {} failed for doc={}: {}", retryCount, event.docId(), e.getMessage(), e);
            retryPublisher.handleFailure(record, e);
        }
    }
}
