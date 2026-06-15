package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Publishes failed indexing events to retry or DLQ topics.
 *
 * Retry topology:
 *   main topic → retry1 (+5 min) → retry2 (+30 min) → retry3 (+2 hr) → dlq
 *
 * Delay is enforced by the retry consumer (RetryConsumer) which re-queues
 * messages not yet ready back to the same retry topic.
 */
@Component
public class RetryPublisher {
    private static final Logger log = LoggerFactory.getLogger(RetryPublisher.class);

    static final String HEADER_RETRY_COUNT = "X-Retry-Count";
    static final String HEADER_RETRY_AFTER = "X-Retry-After";
    static final String HEADER_FAILURE     = "X-Last-Failure";
    static final String HEADER_ORIG_TOPIC  = "X-Original-Topic";
    static final int    MAX_RETRIES        = 3;

    private static final long DELAY_RETRY1 = 5L  * 60 * 1000;   // 5 min
    private static final long DELAY_RETRY2 = 30L * 60 * 1000;   // 30 min
    private static final long DELAY_RETRY3 = 2L  * 60 * 60 * 1000; // 2 hr

    private final KafkaTemplate<String, IndexingEvent> template;
    private final String retry1Topic;
    private final String retry2Topic;
    private final String retry3Topic;
    private final String dlqTopic;

    public RetryPublisher(
            KafkaTemplate<String, IndexingEvent> template,
            @Value("${searchly.kafka.retry1-topic:indexing.retry1}") String retry1Topic,
            @Value("${searchly.kafka.retry2-topic:indexing.retry2}") String retry2Topic,
            @Value("${searchly.kafka.retry3-topic:indexing.retry3}") String retry3Topic,
            @Value("${searchly.kafka.dlq-topic:indexing.dlq}")       String dlqTopic) {
        this.template   = template;
        this.retry1Topic = retry1Topic;
        this.retry2Topic = retry2Topic;
        this.retry3Topic = retry3Topic;
        this.dlqTopic    = dlqTopic;
    }

    /** Routes failure to retry-1/2/3 or DLQ depending on existing retry count. */
    public void handleFailure(ConsumerRecord<String, IndexingEvent> record, Exception cause) {
        int retryCount = getIntHeader(record, HEADER_RETRY_COUNT, 0);
        String origTopic = getStringHeader(record, HEADER_ORIG_TOPIC, record.topic());
        String reason = trimReason(cause);
        int nextCount = retryCount + 1;

        if (nextCount > MAX_RETRIES) {
            publishToDlq(record, origTopic, retryCount, reason);
        } else {
            publishToRetry(record, nextCount, origTopic, reason);
        }
    }

    /** Re-queues a not-yet-ready message back to the same topic. */
    public void requeue(ConsumerRecord<String, IndexingEvent> record) {
        ProducerRecord<String, IndexingEvent> out = new ProducerRecord<>(
                record.topic(), null, record.key(), record.value());
        record.headers().forEach(h -> out.headers().add(h));
        template.send(out);
    }

    private void publishToRetry(ConsumerRecord<String, IndexingEvent> record,
                                 int nextCount, String origTopic, String reason) {
        String targetTopic = retryTopicFor(nextCount);
        long retryAfter = System.currentTimeMillis() + delayFor(nextCount);

        ProducerRecord<String, IndexingEvent> out = new ProducerRecord<>(
                targetTopic, null, record.key(), record.value());
        copyNonRetryHeaders(record, out);
        out.headers().add(HEADER_RETRY_COUNT, intBytes(nextCount));
        out.headers().add(HEADER_RETRY_AFTER, longBytes(retryAfter));
        out.headers().add(HEADER_FAILURE,     reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add(HEADER_ORIG_TOPIC,  origTopic.getBytes(StandardCharsets.UTF_8));

        template.send(out);
        log.warn("doc={} queued for retry {}/{} → {} delay=+{}min reason={}",
                docId(record), nextCount, MAX_RETRIES, targetTopic,
                delayFor(nextCount) / 60_000, reason);
    }

    private void publishToDlq(ConsumerRecord<String, IndexingEvent> record,
                               String origTopic, int retryCount, String reason) {
        ProducerRecord<String, IndexingEvent> out = new ProducerRecord<>(
                dlqTopic, null, record.key(), record.value());
        copyNonRetryHeaders(record, out);
        out.headers().add(HEADER_RETRY_COUNT, intBytes(retryCount));
        out.headers().add(HEADER_FAILURE,     reason.getBytes(StandardCharsets.UTF_8));
        out.headers().add(HEADER_ORIG_TOPIC,  origTopic.getBytes(StandardCharsets.UTF_8));
        template.send(out);
        log.error("doc={} sent to DLQ after {} retries — last failure: {}",
                docId(record), retryCount, reason);
    }

    // ── Header helpers ────────────────────────────────────────────────────────

    public static int getIntHeader(ConsumerRecord<?, ?> record, String name, int defaultVal) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null || h.value().length < 4) return defaultVal;
        return ByteBuffer.wrap(h.value()).getInt();
    }

    public static long getLongHeader(ConsumerRecord<?, ?> record, String name, long defaultVal) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null || h.value().length < 8) return defaultVal;
        return ByteBuffer.wrap(h.value()).getLong();
    }

    public static String getStringHeader(ConsumerRecord<?, ?> record, String name, String defaultVal) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) return defaultVal;
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private String retryTopicFor(int count) {
        return switch (count) {
            case 1 -> retry1Topic;
            case 2 -> retry2Topic;
            case 3 -> retry3Topic;
            default -> dlqTopic;
        };
    }

    private static long delayFor(int count) {
        return switch (count) {
            case 1 -> DELAY_RETRY1;
            case 2 -> DELAY_RETRY2;
            case 3 -> DELAY_RETRY3;
            default -> 0L;
        };
    }

    private static void copyNonRetryHeaders(ConsumerRecord<String, IndexingEvent> src,
                                            ProducerRecord<String, IndexingEvent> dst) {
        for (Header h : src.headers()) {
            String name = h.key();
            if (!name.equals(HEADER_RETRY_COUNT) && !name.equals(HEADER_RETRY_AFTER)
                    && !name.equals(HEADER_FAILURE) && !name.equals(HEADER_ORIG_TOPIC)) {
                dst.headers().add(h);
            }
        }
    }

    private static String docId(ConsumerRecord<String, IndexingEvent> r) {
        return r.value() != null ? r.value().docId() : "?";
    }

    private static String trimReason(Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        return msg.length() > 256 ? msg.substring(0, 256) : msg;
    }

    private static byte[] intBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private static byte[] longBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }
}
