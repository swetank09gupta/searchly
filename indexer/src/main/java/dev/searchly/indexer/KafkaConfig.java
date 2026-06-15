package dev.searchly.indexer;

import dev.searchly.common.IndexingEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires the batch-mode Kafka listener factory (P0.6).
 * The main IndexingConsumer uses this to bulk-index up to 500 docs per poll.
 *
 * Spring Boot auto-configures a KafkaTemplate from spring.kafka.producer.*
 * which RetryPublisher uses — no additional template bean needed here.
 */
@Configuration
public class KafkaConfig {

    /**
     * Batch listener factory: up to 500 records per poll, 500 ms max wait.
     */
    @Bean("batchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, IndexingEvent> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, IndexingEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, IndexingEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        Map<String, Object> overrides = new HashMap<>(
                consumerFactory.getConfigurationProperties());
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        overrides.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(overrides));

        return factory;
    }
}
