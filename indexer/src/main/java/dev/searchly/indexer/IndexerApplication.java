package dev.searchly.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class IndexerApplication {

    // spring-boot-starter (no -web) does not auto-configure ObjectMapper.
    // Declare it explicitly so EmbeddingClient and ChunkIndexClient can inject it.
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public NewTopic indexingSharedTopic() {
        return new NewTopic("indexing.shared", 6, (short) 1);
    }

    @Bean
    public NewTopic indexingDlqTopic() {
        return new NewTopic("indexing.dlq", 1, (short) 1);
    }

    public static void main(String[] args) {
        SpringApplication.run(IndexerApplication.class, args);
    }
}
