package dev.searchly.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class IndexerApplication {

    // spring-boot-starter (no -web) does not auto-configure ObjectMapper.
    // Declare it explicitly so EmbeddingClient and any other component can inject it.
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


    @Value("${searchly.opensearch.host}") String host;
    @Value("${searchly.opensearch.port}") int port;
    @Value("${searchly.opensearch.scheme}") String scheme;

    public static void main(String[] args) {
        SpringApplication.run(IndexerApplication.class, args);
    }

    @Bean
    public OpenSearchClient openSearchClient() {
        OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(new HttpHost(scheme, host, port))
                .build();
        return new OpenSearchClient(transport);
    }
}
