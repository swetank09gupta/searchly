package dev.searchly.api.config;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenSearchConfig {

    @Value("${searchly.opensearch.host}") String host;
    @Value("${searchly.opensearch.port}") int port;
    @Value("${searchly.opensearch.scheme}") String scheme;

    @Bean
    public OpenSearchClient openSearchClient() {
        OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(new HttpHost(scheme, host, port))
                .build();
        return new OpenSearchClient(transport);
    }
}
