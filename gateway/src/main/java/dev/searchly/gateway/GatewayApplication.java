package dev.searchly.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    /**
     * Per-tenant rate-limit key. In production this comes from the JWT 'tenant_id' claim.
     * Falls back to X-Tenant-Id header for prototype convenience.
     */
    @Bean
    public KeyResolver tenantKeyResolver() {
        return exchange -> {
            String tenant = exchange.getRequest().getHeaders().getFirst("X-Tenant-Id");
            if (tenant == null) tenant = exchange.getRequest().getQueryParams().getFirst("tenant");
            return Mono.just(tenant != null ? tenant : "anonymous");
        };
    }
}
