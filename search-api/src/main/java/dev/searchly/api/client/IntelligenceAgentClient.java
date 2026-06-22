package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * HTTP client for the Python intelligence-agent /api/v1/chat endpoint.
 * Protected by a circuit breaker so agent timeouts don't cascade to search.
 */
@Component
public class IntelligenceAgentClient {
    private static final Logger log = LoggerFactory.getLogger(IntelligenceAgentClient.class);

    private final String agentUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final boolean enabled;

    public IntelligenceAgentClient(
            @Value("${searchly.intelligence-agent.url:}") String agentUrl,
            ObjectMapper mapper) {
        this.agentUrl = agentUrl;
        this.mapper = mapper;
        this.enabled = agentUrl != null && !agentUrl.isBlank();
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)  // FastAPI/uvicorn doesn't support HTTP/2
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        if (this.enabled)
            log.info("Intelligence agent enabled at {}", agentUrl);
        else
            log.info("Intelligence agent not configured — operational queries use static RAG only");
    }

    public boolean isEnabled() { return enabled; }

    @CircuitBreaker(name = "intelligenceAgent", fallbackMethod = "chatFallback")
    @SuppressWarnings("unchecked")
    public AgentChatResult chat(String message, String sessionId,
                                String customer, String env, String product) throws Exception {
        if (!enabled) return null;

        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("message", message);
        if (sessionId != null && !sessionId.isBlank()) body.put("session_id", sessionId);
        if (customer  != null && !customer.isBlank())  body.put("customer",   customer);
        if (env       != null && !env.isBlank())       body.put("env",        env);
        if (product   != null && !product.isBlank())   body.put("product",    product);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(agentUrl + "/api/v1/chat"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(90))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("Intelligence agent HTTP " + resp.statusCode() + ": "
                    + resp.body().substring(0, Math.min(300, resp.body().length())));
        }

        Map<String, Object> r = mapper.readValue(resp.body(), Map.class);
        return new AgentChatResult(
            (String)  r.getOrDefault("session_id",          ""),
            (String)  r.getOrDefault("answer",              ""),
            (String)  r.getOrDefault("resolved_customer",   null),
            (String)  r.getOrDefault("resolved_env",        null),
            (String)  r.getOrDefault("lifecycle_stage",     null),
            (String)  r.getOrDefault("lifecycle_label",     null),
            Boolean.TRUE.equals(r.get("needs_clarification")),
            Boolean.TRUE.equals(r.get("has_live_data")),
            Boolean.TRUE.equals(r.get("is_operational")),
            (List<String>) r.getOrDefault("tools_called",   List.of())
        );
    }

    @SuppressWarnings("unused")
    private AgentChatResult chatFallback(String message, String sessionId,
                                          String customer, String env, String product,
                                          Throwable t) {
        log.warn("Intelligence agent circuit open or failed — falling back to static RAG: {}", t.getMessage());
        return null;
    }

    public record AgentChatResult(
            String  sessionId,
            String  answer,
            String  resolvedCustomer,
            String  resolvedEnv,
            String  lifecycleStage,
            String  lifecycleLabel,
            boolean needsClarification,
            boolean hasLiveData,
            boolean isOperational,
            List<String> toolsCalled
    ) {}
}
