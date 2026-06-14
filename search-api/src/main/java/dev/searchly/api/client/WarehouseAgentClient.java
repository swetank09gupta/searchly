package dev.searchly.api.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * HTTP client for the Python warehouse-agent /api/v1/chat endpoint.
 *
 * The agent:
 *   - Fuzzy-resolves customer names ("samsclub atl" → "sams-club-atlanta")
 *   - Extracts env hint from the question if not supplied
 *   - Auto-registers unknown customers via conversational clarification
 *   - Queries live k8s clusters for operational questions
 *   - Returns needs_clarification=true when it needs more info from the user
 *
 * session_id is passed through from the user's request and returned in the
 * response so the caller can maintain multi-turn conversation context.
 */
@Component
public class WarehouseAgentClient {
    private static final Logger log = LoggerFactory.getLogger(WarehouseAgentClient.class);

    private final String agentUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final boolean enabled;

    public WarehouseAgentClient(
            @Value("${searchly.warehouse-agent.url:}") String agentUrl,
            ObjectMapper mapper) {
        this.agentUrl = agentUrl;
        this.mapper = mapper;
        this.enabled = agentUrl != null && !agentUrl.isBlank();
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        if (this.enabled)
            log.info("Warehouse agent enabled at {}", agentUrl);
        else
            log.info("Warehouse agent not configured — operational queries use static RAG only");
    }

    public boolean isEnabled() { return enabled; }

    /**
     * Send a message to the warehouse agent chat endpoint.
     *
     * @param message    the user's question (natural language)
     * @param sessionId  prior session ID for multi-turn context (null for new session)
     * @param customer   optional customer hint (fuzzy — agent resolves it)
     * @param env        optional env hint (agent extracts from message if null)
     * @param product    optional product filter
     */
    @SuppressWarnings("unchecked")
    public AgentChatResult chat(String message, String sessionId,
                                String customer, String env, String product) {
        if (!enabled) return null;
        try {
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
                log.warn("Warehouse agent HTTP {}: {}", resp.statusCode(),
                         resp.body().substring(0, Math.min(300, resp.body().length())));
                return null;
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
        } catch (Exception e) {
            log.warn("Warehouse agent call failed: {}", e.getMessage());
            return null;
        }
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
